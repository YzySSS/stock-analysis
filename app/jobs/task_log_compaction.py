from __future__ import annotations

import json
from typing import Any

from app.data_ingestion.market_opinion_task_log import (
    compact_market_opinion_task_metadata,
)
from app.shared.db import mysql_maintenance_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.shared.task_log import TASK_RUN_METADATA_MAX_BYTES, _serialize_metadata


MARKET_OPINION_TASK_NAME = "market_opinion_update"
TASK_LOG_COMPACTION_LOCK_NAME = "task_run_metadata_compaction"


def _decode_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        decoded = json.loads(value or "{}")
        return dict(decoded) if isinstance(decoded, dict) else {}
    return {}


def prepare_market_opinion_metadata_compaction(
    metadata: Any,
    *,
    max_bytes: int = TASK_RUN_METADATA_MAX_BYTES,
) -> tuple[str, int]:
    compacted = compact_market_opinion_task_metadata(_decode_metadata(metadata))
    serialized = _serialize_metadata(compacted, max_bytes=max_bytes)
    return serialized, len(serialized.encode("utf-8"))


class TaskRunMetadataCompactionService:
    def __init__(
        self,
        *,
        max_bytes: int = TASK_RUN_METADATA_MAX_BYTES,
        batch_size: int = 100,
    ) -> None:
        if max_bytes < 1024:
            raise ValueError("max_bytes must be at least 1024")
        if not 1 <= batch_size <= 500:
            raise ValueError("batch_size must be between 1 and 500")
        self.max_bytes = int(max_bytes)
        self.batch_size = int(batch_size)

    def preview(self) -> dict[str, Any]:
        with mysql_maintenance_conn(timeout_seconds=120) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total_rows,
                        COALESCE(SUM(OCTET_LENGTH(metadata_json)), 0) AS total_bytes,
                        COALESCE(SUM(
                            CASE WHEN OCTET_LENGTH(metadata_json) > %s THEN 1 ELSE 0 END
                        ), 0) AS oversized_rows,
                        COALESCE(SUM(
                            CASE
                                WHEN OCTET_LENGTH(metadata_json) > %s
                                THEN OCTET_LENGTH(metadata_json)
                                ELSE 0
                            END
                        ), 0) AS oversized_bytes,
                        MIN(started_at) AS first_started_at,
                        MAX(started_at) AS last_started_at
                    FROM task_run_log FORCE INDEX (idx_task_name)
                    WHERE task_name=%s
                    """,
                    (
                        self.max_bytes,
                        self.max_bytes,
                        MARKET_OPINION_TASK_NAME,
                    ),
                )
                row = cursor.fetchone() or {}
        return {
            "task_name": MARKET_OPINION_TASK_NAME,
            "max_bytes": self.max_bytes,
            "total_rows": int(row.get("total_rows") or 0),
            "total_bytes": int(row.get("total_bytes") or 0),
            "oversized_rows": int(row.get("oversized_rows") or 0),
            "oversized_bytes": int(row.get("oversized_bytes") or 0),
            "first_started_at": str(row.get("first_started_at"))
            if row.get("first_started_at")
            else None,
            "last_started_at": str(row.get("last_started_at"))
            if row.get("last_started_at")
            else None,
        }

    def apply(self) -> dict[str, Any]:
        lock_handle = acquire_mysql_advisory_lock(TASK_LOG_COMPACTION_LOCK_NAME)
        if lock_handle is None:
            return {
                "status": "skipped",
                "reason": "task_run_metadata_compaction_is_active",
            }

        try:
            before = self.preview()
            upper_bound = self._upper_bound_id()
            changed_rows = 0
            original_bytes = 0
            compacted_bytes = 0
            last_id = 0

            while True:
                rows = self._fetch_batch(last_id=last_id, upper_bound=upper_bound)
                if not rows:
                    break
                last_id = max(int(row["id"]) for row in rows)
                updates = []
                for row in rows:
                    serialized, after_bytes = prepare_market_opinion_metadata_compaction(
                        row.get("metadata_json"),
                        max_bytes=self.max_bytes,
                    )
                    before_bytes = int(row.get("metadata_bytes") or 0)
                    if after_bytes >= before_bytes:
                        continue
                    updates.append((serialized, int(row["id"]), MARKET_OPINION_TASK_NAME))
                    original_bytes += before_bytes
                    compacted_bytes += after_bytes
                changed_rows += self._update_batch(updates)

            after = self.preview()
            result = {
                "status": "success",
                "task_name": MARKET_OPINION_TASK_NAME,
                "max_bytes": self.max_bytes,
                "batch_size": self.batch_size,
                "upper_bound_id": upper_bound,
                "before": before,
                "changed_rows": changed_rows,
                "original_bytes": original_bytes,
                "compacted_bytes": compacted_bytes,
                "reclaimed_logical_bytes": max(original_bytes - compacted_bytes, 0),
                "after": after,
            }
        except BaseException as exc:
            release_error = release_mysql_advisory_lock(lock_handle)
            if release_error:
                exc.add_note(f"task-log compaction lock release warning: {release_error}")
            raise
        else:
            release_error = release_mysql_advisory_lock(lock_handle)
            if release_error:
                result["lock_release_warning"] = release_error
            return result

    @staticmethod
    def _upper_bound_id() -> int:
        with mysql_maintenance_conn(timeout_seconds=120) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(MAX(id), 0) AS max_id
                    FROM task_run_log FORCE INDEX (idx_task_name)
                    WHERE task_name=%s
                    """,
                    (MARKET_OPINION_TASK_NAME,),
                )
                row = cursor.fetchone() or {}
        return int(row.get("max_id") or 0)

    def _fetch_batch(self, *, last_id: int, upper_bound: int) -> list[dict[str, Any]]:
        with mysql_maintenance_conn(timeout_seconds=120) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, metadata_json, OCTET_LENGTH(metadata_json) AS metadata_bytes
                    FROM task_run_log FORCE INDEX (idx_task_name)
                    WHERE task_name=%s
                      AND id > %s
                      AND id <= %s
                      AND OCTET_LENGTH(metadata_json) > %s
                    ORDER BY id
                    LIMIT %s
                    """,
                    (
                        MARKET_OPINION_TASK_NAME,
                        last_id,
                        upper_bound,
                        self.max_bytes,
                        self.batch_size,
                    ),
                )
                return list(cursor.fetchall() or [])

    @staticmethod
    def _update_batch(updates: list[tuple[str, int, str]]) -> int:
        if not updates:
            return 0
        with mysql_maintenance_conn(dict_cursor=False, timeout_seconds=120) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    UPDATE task_run_log
                    SET metadata_json=%s
                    WHERE id=%s AND task_name=%s
                    """,
                    updates,
                )
                return int(cursor.rowcount)
