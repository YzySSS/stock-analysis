from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

from app.jobs.errors import error_fingerprint, infer_error_code, record_job_error, sanitize_error_message
from app.shared.db import mysql_conn


TASK_RUN_METADATA_MAX_BYTES = 64 * 1024
TASK_RUN_METADATA_MARKER = "_task_log_metadata"


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )


def _serialized_bytes(value: Any) -> tuple[str, int]:
    serialized = _json_dumps(value)
    return serialized, len(serialized.encode("utf-8"))


def _compact_metadata_value(
    value: Any,
    *,
    depth: int,
    max_depth: int,
    max_items: int,
    max_string_chars: int,
) -> Any:
    if isinstance(value, str):
        if len(value) <= max_string_chars:
            return value
        return f"{value[:max_string_chars]}…"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, dict):
        if depth >= max_depth:
            return {"_omitted_type": "dict", "item_count": len(value)}
        return {
            str(key): _compact_metadata_value(
                item,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string_chars=max_string_chars,
            )
            for key, item in list(value.items())[:max_items]
        }
    if isinstance(value, (list, tuple)):
        if depth >= max_depth:
            return [{"_omitted_type": "list", "item_count": len(value)}]
        return [
            _compact_metadata_value(
                item,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string_chars=max_string_chars,
            )
            for item in list(value)[:max_items]
        ]
    text = str(value)
    if len(text) <= max_string_chars:
        return text
    return f"{text[:max_string_chars]}…"


def _bounded_metadata(metadata: Dict[str, Any], *, max_bytes: int) -> Dict[str, Any]:
    _original_serialized, original_bytes = _serialized_bytes(metadata)
    if original_bytes <= max_bytes:
        return metadata

    limits = (
        (20, 1000, 4),
        (10, 500, 3),
        (5, 240, 3),
        (3, 120, 2),
        (1, 80, 2),
    )
    for max_items, max_string_chars, max_depth in limits:
        compacted = {
            str(key): _compact_metadata_value(
                value,
                depth=0,
                max_depth=max_depth,
                max_items=max_items,
                max_string_chars=max_string_chars,
            )
            for key, value in metadata.items()
            if str(key) != TASK_RUN_METADATA_MARKER
        }
        compacted[TASK_RUN_METADATA_MARKER] = {
            "truncated": True,
            "original_bytes": original_bytes,
            "max_bytes": max_bytes,
            "strategy": "bounded_recursive_sample",
        }
        _serialized, compacted_bytes = _serialized_bytes(compacted)
        if compacted_bytes <= max_bytes:
            return compacted

    return {
        TASK_RUN_METADATA_MARKER: {
            "truncated": True,
            "original_bytes": original_bytes,
            "max_bytes": max_bytes,
            "strategy": "marker_only",
            "top_level_keys": [str(key)[:120] for key in list(metadata)[:50]],
        }
    }


def _serialize_metadata(
    metadata: Optional[Dict[str, Any]],
    *,
    max_bytes: int = TASK_RUN_METADATA_MAX_BYTES,
) -> str:
    if max_bytes < 1024:
        raise ValueError("task metadata max_bytes must be at least 1024")
    bounded = _bounded_metadata(dict(metadata or {}), max_bytes=max_bytes)
    serialized, serialized_bytes = _serialized_bytes(bounded)
    if serialized_bytes > max_bytes:
        raise ValueError("bounded task metadata exceeds max_bytes")
    return serialized


class TaskRunLogger:
    def start(self, task_name: str, run_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        sql = """
        INSERT INTO task_run_log (task_name, run_id, status, started_at, metadata_json)
        VALUES (%s, %s, %s, %s, %s)
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        task_name,
                        run_id,
                        "running",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        _serialize_metadata(metadata),
                    ),
                )

    def finish(
        self,
        task_name: str,
        run_id: str,
        status: str,
        message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error_code: Optional[str] = None,
    ) -> None:
        is_error = status in {"failed", "killed"}
        safe_message = sanitize_error_message(message) if is_error else message
        resolved_error_code = (error_code or infer_error_code(safe_message)) if is_error else None
        fingerprint = error_fingerprint(safe_message) if is_error else None
        sql = """
        UPDATE task_run_log
        SET status = %s,
            finished_at = %s,
            message = %s,
            error_code = %s,
            error_fingerprint = %s,
            metadata_json = %s
        WHERE task_name = %s AND run_id = %s
        ORDER BY id DESC
        LIMIT 1
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        status,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        safe_message,
                        resolved_error_code,
                        fingerprint,
                        _serialize_metadata(metadata),
                        task_name,
                        run_id,
                    ),
                )
        if is_error:
            record_job_error(
                "scheduled_task",
                task_name,
                resolved_error_code,
                safe_message,
            )
