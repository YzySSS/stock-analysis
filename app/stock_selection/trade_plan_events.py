from __future__ import annotations

import hashlib
import json
from contextlib import AbstractContextManager
from typing import Any, Callable, Iterable

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]


def immutable_trade_plan_id(
    *,
    source_kind: str,
    source_id: str,
    code: str,
    spec_hash: str,
) -> str:
    """Build a stable identifier without depending on mutable database IDs."""

    material = "|".join(
        [
            str(source_kind or "").strip(),
            str(source_id or "").strip(),
            str(code or "").strip(),
            str(spec_hash or "").strip(),
        ]
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"{str(source_kind or 'plan').strip()[:16]}:{digest}"


class TradePlanEventRepository:
    """Append-only persistence for observed trade-plan events."""

    def __init__(
        self,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        )

    def append(self, events: Iterable[dict[str, Any]]) -> int:
        rows = [dict(item) for item in events]
        if not rows:
            return 0
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                affected = cursor.executemany(
                    """
                    INSERT IGNORE INTO selection_trade_plan_event (
                        plan_id, selection_result_id, snapshot_id, code,
                        trade_plan_version, spec_hash, event_time, event_type,
                        planned_price, observed_price, executable, block_reason,
                        metadata_json
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    """,
                    [
                        (
                            item["plan_id"],
                            item.get("selection_result_id"),
                            item.get("snapshot_id"),
                            item["code"],
                            item["trade_plan_version"],
                            item.get("spec_hash"),
                            item["event_time"],
                            item["event_type"],
                            item.get("planned_price"),
                            item.get("observed_price"),
                            int(bool(item.get("executable"))),
                            item.get("block_reason"),
                            self._json(item.get("metadata") or {}),
                        )
                        for item in rows
                    ],
                )
        return int(affected or 0)

    def list_for_plan(self, plan_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM selection_trade_plan_event
                    WHERE plan_id=%s
                    ORDER BY event_time, id
                    """,
                    (plan_id,),
                )
                return cursor.fetchall() or []
