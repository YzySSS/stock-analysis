from __future__ import annotations

from typing import Any

from app.shared.db import mysql_maintenance_conn


TABLE_NAME = "stock_realtime_intraday_tracked"
INDEX_NAME = "idx_tracked_code_minute_price"
INDEX_COLUMNS = ("code", "quote_minute", "latest_price")

# The partitioned primary key also supplies trade_date to this secondary index.
# Keep the older index: other ingestion/read paths can still use it.
ADD_INDEX_SQL = """
ALTER TABLE `stock_realtime_intraday_tracked`
ADD KEY `idx_tracked_code_minute_price` (`code`, `quote_minute`, `latest_price`),
ALGORITHM=INPLACE,
LOCK=NONE
"""


def _validate_existing_index(rows: list[dict[str, Any]]) -> None:
    ordered = sorted(rows, key=lambda row: int(row["seq_in_index"]))
    columns = tuple(str(row["column_name"]) for row in ordered)
    if columns != INDEX_COLUMNS or any(
        int(row["non_unique"]) != 1
        or str(row["index_type"]).upper() != "BTREE"
        or str(row.get("is_visible") or "YES").upper() != "YES"
        or row.get("sub_part") is not None
        for row in ordered
    ):
        raise RuntimeError(f"{TABLE_NAME}: incompatible existing {INDEX_NAME}")


def ensure_tracking_minute_covering_index() -> dict[str, Any]:
    with mysql_maintenance_conn(dict_cursor=True, timeout_seconds=600) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index,
                       COLUMN_NAME AS column_name, SUB_PART AS sub_part,
                       INDEX_TYPE AS index_type, IS_VISIBLE AS is_visible
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND INDEX_NAME=%s
                ORDER BY SEQ_IN_INDEX
                """,
                (TABLE_NAME, INDEX_NAME),
            )
            rows = list(cursor.fetchall() or [])
            if rows:
                _validate_existing_index(rows)
            else:
                # Fail quickly instead of queueing behind a long transaction.
                # Explicit online requirements must never fall back to COPY or
                # a blocking LOCK. This connection is unpooled and then closed.
                cursor.execute("SET SESSION lock_wait_timeout = 5")
                cursor.execute(ADD_INDEX_SQL)
    return {
        "status": "ok",
        "table": TABLE_NAME,
        "index": INDEX_NAME,
        "columns": list(INDEX_COLUMNS),
        "created": not bool(rows),
    }
