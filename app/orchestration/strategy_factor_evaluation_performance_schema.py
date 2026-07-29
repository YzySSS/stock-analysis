from __future__ import annotations

from typing import Any

from app.shared.db import mysql_maintenance_conn


TABLE_NAME = "strategy_factor_snapshot"
INDEX_NAME = "idx_factor_snapshot_manifest_scope"
INDEX_COLUMNS = ("manifest_id", "in_eligible_pool", "code")

ADD_INDEX_SQL = """
ALTER TABLE `strategy_factor_snapshot`
ADD KEY `idx_factor_snapshot_manifest_scope` (
    `manifest_id`, `in_eligible_pool`, `code`
),
ALGORITHM=INPLACE,
LOCK=NONE
"""


def _validate_existing_index(rows: list[dict[str, Any]]) -> None:
    ordered = sorted(rows, key=lambda row: int(row["seq_in_index"]))
    columns = tuple(str(row["column_name"]) for row in ordered)
    non_unique = {bool(int(row["non_unique"])) for row in ordered}
    index_types = {str(row["index_type"]).upper() for row in ordered}
    visible = {
        str(row.get("is_visible") or "YES").upper() == "YES"
        for row in ordered
    }
    sub_parts = {row.get("sub_part") for row in ordered}

    if columns != INDEX_COLUMNS:
        raise RuntimeError(
            f"{TABLE_NAME}: {INDEX_NAME} columns are {columns}, "
            f"expected {INDEX_COLUMNS}"
        )
    if non_unique != {True}:
        raise RuntimeError(
            f"{TABLE_NAME}: {INDEX_NAME} must be a non-unique index"
        )
    if index_types != {"BTREE"}:
        raise RuntimeError(
            f"{TABLE_NAME}: {INDEX_NAME} must use BTREE"
        )
    if visible != {True}:
        raise RuntimeError(
            f"{TABLE_NAME}: {INDEX_NAME} must be visible"
        )
    if sub_parts != {None}:
        raise RuntimeError(
            f"{TABLE_NAME}: {INDEX_NAME} must not use prefix columns"
        )


def ensure_strategy_factor_evaluation_performance_index() -> dict[str, Any]:
    with mysql_maintenance_conn(
        dict_cursor=True,
        timeout_seconds=600,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    INDEX_NAME AS index_name,
                    NON_UNIQUE AS non_unique,
                    SEQ_IN_INDEX AS seq_in_index,
                    COLUMN_NAME AS column_name,
                    SUB_PART AS sub_part,
                    INDEX_TYPE AS index_type,
                    IS_VISIBLE AS is_visible
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME=%s
                  AND INDEX_NAME=%s
                ORDER BY SEQ_IN_INDEX
                """,
                (TABLE_NAME, INDEX_NAME),
            )
            rows = list(cursor.fetchall() or [])
            if rows:
                _validate_existing_index(rows)
                return {
                    "status": "ok",
                    "table": TABLE_NAME,
                    "index": INDEX_NAME,
                    "created": False,
                    "columns": list(INDEX_COLUMNS),
                }

            cursor.execute(ADD_INDEX_SQL)

    return {
        "status": "ok",
        "table": TABLE_NAME,
        "index": INDEX_NAME,
        "created": True,
        "columns": list(INDEX_COLUMNS),
    }
