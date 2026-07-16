from __future__ import annotations

from datetime import datetime
from typing import Any

from app.data_ingestion.market_opinion_lifecycle import MARKET_OPINION_LOCK_NAME
from app.shared.db import mysql_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock


MARKET_OPINION_TABLES = (
    "sector_opinion_daily",
    "sector_opinion_stock",
    "sector_opinion_news_ref",
    "sector_opinion_source_ref",
)


def classify_storage_reclamation(*, file_per_table: bool, logical_cleanup_complete: bool) -> dict[str, Any]:
    if not logical_cleanup_complete:
        return {
            "status": "blocked_pending_logical_cleanup",
            "local_rebuild_recommended": False,
            "optimize_table_recommended": False,
            "reason": "legacy JSON or non-V2 parent rows still exist",
            "next_action": "finish and validate the market opinion lifecycle before storage maintenance",
        }
    if not file_per_table:
        return {
            "status": "shared_tablespace_reusable",
            "local_rebuild_recommended": False,
            "optimize_table_recommended": False,
            "reason": (
                "innodb_file_per_table is disabled; freed pages belong to the shared InnoDB "
                "tablespace and a single-table rebuild cannot shrink that shared physical file"
            ),
            "next_action": (
                "refresh optimizer statistics and monitor provider-level storage; use a provider-approved "
                "instance migration/rebuild only if physical allocation must shrink"
            ),
        }
    return {
        "status": "file_per_table_maintenance_window_required",
        "local_rebuild_recommended": False,
        "optimize_table_recommended": False,
        "reason": (
            "the table may have an individually reclaimable tablespace, but reclaim still requires "
            "capacity, lock and provider checks"
        ),
        "next_action": "prepare a separate provider-approved shadow rebuild in a maintenance window",
    }


def _actual_row_counts(cursor) -> dict[str, int]:
    cursor.execute(
        """
        SELECT 'sector_opinion_daily' AS table_name, COUNT(*) AS actual_rows FROM sector_opinion_daily
        UNION ALL
        SELECT 'sector_opinion_stock', COUNT(*) FROM sector_opinion_stock
        UNION ALL
        SELECT 'sector_opinion_news_ref', COUNT(*) FROM sector_opinion_news_ref
        UNION ALL
        SELECT 'sector_opinion_source_ref', COUNT(*) FROM sector_opinion_source_ref
        """
    )
    return {str(row["table_name"]): int(row["actual_rows"] or 0) for row in cursor.fetchall() or []}


def build_market_opinion_storage_report() -> dict[str, Any]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DATABASE() AS database_name,
                       VERSION() AS mysql_version,
                       @@innodb_file_per_table AS innodb_file_per_table
                """
            )
            server = cursor.fetchone() or {}
            cursor.execute(
                """
                SELECT COUNT(*) AS actual_rows,
                       COALESCE(SUM(payload_version >= 2), 0) AS normalized_rows,
                       COALESCE(SUM(
                           top_stocks_json IS NOT NULL
                           OR top_news_json IS NOT NULL
                           OR source_json IS NOT NULL
                       ), 0) AS legacy_json_rows,
                       COALESCE(SUM(OCTET_LENGTH(top_stocks_json)), 0)
                         + COALESCE(SUM(OCTET_LENGTH(top_news_json)), 0)
                         + COALESCE(SUM(OCTET_LENGTH(source_json)), 0) AS legacy_json_bytes,
                       MIN(trade_date) AS min_trade_date,
                       MAX(trade_date) AS max_trade_date
                FROM sector_opinion_daily
                """
            )
            logical = cursor.fetchone() or {}
            actual_rows = _actual_row_counts(cursor)
            placeholders = ",".join(["%s"] * len(MARKET_OPINION_TABLES))
            cursor.execute(
                f"""
                SELECT table_name AS table_name,
                       engine AS engine,
                       table_rows AS estimated_rows,
                       data_length AS data_bytes,
                       index_length AS index_bytes,
                       data_free AS reported_data_free_bytes
                FROM information_schema.tables
                WHERE table_schema=DATABASE()
                  AND table_name IN ({placeholders})
                ORDER BY table_name
                """,
                MARKET_OPINION_TABLES,
            )
            table_rows = cursor.fetchall() or []

    normalized_rows = int(logical.get("normalized_rows") or 0)
    parent_rows = int(logical.get("actual_rows") or 0)
    legacy_json_rows = int(logical.get("legacy_json_rows") or 0)
    logical_cleanup_complete = normalized_rows == parent_rows and legacy_json_rows == 0
    file_per_table = bool(int(server.get("innodb_file_per_table") or 0))
    tables = []
    for row in table_rows:
        table_name = str(row.get("table_name"))
        data_bytes = int(row.get("data_bytes") or 0)
        index_bytes = int(row.get("index_bytes") or 0)
        tables.append(
            {
                "table_name": table_name,
                "engine": row.get("engine"),
                "actual_rows": actual_rows.get(table_name, 0),
                "estimated_rows": int(row.get("estimated_rows") or 0),
                "data_bytes": data_bytes,
                "index_bytes": index_bytes,
                "allocated_bytes_reported": data_bytes + index_bytes,
                "reported_data_free_bytes": int(row.get("reported_data_free_bytes") or 0),
            }
        )
    return {
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "server": {
            "database_name": server.get("database_name"),
            "mysql_version": server.get("mysql_version"),
            "innodb_file_per_table": file_per_table,
            "tablespace_mode": "file_per_table" if file_per_table else "shared",
        },
        "logical_cleanup": {
            "actual_rows": parent_rows,
            "normalized_rows": normalized_rows,
            "legacy_json_rows": legacy_json_rows,
            "legacy_json_bytes": int(logical.get("legacy_json_bytes") or 0),
            "min_trade_date": str(logical.get("min_trade_date")) if logical.get("min_trade_date") else None,
            "max_trade_date": str(logical.get("max_trade_date")) if logical.get("max_trade_date") else None,
            "complete": logical_cleanup_complete,
        },
        "tables": tables,
        "reclamation": classify_storage_reclamation(
            file_per_table=file_per_table,
            logical_cleanup_complete=logical_cleanup_complete,
        ),
        "notes": [
            "information_schema row and byte figures are engine statistics, not a billing or filesystem measurement",
            "reported data_free is not attributable to one table when the server uses a shared tablespace",
            "this report never runs OPTIMIZE TABLE or a table rebuild",
        ],
    }


def analyze_market_opinion_statistics(*, lock_wait_seconds: int = 5) -> dict[str, Any]:
    wait_seconds = int(lock_wait_seconds)
    if not 1 <= wait_seconds <= 60:
        raise ValueError("lock_wait_seconds must be between 1 and 60")
    lock_handle = acquire_mysql_advisory_lock(MARKET_OPINION_LOCK_NAME)
    if lock_handle is None:
        return {"status": "skipped", "reason": "market_opinion_update_is_active"}
    try:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SET SESSION lock_wait_timeout={wait_seconds}")
                table_sql = ", ".join(f"`{table_name}`" for table_name in MARKET_OPINION_TABLES)
                cursor.execute(f"ANALYZE TABLE {table_sql}")
                rows = cursor.fetchall() or []
        return {
            "status": "success",
            "operation": "analyze_statistics_only",
            "tables": list(MARKET_OPINION_TABLES),
            "messages": rows,
            "optimize_table_executed": False,
            "table_rebuild_executed": False,
        }
    finally:
        release_mysql_advisory_lock(lock_handle)
