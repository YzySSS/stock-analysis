from __future__ import annotations

from app.shared.db import mysql_conn


ROLLUP_MANIFEST_COLUMN_MIGRATIONS = {
    "source_fingerprint": (
        "ALTER TABLE stock_realtime_rollup_manifest "
        "ADD COLUMN source_fingerprint CHAR(64) DEFAULT NULL AFTER last_quote_minute"
    ),
}


def ensure_realtime_lifecycle_schema() -> dict:
    applied_columns: list[str] = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM stock_realtime_rollup_manifest")
            existing_columns = {str(row[0]) for row in (cursor.fetchall() or [])}
            for column_name, sql in ROLLUP_MANIFEST_COLUMN_MIGRATIONS.items():
                if column_name not in existing_columns:
                    cursor.execute(sql)
                    applied_columns.append(column_name)
    return {
        "status": "ok",
        "tables": ["stock_realtime_rollup_manifest"],
        "applied_columns": applied_columns or ["realtime_lifecycle_schema_already_current"],
    }
