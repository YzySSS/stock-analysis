from __future__ import annotations

from app.shared.db import mysql_conn


COLUMN_MIGRATIONS = {
    "state_label": (
        "ALTER TABLE market_leadership_state_daily "
        "MODIFY COLUMN state_label VARCHAR(32) NOT NULL"
    ),
    "cycle_state": (
        "ALTER TABLE market_leadership_state_daily "
        "ADD COLUMN cycle_state VARCHAR(32) DEFAULT NULL AFTER state_label"
    ),
    "cycle_label": (
        "ALTER TABLE market_leadership_state_daily "
        "ADD COLUMN cycle_label VARCHAR(32) DEFAULT NULL AFTER cycle_state"
    ),
    "price_score": (
        "ALTER TABLE market_leadership_state_daily "
        "ADD COLUMN price_score DECIMAL(10,4) DEFAULT NULL AFTER crowding_score"
    ),
    "price_evidence_status": (
        "ALTER TABLE market_leadership_state_daily "
        "ADD COLUMN price_evidence_status VARCHAR(32) DEFAULT NULL AFTER price_score"
    ),
    "price_metrics_json": (
        "ALTER TABLE market_leadership_state_daily "
        "ADD COLUMN price_metrics_json JSON DEFAULT NULL AFTER price_evidence_status"
    ),
    "breadth_metrics_json": (
        "ALTER TABLE market_leadership_state_daily "
        "ADD COLUMN breadth_metrics_json JSON DEFAULT NULL AFTER price_metrics_json"
    ),
    "data_quality_json": (
        "ALTER TABLE market_leadership_state_daily "
        "ADD COLUMN data_quality_json JSON DEFAULT NULL AFTER source_lineage_json"
    ),
}


def ensure_leadership_cycle_schema() -> dict:
    added = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COLUMN_NAME, COLUMN_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME='market_leadership_state_daily'
                """
            )
            columns = {
                str(row[0]): str(row[1]).lower()
                for row in (cursor.fetchall() or [])
            }
            if columns.get("state_label") != "varchar(32)":
                cursor.execute(COLUMN_MIGRATIONS["state_label"])
                added.append("state_label:varchar(32)")
            for column, sql in COLUMN_MIGRATIONS.items():
                if column == "state_label" or column in columns:
                    continue
                cursor.execute(sql)
                added.append(column)
    return {
        "status": "ok",
        "table": "market_leadership_state_daily",
        "added": added,
    }
