from __future__ import annotations

from app.orchestration.init_project import init_mysql_schema
from app.shared.db import mysql_conn

DDL = [
    """
    CREATE TABLE IF NOT EXISTS ths_concept_hot_snapshot (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        concept_name VARCHAR(128) NOT NULL,
        concept_code VARCHAR(32) DEFAULT NULL,
        summary_date DATE DEFAULT NULL,
        quote_time DATETIME NOT NULL,
        driver_event TEXT DEFAULT NULL,
        leading_stock VARCHAR(128) DEFAULT NULL,
        member_count INT DEFAULT NULL,
        ths_score DECIMAL(12,4) DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'akshare_stock_board_concept_summary_ths',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_ths_concept_hot_snapshot (concept_name),
        KEY idx_ths_concept_hot_snapshot_quote_time (quote_time),
        KEY idx_ths_concept_hot_snapshot_summary_date (summary_date),
        KEY idx_ths_concept_hot_snapshot_score (ths_score)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
]


def ensure_ths_concept_hot_schema() -> dict:
    init_mysql_schema()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in DDL:
                cursor.execute(ddl)
    return {"status": "ok", "tables": ["ths_concept_hot_snapshot"]}


if __name__ == "__main__":
    print(ensure_ths_concept_hot_schema())
