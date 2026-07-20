from __future__ import annotations

from app.shared.db import mysql_conn


ADJ_FACTOR_MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS adj_factor_sync_manifest (
    trade_date DATE NOT NULL PRIMARY KEY,
    status VARCHAR(24) NOT NULL,
    source VARCHAR(48) NOT NULL DEFAULT 'tushare_adj_factor',
    expected_kline_rows INT NOT NULL DEFAULT 0,
    source_rows INT NOT NULL DEFAULT 0,
    stored_rows INT NOT NULL DEFAULT 0,
    matched_rows INT NOT NULL DEFAULT 0,
    missing_rows INT NOT NULL DEFAULT 0,
    coverage_ratio DECIMAL(12,8) NOT NULL DEFAULT 0,
    sync_run_id VARCHAR(64) DEFAULT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    error_code VARCHAR(64) DEFAULT NULL,
    error_message VARCHAR(1000) DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_adj_factor_manifest_status (status, trade_date),
    KEY idx_adj_factor_manifest_run (sync_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_adj_factor_manifest_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(ADJ_FACTOR_MANIFEST_DDL)
    return {"status": "ok", "tables": ["adj_factor_sync_manifest"]}
