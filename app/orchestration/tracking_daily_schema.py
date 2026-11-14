"""Exact display-only daily reduction of the tracked-minute source."""
from app.shared.db import mysql_maintenance_conn

MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS tracking_minute_daily_manifest (
    trade_date DATE NOT NULL PRIMARY KEY,
    source_revision CHAR(32) NOT NULL,
    status VARCHAR(24) NOT NULL,
    source_scope VARCHAR(48) NOT NULL DEFAULT 'tracked_positive_price_v1',
    source_rows BIGINT NOT NULL DEFAULT 0,
    valid_rows BIGINT NOT NULL DEFAULT 0,
    anomalous_rows BIGINT NOT NULL DEFAULT 0,
    code_count INT NOT NULL DEFAULT 0,
    completeness VARCHAR(32) NOT NULL DEFAULT 'observed_only',
    generated_at DATETIME(6) NOT NULL,
    KEY idx_tracking_daily_status (status, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
DAILY_DDL = """
CREATE TABLE IF NOT EXISTS tracking_minute_daily (
    code VARCHAR(16) NOT NULL,
    trade_date DATE NOT NULL,
    source_revision CHAR(32) NOT NULL,
    min_price DECIMAL(12,4) NOT NULL,
    max_price DECIMAL(12,4) NOT NULL,
    valid_count BIGINT NOT NULL,
    first_quote_minute DATETIME NOT NULL,
    last_quote_minute DATETIME NOT NULL,
    PRIMARY KEY (code, trade_date),
    KEY idx_tracking_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def ensure_tracking_daily_schema() -> dict:
    with mysql_maintenance_conn(dict_cursor=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute('SET SESSION lock_wait_timeout = 5')
            cursor.execute(MANIFEST_DDL)
            cursor.execute(DAILY_DDL)
    return {'status': 'ok', 'tables': ['tracking_minute_daily_manifest', 'tracking_minute_daily']}
