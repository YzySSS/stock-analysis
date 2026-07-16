from __future__ import annotations

from app.shared.db import mysql_conn


SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS stock_realtime_snapshot (
    code VARCHAR(16) NOT NULL PRIMARY KEY,
    source_code VARCHAR(16) DEFAULT NULL,
    name VARCHAR(64) DEFAULT NULL,
    trade_date DATE NOT NULL,
    quote_time DATETIME NOT NULL,
    latest_price DECIMAL(12,4) DEFAULT NULL,
    change_amount DECIMAL(12,4) DEFAULT NULL,
    pct_chg DECIMAL(12,4) DEFAULT NULL,
    bid_price DECIMAL(12,4) DEFAULT NULL,
    ask_price DECIMAL(12,4) DEFAULT NULL,
    pre_close DECIMAL(12,4) DEFAULT NULL,
    open_price DECIMAL(12,4) DEFAULT NULL,
    high_price DECIMAL(12,4) DEFAULT NULL,
    low_price DECIMAL(12,4) DEFAULT NULL,
    volume BIGINT DEFAULT NULL,
    amount DECIMAL(20,2) DEFAULT NULL,
    batch_id VARCHAR(64) DEFAULT NULL,
    received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    freshness_seconds INT DEFAULT NULL,
    is_stale TINYINT(1) NOT NULL DEFAULT 0,
    source VARCHAR(32) NOT NULL DEFAULT 'akshare_stock_zh_a_spot',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_realtime_snapshot_trade_date (trade_date),
    KEY idx_realtime_snapshot_quote_time (quote_time),
    KEY idx_realtime_snapshot_pct_chg (pct_chg),
    KEY idx_realtime_snapshot_batch (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def intraday_table_ddl(table_name: str, *, partitioned: bool = True) -> str:
    if table_name not in {
        "stock_realtime_intraday",
        "stock_realtime_intraday_partitioned",
        "stock_realtime_intraday_tracked",
    }:
        raise ValueError(f"unsupported realtime intraday table: {table_name}")
    primary_key = "PRIMARY KEY (id, trade_date)" if partitioned else "PRIMARY KEY (id)"
    partition_sql = """
PARTITION BY RANGE COLUMNS(trade_date) (
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
)""" if partitioned else ""
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGINT NOT NULL AUTO_INCREMENT,
    code VARCHAR(16) NOT NULL,
    source_code VARCHAR(16) DEFAULT NULL,
    name VARCHAR(64) DEFAULT NULL,
    trade_date DATE NOT NULL,
    quote_time DATETIME NOT NULL,
    quote_minute DATETIME NOT NULL,
    latest_price DECIMAL(12,4) DEFAULT NULL,
    change_amount DECIMAL(12,4) DEFAULT NULL,
    pct_chg DECIMAL(12,4) DEFAULT NULL,
    bid_price DECIMAL(12,4) DEFAULT NULL,
    ask_price DECIMAL(12,4) DEFAULT NULL,
    pre_close DECIMAL(12,4) DEFAULT NULL,
    open_price DECIMAL(12,4) DEFAULT NULL,
    high_price DECIMAL(12,4) DEFAULT NULL,
    low_price DECIMAL(12,4) DEFAULT NULL,
    volume BIGINT DEFAULT NULL,
    amount DECIMAL(20,2) DEFAULT NULL,
    batch_id VARCHAR(64) DEFAULT NULL,
    received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    freshness_seconds INT DEFAULT NULL,
    is_stale TINYINT(1) NOT NULL DEFAULT 0,
    source VARCHAR(32) NOT NULL DEFAULT 'akshare_stock_zh_a_spot',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    {primary_key},
    UNIQUE KEY uniq_realtime_intraday_minute (trade_date, quote_minute, code),
    KEY idx_realtime_intraday_code_time (code, quote_minute),
    KEY idx_realtime_intraday_pct_chg (trade_date, pct_chg)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
{partition_sql};
"""


ROLLUP_DDL = """
CREATE TABLE IF NOT EXISTS stock_realtime_bar_rollup (
    id BIGINT NOT NULL AUTO_INCREMENT,
    code VARCHAR(16) NOT NULL,
    source_code VARCHAR(16) DEFAULT NULL,
    name VARCHAR(64) DEFAULT NULL,
    trade_date DATE NOT NULL,
    interval_minutes TINYINT UNSIGNED NOT NULL,
    bucket_start DATETIME NOT NULL,
    bucket_end DATETIME NOT NULL,
    open_price DECIMAL(12,4) DEFAULT NULL,
    high_price DECIMAL(12,4) DEFAULT NULL,
    low_price DECIMAL(12,4) DEFAULT NULL,
    close_price DECIMAL(12,4) DEFAULT NULL,
    pre_close DECIMAL(12,4) DEFAULT NULL,
    pct_chg_close DECIMAL(12,4) DEFAULT NULL,
    volume_delta BIGINT DEFAULT NULL,
    amount_delta DECIMAL(20,2) DEFAULT NULL,
    cumulative_volume BIGINT DEFAULT NULL,
    cumulative_amount DECIMAL(20,2) DEFAULT NULL,
    sample_count SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    first_quote_time DATETIME DEFAULT NULL,
    last_quote_time DATETIME DEFAULT NULL,
    source VARCHAR(32) NOT NULL DEFAULT 'stock_realtime_intraday',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id, trade_date),
    UNIQUE KEY uniq_realtime_rollup_bucket (trade_date, interval_minutes, bucket_start, code),
    KEY idx_realtime_rollup_code_time (code, interval_minutes, bucket_start),
    KEY idx_realtime_rollup_date_interval (trade_date, interval_minutes)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE COLUMNS(trade_date) (
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);
"""


ROLLUP_MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS stock_realtime_rollup_manifest (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL,
    interval_minutes TINYINT UNSIGNED NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'running',
    source_rows BIGINT NOT NULL DEFAULT 0,
    source_codes INT NOT NULL DEFAULT 0,
    rollup_rows BIGINT NOT NULL DEFAULT 0,
    rollup_codes INT NOT NULL DEFAULT 0,
    first_quote_minute DATETIME DEFAULT NULL,
    last_quote_minute DATETIME DEFAULT NULL,
    error_code VARCHAR(64) DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_realtime_rollup_manifest (trade_date, interval_minutes),
    KEY idx_realtime_rollup_manifest_status (status, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


SNAPSHOT_COLUMN_MIGRATIONS = {
    "batch_id": "ALTER TABLE stock_realtime_snapshot ADD COLUMN batch_id VARCHAR(64) DEFAULT NULL AFTER amount",
    "received_at": "ALTER TABLE stock_realtime_snapshot ADD COLUMN received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER batch_id",
    "freshness_seconds": "ALTER TABLE stock_realtime_snapshot ADD COLUMN freshness_seconds INT DEFAULT NULL AFTER received_at",
    "is_stale": "ALTER TABLE stock_realtime_snapshot ADD COLUMN is_stale TINYINT(1) NOT NULL DEFAULT 0 AFTER freshness_seconds",
}

SNAPSHOT_INDEX_MIGRATIONS = {
    "idx_realtime_snapshot_batch": "ALTER TABLE stock_realtime_snapshot ADD KEY idx_realtime_snapshot_batch (batch_id)",
}

INTRADAY_COLUMN_MIGRATIONS = {
    "batch_id": "ALTER TABLE stock_realtime_intraday ADD COLUMN batch_id VARCHAR(64) DEFAULT NULL AFTER amount",
    "received_at": "ALTER TABLE stock_realtime_intraday ADD COLUMN received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER batch_id",
    "freshness_seconds": "ALTER TABLE stock_realtime_intraday ADD COLUMN freshness_seconds INT DEFAULT NULL AFTER received_at",
    "is_stale": "ALTER TABLE stock_realtime_intraday ADD COLUMN is_stale TINYINT(1) NOT NULL DEFAULT 0 AFTER freshness_seconds",
}


def _apply_columns(cursor, table_name: str, migrations: dict[str, str], applied: list[str]) -> None:
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    existing = {str(row[0]) for row in cursor.fetchall()}
    for column, sql in migrations.items():
        if column not in existing:
            cursor.execute(sql)
            applied.append(f"{table_name}.{column}")


def _apply_indexes(cursor, table_name: str, migrations: dict[str, str], applied: list[str]) -> None:
    cursor.execute(f"SHOW INDEX FROM {table_name}")
    existing = {str(row[2]) for row in cursor.fetchall()}
    for index, sql in migrations.items():
        if index not in existing:
            cursor.execute(sql)
            applied.append(f"{table_name}.{index}")


def ensure_realtime_schema() -> dict:
    applied: list[str] = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(SNAPSHOT_DDL)
            cursor.execute(intraday_table_ddl("stock_realtime_intraday", partitioned=True))
            cursor.execute(intraday_table_ddl("stock_realtime_intraday_tracked", partitioned=True))
            cursor.execute(ROLLUP_DDL)
            cursor.execute(ROLLUP_MANIFEST_DDL)
            _apply_columns(cursor, "stock_realtime_snapshot", SNAPSHOT_COLUMN_MIGRATIONS, applied)
            _apply_columns(cursor, "stock_realtime_intraday", INTRADAY_COLUMN_MIGRATIONS, applied)
            _apply_indexes(cursor, "stock_realtime_snapshot", SNAPSHOT_INDEX_MIGRATIONS, applied)
    return {
        "status": "ok",
        "tables": [
            "stock_realtime_snapshot",
            "stock_realtime_intraday",
            "stock_realtime_intraday_tracked",
            "stock_realtime_bar_rollup",
            "stock_realtime_rollup_manifest",
        ],
        "applied": applied or ["realtime_schema_already_current"],
    }
