from __future__ import annotations

from app.shared.db import mysql_conn


STOCK_STATUS_PIT_DDL = (
    """
    CREATE TABLE IF NOT EXISTS stock_instrument_lifecycle (
        code VARCHAR(16) NOT NULL PRIMARY KEY,
        ts_code VARCHAR(16) NOT NULL,
        name VARCHAR(64) NOT NULL,
        instrument_type VARCHAR(16) NOT NULL DEFAULT 'stock',
        exchange VARCHAR(8) DEFAULT NULL,
        market VARCHAR(32) DEFAULT NULL,
        industry VARCHAR(128) DEFAULT NULL,
        list_status VARCHAR(8) NOT NULL,
        listing_date DATE DEFAULT NULL,
        delisting_date DATE DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare_stock_basic',
        source_sync_id VARCHAR(64) DEFAULT NULL,
        source_updated_at DATETIME NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_lifecycle_ts_code (ts_code),
        KEY idx_stock_lifecycle_status (list_status),
        KEY idx_stock_lifecycle_dates (listing_date, delisting_date),
        KEY idx_stock_lifecycle_type (instrument_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_name_history (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        name VARCHAR(64) NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE DEFAULT NULL,
        announcement_date DATE DEFAULT NULL,
        change_reason VARCHAR(255) DEFAULT NULL,
        is_st TINYINT(1) NOT NULL DEFAULT 0,
        is_delisting_period TINYINT(1) NOT NULL DEFAULT 0,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare_namechange',
        source_sync_id VARCHAR(64) DEFAULT NULL,
        source_updated_at DATETIME NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_name_interval (code, start_date, name),
        KEY idx_stock_name_effective (code, start_date, end_date),
        KEY idx_stock_name_st (is_st, start_date, end_date),
        KEY idx_stock_name_delisting (is_delisting_period, start_date, end_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_suspension_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        suspend_type VARCHAR(8) NOT NULL,
        suspend_timing VARCHAR(64) NOT NULL DEFAULT '',
        source VARCHAR(32) NOT NULL DEFAULT 'tushare_suspend_d',
        source_sync_id VARCHAR(64) DEFAULT NULL,
        source_updated_at DATETIME NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_suspension_event (code, trade_date, suspend_type, suspend_timing),
        KEY idx_stock_suspension_date (trade_date, suspend_type),
        KEY idx_stock_suspension_code (code, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_status_pit_manifest (
        dataset VARCHAR(32) NOT NULL,
        partition_key VARCHAR(64) NOT NULL,
        status VARCHAR(24) NOT NULL,
        source VARCHAR(32) NOT NULL,
        source_rows INT NOT NULL DEFAULT 0,
        sync_run_id VARCHAR(64) DEFAULT NULL,
        started_at DATETIME DEFAULT NULL,
        finished_at DATETIME DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (dataset, partition_key),
        KEY idx_stock_status_pit_manifest_status (dataset, status, updated_at),
        KEY idx_stock_status_pit_manifest_run (sync_run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
)


def ensure_stock_status_pit_schema() -> dict:
    tables = (
        "stock_instrument_lifecycle",
        "stock_name_history",
        "stock_suspension_daily",
        "stock_status_pit_manifest",
    )
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in STOCK_STATUS_PIT_DDL:
                cursor.execute(ddl)
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME='factor_input_daily'
                  AND COLUMN_NAME='source'
                """
            )
            if int(cursor.fetchone()[0] or 0) == 0:
                cursor.execute(
                    """
                    ALTER TABLE factor_input_daily
                    ADD COLUMN source VARCHAR(32) DEFAULT 'tushare_daily_basic'
                    AFTER completeness_score
                    """
                )
    return {
        "status": "ok",
        "tables": list(tables),
        "factor_input_source_column": "ready",
    }
