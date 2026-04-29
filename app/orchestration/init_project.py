from __future__ import annotations

from app.shared.db import mysql_conn, ping_mysql


CORE_TABLE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS stock_basic (
        code VARCHAR(16) NOT NULL PRIMARY KEY,
        name VARCHAR(64) NOT NULL,
        instrument_type VARCHAR(16) DEFAULT 'other',
        market VARCHAR(16) DEFAULT NULL,
        industry VARCHAR(128) DEFAULT NULL,
        pe_tushare DECIMAL(12,4) DEFAULT NULL,
        pb_tushare DECIMAL(12,4) DEFAULT NULL,
        valuation_updated_at DATETIME DEFAULT NULL,
        roe DECIMAL(12,4) DEFAULT NULL,
        roa DECIMAL(12,4) DEFAULT NULL,
        grossprofit_margin DECIMAL(12,4) DEFAULT NULL,
        netprofit_margin DECIMAL(12,4) DEFAULT NULL,
        revenue_yoy DECIMAL(12,4) DEFAULT NULL,
        profit_yoy DECIMAL(12,4) DEFAULT NULL,
        fundamental_period VARCHAR(16) DEFAULT NULL,
        fundamental_updated_at DATETIME DEFAULT NULL,
        is_st TINYINT(1) NOT NULL DEFAULT 0,
        is_delisted TINYINT(1) NOT NULL DEFAULT 0,
        listing_date DATE DEFAULT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_instrument_type (instrument_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_kline (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        open DECIMAL(12,4) DEFAULT NULL,
        high DECIMAL(12,4) DEFAULT NULL,
        low DECIMAL(12,4) DEFAULT NULL,
        close DECIMAL(12,4) DEFAULT NULL,
        volume BIGINT DEFAULT NULL,
        amount DECIMAL(20,2) DEFAULT NULL,
        source VARCHAR(32) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_code_date (code, trade_date),
        KEY idx_trade_date (trade_date),
        KEY idx_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS factor_snapshot (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        factor_name VARCHAR(64) NOT NULL,
        factor_value DECIMAL(20,8) DEFAULT NULL,
        strategy_id VARCHAR(64) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_factor_snapshot (code, trade_date, factor_name, strategy_id),
        KEY idx_factor_date (trade_date, factor_name),
        KEY idx_factor_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS factor_input_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        pe_tushare DECIMAL(12,4) DEFAULT NULL,
        pb_tushare DECIMAL(12,4) DEFAULT NULL,
        roe DECIMAL(12,4) DEFAULT NULL,
        roa DECIMAL(12,4) DEFAULT NULL,
        grossprofit_margin DECIMAL(12,4) DEFAULT NULL,
        netprofit_margin DECIMAL(12,4) DEFAULT NULL,
        revenue_yoy DECIMAL(12,4) DEFAULT NULL,
        profit_yoy DECIMAL(12,4) DEFAULT NULL,
        fundamental_period VARCHAR(16) DEFAULT NULL,
        source VARCHAR(32) DEFAULT 'tushare_stock_basic_snapshot',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_factor_input_daily (code, trade_date),
        KEY idx_factor_input_trade_date (trade_date),
        KEY idx_factor_input_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS selection_result (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        code VARCHAR(16) NOT NULL,
        score DECIMAL(20,8) DEFAULT NULL,
        rank_no INT DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_selection_result (run_id, code),
        KEY idx_selection_date (trade_date),
        KEY idx_selection_strategy (strategy_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_registry (
        strategy_id VARCHAR(64) NOT NULL PRIMARY KEY,
        version VARCHAR(32) NOT NULL,
        display_name VARCHAR(128) DEFAULT NULL,
        status VARCHAR(32) NOT NULL DEFAULT 'active',
        config_json JSON DEFAULT NULL,
        notes TEXT,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS task_run_log (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        task_name VARCHAR(128) NOT NULL,
        run_id VARCHAR(64) DEFAULT NULL,
        status VARCHAR(32) NOT NULL,
        started_at DATETIME NOT NULL,
        finished_at DATETIME DEFAULT NULL,
        message TEXT,
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        KEY idx_task_name (task_name),
        KEY idx_task_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def init_mysql_schema() -> None:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in CORE_TABLE_DDL:
                cursor.execute(ddl)


if __name__ == "__main__":
    info = ping_mysql()
    print(f"Connected to MySQL: {info}")
    init_mysql_schema()
    print("Core schema initialized.")
