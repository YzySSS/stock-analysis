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
        turnover_rate DECIMAL(12,4) DEFAULT NULL,
        turnover_rate_f DECIMAL(12,4) DEFAULT NULL,
        volume_ratio DECIMAL(12,4) DEFAULT NULL,
        total_mv DECIMAL(20,4) DEFAULT NULL,
        circ_mv DECIMAL(20,4) DEFAULT NULL,
        roe DECIMAL(12,4) DEFAULT NULL,
        roa DECIMAL(12,4) DEFAULT NULL,
        grossprofit_margin DECIMAL(12,4) DEFAULT NULL,
        netprofit_margin DECIMAL(12,4) DEFAULT NULL,
        revenue_yoy DECIMAL(12,4) DEFAULT NULL,
        profit_yoy DECIMAL(12,4) DEFAULT NULL,
        fundamental_period VARCHAR(16) DEFAULT NULL,
        fundamental_publish_date DATE DEFAULT NULL,
        valuation_source VARCHAR(32) DEFAULT NULL,
        fundamental_source VARCHAR(32) DEFAULT NULL,
        valuation_updated_at DATETIME DEFAULT NULL,
        fundamental_updated_at DATETIME DEFAULT NULL,
        completeness_score DECIMAL(8,4) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_factor_input_daily (code, trade_date),
        KEY idx_factor_input_trade_date (trade_date),
        KEY idx_factor_input_code (code),
        KEY idx_factor_input_period (fundamental_period)
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
        include_in_stats TINYINT(1) NOT NULL DEFAULT 1,
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
    """
    CREATE TABLE IF NOT EXISTS stock_status_snapshot (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        status_label VARCHAR(32) NOT NULL DEFAULT 'normal',
        status_reason VARCHAR(255) DEFAULT NULL,
        suspension_date DATE DEFAULT NULL,
        resume_date DATE DEFAULT NULL,
        paused_listing_date DATE DEFAULT NULL,
        expected_resume_date DATE DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'derived',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_status_snapshot (code, trade_date),
        KEY idx_stock_status_trade_date (trade_date),
        KEY idx_stock_status_label (status_label),
        KEY idx_stock_status_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_run (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        strategy_version VARCHAR(32) DEFAULT NULL,
        instrument_type VARCHAR(16) NOT NULL DEFAULT 'stock',
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        return_mode VARCHAR(16) NOT NULL,
        use_adjusted_price TINYINT(1) NOT NULL DEFAULT 0,
        status VARCHAR(32) NOT NULL DEFAULT 'running',
        sample_days INT DEFAULT 0,
        total_picks INT DEFAULT 0,
        total_trades INT DEFAULT 0,
        request_json JSON DEFAULT NULL,
        summary_json JSON DEFAULT NULL,
        error_message TEXT,
        started_at DATETIME NOT NULL,
        finished_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_backtest_run_id (run_id),
        KEY idx_backtest_strategy (strategy_id),
        KEY idx_backtest_status (status),
        KEY idx_backtest_date_range (start_date, end_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_pick (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        code VARCHAR(16) NOT NULL,
        rank_no INT DEFAULT NULL,
        score DECIMAL(12,4) DEFAULT NULL,
        entry_price DECIMAL(12,4) DEFAULT NULL,
        entry_price_type VARCHAR(16) DEFAULT 'open',
        factor_json JSON DEFAULT NULL,
        explain_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_backtest_pick (run_id, trade_date, code),
        KEY idx_backtest_pick_run (run_id),
        KEY idx_backtest_pick_date (trade_date),
        KEY idx_backtest_pick_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_trade (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        code VARCHAR(16) NOT NULL,
        entry_date DATE NOT NULL,
        entry_price DECIMAL(12,4) NOT NULL,
        exit_date_1d DATE DEFAULT NULL,
        exit_price_1d DECIMAL(12,4) DEFAULT NULL,
        return_1d_pct DECIMAL(12,4) DEFAULT NULL,
        exit_date_3d DATE DEFAULT NULL,
        exit_price_3d DECIMAL(12,4) DEFAULT NULL,
        return_3d_pct DECIMAL(12,4) DEFAULT NULL,
        max_gain_pct DECIMAL(12,4) DEFAULT NULL,
        max_drawdown_pct DECIMAL(12,4) DEFAULT NULL,
        benchmark_code VARCHAR(16) DEFAULT NULL,
        benchmark_return_1d_pct DECIMAL(12,4) DEFAULT NULL,
        benchmark_return_3d_pct DECIMAL(12,4) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_backtest_trade (run_id, trade_date, code),
        KEY idx_backtest_trade_run (run_id),
        KEY idx_backtest_trade_date (trade_date),
        KEY idx_backtest_trade_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_summary_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        pick_count INT DEFAULT 0,
        avg_return_1d_pct DECIMAL(12,4) DEFAULT NULL,
        avg_return_3d_pct DECIMAL(12,4) DEFAULT NULL,
        win_rate_1d_pct DECIMAL(12,4) DEFAULT NULL,
        win_rate_3d_pct DECIMAL(12,4) DEFAULT NULL,
        benchmark_return_1d_pct DECIMAL(12,4) DEFAULT NULL,
        benchmark_return_3d_pct DECIMAL(12,4) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_backtest_summary_daily (run_id, trade_date),
        KEY idx_backtest_summary_daily_run (run_id),
        KEY idx_backtest_summary_daily_date (trade_date)
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
