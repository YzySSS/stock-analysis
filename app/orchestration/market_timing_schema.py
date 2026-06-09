from __future__ import annotations

from app.orchestration.init_project import init_mysql_schema
from app.shared.db import mysql_conn


DDL = [
    """
    CREATE TABLE IF NOT EXISTS market_index_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        index_code VARCHAR(16) NOT NULL,
        open DECIMAL(14,4) DEFAULT NULL,
        high DECIMAL(14,4) DEFAULT NULL,
        low DECIMAL(14,4) DEFAULT NULL,
        close DECIMAL(14,4) DEFAULT NULL,
        pre_close DECIMAL(14,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        vol DECIMAL(24,4) DEFAULT NULL,
        amount DECIMAL(24,4) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare.index_daily',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_index_daily (trade_date, index_code),
        KEY idx_market_index_code_date (index_code, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_index_valuation_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        index_code VARCHAR(16) NOT NULL,
        pe DECIMAL(14,4) DEFAULT NULL,
        pe_ttm DECIMAL(14,4) DEFAULT NULL,
        pb DECIMAL(14,4) DEFAULT NULL,
        turnover_rate DECIMAL(14,4) DEFAULT NULL,
        total_mv DECIMAL(24,4) DEFAULT NULL,
        float_mv DECIMAL(24,4) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare.index_dailybasic',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_index_valuation_daily (trade_date, index_code),
        KEY idx_market_index_valuation_code_date (index_code, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_margin_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        exchange_id VARCHAR(16) NOT NULL DEFAULT 'ALL',
        rzye DECIMAL(24,4) DEFAULT NULL,
        rzmre DECIMAL(24,4) DEFAULT NULL,
        rzche DECIMAL(24,4) DEFAULT NULL,
        rqye DECIMAL(24,4) DEFAULT NULL,
        rqmcl DECIMAL(24,4) DEFAULT NULL,
        rzrqye DECIMAL(24,4) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare.margin',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_margin_daily (trade_date, exchange_id),
        KEY idx_market_margin_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_bond_yield_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        curve_name VARCHAR(64) NOT NULL DEFAULT 'China Treasury',
        maturity_years DECIMAL(8,4) NOT NULL DEFAULT 10,
        yield_rate DECIMAL(12,6) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare.yc_cb',
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_bond_yield_daily (trade_date, curve_name, maturity_years),
        KEY idx_market_bond_yield_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_option_pcr_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        exchange VARCHAR(16) NOT NULL DEFAULT 'ALL',
        call_volume DECIMAL(24,4) DEFAULT NULL,
        put_volume DECIMAL(24,4) DEFAULT NULL,
        volume_pcr DECIMAL(14,6) DEFAULT NULL,
        call_oi DECIMAL(24,4) DEFAULT NULL,
        put_oi DECIMAL(24,4) DEFAULT NULL,
        oi_pcr DECIMAL(14,6) DEFAULT NULL,
        contract_count INT DEFAULT 0,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare.opt_daily+opt_basic',
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_option_pcr_daily (trade_date, exchange),
        KEY idx_market_option_pcr_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_futures_holding_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        symbol_family VARCHAR(16) NOT NULL DEFAULT 'ALL',
        long_holding DECIMAL(24,4) DEFAULT NULL,
        short_holding DECIMAL(24,4) DEFAULT NULL,
        net_holding DECIMAL(24,4) DEFAULT NULL,
        net_holding_ratio DECIMAL(14,6) DEFAULT NULL,
        long_change DECIMAL(24,4) DEFAULT NULL,
        short_change DECIMAL(24,4) DEFAULT NULL,
        net_change DECIMAL(24,4) DEFAULT NULL,
        row_count INT DEFAULT 0,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare.fut_holding',
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_futures_holding_daily (trade_date, symbol_family),
        KEY idx_market_futures_holding_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_option_qvix_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        qvix_code VARCHAR(32) NOT NULL,
        qvix_name VARCHAR(64) NOT NULL,
        open DECIMAL(14,4) DEFAULT NULL,
        high DECIMAL(14,4) DEFAULT NULL,
        low DECIMAL(14,4) DEFAULT NULL,
        close DECIMAL(14,4) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'akshare.qvix',
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_option_qvix_daily (trade_date, qvix_code),
        KEY idx_market_option_qvix_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_option_iv_skew_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        underlying_code VARCHAR(16) NOT NULL,
        underlying_name VARCHAR(64) DEFAULT NULL,
        maturity_date DATE DEFAULT NULL,
        days_to_maturity INT DEFAULT NULL,
        spot_price DECIMAL(14,4) DEFAULT NULL,
        atm_iv DECIMAL(14,8) DEFAULT NULL,
        put_iv DECIMAL(14,8) DEFAULT NULL,
        call_iv DECIMAL(14,8) DEFAULT NULL,
        skew_value DECIMAL(14,8) DEFAULT NULL,
        sample_count INT DEFAULT 0,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare.opt_daily+opt_basic',
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_option_iv_skew_daily (trade_date, underlying_code),
        KEY idx_market_option_iv_skew_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_timing_indicator_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        index_code VARCHAR(16) NOT NULL DEFAULT '000300.SH',
        dimension VARCHAR(32) NOT NULL,
        indicator_id VARCHAR(64) NOT NULL,
        indicator_name VARCHAR(128) NOT NULL,
        value DECIMAL(24,6) DEFAULT NULL,
        value_label VARCHAR(64) DEFAULT NULL,
        score DECIMAL(8,4) DEFAULT NULL,
        signal_value TINYINT DEFAULT 0,
        signal_label VARCHAR(16) DEFAULT NULL,
        source_status VARCHAR(32) NOT NULL DEFAULT '已接入',
        source VARCHAR(64) DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_timing_indicator (trade_date, index_code, indicator_id),
        KEY idx_market_timing_indicator_date (trade_date),
        KEY idx_market_timing_indicator_dimension (dimension)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_timing_signal_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        index_code VARCHAR(16) NOT NULL DEFAULT '000300.SH',
        model_id VARCHAR(64) NOT NULL,
        model_name VARCHAR(128) NOT NULL,
        version VARCHAR(32) NOT NULL DEFAULT 'v1.5',
        combined_signal TINYINT DEFAULT 0,
        timing_score DECIMAL(8,4) DEFAULT NULL,
        state VARCHAR(32) NOT NULL DEFAULT 'cautious',
        state_label VARCHAR(32) NOT NULL DEFAULT '谨慎试探',
        position_upper DECIMAL(8,4) DEFAULT NULL,
        confidence DECIMAL(8,4) DEFAULT NULL,
        reasons_json JSON DEFAULT NULL,
        risk_notes_json JSON DEFAULT NULL,
        coverage_json JSON DEFAULT NULL,
        source VARCHAR(128) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_timing_signal (trade_date, index_code, model_id),
        KEY idx_market_timing_signal_date (trade_date),
        KEY idx_market_timing_signal_state (state)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_market_timing_schema() -> dict:
    init_mysql_schema()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for sql in DDL:
                cursor.execute(sql)
    return {
        "status": "ok",
        "tables": [
            "market_index_daily",
            "market_index_valuation_daily",
            "market_margin_daily",
            "market_bond_yield_daily",
            "market_option_pcr_daily",
            "market_futures_holding_daily",
            "market_option_qvix_daily",
            "market_option_iv_skew_daily",
            "market_timing_indicator_daily",
            "market_timing_signal_daily",
        ],
    }


if __name__ == "__main__":
    print(ensure_market_timing_schema())
