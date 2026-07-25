from __future__ import annotations

from app.shared.db import mysql_conn


DDL = [
    """
    CREATE TABLE IF NOT EXISTS etf_rotation_trade_calendar (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        exchange_code VARCHAR(16) NOT NULL DEFAULT 'SSE',
        cal_date DATE NOT NULL,
        is_open TINYINT(1) NOT NULL,
        pretrade_date DATE DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'tushare.trade_cal',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_etf_rotation_trade_calendar (
            exchange_code, cal_date
        ),
        KEY idx_etf_rotation_trade_calendar_open (
            exchange_code, is_open, cal_date
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS etf_rotation_sector_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        industry_code VARCHAR(32) NOT NULL,
        industry_name VARCHAR(128) NOT NULL,
        close DECIMAL(20,6) DEFAULT NULL,
        pct_change DECIMAL(14,6) DEFAULT NULL,
        company_num INT DEFAULT NULL,
        lead_stock VARCHAR(64) DEFAULT NULL,
        lead_stock_pct_change DECIMAL(14,6) DEFAULT NULL,
        lead_stock_close DECIMAL(20,6) DEFAULT NULL,
        net_buy_amount DECIMAL(24,6) DEFAULT NULL,
        net_sell_amount DECIMAL(24,6) DEFAULT NULL,
        net_amount DECIMAL(24,6) DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'tushare.moneyflow_ind_ths',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_etf_rotation_sector_daily (
            trade_date, industry_code
        ),
        KEY idx_etf_rotation_sector_name_date (
            industry_name, trade_date
        ),
        KEY idx_etf_rotation_sector_date_flow (
            trade_date, net_amount
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS etf_rotation_fund_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        ts_code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        fund_name VARCHAR(128) NOT NULL,
        list_date DATE NOT NULL,
        benchmark VARCHAR(512) DEFAULT NULL,
        open DECIMAL(20,6) DEFAULT NULL,
        high DECIMAL(20,6) DEFAULT NULL,
        low DECIMAL(20,6) DEFAULT NULL,
        close DECIMAL(20,6) DEFAULT NULL,
        pre_close DECIMAL(20,6) DEFAULT NULL,
        change_amount DECIMAL(20,6) DEFAULT NULL,
        pct_chg DECIMAL(14,6) DEFAULT NULL,
        volume_hand DECIMAL(24,4) DEFAULT NULL,
        amount_yuan DECIMAL(24,2) DEFAULT NULL,
        fund_share_10k DECIMAL(24,4) DEFAULT NULL,
        nav_date DATE DEFAULT NULL,
        unit_nav DECIMAL(20,8) DEFAULT NULL,
        accum_nav DECIMAL(20,8) DEFAULT NULL,
        net_asset DECIMAL(24,4) DEFAULT NULL,
        total_netasset DECIMAL(24,4) DEFAULT NULL,
        premium_discount_pct DECIMAL(14,6) DEFAULT NULL,
        source VARCHAR(128) NOT NULL
            DEFAULT 'tushare.fund_daily+fund_share+fund_nav',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_etf_rotation_fund_daily (
            ts_code, trade_date
        ),
        KEY idx_etf_rotation_fund_date (
            trade_date, ts_code
        ),
        KEY idx_etf_rotation_fund_liquidity (
            ts_code, trade_date, amount_yuan
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS etf_rotation_signal_run (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(128) NOT NULL,
        model_id VARCHAR(64) NOT NULL,
        version VARCHAR(32) NOT NULL,
        spec_hash CHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        data_cutoff_datetime DATETIME(6) NOT NULL,
        decision_as_of DATETIME(6) NOT NULL,
        earliest_execution_at DATETIME(6) NOT NULL,
        timing_model_id VARCHAR(64) NOT NULL,
        timing_state VARCHAR(32) NOT NULL,
        timing_score DECIMAL(14,6) DEFAULT NULL,
        position_upper DECIMAL(10,6) DEFAULT NULL,
        candidate_count INT NOT NULL DEFAULT 0,
        eligible_count INT NOT NULL DEFAULT 0,
        selected_count INT NOT NULL DEFAULT 0,
        selection_cap INT NOT NULL DEFAULT 0,
        status VARCHAR(32) NOT NULL DEFAULT 'ready',
        source_lineage_json JSON DEFAULT NULL,
        diagnostics_json JSON DEFAULT NULL,
        payload_hash CHAR(64) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_etf_rotation_signal_run (run_id),
        UNIQUE KEY uniq_etf_rotation_signal_date (
            model_id, version, trade_date
        ),
        KEY idx_etf_rotation_signal_latest (
            model_id, trade_date, status
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS etf_rotation_signal_candidate (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        signal_run_id BIGINT NOT NULL,
        run_id VARCHAR(128) NOT NULL,
        trade_date DATE NOT NULL,
        rank_no INT NOT NULL,
        sector_id VARCHAR(64) NOT NULL,
        sector_name VARCHAR(128) NOT NULL,
        ts_code VARCHAR(16) NOT NULL,
        fund_name VARCHAR(128) NOT NULL,
        is_eligible TINYINT(1) NOT NULL DEFAULT 0,
        is_selected TINYINT(1) NOT NULL DEFAULT 0,
        sector_score DECIMAL(14,6) DEFAULT NULL,
        etf_score DECIMAL(14,6) DEFAULT NULL,
        combined_score DECIMAL(14,6) DEFAULT NULL,
        flow_strength_score DECIMAL(14,6) DEFAULT NULL,
        flow_persistence_score DECIMAL(14,6) DEFAULT NULL,
        sector_trend_score DECIMAL(14,6) DEFAULT NULL,
        opinion_score DECIMAL(14,6) DEFAULT NULL,
        liquidity_score DECIMAL(14,6) DEFAULT NULL,
        etf_trend_score DECIMAL(14,6) DEFAULT NULL,
        share_change_score DECIMAL(14,6) DEFAULT NULL,
        tracking_score DECIMAL(14,6) DEFAULT NULL,
        latest_close DECIMAL(20,6) DEFAULT NULL,
        average_amount_20d_yuan DECIMAL(24,2) DEFAULT NULL,
        share_change_20d_pct DECIMAL(14,6) DEFAULT NULL,
        premium_discount_pct DECIMAL(14,6) DEFAULT NULL,
        gate_json JSON DEFAULT NULL,
        evidence_json JSON DEFAULT NULL,
        payload_hash CHAR(64) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_etf_rotation_candidate (
            run_id, sector_id
        ),
        KEY idx_etf_rotation_candidate_rank (
            signal_run_id, rank_no
        ),
        KEY idx_etf_rotation_candidate_selected (
            is_selected, trade_date
        ),
        KEY idx_etf_rotation_candidate_code_date (
            ts_code, trade_date
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS etf_rotation_forward_outcome (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        signal_candidate_id BIGINT NOT NULL,
        run_id VARCHAR(128) NOT NULL,
        sector_id VARCHAR(64) NOT NULL,
        ts_code VARCHAR(16) NOT NULL,
        signal_trade_date DATE NOT NULL,
        horizon_days INT NOT NULL,
        entry_trade_date DATE DEFAULT NULL,
        exit_trade_date DATE DEFAULT NULL,
        entry_price DECIMAL(20,6) DEFAULT NULL,
        exit_price DECIMAL(20,6) DEFAULT NULL,
        gross_return_pct DECIMAL(18,8) DEFAULT NULL,
        maximum_favorable_excursion_pct DECIMAL(18,8) DEFAULT NULL,
        maximum_adverse_excursion_pct DECIMAL(18,8) DEFAULT NULL,
        outcome_status VARCHAR(32) NOT NULL DEFAULT 'pending',
        block_reason VARCHAR(255) DEFAULT NULL,
        outcome_hash CHAR(64) DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        computed_at DATETIME(6) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_etf_rotation_forward_outcome (
            signal_candidate_id, horizon_days
        ),
        KEY idx_etf_rotation_outcome_maturity (
            outcome_status, exit_trade_date
        ),
        KEY idx_etf_rotation_outcome_code (
            ts_code, signal_trade_date, horizon_days
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_etf_rotation_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for sql in DDL:
                cursor.execute(sql)
    return {
        "status": "ok",
        "tables": [
            "etf_rotation_trade_calendar",
            "etf_rotation_sector_daily",
            "etf_rotation_fund_daily",
            "etf_rotation_signal_run",
            "etf_rotation_signal_candidate",
            "etf_rotation_forward_outcome",
        ],
    }
