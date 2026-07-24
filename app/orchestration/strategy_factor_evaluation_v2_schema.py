from __future__ import annotations

from app.shared.db import mysql_conn


DDL = [
    """
    CREATE TABLE IF NOT EXISTS strategy_factor_snapshot_manifest (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        protocol_id VARCHAR(64) NOT NULL,
        spec_hash CHAR(64) NOT NULL,
        snapshot_id VARCHAR(96) NOT NULL,
        source_snapshot_id VARCHAR(96) DEFAULT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        strategy_version VARCHAR(32) NOT NULL,
        strategy_config_hash CHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        decision_as_of DATETIME(6) NOT NULL,
        earliest_execution_at DATETIME(6) DEFAULT NULL,
        expected_entity_count INT NOT NULL DEFAULT 0,
        pre_filter_count INT NOT NULL DEFAULT 0,
        eligible_count INT NOT NULL DEFAULT 0,
        selected_count INT NOT NULL DEFAULT 0,
        trace_mode VARCHAR(32) NOT NULL DEFAULT 'full_forward_trace',
        maturity_state VARCHAR(32) NOT NULL DEFAULT 'data_only',
        source_lineage_json JSON DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        payload_hash CHAR(64) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_factor_snapshot_manifest (snapshot_id),
        KEY idx_factor_manifest_strategy_date (
            strategy_id, strategy_version, trade_date
        ),
        KEY idx_factor_manifest_maturity (
            maturity_state, trade_date
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_factor_snapshot (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        manifest_id BIGINT NOT NULL,
        snapshot_id VARCHAR(96) NOT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        strategy_version VARCHAR(32) NOT NULL,
        trade_date DATE NOT NULL,
        decision_as_of DATETIME(6) NOT NULL,
        code VARCHAR(16) NOT NULL,
        name VARCHAR(64) DEFAULT NULL,
        industry VARCHAR(128) DEFAULT NULL,
        theme_name VARCHAR(128) DEFAULT NULL,
        candidate_lane VARCHAR(32) DEFAULT NULL,
        in_pre_filter TINYINT(1) NOT NULL DEFAULT 1,
        in_eligible_pool TINYINT(1) NOT NULL DEFAULT 0,
        is_selected TINYINT(1) NOT NULL DEFAULT 0,
        hard_gate_pass TINYINT(1) DEFAULT NULL,
        signal_grade VARCHAR(32) DEFAULT NULL,
        score DECIMAL(14,6) DEFAULT NULL,
        factor_json JSON DEFAULT NULL,
        contribution_json JSON DEFAULT NULL,
        gate_json JSON DEFAULT NULL,
        rejection_reasons_json JSON DEFAULT NULL,
        market_context_json JSON DEFAULT NULL,
        data_lineage_json JSON DEFAULT NULL,
        payload_hash CHAR(64) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_factor_snapshot (snapshot_id, code),
        KEY idx_factor_snapshot_strategy_date (
            strategy_id, strategy_version, trade_date
        ),
        KEY idx_factor_snapshot_scope (
            strategy_id, strategy_version, in_eligible_pool, is_selected, trade_date
        ),
        KEY idx_factor_snapshot_code_date (code, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_factor_outcome (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        factor_snapshot_id BIGINT NOT NULL,
        snapshot_id VARCHAR(96) NOT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        strategy_version VARCHAR(32) NOT NULL,
        code VARCHAR(16) NOT NULL,
        signal_trade_date DATE NOT NULL,
        horizon_days INT NOT NULL,
        entry_trade_date DATE DEFAULT NULL,
        exit_trade_date DATE DEFAULT NULL,
        entry_price DECIMAL(16,4) DEFAULT NULL,
        exit_price DECIMAL(16,4) DEFAULT NULL,
        gross_return_pct DECIMAL(18,8) DEFAULT NULL,
        cost_adjusted_return_pct DECIMAL(18,8) DEFAULT NULL,
        benchmark_code VARCHAR(16) NOT NULL DEFAULT '000300.SH',
        benchmark_return_pct DECIMAL(18,8) DEFAULT NULL,
        benchmark_excess_return_pct DECIMAL(18,8) DEFAULT NULL,
        industry_excess_return_pct DECIMAL(18,8) DEFAULT NULL,
        mfe_pct DECIMAL(18,8) DEFAULT NULL,
        mae_pct DECIMAL(18,8) DEFAULT NULL,
        execution_status VARCHAR(32) NOT NULL DEFAULT 'pending',
        block_reason VARCHAR(160) DEFAULT NULL,
        label_hash CHAR(64) DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        computed_at DATETIME(6) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_factor_outcome (
            factor_snapshot_id, horizon_days
        ),
        KEY idx_factor_outcome_strategy_horizon (
            strategy_id, strategy_version, horizon_days, exit_trade_date
        ),
        KEY idx_factor_outcome_snapshot (snapshot_id, horizon_days)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_factor_evaluation (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        protocol_id VARCHAR(64) NOT NULL,
        spec_hash CHAR(64) NOT NULL,
        evaluation_id VARCHAR(128) NOT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        strategy_version VARCHAR(32) NOT NULL,
        scope_name VARCHAR(32) NOT NULL,
        factor_key VARCHAR(64) NOT NULL,
        horizon_days INT NOT NULL,
        sample_start_date DATE DEFAULT NULL,
        sample_end_date DATE DEFAULT NULL,
        observation_days INT NOT NULL DEFAULT 0,
        sample_size INT NOT NULL DEFAULT 0,
        valid_sample_size INT NOT NULL DEFAULT 0,
        coverage DECIMAL(10,6) DEFAULT NULL,
        missing_rate DECIMAL(10,6) DEFAULT NULL,
        factor_mean DECIMAL(18,8) DEFAULT NULL,
        factor_std DECIMAL(18,8) DEFAULT NULL,
        pearson_ic_mean DECIMAL(18,8) DEFAULT NULL,
        rank_ic_mean DECIMAL(18,8) DEFAULT NULL,
        rank_ic_std DECIMAL(18,8) DEFAULT NULL,
        rank_ic_ir DECIMAL(18,8) DEFAULT NULL,
        positive_ic_ratio DECIMAL(10,6) DEFAULT NULL,
        newey_west_t DECIMAL(18,8) DEFAULT NULL,
        p_value DECIMAL(18,8) DEFAULT NULL,
        bootstrap_ci_low DECIMAL(18,8) DEFAULT NULL,
        bootstrap_ci_high DECIMAL(18,8) DEFAULT NULL,
        group_count INT DEFAULT NULL,
        monotonicity_score DECIMAL(18,8) DEFAULT NULL,
        top_bottom_return_pct DECIMAL(18,8) DEFAULT NULL,
        fdr_q_value DECIMAL(18,8) DEFAULT NULL,
        maturity_state VARCHAR(32) NOT NULL DEFAULT 'data_only',
        validation_status VARCHAR(32) NOT NULL DEFAULT 'insufficient_evidence',
        details_json JSON DEFAULT NULL,
        computed_at DATETIME(6) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_factor_evaluation (evaluation_id),
        KEY idx_factor_evaluation_latest (
            strategy_id, strategy_version, scope_name, horizon_days, computed_at
        ),
        KEY idx_factor_evaluation_factor (
            strategy_id, factor_key, horizon_days
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_factor_group_result (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        evaluation_id VARCHAR(128) NOT NULL,
        factor_key VARCHAR(64) NOT NULL,
        horizon_days INT NOT NULL,
        group_no INT NOT NULL,
        group_label VARCHAR(32) NOT NULL,
        sample_size INT NOT NULL DEFAULT 0,
        factor_min DECIMAL(18,8) DEFAULT NULL,
        factor_max DECIMAL(18,8) DEFAULT NULL,
        average_return_pct DECIMAL(18,8) DEFAULT NULL,
        median_return_pct DECIMAL(18,8) DEFAULT NULL,
        win_rate DECIMAL(10,6) DEFAULT NULL,
        cost_adjusted_return_pct DECIMAL(18,8) DEFAULT NULL,
        details_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_factor_group (
            evaluation_id, group_no
        ),
        KEY idx_factor_group_factor (factor_key, horizon_days)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_factor_ablation_result (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        evaluation_id VARCHAR(128) NOT NULL,
        factor_key VARCHAR(64) NOT NULL,
        horizon_days INT NOT NULL,
        sample_size INT NOT NULL DEFAULT 0,
        baseline_rank_ic DECIMAL(18,8) DEFAULT NULL,
        ablated_rank_ic DECIMAL(18,8) DEFAULT NULL,
        rank_ic_delta DECIMAL(18,8) DEFAULT NULL,
        baseline_top_bottom_return_pct DECIMAL(18,8) DEFAULT NULL,
        ablated_top_bottom_return_pct DECIMAL(18,8) DEFAULT NULL,
        top_bottom_delta_pct DECIMAL(18,8) DEFAULT NULL,
        redundancy_max_abs_corr DECIMAL(18,8) DEFAULT NULL,
        conclusion VARCHAR(32) NOT NULL DEFAULT 'insufficient_evidence',
        details_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_factor_ablation (
            evaluation_id, factor_key
        ),
        KEY idx_factor_ablation_factor (factor_key, horizon_days)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_strategy_factor_evaluation_v2_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for sql in DDL:
                cursor.execute(sql)
    return {
        "status": "ok",
        "tables": [
            "strategy_factor_snapshot_manifest",
            "strategy_factor_snapshot",
            "strategy_factor_outcome",
            "strategy_factor_evaluation",
            "strategy_factor_group_result",
            "strategy_factor_ablation_result",
        ],
    }
