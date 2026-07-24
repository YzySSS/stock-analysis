from __future__ import annotations

from app.shared.db import mysql_conn


DDL = [
    """
    CREATE TABLE IF NOT EXISTS market_scenario_forecast_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        forecast_id VARCHAR(128) NOT NULL,
        model_id VARCHAR(64) NOT NULL,
        version VARCHAR(32) NOT NULL,
        spec_hash CHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        as_of_datetime DATETIME(6) NOT NULL,
        data_cutoff_datetime DATETIME(6) NOT NULL,
        earliest_execution_at DATETIME(6) DEFAULT NULL,
        index_code VARCHAR(16) NOT NULL DEFAULT '000300.SH',
        horizon_days INT NOT NULL,
        probabilities_json JSON DEFAULT NULL,
        return_quantiles_json JSON DEFAULT NULL,
        drawdown_probabilities_json JSON DEFAULT NULL,
        state_transition_json JSON DEFAULT NULL,
        confidence DECIMAL(10,6) DEFAULT NULL,
        similar_history_count INT NOT NULL DEFAULT 0,
        evidence_status VARCHAR(48) NOT NULL DEFAULT 'insufficient_evidence',
        validation_status VARCHAR(48) NOT NULL DEFAULT 'insufficient_evidence',
        validation_json JSON DEFAULT NULL,
        bullish_triggers_json JSON DEFAULT NULL,
        bearish_triggers_json JSON DEFAULT NULL,
        action_plan_json JSON DEFAULT NULL,
        feature_json JSON DEFAULT NULL,
        feature_hash CHAR(64) NOT NULL,
        source_lineage_json JSON DEFAULT NULL,
        payload_hash CHAR(64) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_scenario_forecast (forecast_id),
        KEY idx_market_scenario_latest (
            model_id, index_code, horizon_days, trade_date
        ),
        KEY idx_market_scenario_validation (
            validation_status, trade_date
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_scenario_outcome (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        forecast_id VARCHAR(128) NOT NULL,
        trade_date DATE NOT NULL,
        horizon_days INT NOT NULL,
        entry_trade_date DATE DEFAULT NULL,
        exit_trade_date DATE DEFAULT NULL,
        expected_sigma_pct DECIMAL(18,8) DEFAULT NULL,
        realized_return_pct DECIMAL(18,8) DEFAULT NULL,
        realized_scenario VARCHAR(16) DEFAULT NULL,
        realized_max_drawdown_pct DECIMAL(18,8) DEFAULT NULL,
        outcome_status VARCHAR(32) NOT NULL DEFAULT 'pending',
        outcome_hash CHAR(64) DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        computed_at DATETIME(6) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_scenario_outcome (forecast_id),
        KEY idx_market_scenario_outcome_maturity (
            outcome_status, exit_trade_date
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_leadership_state_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        model_id VARCHAR(64) NOT NULL,
        version VARCHAR(32) NOT NULL,
        spec_hash CHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        as_of_datetime DATETIME(6) NOT NULL,
        data_cutoff_datetime DATETIME(6) NOT NULL,
        sector_type VARCHAR(32) NOT NULL,
        sector_name VARCHAR(128) NOT NULL,
        leadership_state VARCHAR(16) NOT NULL,
        state_label VARCHAR(16) NOT NULL,
        leadership_score DECIMAL(10,4) DEFAULT NULL,
        confidence DECIMAL(10,6) DEFAULT NULL,
        heat_score DECIMAL(10,4) DEFAULT NULL,
        capital_score DECIMAL(10,4) DEFAULT NULL,
        breadth_score DECIMAL(10,4) DEFAULT NULL,
        persistence_score DECIMAL(10,4) DEFAULT NULL,
        crowding_score DECIMAL(10,4) DEFAULT NULL,
        evidence_json JSON DEFAULT NULL,
        contradiction_json JSON DEFAULT NULL,
        upgrade_triggers_json JSON DEFAULT NULL,
        downgrade_triggers_json JSON DEFAULT NULL,
        source_lineage_json JSON DEFAULT NULL,
        payload_hash CHAR(64) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_leadership_state (
            model_id, trade_date, sector_type, sector_name
        ),
        KEY idx_market_leadership_latest (
            model_id, trade_date, leadership_state, leadership_score
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_market_scenario_forecast_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for sql in DDL:
                cursor.execute(sql)
    return {
        "status": "ok",
        "tables": [
            "market_scenario_forecast_daily",
            "market_scenario_outcome",
            "market_leadership_state_daily",
        ],
    }
