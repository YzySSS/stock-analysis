from __future__ import annotations

from app.shared.db import mysql_conn


FORWARD_OBSERVATION_DDL = (
    """
    CREATE TABLE IF NOT EXISTS strategy_forward_protocol (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        protocol_id VARCHAR(96) NOT NULL,
        campaign_id VARCHAR(96) DEFAULT NULL,
        strategy_id VARCHAR(64) NOT NULL,
        strategy_version VARCHAR(32) NOT NULL,
        protocol_version VARCHAR(48) NOT NULL,
        status VARCHAR(24) NOT NULL DEFAULT 'active',
        observation_source VARCHAR(32) NOT NULL DEFAULT 'scheduled_forward',
        execution_time TIME NOT NULL,
        timezone VARCHAR(48) NOT NULL DEFAULT 'Asia/Shanghai',
        entry_rule VARCHAR(64) NOT NULL,
        horizons_json JSON NOT NULL,
        benchmark_codes_json JSON NOT NULL,
        minimum_observation_days INT NOT NULL,
        minimum_candidate_count INT NOT NULL,
        immutable_tag VARCHAR(96) NOT NULL,
        implementation_commit CHAR(40) NOT NULL,
        strategy_config_hash CHAR(64) NOT NULL,
        ai_policy VARCHAR(64) NOT NULL,
        strategy_snapshot_json JSON NOT NULL,
        request_json JSON NOT NULL,
        started_on DATE NOT NULL,
        locked_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_forward_protocol (protocol_id),
        KEY idx_strategy_forward_protocol_strategy (strategy_id, status),
        KEY idx_strategy_forward_protocol_started (started_on),
        KEY idx_strategy_forward_protocol_campaign (campaign_id, strategy_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_forward_observation (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        observation_id VARCHAR(96) NOT NULL,
        protocol_id VARCHAR(96) NOT NULL,
        campaign_id VARCHAR(96) DEFAULT NULL,
        signal_trade_date DATE NOT NULL,
        selection_run_id VARCHAR(64) DEFAULT NULL,
        source_snapshot_id VARCHAR(96) DEFAULT NULL,
        paired_input_hash CHAR(64) DEFAULT NULL,
        status VARCHAR(32) NOT NULL DEFAULT 'pending_submission',
        observation_source VARCHAR(32) NOT NULL DEFAULT 'scheduled_forward',
        result_count INT NOT NULL DEFAULT 0,
        data_as_of_at DATETIME DEFAULT NULL,
        ai_mode VARCHAR(48) DEFAULT NULL,
        ai_status_json JSON DEFAULT NULL,
        request_json JSON NOT NULL,
        result_json JSON DEFAULT NULL,
        error_code VARCHAR(64) DEFAULT NULL,
        error_message VARCHAR(1000) DEFAULT NULL,
        submitted_at DATETIME DEFAULT NULL,
        completed_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_forward_observation (observation_id),
        UNIQUE KEY uniq_strategy_forward_protocol_date (protocol_id, signal_trade_date),
        UNIQUE KEY uniq_strategy_forward_selection_run (selection_run_id),
        KEY idx_strategy_forward_observation_status (status, signal_trade_date),
        KEY idx_strategy_forward_observation_protocol (protocol_id, signal_trade_date),
        KEY idx_strategy_forward_observation_campaign (campaign_id, signal_trade_date, status),
        KEY idx_strategy_forward_observation_snapshot (source_snapshot_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_forward_pick (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        observation_id VARCHAR(96) NOT NULL,
        protocol_id VARCHAR(96) NOT NULL,
        campaign_id VARCHAR(96) DEFAULT NULL,
        observation_source VARCHAR(32) NOT NULL DEFAULT 'scheduled_forward',
        source_snapshot_id VARCHAR(96) DEFAULT NULL,
        signal_trade_date DATE NOT NULL,
        code VARCHAR(16) NOT NULL,
        name VARCHAR(64) DEFAULT NULL,
        rank_no INT DEFAULT NULL,
        score DECIMAL(12,4) DEFAULT NULL,
        signal_price DECIMAL(16,4) DEFAULT NULL,
        theme_name VARCHAR(128) DEFAULT NULL,
        trade_grade_state VARCHAR(32) DEFAULT NULL,
        selection_phase VARCHAR(96) DEFAULT NULL,
        opinion_as_of_at DATETIME DEFAULT NULL,
        raw_json JSON NOT NULL,
        entry_trade_date DATE DEFAULT NULL,
        entry_price DECIMAL(16,4) DEFAULT NULL,
        price_adjustment_mode VARCHAR(32) DEFAULT NULL,
        return_1d_pct DECIMAL(12,4) DEFAULT NULL,
        return_3d_pct DECIMAL(12,4) DEFAULT NULL,
        return_5d_pct DECIMAL(12,4) DEFAULT NULL,
        return_20d_pct DECIMAL(12,4) DEFAULT NULL,
        max_favorable_5d_pct DECIMAL(12,4) DEFAULT NULL,
        max_adverse_5d_pct DECIMAL(12,4) DEFAULT NULL,
        max_favorable_20d_pct DECIMAL(12,4) DEFAULT NULL,
        max_adverse_20d_pct DECIMAL(12,4) DEFAULT NULL,
        outcome_status VARCHAR(24) NOT NULL DEFAULT 'pending',
        outcome_json JSON DEFAULT NULL,
        last_outcome_trade_date DATE DEFAULT NULL,
        outcome_updated_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_strategy_forward_pick (observation_id, code),
        KEY idx_strategy_forward_pick_protocol (protocol_id, signal_trade_date),
        KEY idx_strategy_forward_pick_code (code, signal_trade_date),
        KEY idx_strategy_forward_pick_outcome (outcome_status, signal_trade_date),
        KEY idx_strategy_forward_pick_campaign (campaign_id, signal_trade_date, rank_no)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_forward_action (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        observation_id VARCHAR(96) NOT NULL,
        protocol_id VARCHAR(96) NOT NULL,
        code VARCHAR(16) NOT NULL,
        action_type VARCHAR(32) NOT NULL,
        action_at DATETIME NOT NULL,
        action_price DECIMAL(16,4) DEFAULT NULL,
        note VARCHAR(500) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'user',
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        KEY idx_strategy_forward_action_pick (observation_id, code, action_at),
        KEY idx_strategy_forward_action_protocol (protocol_id, action_at),
        KEY idx_strategy_forward_action_type (action_type, action_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
)


def ensure_forward_observation_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for statement in FORWARD_OBSERVATION_DDL:
                cursor.execute(statement)
    return {
        "status": "ok",
        "tables": [
            "strategy_forward_protocol",
            "strategy_forward_observation",
            "strategy_forward_pick",
            "strategy_forward_action",
        ],
    }
