from __future__ import annotations

from app.shared.db import mysql_conn


CREATE_PORTFOLIO_POSITION_SQL = """
CREATE TABLE IF NOT EXISTS portfolio_position (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(16) NOT NULL,
    name VARCHAR(64) DEFAULT NULL,
    strategy_id VARCHAR(64) NOT NULL DEFAULT 'short_term',
    cost_price DECIMAL(12,4) NOT NULL,
    quantity INT NOT NULL,
    buy_datetime DATETIME NOT NULL,
    target_style VARCHAR(32) NOT NULL DEFAULT 'short_swing',
    max_loss_pct DECIMAL(8,4) DEFAULT NULL,
    note VARCHAR(500) DEFAULT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_portfolio_active (is_active, updated_at),
    KEY idx_portfolio_code (code),
    KEY idx_portfolio_strategy (strategy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_PORTFOLIO_ADVICE_RUN_SQL = """
CREATE TABLE IF NOT EXISTS portfolio_advice_run (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    position_id BIGINT NOT NULL,
    code VARCHAR(16) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'queued',
    decision_level VARCHAR(32) DEFAULT NULL,
    model_name VARCHAR(128) DEFAULT NULL,
    prompt_version VARCHAR(64) DEFAULT NULL,
    input_snapshot_json JSON DEFAULT NULL,
    raw_response MEDIUMTEXT DEFAULT NULL,
    parsed_review_json JSON DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    expires_at DATETIME DEFAULT NULL,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_portfolio_advice_position_status (position_id, status, expires_at),
    KEY idx_portfolio_advice_code_created (code, created_at),
    KEY idx_portfolio_advice_status_created (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_PORTFOLIO_ADVICE_OUTCOME_SQL = """
CREATE TABLE IF NOT EXISTS portfolio_advice_outcome (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    advice_run_id BIGINT NOT NULL,
    position_id BIGINT NOT NULL,
    code VARCHAR(16) NOT NULL,
    decision_level VARCHAR(32) DEFAULT NULL,
    base_price DECIMAL(12,4) DEFAULT NULL,
    base_trade_date DATE DEFAULT NULL,
    evaluate_at DATETIME NOT NULL,
    horizon_days INT NOT NULL,
    latest_price DECIMAL(12,4) DEFAULT NULL,
    return_pct DECIMAL(10,4) DEFAULT NULL,
    max_gain_pct DECIMAL(10,4) DEFAULT NULL,
    max_drawdown_pct DECIMAL(10,4) DEFAULT NULL,
    stop_loss_touched TINYINT(1) NOT NULL DEFAULT 0,
    take_profit_touched TINYINT(1) NOT NULL DEFAULT 0,
    support_broken TINYINT(1) NOT NULL DEFAULT 0,
    resistance_broken TINYINT(1) NOT NULL DEFAULT 0,
    outcome_label VARCHAR(32) NOT NULL DEFAULT 'neutral',
    quality_score DECIMAL(8,4) DEFAULT NULL,
    evidence_json JSON DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_portfolio_advice_outcome_horizon (advice_run_id, horizon_days),
    KEY idx_portfolio_outcome_position_created (position_id, created_at),
    KEY idx_portfolio_outcome_code_horizon (code, horizon_days, evaluate_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_portfolio_schema() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_PORTFOLIO_POSITION_SQL)
            cursor.execute(CREATE_PORTFOLIO_ADVICE_RUN_SQL)
            cursor.execute(CREATE_PORTFOLIO_ADVICE_OUTCOME_SQL)
    return {
        "portfolio_position": "ok",
        "portfolio_advice_run": "ok",
        "portfolio_advice_outcome": "ok",
    }


if __name__ == "__main__":
    print(ensure_portfolio_schema())
