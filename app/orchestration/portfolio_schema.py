from __future__ import annotations

from functools import lru_cache

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
    idempotency_key VARCHAR(64) DEFAULT NULL,
    active_idempotency_key VARCHAR(64) DEFAULT NULL,
    worker_id VARCHAR(128) DEFAULT NULL,
    locked_at DATETIME DEFAULT NULL,
    worker_heartbeat_at DATETIME DEFAULT NULL,
    cancel_requested TINYINT(1) NOT NULL DEFAULT 0,
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 2,
    phase VARCHAR(64) DEFAULT NULL,
    progress_pct DECIMAL(8,4) DEFAULT 0,
    estimated_seconds_left INT DEFAULT NULL,
    decision_level VARCHAR(32) DEFAULT NULL,
    model_name VARCHAR(128) DEFAULT NULL,
    prompt_version VARCHAR(64) DEFAULT NULL,
    input_snapshot_json JSON DEFAULT NULL,
    raw_response MEDIUMTEXT DEFAULT NULL,
    parsed_review_json JSON DEFAULT NULL,
    error_code VARCHAR(64) DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    expires_at DATETIME DEFAULT NULL,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_portfolio_advice_position_status (position_id, status, expires_at),
    KEY idx_portfolio_advice_code_created (code, created_at),
    KEY idx_portfolio_advice_status_created (status, created_at),
    UNIQUE KEY uniq_portfolio_advice_active_idempotency (active_idempotency_key),
    KEY idx_portfolio_advice_claim (status, cancel_requested, id),
    KEY idx_portfolio_advice_stale (status, worker_heartbeat_at),
    KEY idx_portfolio_advice_idempotency (idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


PORTFOLIO_ADVICE_COLUMN_MIGRATIONS = {
    "idempotency_key": "ALTER TABLE portfolio_advice_run ADD COLUMN idempotency_key VARCHAR(64) DEFAULT NULL AFTER status",
    "active_idempotency_key": "ALTER TABLE portfolio_advice_run ADD COLUMN active_idempotency_key VARCHAR(64) DEFAULT NULL AFTER idempotency_key",
    "worker_id": "ALTER TABLE portfolio_advice_run ADD COLUMN worker_id VARCHAR(128) DEFAULT NULL AFTER active_idempotency_key",
    "locked_at": "ALTER TABLE portfolio_advice_run ADD COLUMN locked_at DATETIME DEFAULT NULL AFTER worker_id",
    "worker_heartbeat_at": "ALTER TABLE portfolio_advice_run ADD COLUMN worker_heartbeat_at DATETIME DEFAULT NULL AFTER locked_at",
    "cancel_requested": "ALTER TABLE portfolio_advice_run ADD COLUMN cancel_requested TINYINT(1) NOT NULL DEFAULT 0 AFTER worker_heartbeat_at",
    "attempt_count": "ALTER TABLE portfolio_advice_run ADD COLUMN attempt_count INT NOT NULL DEFAULT 0 AFTER cancel_requested",
    "max_attempts": "ALTER TABLE portfolio_advice_run ADD COLUMN max_attempts INT NOT NULL DEFAULT 2 AFTER attempt_count",
    "phase": "ALTER TABLE portfolio_advice_run ADD COLUMN phase VARCHAR(64) DEFAULT NULL AFTER max_attempts",
    "progress_pct": "ALTER TABLE portfolio_advice_run ADD COLUMN progress_pct DECIMAL(8,4) DEFAULT 0 AFTER phase",
    "estimated_seconds_left": "ALTER TABLE portfolio_advice_run ADD COLUMN estimated_seconds_left INT DEFAULT NULL AFTER progress_pct",
    "error_code": "ALTER TABLE portfolio_advice_run ADD COLUMN error_code VARCHAR(64) DEFAULT NULL AFTER parsed_review_json",
}


PORTFOLIO_ADVICE_INDEX_MIGRATIONS = {
    "uniq_portfolio_advice_active_idempotency": "ALTER TABLE portfolio_advice_run ADD UNIQUE KEY uniq_portfolio_advice_active_idempotency (active_idempotency_key)",
    "idx_portfolio_advice_claim": "ALTER TABLE portfolio_advice_run ADD KEY idx_portfolio_advice_claim (status, cancel_requested, id)",
    "idx_portfolio_advice_stale": "ALTER TABLE portfolio_advice_run ADD KEY idx_portfolio_advice_stale (status, worker_heartbeat_at)",
    "idx_portfolio_advice_idempotency": "ALTER TABLE portfolio_advice_run ADD KEY idx_portfolio_advice_idempotency (idempotency_key)",
}

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


@lru_cache(maxsize=1)
def ensure_portfolio_schema() -> dict:
    applied = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_PORTFOLIO_POSITION_SQL)
            cursor.execute(CREATE_PORTFOLIO_ADVICE_RUN_SQL)
            cursor.execute(CREATE_PORTFOLIO_ADVICE_OUTCOME_SQL)
            cursor.execute("SHOW COLUMNS FROM portfolio_advice_run")
            existing_columns = {str(row[0]) for row in cursor.fetchall()}
            for column, sql in PORTFOLIO_ADVICE_COLUMN_MIGRATIONS.items():
                if column not in existing_columns:
                    cursor.execute(sql)
                    applied.append(column)

            cursor.execute("SHOW INDEX FROM portfolio_advice_run")
            existing_indexes = {str(row[2]) for row in cursor.fetchall()}
            for index, sql in PORTFOLIO_ADVICE_INDEX_MIGRATIONS.items():
                if index not in existing_indexes:
                    cursor.execute(sql)
                    applied.append(index)
    return {
        "portfolio_position": "ok",
        "portfolio_advice_run": "ok",
        "portfolio_advice_outcome": "ok",
        "applied": applied or ["portfolio_schema_already_current"],
    }
