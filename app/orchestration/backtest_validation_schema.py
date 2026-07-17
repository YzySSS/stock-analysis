from __future__ import annotations

from app.shared.db import mysql_conn


BACKTEST_VALIDATION_DDL = """
CREATE TABLE IF NOT EXISTS strategy_validation_protocol (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    protocol_id VARCHAR(80) NOT NULL,
    batch_id VARCHAR(80) DEFAULT NULL,
    strategy_id VARCHAR(64) NOT NULL,
    strategy_version VARCHAR(32) DEFAULT NULL,
    strategy_config_hash CHAR(64) NOT NULL,
    methodology_version VARCHAR(64) NOT NULL,
    protocol_version VARCHAR(48) NOT NULL,
    validation_mode VARCHAR(32) NOT NULL,
    eligible_for_validation TINYINT(1) NOT NULL DEFAULT 0,
    frozen_at DATETIME NOT NULL,
    freeze_data_cutoff_date DATE NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    universe_code VARCHAR(16) NOT NULL DEFAULT 'ALL_A',
    return_mode VARCHAR(16) NOT NULL DEFAULT '1d',
    benchmark_index_code VARCHAR(16) NOT NULL DEFAULT '000300.SH',
    max_picks INT NOT NULL,
    score_threshold DECIMAL(12,4) NOT NULL,
    use_adjusted_price TINYINT(1) NOT NULL DEFAULT 0,
    commission_bps DECIMAL(10,4) NOT NULL DEFAULT 0,
    stamp_tax_bps DECIMAL(10,4) NOT NULL DEFAULT 0,
    slippage_bps DECIMAL(10,4) NOT NULL DEFAULT 0,
    execution_constraints_enabled TINYINT(1) NOT NULL DEFAULT 0,
    minimum_trade_days INT NOT NULL DEFAULT 120,
    minimum_trades INT NOT NULL DEFAULT 120,
    strategy_snapshot_json JSON NOT NULL,
    request_json JSON NOT NULL,
    criteria_json JSON NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'frozen',
    verdict VARCHAR(48) NOT NULL DEFAULT 'pending',
    validation_status VARCHAR(32) NOT NULL DEFAULT 'validation_pending',
    run_id VARCHAR(64) DEFAULT NULL,
    report_json JSON DEFAULT NULL,
    error_message VARCHAR(1000) DEFAULT NULL,
    executed_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_strategy_validation_protocol (protocol_id),
    KEY idx_strategy_validation_batch (batch_id, strategy_id),
    KEY idx_strategy_validation_status (status, end_date),
    KEY idx_strategy_validation_strategy (strategy_id, frozen_at),
    KEY idx_strategy_validation_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_backtest_validation_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(BACKTEST_VALIDATION_DDL)
    return {"status": "ok", "tables": ["strategy_validation_protocol"]}
