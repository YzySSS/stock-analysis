from __future__ import annotations

from app.shared.db import mysql_conn


SELECTION_RUN_DDL = """
CREATE TABLE IF NOT EXISTS selection_run (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    strategy_id VARCHAR(64) DEFAULT NULL,
    instrument_type VARCHAR(16) NOT NULL DEFAULT 'stock',
    market_board VARCHAR(16) DEFAULT NULL,
    max_picks INT DEFAULT NULL,
    score_threshold DECIMAL(12,4) DEFAULT NULL,
    save_requested TINYINT(1) NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'queued',
    idempotency_key VARCHAR(64) DEFAULT NULL,
    active_idempotency_key VARCHAR(64) DEFAULT NULL,
    idempotency_date DATE DEFAULT NULL,
    worker_id VARCHAR(128) DEFAULT NULL,
    locked_at DATETIME DEFAULT NULL,
    worker_heartbeat_at DATETIME DEFAULT NULL,
    cancel_requested TINYINT(1) NOT NULL DEFAULT 0,
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 2,
    phase VARCHAR(64) DEFAULT NULL,
    progress_pct DECIMAL(8,4) DEFAULT 0,
    estimated_seconds_left INT DEFAULT NULL,
    result_count INT DEFAULT 0,
    request_json JSON DEFAULT NULL,
    result_json JSON DEFAULT NULL,
    error_code VARCHAR(64) DEFAULT NULL,
    error_message TEXT,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_selection_run_id (run_id),
    UNIQUE KEY uniq_selection_run_active_idempotency (active_idempotency_key),
    KEY idx_selection_run_status (status),
    KEY idx_selection_run_claim (status, cancel_requested, id),
    KEY idx_selection_run_stale (status, worker_heartbeat_at),
    KEY idx_selection_run_idempotency (idempotency_key, idempotency_date),
    KEY idx_selection_run_strategy (strategy_id),
    KEY idx_selection_run_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


SELECTION_RUN_COLUMN_MIGRATIONS = {
    "idempotency_key": "ALTER TABLE selection_run ADD COLUMN idempotency_key VARCHAR(64) DEFAULT NULL AFTER status",
    "active_idempotency_key": "ALTER TABLE selection_run ADD COLUMN active_idempotency_key VARCHAR(64) DEFAULT NULL AFTER idempotency_key",
    "idempotency_date": "ALTER TABLE selection_run ADD COLUMN idempotency_date DATE DEFAULT NULL AFTER active_idempotency_key",
    "worker_id": "ALTER TABLE selection_run ADD COLUMN worker_id VARCHAR(128) DEFAULT NULL AFTER idempotency_date",
    "locked_at": "ALTER TABLE selection_run ADD COLUMN locked_at DATETIME DEFAULT NULL AFTER worker_id",
    "worker_heartbeat_at": "ALTER TABLE selection_run ADD COLUMN worker_heartbeat_at DATETIME DEFAULT NULL AFTER locked_at",
    "cancel_requested": "ALTER TABLE selection_run ADD COLUMN cancel_requested TINYINT(1) NOT NULL DEFAULT 0 AFTER worker_heartbeat_at",
    "attempt_count": "ALTER TABLE selection_run ADD COLUMN attempt_count INT NOT NULL DEFAULT 0 AFTER cancel_requested",
    "max_attempts": "ALTER TABLE selection_run ADD COLUMN max_attempts INT NOT NULL DEFAULT 2 AFTER attempt_count",
    "error_code": "ALTER TABLE selection_run ADD COLUMN error_code VARCHAR(64) DEFAULT NULL AFTER result_json",
}


SELECTION_RUN_INDEX_MIGRATIONS = {
    "uniq_selection_run_active_idempotency": "ALTER TABLE selection_run ADD UNIQUE KEY uniq_selection_run_active_idempotency (active_idempotency_key)",
    "idx_selection_run_claim": "ALTER TABLE selection_run ADD KEY idx_selection_run_claim (status, cancel_requested, id)",
    "idx_selection_run_stale": "ALTER TABLE selection_run ADD KEY idx_selection_run_stale (status, worker_heartbeat_at)",
    "idx_selection_run_idempotency": "ALTER TABLE selection_run ADD KEY idx_selection_run_idempotency (idempotency_key, idempotency_date)",
}


def ensure_selection_run_schema() -> dict:
    applied = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(SELECTION_RUN_DDL)
            cursor.execute("SHOW COLUMNS FROM selection_run")
            existing_columns = {str(row[0]) for row in cursor.fetchall()}
            for column, sql in SELECTION_RUN_COLUMN_MIGRATIONS.items():
                if column not in existing_columns:
                    cursor.execute(sql)
                    applied.append(column)

            cursor.execute("SHOW INDEX FROM selection_run")
            existing_indexes = {str(row[2]) for row in cursor.fetchall()}
            for index, sql in SELECTION_RUN_INDEX_MIGRATIONS.items():
                if index not in existing_indexes:
                    cursor.execute(sql)
                    applied.append(index)
    return {"status": "ok", "applied": applied or ["selection_run_already_current"]}
