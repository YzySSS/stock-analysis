from __future__ import annotations

from app.shared.db import mysql_conn


DURABLE_TASK_DDL = """
CREATE TABLE IF NOT EXISTS durable_task (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    job_type VARCHAR(64) NOT NULL,
    related_entity_id VARCHAR(96) DEFAULT NULL,
    idempotency_key CHAR(64) NOT NULL,
    active_idempotency_key CHAR(64) DEFAULT NULL,
    payload_json JSON NOT NULL,
    result_json JSON DEFAULT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'queued',
    worker_id VARCHAR(128) DEFAULT NULL,
    locked_at DATETIME DEFAULT NULL,
    worker_heartbeat_at DATETIME DEFAULT NULL,
    cancel_requested TINYINT(1) NOT NULL DEFAULT 0,
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    phase VARCHAR(64) DEFAULT '等待执行',
    progress_pct DECIMAL(6,2) NOT NULL DEFAULT 0,
    estimated_seconds_left INT DEFAULT NULL,
    error_code VARCHAR(64) DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_durable_task_id (task_id),
    UNIQUE KEY uniq_durable_task_active_idempotency (active_idempotency_key),
    KEY idx_durable_task_claim (status, cancel_requested, id),
    KEY idx_durable_task_stale (status, worker_heartbeat_at),
    KEY idx_durable_task_type_created (job_type, created_at),
    KEY idx_durable_task_related (related_entity_id, job_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_durable_task_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(DURABLE_TASK_DDL)
    return {"status": "ok", "tables": ["durable_task"]}
