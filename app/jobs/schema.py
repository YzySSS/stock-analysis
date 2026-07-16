from __future__ import annotations

from functools import lru_cache

from app.shared.db import mysql_conn


WORKER_RUNTIME_DDL = """
CREATE TABLE IF NOT EXISTS worker_runtime_heartbeat (
    worker_type VARCHAR(64) NOT NULL,
    worker_id VARCHAR(128) NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'starting',
    current_job_id VARCHAR(128) DEFAULT NULL,
    started_at DATETIME NOT NULL,
    heartbeat_at DATETIME NOT NULL,
    last_job_started_at DATETIME DEFAULT NULL,
    last_job_finished_at DATETIME DEFAULT NULL,
    stopped_at DATETIME DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (worker_type, worker_id),
    KEY idx_worker_runtime_latest (worker_type, heartbeat_at),
    KEY idx_worker_runtime_status (status, heartbeat_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


TASK_RUN_DAILY_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS task_run_daily_summary (
    run_date DATE NOT NULL,
    task_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    run_count BIGINT NOT NULL DEFAULT 0,
    first_started_at DATETIME DEFAULT NULL,
    last_finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (run_date, task_name, status),
    KEY idx_task_daily_name_date (task_name, run_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


JOB_ERROR_DAILY_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS job_error_daily_summary (
    error_date DATE NOT NULL,
    source_kind VARCHAR(32) NOT NULL,
    job_type VARCHAR(128) NOT NULL,
    error_code VARCHAR(64) NOT NULL,
    error_fingerprint CHAR(64) NOT NULL,
    occurrence_count BIGINT NOT NULL DEFAULT 0,
    first_seen_at DATETIME NOT NULL,
    last_seen_at DATETIME NOT NULL,
    last_message VARCHAR(500) DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (error_date, source_kind, job_type, error_code, error_fingerprint),
    KEY idx_job_error_latest (last_seen_at),
    KEY idx_job_error_code (error_code, error_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


TASK_RUN_LOG_COLUMN_MIGRATIONS = {
    "error_code": "ALTER TABLE task_run_log ADD COLUMN error_code VARCHAR(64) DEFAULT NULL AFTER message",
    "error_fingerprint": "ALTER TABLE task_run_log ADD COLUMN error_fingerprint CHAR(64) DEFAULT NULL AFTER error_code",
}


TASK_RUN_LOG_INDEX_MIGRATIONS = {
    "idx_task_created_at": "ALTER TABLE task_run_log ADD KEY idx_task_created_at (created_at)",
    "idx_task_status_created": "ALTER TABLE task_run_log ADD KEY idx_task_status_created (status, created_at)",
}


BACKTEST_JOB_COLUMN_MIGRATIONS = {
    "idempotency_key": "ALTER TABLE backtest_run ADD COLUMN idempotency_key CHAR(64) DEFAULT NULL AFTER status",
    "active_idempotency_key": "ALTER TABLE backtest_run ADD COLUMN active_idempotency_key CHAR(64) DEFAULT NULL AFTER idempotency_key",
    "attempt_count": "ALTER TABLE backtest_run ADD COLUMN attempt_count INT NOT NULL DEFAULT 0 AFTER cancel_requested",
    "max_attempts": "ALTER TABLE backtest_run ADD COLUMN max_attempts INT NOT NULL DEFAULT 2 AFTER attempt_count",
    "phase": "ALTER TABLE backtest_run ADD COLUMN phase VARCHAR(64) DEFAULT NULL AFTER max_attempts",
    "error_code": "ALTER TABLE backtest_run ADD COLUMN error_code VARCHAR(64) DEFAULT NULL AFTER summary_json",
}


BACKTEST_JOB_INDEX_MIGRATIONS = {
    "uniq_backtest_active_idempotency": "ALTER TABLE backtest_run ADD UNIQUE KEY uniq_backtest_active_idempotency (active_idempotency_key)",
    "idx_backtest_claim": "ALTER TABLE backtest_run ADD KEY idx_backtest_claim (status, cancel_requested, id)",
    "idx_backtest_stale": "ALTER TABLE backtest_run ADD KEY idx_backtest_stale (status, worker_heartbeat_at)",
    "idx_backtest_idempotency": "ALTER TABLE backtest_run ADD KEY idx_backtest_idempotency (idempotency_key)",
}


def _existing_columns(cursor, table: str) -> set[str]:  # noqa: ANN001
    cursor.execute(f"SHOW COLUMNS FROM {table}")
    return {str(row[0]) for row in cursor.fetchall()}


def _existing_indexes(cursor, table: str) -> set[str]:  # noqa: ANN001
    cursor.execute(f"SHOW INDEX FROM {table}")
    return {str(row[2]) for row in cursor.fetchall()}


@lru_cache(maxsize=1)
def ensure_job_ops_schema() -> dict:
    """Create the shared worker/readiness and retention schema.

    Product job tables stay separate. This migration only adds the lifecycle
    columns needed to expose one operational contract across the three workers.
    """

    applied: list[str] = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for name, ddl in (
                ("worker_runtime_heartbeat", WORKER_RUNTIME_DDL),
                ("task_run_daily_summary", TASK_RUN_DAILY_SUMMARY_DDL),
                ("job_error_daily_summary", JOB_ERROR_DAILY_SUMMARY_DDL),
            ):
                cursor.execute(ddl)
                applied.append(name)

            task_columns = _existing_columns(cursor, "task_run_log")
            for column, sql in TASK_RUN_LOG_COLUMN_MIGRATIONS.items():
                if column not in task_columns:
                    cursor.execute(sql)
                    applied.append(f"task_run_log.{column}")

            task_indexes = _existing_indexes(cursor, "task_run_log")
            for index, sql in TASK_RUN_LOG_INDEX_MIGRATIONS.items():
                if index not in task_indexes:
                    cursor.execute(sql)
                    applied.append(f"task_run_log.{index}")

            backtest_columns = _existing_columns(cursor, "backtest_run")
            for column, sql in BACKTEST_JOB_COLUMN_MIGRATIONS.items():
                if column not in backtest_columns:
                    cursor.execute(sql)
                    applied.append(f"backtest_run.{column}")

            backtest_indexes = _existing_indexes(cursor, "backtest_run")
            for index, sql in BACKTEST_JOB_INDEX_MIGRATIONS.items():
                if index not in backtest_indexes:
                    cursor.execute(sql)
                    applied.append(f"backtest_run.{index}")

    return {"status": "ok", "applied": applied or ["job_ops_schema_already_current"]}
