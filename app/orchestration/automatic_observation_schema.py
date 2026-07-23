from __future__ import annotations

from app.shared.db import mysql_conn


CAMPAIGN_DDL = """
CREATE TABLE IF NOT EXISTS strategy_observation_campaign (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    campaign_id VARCHAR(96) NOT NULL,
    baseline_strategy_id VARCHAR(64) NOT NULL,
    baseline_strategy_version VARCHAR(32) NOT NULL,
    candidate_strategy_id VARCHAR(64) NOT NULL,
    candidate_strategy_version VARCHAR(32) NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'active',
    observation_source VARCHAR(32) NOT NULL DEFAULT 'automatic_observation',
    execution_time TIME NOT NULL,
    timezone VARCHAR(48) NOT NULL DEFAULT 'Asia/Shanghai',
    entry_rule VARCHAR(64) NOT NULL DEFAULT 'same_day_open',
    target_trade_days INT NOT NULL DEFAULT 5,
    completed_trade_days INT NOT NULL DEFAULT 0,
    started_on DATE NOT NULL,
    completed_on DATE DEFAULT NULL,
    last_signal_trade_date DATE DEFAULT NULL,
    protocol_ids_json JSON DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_strategy_observation_campaign (campaign_id),
    UNIQUE KEY uniq_strategy_observation_candidate_version (
        candidate_strategy_id, candidate_strategy_version
    ),
    KEY idx_strategy_observation_campaign_status (status, started_on),
    KEY idx_strategy_observation_campaign_schedule (execution_time, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


TABLE_COLUMN_DDL = {
    "strategy_forward_protocol": {
        "campaign_id": (
            "ALTER TABLE strategy_forward_protocol "
            "ADD COLUMN campaign_id VARCHAR(96) DEFAULT NULL AFTER protocol_id"
        ),
        "observation_source": (
            "ALTER TABLE strategy_forward_protocol "
            "ADD COLUMN observation_source VARCHAR(32) NOT NULL "
            "DEFAULT 'scheduled_forward' AFTER status"
        ),
    },
    "strategy_forward_observation": {
        "campaign_id": (
            "ALTER TABLE strategy_forward_observation "
            "ADD COLUMN campaign_id VARCHAR(96) DEFAULT NULL AFTER protocol_id"
        ),
        "observation_source": (
            "ALTER TABLE strategy_forward_observation "
            "ADD COLUMN observation_source VARCHAR(32) NOT NULL "
            "DEFAULT 'scheduled_forward' AFTER status"
        ),
        "source_snapshot_id": (
            "ALTER TABLE strategy_forward_observation "
            "ADD COLUMN source_snapshot_id VARCHAR(96) DEFAULT NULL AFTER selection_run_id"
        ),
        "paired_input_hash": (
            "ALTER TABLE strategy_forward_observation "
            "ADD COLUMN paired_input_hash CHAR(64) DEFAULT NULL AFTER source_snapshot_id"
        ),
    },
    "strategy_forward_pick": {
        "campaign_id": (
            "ALTER TABLE strategy_forward_pick "
            "ADD COLUMN campaign_id VARCHAR(96) DEFAULT NULL AFTER protocol_id"
        ),
        "observation_source": (
            "ALTER TABLE strategy_forward_pick "
            "ADD COLUMN observation_source VARCHAR(32) NOT NULL "
            "DEFAULT 'scheduled_forward' AFTER campaign_id"
        ),
        "source_snapshot_id": (
            "ALTER TABLE strategy_forward_pick "
            "ADD COLUMN source_snapshot_id VARCHAR(96) DEFAULT NULL AFTER observation_source"
        ),
    },
}


TABLE_INDEX_DDL = {
    "strategy_forward_protocol": {
        "idx_strategy_forward_protocol_campaign": (
            "ALTER TABLE strategy_forward_protocol "
            "ADD KEY idx_strategy_forward_protocol_campaign (campaign_id, strategy_id)"
        ),
    },
    "strategy_forward_observation": {
        "idx_strategy_forward_observation_campaign": (
            "ALTER TABLE strategy_forward_observation "
            "ADD KEY idx_strategy_forward_observation_campaign "
            "(campaign_id, signal_trade_date, status)"
        ),
        "idx_strategy_forward_observation_snapshot": (
            "ALTER TABLE strategy_forward_observation "
            "ADD KEY idx_strategy_forward_observation_snapshot (source_snapshot_id)"
        ),
    },
    "strategy_forward_pick": {
        "idx_strategy_forward_pick_campaign": (
            "ALTER TABLE strategy_forward_pick "
            "ADD KEY idx_strategy_forward_pick_campaign "
            "(campaign_id, signal_trade_date, rank_no)"
        ),
    },
}


def _columns(cursor, table_name: str) -> set[str]:
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    return {str(row[0]) for row in (cursor.fetchall() or [])}


def _indexes(cursor, table_name: str) -> set[str]:
    cursor.execute(f"SHOW INDEX FROM {table_name}")
    return {str(row[2]) for row in (cursor.fetchall() or [])}


def ensure_automatic_observation_schema() -> dict:
    applied_columns: list[str] = []
    applied_indexes: list[str] = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(CAMPAIGN_DDL)
            for table_name, definitions in TABLE_COLUMN_DDL.items():
                existing_columns = _columns(cursor, table_name)
                for column_name, statement in definitions.items():
                    if column_name in existing_columns:
                        continue
                    cursor.execute(statement)
                    applied_columns.append(f"{table_name}.{column_name}")
            for table_name, definitions in TABLE_INDEX_DDL.items():
                existing_indexes = _indexes(cursor, table_name)
                for index_name, statement in definitions.items():
                    if index_name in existing_indexes:
                        continue
                    cursor.execute(statement)
                    applied_indexes.append(f"{table_name}.{index_name}")

    return {
        "status": "ok",
        "tables": [
            "strategy_observation_campaign",
            "strategy_forward_protocol",
            "strategy_forward_observation",
            "strategy_forward_pick",
        ],
        "applied_columns": applied_columns,
        "applied_indexes": applied_indexes,
    }
