from __future__ import annotations

from app.shared.db import mysql_conn


INDEX_CONSTITUENT_PIT_DDL = (
    """
    CREATE TABLE IF NOT EXISTS index_constituent_pit (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        index_code VARCHAR(16) NOT NULL,
        code VARCHAR(16) NOT NULL,
        constituent_ts_code VARCHAR(16) NOT NULL,
        effective_date DATE NOT NULL,
        weight DECIMAL(18,8) DEFAULT NULL,
        source VARCHAR(48) NOT NULL DEFAULT 'tushare_index_weight',
        source_sync_id VARCHAR(64) DEFAULT NULL,
        source_updated_at DATETIME NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_index_constituent_pit (
            index_code, effective_date, code
        ),
        KEY idx_index_constituent_pit_asof (
            index_code, effective_date, code
        ),
        KEY idx_index_constituent_pit_code (
            code, effective_date, index_code
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS index_constituent_pit_manifest (
        index_code VARCHAR(16) NOT NULL,
        period_month DATE NOT NULL,
        status VARCHAR(24) NOT NULL,
        source VARCHAR(48) NOT NULL,
        snapshot_date DATE DEFAULT NULL,
        snapshot_count INT NOT NULL DEFAULT 0,
        source_rows INT NOT NULL DEFAULT 0,
        matched_rows INT NOT NULL DEFAULT 0,
        distinct_codes INT NOT NULL DEFAULT 0,
        expected_members INT NOT NULL DEFAULT 0,
        weight_sum DECIMAL(18,8) DEFAULT NULL,
        sync_run_id VARCHAR(64) DEFAULT NULL,
        started_at DATETIME DEFAULT NULL,
        finished_at DATETIME DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (index_code, period_month),
        KEY idx_index_constituent_manifest_status (status, period_month),
        KEY idx_index_constituent_manifest_run (sync_run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
)


def ensure_index_constituent_pit_schema() -> dict:
    tables = ("index_constituent_pit", "index_constituent_pit_manifest")
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in INDEX_CONSTITUENT_PIT_DDL:
                cursor.execute(ddl)
    return {"status": "ok", "tables": list(tables)}
