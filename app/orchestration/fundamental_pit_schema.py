from __future__ import annotations

from app.shared.db import mysql_conn


FUNDAMENTAL_PIT_DDL = (
    """
    CREATE TABLE IF NOT EXISTS stock_fundamental_pit (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        ts_code VARCHAR(16) NOT NULL,
        announcement_date DATE NOT NULL,
        period_end_date DATE NOT NULL,
        update_flag VARCHAR(4) NOT NULL DEFAULT '',
        roe DECIMAL(20,6) DEFAULT NULL,
        roa DECIMAL(20,6) DEFAULT NULL,
        grossprofit_margin DECIMAL(20,6) DEFAULT NULL,
        netprofit_margin DECIMAL(20,6) DEFAULT NULL,
        revenue_yoy DECIMAL(20,6) DEFAULT NULL,
        profit_yoy DECIMAL(20,6) DEFAULT NULL,
        eps DECIMAL(20,6) DEFAULT NULL,
        source VARCHAR(48) NOT NULL DEFAULT 'tushare_fina_indicator_vip',
        source_sync_id VARCHAR(64) DEFAULT NULL,
        source_updated_at DATETIME NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_fundamental_pit_version (
            code, period_end_date, announcement_date, update_flag
        ),
        KEY idx_fundamental_pit_announcement (announcement_date, code),
        KEY idx_fundamental_pit_period (period_end_date, code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS fundamental_pit_manifest (
        period_end_date DATE NOT NULL PRIMARY KEY,
        status VARCHAR(24) NOT NULL,
        source VARCHAR(48) NOT NULL,
        source_rows INT NOT NULL DEFAULT 0,
        matched_rows INT NOT NULL DEFAULT 0,
        distinct_codes INT NOT NULL DEFAULT 0,
        sync_run_id VARCHAR(64) DEFAULT NULL,
        started_at DATETIME DEFAULT NULL,
        finished_at DATETIME DEFAULT NULL,
        metadata_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_fundamental_pit_manifest_status (status, period_end_date),
        KEY idx_fundamental_pit_manifest_run (sync_run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
)


def ensure_fundamental_pit_schema() -> dict:
    tables = ("stock_fundamental_pit", "fundamental_pit_manifest")
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in FUNDAMENTAL_PIT_DDL:
                cursor.execute(ddl)
    return {"status": "ok", "tables": list(tables)}
