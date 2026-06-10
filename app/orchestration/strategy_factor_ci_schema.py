from __future__ import annotations

from app.orchestration.init_project import init_mysql_schema
from app.shared.db import mysql_conn


STRATEGY_FACTOR_CI_DDL = """
CREATE TABLE IF NOT EXISTS strategy_factor_ci_daily (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    strategy_id VARCHAR(64) NOT NULL,
    instrument_type VARCHAR(16) NOT NULL DEFAULT 'stock',
    trade_date DATE NOT NULL,
    horizon_days INT NOT NULL DEFAULT 1,
    factor_key VARCHAR(64) NOT NULL,
    factor_name VARCHAR(128) DEFAULT NULL,
    sample_size INT NOT NULL DEFAULT 0,
    valid_sample_size INT NOT NULL DEFAULT 0,
    coverage DECIMAL(8,4) DEFAULT NULL,
    missing_rate DECIMAL(8,4) DEFAULT NULL,
    factor_mean DECIMAL(18,8) DEFAULT NULL,
    forward_return_mean_pct DECIMAL(18,8) DEFAULT NULL,
    ic DECIMAL(18,8) DEFAULT NULL,
    rank_ic DECIMAL(18,8) DEFAULT NULL,
    ci DECIMAL(18,8) DEFAULT NULL,
    source VARCHAR(32) NOT NULL DEFAULT 'daily_full_sample',
    metadata_json JSON DEFAULT NULL,
    computed_at DATETIME NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_strategy_factor_ci (strategy_id, instrument_type, trade_date, horizon_days, factor_key),
    KEY idx_strategy_factor_ci_latest (strategy_id, instrument_type, trade_date),
    KEY idx_strategy_factor_ci_factor (factor_key),
    KEY idx_strategy_factor_ci_computed_at (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_strategy_factor_ci_schema() -> dict:
    init_mysql_schema()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(STRATEGY_FACTOR_CI_DDL)
    return {"strategy_factor_ci_daily": "ok"}


if __name__ == "__main__":
    print(ensure_strategy_factor_ci_schema())
