from __future__ import annotations

from app.shared.db import mysql_conn


LOWVOL_REVERSAL_FEATURE_DDL = """
CREATE TABLE IF NOT EXISTS lowvol_reversal_feature_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    ma20 DECIMAL(18, 6) NULL,
    ma60 DECIMAL(18, 6) NULL,
    close_5d DECIMAL(18, 6) NULL,
    close_20d DECIMAL(18, 6) NULL,
    prev_close_1d DECIMAL(18, 6) NULL,
    max_close_20 DECIMAL(18, 6) NULL,
    min_close_20 DECIMAL(18, 6) NULL,
    avg_amount_20 DECIMAL(24, 6) NULL,
    kline_count_20 INT NULL,
    kline_count_60 INT NULL,
    std_return_20 DECIMAL(18, 10) NULL,
    pct_chg_1d DECIMAL(18, 8) NULL,
    turnover_rate_5d_avg DECIMAL(18, 8) NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_lowvol_feature_code_date (code, trade_date),
    KEY idx_lowvol_feature_trade_date (trade_date),
    KEY idx_lowvol_feature_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_feature_cache_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(LOWVOL_REVERSAL_FEATURE_DDL)
    return {"status": "ok", "tables": ["lowvol_reversal_feature_daily"]}
