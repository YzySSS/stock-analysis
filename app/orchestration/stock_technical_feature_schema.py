from __future__ import annotations

from app.shared.db import mysql_conn


STOCK_TECHNICAL_FEATURE_DDL = """
CREATE TABLE IF NOT EXISTS stock_technical_feature_daily (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    source_trade_date DATE DEFAULT NULL,
    latest_close DECIMAL(18,6) DEFAULT NULL,
    ma5 DECIMAL(18,6) DEFAULT NULL,
    ma10 DECIMAL(18,6) DEFAULT NULL,
    ma20 DECIMAL(18,6) DEFAULT NULL,
    ma30 DECIMAL(18,6) DEFAULT NULL,
    ma60 DECIMAL(18,6) DEFAULT NULL,
    close_5d DECIMAL(18,6) DEFAULT NULL,
    close_20d DECIMAL(18,6) DEFAULT NULL,
    prev_close_1d DECIMAL(18,6) DEFAULT NULL,
    max_close_20 DECIMAL(18,6) DEFAULT NULL,
    min_close_20 DECIMAL(18,6) DEFAULT NULL,
    pct_chg_1d DECIMAL(18,8) DEFAULT NULL,
    return_5d_pct DECIMAL(18,8) DEFAULT NULL,
    return_20d_pct DECIMAL(18,8) DEFAULT NULL,
    std_return_20 DECIMAL(18,10) DEFAULT NULL,
    latest_amount DECIMAL(24,6) DEFAULT NULL,
    avg_amount_5 DECIMAL(24,6) DEFAULT NULL,
    avg_amount_20 DECIMAL(24,6) DEFAULT NULL,
    median_amount_20 DECIMAL(24,6) DEFAULT NULL,
    amount_ratio_5_20 DECIMAL(18,8) DEFAULT NULL,
    kline_count_20 INT NOT NULL DEFAULT 0,
    kline_count_60 INT NOT NULL DEFAULT 0,
    computed_at DATETIME NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_stock_technical_feature_code_date (code, trade_date),
    KEY idx_stock_technical_feature_date_code (trade_date, code),
    KEY idx_stock_technical_feature_code_source_date (code, source_trade_date),
    KEY idx_stock_technical_feature_liquidity (trade_date, median_amount_20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


SELECTION_RESULT_INDEX_MIGRATIONS = {
    "idx_selection_result_strategy_trade_created": (
        "ALTER TABLE selection_result "
        "ADD KEY idx_selection_result_strategy_trade_created (strategy_id, trade_date, created_at)"
    ),
    "idx_selection_result_code_trade_strategy": (
        "ALTER TABLE selection_result "
        "ADD KEY idx_selection_result_code_trade_strategy (code, trade_date, strategy_id)"
    ),
}


def _index_name(row) -> str:
    if isinstance(row, dict):
        return str(row.get("Key_name") or row.get("key_name") or "")
    return str(row[2])


def ensure_stock_technical_feature_schema() -> dict:
    applied_indexes: list[str] = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(STOCK_TECHNICAL_FEATURE_DDL)
            cursor.execute("SHOW INDEX FROM selection_result")
            existing_indexes = {_index_name(row) for row in (cursor.fetchall() or [])}
            for index_name, sql in SELECTION_RESULT_INDEX_MIGRATIONS.items():
                if index_name not in existing_indexes:
                    cursor.execute(sql)
                    applied_indexes.append(index_name)
    return {
        "status": "ok",
        "tables": ["stock_technical_feature_daily"],
        "applied_indexes": applied_indexes,
    }
