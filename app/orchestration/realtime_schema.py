from __future__ import annotations

from app.orchestration.init_project import init_mysql_schema
from app.shared.db import mysql_conn

DDL = [
    """
    CREATE TABLE IF NOT EXISTS stock_realtime_snapshot (
        code VARCHAR(16) NOT NULL PRIMARY KEY,
        source_code VARCHAR(16) DEFAULT NULL,
        name VARCHAR(64) DEFAULT NULL,
        trade_date DATE NOT NULL,
        quote_time DATETIME NOT NULL,
        latest_price DECIMAL(12,4) DEFAULT NULL,
        change_amount DECIMAL(12,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        bid_price DECIMAL(12,4) DEFAULT NULL,
        ask_price DECIMAL(12,4) DEFAULT NULL,
        pre_close DECIMAL(12,4) DEFAULT NULL,
        open_price DECIMAL(12,4) DEFAULT NULL,
        high_price DECIMAL(12,4) DEFAULT NULL,
        low_price DECIMAL(12,4) DEFAULT NULL,
        volume BIGINT DEFAULT NULL,
        amount DECIMAL(20,2) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'akshare_stock_zh_a_spot',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_realtime_snapshot_trade_date (trade_date),
        KEY idx_realtime_snapshot_quote_time (quote_time),
        KEY idx_realtime_snapshot_pct_chg (pct_chg)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_realtime_intraday (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        source_code VARCHAR(16) DEFAULT NULL,
        name VARCHAR(64) DEFAULT NULL,
        trade_date DATE NOT NULL,
        quote_time DATETIME NOT NULL,
        quote_minute DATETIME NOT NULL,
        latest_price DECIMAL(12,4) DEFAULT NULL,
        change_amount DECIMAL(12,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        bid_price DECIMAL(12,4) DEFAULT NULL,
        ask_price DECIMAL(12,4) DEFAULT NULL,
        pre_close DECIMAL(12,4) DEFAULT NULL,
        open_price DECIMAL(12,4) DEFAULT NULL,
        high_price DECIMAL(12,4) DEFAULT NULL,
        low_price DECIMAL(12,4) DEFAULT NULL,
        volume BIGINT DEFAULT NULL,
        amount DECIMAL(20,2) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'akshare_stock_zh_a_spot',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_realtime_intraday_minute (trade_date, quote_minute, code),
        KEY idx_realtime_intraday_code_time (code, quote_minute),
        KEY idx_realtime_intraday_trade_date (trade_date),
        KEY idx_realtime_intraday_pct_chg (trade_date, pct_chg)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_realtime_schema() -> dict:
    init_mysql_schema()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in DDL:
                cursor.execute(ddl)
    return {"status": "ok", "tables": ["stock_realtime_snapshot", "stock_realtime_intraday"]}


if __name__ == "__main__":
    print(ensure_realtime_schema())
