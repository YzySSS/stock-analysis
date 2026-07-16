from __future__ import annotations

from app.shared.db import mysql_conn

DDL = [
    """
    CREATE TABLE IF NOT EXISTS stock_popularity_snapshot (
        code VARCHAR(16) NOT NULL PRIMARY KEY,
        name VARCHAR(64) DEFAULT NULL,
        trade_date DATE NOT NULL,
        quote_time DATETIME NOT NULL,
        source VARCHAR(64) NOT NULL,
        source_rank INT DEFAULT NULL,
        source_score DECIMAL(20,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        popularity_score DECIMAL(12,4) DEFAULT NULL,
        raw_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_stock_popularity_trade_date (trade_date),
        KEY idx_stock_popularity_quote_time (quote_time),
        KEY idx_stock_popularity_score (popularity_score),
        KEY idx_stock_popularity_source_rank (source, source_rank)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_popularity_intraday (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        name VARCHAR(64) DEFAULT NULL,
        trade_date DATE NOT NULL,
        quote_time DATETIME NOT NULL,
        quote_minute DATETIME NOT NULL,
        source VARCHAR(64) NOT NULL,
        source_rank INT DEFAULT NULL,
        source_score DECIMAL(20,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        popularity_score DECIMAL(12,4) DEFAULT NULL,
        raw_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_popularity_intraday (code, source, quote_minute),
        KEY idx_stock_popularity_intraday_time (quote_minute),
        KEY idx_stock_popularity_intraday_score (trade_date, popularity_score)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_stock_popularity_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in DDL:
                cursor.execute(ddl)
    return {
        "status": "ok",
        "tables": ["stock_popularity_snapshot", "stock_popularity_intraday"],
    }
