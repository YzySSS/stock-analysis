from __future__ import annotations

from app.shared.db import mysql_conn

DDL = [
    """
    CREATE TABLE IF NOT EXISTS market_context_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        index_code VARCHAR(16) NOT NULL DEFAULT '000300.SH',
        market_state VARCHAR(32) NOT NULL DEFAULT 'neutral',
        trend_score DECIMAL(12,4) DEFAULT NULL,
        breadth_score DECIMAL(12,4) DEFAULT NULL,
        volume_score DECIMAL(12,4) DEFAULT NULL,
        sentiment_score DECIMAL(12,4) DEFAULT NULL,
        market_strength DECIMAL(12,4) DEFAULT NULL,
        index_close DECIMAL(12,4) DEFAULT NULL,
        index_pct_chg DECIMAL(12,4) DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'tushare+daily_kline',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_context_daily (trade_date, index_code),
        KEY idx_market_context_date (trade_date),
        KEY idx_market_context_state (market_state)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_news (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        title VARCHAR(512) NOT NULL,
        summary TEXT,
        source VARCHAR(128) DEFAULT NULL,
        url VARCHAR(1024) DEFAULT NULL,
        published_at DATETIME DEFAULT NULL,
        sentiment_score DECIMAL(12,4) DEFAULT NULL,
        credibility_score DECIMAL(12,4) DEFAULT NULL,
        credibility_level VARCHAR(8) DEFAULT NULL,
        credibility_reason VARCHAR(255) DEFAULT NULL,
        quality_score DECIMAL(12,4) DEFAULT NULL,
        quality_level VARCHAR(16) DEFAULT NULL,
        raw_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_news (code, title(191), published_at),
        KEY idx_stock_news_code_time (code, published_at),
        KEY idx_stock_news_source (source),
        KEY idx_stock_news_quality (quality_score),
        KEY idx_stock_news_credibility (credibility_score)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_sentiment_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        sentiment_score DECIMAL(12,4) DEFAULT NULL,
        news_count INT DEFAULT 0,
        raw_news_count INT DEFAULT 0,
        filtered_news_count INT DEFAULT 0,
        positive_count INT DEFAULT 0,
        negative_count INT DEFAULT 0,
        credibility_avg DECIMAL(12,4) DEFAULT NULL,
        quality_avg DECIMAL(12,4) DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'news_aggregator',
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_sentiment_daily (code, trade_date),
        KEY idx_stock_sentiment_date (trade_date),
        KEY idx_stock_sentiment_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


SCHEMA_UPGRADES = {
    "stock_news": {
        "credibility_level": "ALTER TABLE stock_news ADD COLUMN credibility_level VARCHAR(8) DEFAULT NULL AFTER credibility_score",
        "credibility_reason": "ALTER TABLE stock_news ADD COLUMN credibility_reason VARCHAR(255) DEFAULT NULL AFTER credibility_level",
        "quality_score": "ALTER TABLE stock_news ADD COLUMN quality_score DECIMAL(12,4) DEFAULT NULL AFTER credibility_reason",
        "quality_level": "ALTER TABLE stock_news ADD COLUMN quality_level VARCHAR(16) DEFAULT NULL AFTER quality_score",
    },
    "stock_sentiment_daily": {
        "raw_news_count": "ALTER TABLE stock_sentiment_daily ADD COLUMN raw_news_count INT DEFAULT 0 AFTER news_count",
        "filtered_news_count": "ALTER TABLE stock_sentiment_daily ADD COLUMN filtered_news_count INT DEFAULT 0 AFTER raw_news_count",
        "quality_avg": "ALTER TABLE stock_sentiment_daily ADD COLUMN quality_avg DECIMAL(12,4) DEFAULT NULL AFTER credibility_avg",
    },
}

INDEX_UPGRADES = {
    "stock_news": {
        "idx_stock_news_quality": "ALTER TABLE stock_news ADD KEY idx_stock_news_quality (quality_score)",
        "idx_stock_news_credibility": "ALTER TABLE stock_news ADD KEY idx_stock_news_credibility (credibility_score)",
    }
}


def _column_exists(cursor, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (table, column),
    )
    row = cursor.fetchone() or {}
    return bool(row.get("cnt") if isinstance(row, dict) else row[0])


def _index_exists(cursor, table: str, index_name: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND INDEX_NAME = %s
        """,
        (table, index_name),
    )
    row = cursor.fetchone() or {}
    return bool(row.get("cnt") if isinstance(row, dict) else row[0])


def ensure_market_sentiment_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for sql in DDL:
                cursor.execute(sql)
            for table, columns in SCHEMA_UPGRADES.items():
                for column, sql in columns.items():
                    if not _column_exists(cursor, table, column):
                        cursor.execute(sql)
            for table, indexes in INDEX_UPGRADES.items():
                for index_name, sql in indexes.items():
                    if not _index_exists(cursor, table, index_name):
                        cursor.execute(sql)
    return {"status": "ok", "tables": ["market_context_daily", "stock_news", "stock_sentiment_daily"]}
