from __future__ import annotations

from app.shared.db import mysql_conn

DDL = [
    """
    CREATE TABLE IF NOT EXISTS market_opinion_raw (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(64) NOT NULL,
        source_name VARCHAR(128) DEFAULT NULL,
        source_column VARCHAR(32) DEFAULT NULL,
        source_type VARCHAR(32) DEFAULT NULL,
        rank_no INT DEFAULT NULL,
        item_id VARCHAR(191) DEFAULT NULL,
        title VARCHAR(512) NOT NULL,
        summary TEXT,
        url VARCHAR(1024) DEFAULT NULL,
        mobile_url VARCHAR(1024) DEFAULT NULL,
        published_at DATETIME DEFAULT NULL,
        crawl_time DATETIME NOT NULL,
        trade_date DATE NOT NULL,
        status VARCHAR(32) DEFAULT NULL,
        source_score DECIMAL(12,4) DEFAULT NULL,
        importance_score DECIMAL(12,4) DEFAULT NULL,
        amplification_score DECIMAL(12,4) DEFAULT NULL,
        timeliness_score DECIMAL(12,4) DEFAULT NULL,
        timeliness_level VARCHAR(32) DEFAULT NULL,
        effective_until DATETIME DEFAULT NULL,
        impact_score DECIMAL(12,4) DEFAULT NULL,
        direction VARCHAR(16) NOT NULL DEFAULT 'neutral',
        event_type VARCHAR(64) DEFAULT NULL,
        title_hash CHAR(64) NOT NULL,
        raw_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_opinion_raw_source_title (source_id, title_hash),
        KEY idx_market_opinion_source (source_id),
        KEY idx_market_opinion_trade_date (trade_date),
        KEY idx_market_opinion_published_at (published_at),
        KEY idx_market_opinion_score (impact_score),
        KEY idx_market_opinion_direction (direction)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_opinion_stock_match (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        raw_id BIGINT NOT NULL,
        code VARCHAR(16) NOT NULL,
        name VARCHAR(64) DEFAULT NULL,
        industry VARCHAR(128) DEFAULT NULL,
        match_score DECIMAL(12,4) DEFAULT NULL,
        match_reason VARCHAR(255) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_opinion_stock_match (raw_id, code),
        KEY idx_market_opinion_match_code (code),
        KEY idx_market_opinion_match_industry (industry),
        KEY idx_market_opinion_match_raw (raw_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_opinion_sector_match (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        raw_id BIGINT NOT NULL,
        sector_type VARCHAR(32) NOT NULL,
        sector_name VARCHAR(128) NOT NULL,
        match_score DECIMAL(12,4) DEFAULT NULL,
        match_reason VARCHAR(255) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_opinion_sector_match (raw_id, sector_type, sector_name),
        KEY idx_market_opinion_sector_match (sector_type, sector_name),
        KEY idx_market_opinion_sector_raw (raw_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS sector_opinion_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL,
        sector_type VARCHAR(32) NOT NULL DEFAULT 'industry',
        sector_name VARCHAR(128) NOT NULL,
        as_of_datetime DATETIME NOT NULL,
        sector_score DECIMAL(12,4) DEFAULT NULL,
        weighted_impact_score DECIMAL(12,4) DEFAULT NULL,
        news_count INT NOT NULL DEFAULT 0,
        source_count INT NOT NULL DEFAULT 0,
        stock_count INT NOT NULL DEFAULT 0,
        positive_news_count INT NOT NULL DEFAULT 0,
        negative_news_count INT NOT NULL DEFAULT 0,
        top_stocks_json JSON DEFAULT NULL,
        top_news_json JSON DEFAULT NULL,
        source_json JSON DEFAULT NULL,
        payload_version TINYINT UNSIGNED NOT NULL DEFAULT 1,
        payload_migrated_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_sector_opinion_snapshot (trade_date, as_of_datetime, sector_type, sector_name),
        KEY idx_sector_opinion_date_score (trade_date, sector_score),
        KEY idx_sector_opinion_asof (as_of_datetime)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS sector_opinion_stock (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        snapshot_id BIGINT NOT NULL,
        rank_no SMALLINT UNSIGNED NOT NULL,
        code VARCHAR(16) NOT NULL,
        name VARCHAR(64) DEFAULT NULL,
        industry VARCHAR(128) DEFAULT NULL,
        score DECIMAL(12,4) DEFAULT NULL,
        news_count INT NOT NULL DEFAULT 0,
        match_type VARCHAR(64) DEFAULT NULL,
        match_reason VARCHAR(500) DEFAULT NULL,
        data_trade_date DATE DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        amount DECIMAL(20,2) DEFAULT NULL,
        extra_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_sector_opinion_stock (snapshot_id, code),
        KEY idx_sector_opinion_stock_code (code, snapshot_id),
        KEY idx_sector_opinion_stock_snapshot_rank (snapshot_id, rank_no)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS sector_opinion_news_ref (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        snapshot_id BIGINT NOT NULL,
        scope_type VARCHAR(16) NOT NULL DEFAULT 'sector',
        stock_code VARCHAR(16) NOT NULL DEFAULT '',
        rank_no SMALLINT UNSIGNED NOT NULL,
        raw_id BIGINT DEFAULT NULL,
        impact_score DECIMAL(12,4) DEFAULT NULL,
        signed_score DECIMAL(12,4) DEFAULT NULL,
        timeliness_score DECIMAL(12,4) DEFAULT NULL,
        timeliness_level VARCHAR(32) DEFAULT NULL,
        age_days DECIMAL(12,4) DEFAULT NULL,
        effective_until DATETIME DEFAULT NULL,
        published_at DATETIME DEFAULT NULL,
        fallback_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_sector_opinion_news_ref (snapshot_id, scope_type, stock_code, rank_no),
        KEY idx_sector_opinion_news_raw (raw_id),
        KEY idx_sector_opinion_news_snapshot (snapshot_id, scope_type, stock_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS sector_opinion_source_ref (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        snapshot_id BIGINT NOT NULL,
        rank_no SMALLINT UNSIGNED NOT NULL,
        source_id VARCHAR(64) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_sector_opinion_source_ref (snapshot_id, source_id),
        KEY idx_sector_opinion_source_snapshot_rank (snapshot_id, rank_no)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


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


def ensure_market_opinion_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for sql in DDL:
                cursor.execute(sql)
            # Lightweight forward-compatible upgrades for early local iterations.
            upgrades = {
                "market_opinion_raw": {
                    "event_type": "ALTER TABLE market_opinion_raw ADD COLUMN event_type VARCHAR(64) DEFAULT NULL AFTER direction",
                    "first_seen_at": "ALTER TABLE market_opinion_raw ADD COLUMN first_seen_at DATETIME DEFAULT NULL AFTER crawl_time",
                    "last_seen_at": "ALTER TABLE market_opinion_raw ADD COLUMN last_seen_at DATETIME DEFAULT NULL AFTER first_seen_at",
                    "timeliness_score": "ALTER TABLE market_opinion_raw ADD COLUMN timeliness_score DECIMAL(12,4) DEFAULT NULL AFTER amplification_score",
                    "timeliness_level": "ALTER TABLE market_opinion_raw ADD COLUMN timeliness_level VARCHAR(32) DEFAULT NULL AFTER timeliness_score",
                    "effective_until": "ALTER TABLE market_opinion_raw ADD COLUMN effective_until DATETIME DEFAULT NULL AFTER timeliness_level",
                },
                "sector_opinion_daily": {
                    "payload_version": "ALTER TABLE sector_opinion_daily ADD COLUMN payload_version TINYINT UNSIGNED NOT NULL DEFAULT 1 AFTER source_json",
                    "payload_migrated_at": "ALTER TABLE sector_opinion_daily ADD COLUMN payload_migrated_at DATETIME DEFAULT NULL AFTER payload_version",
                },
            }
            for table, columns in upgrades.items():
                for column, sql in columns.items():
                    if not _column_exists(cursor, table, column):
                        cursor.execute(sql)
            cursor.execute(
                """
                UPDATE market_opinion_raw
                SET first_seen_at = COALESCE(first_seen_at, crawl_time),
                    last_seen_at = COALESCE(last_seen_at, crawl_time)
                WHERE first_seen_at IS NULL OR last_seen_at IS NULL
                """
            )
            if _index_exists(cursor, "sector_opinion_daily", "uniq_sector_opinion_daily"):
                cursor.execute("ALTER TABLE sector_opinion_daily DROP INDEX uniq_sector_opinion_daily")
            if not _index_exists(cursor, "sector_opinion_daily", "uniq_sector_opinion_snapshot"):
                cursor.execute(
                    "ALTER TABLE sector_opinion_daily ADD UNIQUE KEY uniq_sector_opinion_snapshot (trade_date, as_of_datetime, sector_type, sector_name)"
                )
            if not _index_exists(cursor, "sector_opinion_daily", "idx_sector_opinion_asof"):
                cursor.execute("ALTER TABLE sector_opinion_daily ADD KEY idx_sector_opinion_asof (as_of_datetime)")
    return {
        "status": "ok",
        "tables": [
            "market_opinion_raw",
            "market_opinion_stock_match",
            "market_opinion_sector_match",
            "sector_opinion_daily",
            "sector_opinion_stock",
            "sector_opinion_news_ref",
            "sector_opinion_source_ref",
        ],
    }
