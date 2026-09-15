from __future__ import annotations

from app.shared.db import mysql_conn


MARKET_OPINION_EVENT_DDL = """
CREATE TABLE IF NOT EXISTS market_opinion_event (
    canonical_event_id CHAR(64) NOT NULL PRIMARY KEY,
    identity_version VARCHAR(32) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    content_role VARCHAR(32) NOT NULL,
    direction VARCHAR(16) NOT NULL DEFAULT 'neutral',
    confirmation_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    primary_subject VARCHAR(191) DEFAULT NULL,
    event_date DATE NOT NULL,
    current_revision INT UNSIGNED NOT NULL DEFAULT 1,
    first_available_at DATETIME NOT NULL,
    last_available_at DATETIME NOT NULL,
    effective_until DATETIME DEFAULT NULL,
    driver_horizon VARCHAR(32) NOT NULL DEFAULT 'unknown',
    next_milestone VARCHAR(255) DEFAULT NULL,
    is_one_off TINYINT(1) NOT NULL DEFAULT 0,
    is_terminal TINYINT(1) NOT NULL DEFAULT 0,
    identity_material_json JSON NOT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_market_opinion_event_date (event_date, content_role, direction),
    KEY idx_market_opinion_event_available (first_available_at, effective_until),
    KEY idx_market_opinion_event_confirmation (confirmation_status, event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


MARKET_OPINION_EVENT_REVISION_DDL = """
CREATE TABLE IF NOT EXISTS market_opinion_event_revision (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    canonical_event_id CHAR(64) NOT NULL,
    event_revision INT UNSIGNED NOT NULL,
    revision_hash CHAR(64) NOT NULL,
    evidence_rule_version VARCHAR(32) NOT NULL,
    content_role VARCHAR(32) NOT NULL,
    direction VARCHAR(16) NOT NULL DEFAULT 'neutral',
    confirmation_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    delta_from_previous VARCHAR(32) NOT NULL DEFAULT 'initial',
    source_time DATETIME DEFAULT NULL,
    received_at DATETIME NOT NULL,
    available_at DATETIME NOT NULL,
    effective_until DATETIME DEFAULT NULL,
    facts_json JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_market_opinion_event_revision (canonical_event_id, event_revision),
    UNIQUE KEY uniq_market_opinion_event_revision_hash (canonical_event_id, revision_hash),
    KEY idx_market_opinion_revision_available (available_at, canonical_event_id),
    KEY idx_market_opinion_revision_hash (revision_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


MARKET_OPINION_EVENT_EVIDENCE_DDL = """
CREATE TABLE IF NOT EXISTS market_opinion_event_evidence (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    evidence_id CHAR(64) NOT NULL,
    canonical_event_id CHAR(64) NOT NULL,
    event_revision INT UNSIGNED NOT NULL,
    raw_id BIGINT DEFAULT NULL,
    original_news_id VARCHAR(191) DEFAULT NULL,
    source_id VARCHAR(64) DEFAULT NULL,
    original_publisher VARCHAR(128) DEFAULT NULL,
    collection_channel VARCHAR(64) DEFAULT NULL,
    source_type VARCHAR(32) DEFAULT NULL,
    credibility_rule_version VARCHAR(32) NOT NULL,
    credibility_score DECIMAL(12,4) DEFAULT NULL,
    impact_score DECIMAL(12,4) DEFAULT NULL,
    is_primary_source TINYINT(1) NOT NULL DEFAULT 0,
    is_independent_confirmation TINYINT(1) NOT NULL DEFAULT 0,
    repost_of_evidence_id CHAR(64) DEFAULT NULL,
    source_time DATETIME DEFAULT NULL,
    published_at DATETIME DEFAULT NULL,
    first_seen_at DATETIME DEFAULT NULL,
    received_at DATETIME NOT NULL,
    available_at DATETIME NOT NULL,
    content_role VARCHAR(32) NOT NULL,
    title VARCHAR(512) NOT NULL,
    evidence_excerpt VARCHAR(1000) DEFAULT NULL,
    source_url VARCHAR(1024) DEFAULT NULL,
    raw_payload_hash CHAR(64) NOT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_market_opinion_event_evidence (evidence_id),
    UNIQUE KEY uniq_market_opinion_event_raw (canonical_event_id, raw_id),
    KEY idx_market_opinion_evidence_event (canonical_event_id, event_revision),
    KEY idx_market_opinion_evidence_available (available_at, canonical_event_id),
    KEY idx_market_opinion_evidence_publisher (original_publisher, available_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


MARKET_OPINION_STOCK_RELATION_DDL = """
CREATE TABLE IF NOT EXISTS market_opinion_stock_relation (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    relation_id CHAR(64) NOT NULL,
    canonical_event_id CHAR(64) NOT NULL,
    event_revision INT UNSIGNED NOT NULL,
    code VARCHAR(16) NOT NULL,
    relation_type VARCHAR(32) NOT NULL,
    relation_status VARCHAR(32) NOT NULL DEFAULT 'unconfirmed',
    relation_score DECIMAL(12,4) DEFAULT NULL,
    benefit_scale DECIMAL(18,6) DEFAULT NULL,
    benefit_scale_unit VARCHAR(32) DEFAULT NULL,
    evidence_id CHAR(64) DEFAULT NULL,
    evidence_excerpt VARCHAR(1000) DEFAULT NULL,
    relation_reason VARCHAR(500) DEFAULT NULL,
    is_adverse_veto TINYINT(1) NOT NULL DEFAULT 0,
    valid_from DATETIME NOT NULL,
    valid_until DATETIME DEFAULT NULL,
    relation_rule_version VARCHAR(32) NOT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_market_opinion_stock_relation (relation_id),
    UNIQUE KEY uniq_market_opinion_event_stock_relation (
        canonical_event_id, event_revision, code, relation_type
    ),
    KEY idx_market_opinion_relation_code (code, valid_from, valid_until),
    KEY idx_market_opinion_relation_event (canonical_event_id, event_revision),
    KEY idx_market_opinion_relation_status (relation_status, is_adverse_veto)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


SENTIMENT_V06_DDL = (
    MARKET_OPINION_EVENT_DDL,
    MARKET_OPINION_EVENT_REVISION_DDL,
    MARKET_OPINION_EVENT_EVIDENCE_DDL,
    MARKET_OPINION_STOCK_RELATION_DDL,
)


def ensure_sentiment_v06_schema() -> dict:
    """Install the append-only event evidence contract used by sentiment v0.6.

    Relations intentionally use indexed identifiers instead of foreign keys:
    the production application migration account cannot grant ``REFERENCES``.
    """

    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for statement in SENTIMENT_V06_DDL:
                cursor.execute(statement)
    return {
        "status": "ok",
        "tables": [
            "market_opinion_event",
            "market_opinion_event_revision",
            "market_opinion_event_evidence",
            "market_opinion_stock_relation",
        ],
    }
