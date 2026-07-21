from __future__ import annotations

from app.shared.db import mysql_conn


SOURCE_BATCH_MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS source_batch_manifest (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(96) NOT NULL,
    source_name VARCHAR(64) NOT NULL,
    dataset_name VARCHAR(64) NOT NULL,
    logical_trade_date DATE DEFAULT NULL,
    source_event_time_min DATETIME DEFAULT NULL,
    source_event_time_max DATETIME DEFAULT NULL,
    received_at DATETIME NOT NULL,
    published_at DATETIME DEFAULT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'received',
    quality_status VARCHAR(24) NOT NULL DEFAULT 'pending',
    quality_reason VARCHAR(500) DEFAULT NULL,
    expected_rows BIGINT DEFAULT NULL,
    actual_rows BIGINT NOT NULL DEFAULT 0,
    expected_entities INT DEFAULT NULL,
    actual_entities INT NOT NULL DEFAULT 0,
    stale_rows BIGINT NOT NULL DEFAULT 0,
    rejected_rows BIGINT NOT NULL DEFAULT 0,
    coverage_ratio DECIMAL(12,8) DEFAULT NULL,
    schema_version VARCHAR(32) DEFAULT NULL,
    payload_hash CHAR(64) DEFAULT NULL,
    parent_batch_ids_json JSON DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_source_batch_manifest (source_name, dataset_name, batch_id),
    KEY idx_source_batch_dataset_date (dataset_name, logical_trade_date),
    KEY idx_source_batch_status (quality_status, published_at),
    KEY idx_source_batch_event_time (source_name, source_event_time_max)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


SENTIMENT_CANDIDATE_SNAPSHOT_MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS sentiment_candidate_snapshot_manifest (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    snapshot_id VARCHAR(96) NOT NULL,
    strategy_id VARCHAR(64) NOT NULL,
    strategy_version VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    decision_as_of DATETIME NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'building',
    quality_status VARCHAR(24) NOT NULL DEFAULT 'pending',
    quality_reason VARCHAR(500) DEFAULT NULL,
    source_manifest_ids_json JSON NOT NULL,
    source_batch_set_hash CHAR(64) NOT NULL,
    strategy_config_hash CHAR(64) NOT NULL,
    implementation_hash CHAR(64) DEFAULT NULL,
    news_event_set_hash CHAR(64) DEFAULT NULL,
    candidate_count INT NOT NULL DEFAULT 0,
    eligible_count INT NOT NULL DEFAULT 0,
    selected_count INT NOT NULL DEFAULT 0,
    tradable_count INT NOT NULL DEFAULT 0,
    coverage_ratio DECIMAL(12,8) DEFAULT NULL,
    freshness_seconds INT DEFAULT NULL,
    ai_mode VARCHAR(32) NOT NULL DEFAULT 'local_core',
    prompt_version VARCHAR(64) DEFAULT NULL,
    model_version VARCHAR(128) DEFAULT NULL,
    generated_at DATETIME NOT NULL,
    published_at DATETIME DEFAULT NULL,
    supersedes_snapshot_id VARCHAR(96) DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_sentiment_candidate_manifest (snapshot_id),
    KEY idx_sentiment_manifest_strategy_time (strategy_id, decision_as_of),
    KEY idx_sentiment_manifest_date_status (trade_date, status, quality_status),
    KEY idx_sentiment_manifest_published (published_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


SENTIMENT_CANDIDATE_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS sentiment_candidate_snapshot (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    snapshot_id VARCHAR(96) NOT NULL,
    code VARCHAR(16) NOT NULL,
    name VARCHAR(64) DEFAULT NULL,
    candidate_state VARCHAR(24) NOT NULL DEFAULT 'eligible',
    eligibility_reason VARCHAR(500) DEFAULT NULL,
    is_selected TINYINT(1) NOT NULL DEFAULT 0,
    is_tradable TINYINT(1) NOT NULL DEFAULT 0,
    rank_no INT DEFAULT NULL,
    score DECIMAL(12,4) DEFAULT NULL,
    trade_grade_state VARCHAR(32) DEFAULT NULL,
    opinion_sector_type VARCHAR(32) DEFAULT NULL,
    opinion_sector_name VARCHAR(128) DEFAULT NULL,
    opinion_match_type VARCHAR(64) DEFAULT NULL,
    market_opinion_snapshot_id BIGINT DEFAULT NULL,
    selected_price DECIMAL(16,4) DEFAULT NULL,
    selected_price_source VARCHAR(64) DEFAULT NULL,
    selected_price_quote_time DATETIME DEFAULT NULL,
    factor_json JSON DEFAULT NULL,
    explain_json JSON DEFAULT NULL,
    trade_plan_json JSON DEFAULT NULL,
    source_lineage_json JSON NOT NULL,
    row_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_sentiment_candidate_snapshot (snapshot_id, code),
    KEY idx_sentiment_candidate_rank (snapshot_id, is_selected, rank_no),
    KEY idx_sentiment_candidate_code (code, created_at),
    KEY idx_sentiment_candidate_theme (snapshot_id, opinion_sector_type, opinion_sector_name),
    KEY idx_sentiment_candidate_trade_grade (snapshot_id, trade_grade_state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


STOCK_REALTIME_RANK_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS stock_realtime_rank_snapshot (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    snapshot_id VARCHAR(96) NOT NULL,
    source_batch_id VARCHAR(96) DEFAULT NULL,
    trade_date DATE NOT NULL,
    quote_time DATETIME NOT NULL,
    rank_type VARCHAR(32) NOT NULL,
    rank_no INT NOT NULL,
    code VARCHAR(16) NOT NULL,
    name VARCHAR(64) DEFAULT NULL,
    latest_price DECIMAL(16,4) DEFAULT NULL,
    pct_chg DECIMAL(12,4) DEFAULT NULL,
    amount DECIMAL(24,4) DEFAULT NULL,
    net_amount DECIMAL(24,4) DEFAULT NULL,
    rank_score DECIMAL(12,4) DEFAULT NULL,
    is_stale TINYINT(1) NOT NULL DEFAULT 0,
    source VARCHAR(64) NOT NULL,
    metrics_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_stock_realtime_rank_snapshot (snapshot_id, rank_type, code),
    KEY idx_stock_realtime_rank_order (snapshot_id, rank_type, rank_no),
    KEY idx_stock_realtime_rank_code_time (code, quote_time),
    KEY idx_stock_realtime_rank_trade_date (trade_date, quote_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


TRACKING_SUMMARY_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS tracking_summary_daily (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    summary_date DATE NOT NULL,
    strategy_id VARCHAR(64) NOT NULL,
    strategy_version VARCHAR(32) NOT NULL DEFAULT '',
    instrument_type VARCHAR(16) NOT NULL DEFAULT 'stock',
    selection_count INT NOT NULL DEFAULT 0,
    tradable_count INT NOT NULL DEFAULT 0,
    matured_1d_count INT NOT NULL DEFAULT 0,
    matured_3d_count INT NOT NULL DEFAULT 0,
    matured_5d_count INT NOT NULL DEFAULT 0,
    matured_20d_count INT NOT NULL DEFAULT 0,
    win_rate_1d_pct DECIMAL(12,4) DEFAULT NULL,
    win_rate_3d_pct DECIMAL(12,4) DEFAULT NULL,
    win_rate_5d_pct DECIMAL(12,4) DEFAULT NULL,
    win_rate_20d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_return_1d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_return_3d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_return_5d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_return_20d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_excess_1d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_excess_3d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_excess_5d_pct DECIMAL(12,4) DEFAULT NULL,
    avg_excess_20d_pct DECIMAL(12,4) DEFAULT NULL,
    source_cutoff_at DATETIME DEFAULT NULL,
    source_snapshot_hash CHAR(64) DEFAULT NULL,
    quality_status VARCHAR(24) NOT NULL DEFAULT 'pending',
    summary_json JSON DEFAULT NULL,
    calculated_at DATETIME NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_tracking_summary_daily (summary_date, strategy_id, strategy_version, instrument_type),
    KEY idx_tracking_summary_strategy_date (strategy_id, summary_date),
    KEY idx_tracking_summary_quality (quality_status, calculated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


OPERATIONAL_STATUS_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS operational_status_snapshot (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    snapshot_id VARCHAR(96) NOT NULL,
    captured_at DATETIME NOT NULL,
    component_type VARCHAR(32) NOT NULL,
    component_name VARCHAR(96) NOT NULL,
    status VARCHAR(24) NOT NULL,
    severity VARCHAR(16) NOT NULL DEFAULT 'info',
    logical_trade_date DATE DEFAULT NULL,
    current_batch_id VARCHAR(96) DEFAULT NULL,
    source_event_time DATETIME DEFAULT NULL,
    last_success_at DATETIME DEFAULT NULL,
    freshness_seconds INT DEFAULT NULL,
    coverage_ratio DECIMAL(12,8) DEFAULT NULL,
    latency_ms INT DEFAULT NULL,
    error_code VARCHAR(64) DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    metrics_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_operational_status_component (snapshot_id, component_type, component_name),
    KEY idx_operational_status_time (captured_at),
    KEY idx_operational_status_component_time (component_type, component_name, captured_at),
    KEY idx_operational_status_severity (severity, status, captured_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


AI_ADVICE_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS ai_advice_snapshot (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    advice_id VARCHAR(96) NOT NULL,
    cache_key CHAR(64) NOT NULL,
    sentiment_snapshot_id VARCHAR(96) DEFAULT NULL,
    selection_run_id VARCHAR(64) DEFAULT NULL,
    strategy_id VARCHAR(64) NOT NULL,
    strategy_version VARCHAR(32) NOT NULL,
    code VARCHAR(16) NOT NULL DEFAULT '',
    advice_type VARCHAR(32) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    model_version VARCHAR(128) NOT NULL,
    prompt_version VARCHAR(64) NOT NULL,
    input_hash CHAR(64) NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'pending',
    score DECIMAL(12,4) DEFAULT NULL,
    confidence DECIMAL(12,8) DEFAULT NULL,
    label VARCHAR(128) DEFAULT NULL,
    summary TEXT,
    opportunities_json JSON DEFAULT NULL,
    risks_json JSON DEFAULT NULL,
    response_json JSON DEFAULT NULL,
    requested_at DATETIME NOT NULL,
    completed_at DATETIME DEFAULT NULL,
    latency_ms INT DEFAULT NULL,
    expires_at DATETIME DEFAULT NULL,
    error_code VARCHAR(64) DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_advice_snapshot (advice_id),
    UNIQUE KEY uniq_ai_advice_cache_key (cache_key),
    KEY idx_ai_advice_sentiment_snapshot (sentiment_snapshot_id, advice_type),
    KEY idx_ai_advice_selection_run (selection_run_id, code),
    KEY idx_ai_advice_status_time (status, requested_at),
    KEY idx_ai_advice_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


SENTIMENT_CONSISTENCY_DDL = (
    SOURCE_BATCH_MANIFEST_DDL,
    SENTIMENT_CANDIDATE_SNAPSHOT_MANIFEST_DDL,
    SENTIMENT_CANDIDATE_SNAPSHOT_DDL,
    STOCK_REALTIME_RANK_SNAPSHOT_DDL,
    TRACKING_SUMMARY_DAILY_DDL,
    OPERATIONAL_STATUS_SNAPSHOT_DDL,
    AI_ADVICE_SNAPSHOT_DDL,
)


def ensure_sentiment_consistency_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for statement in SENTIMENT_CONSISTENCY_DDL:
                cursor.execute(statement)
    return {
        "status": "ok",
        "tables": [
            "source_batch_manifest",
            "sentiment_candidate_snapshot_manifest",
            "sentiment_candidate_snapshot",
            "stock_realtime_rank_snapshot",
            "tracking_summary_daily",
            "operational_status_snapshot",
            "ai_advice_snapshot",
        ],
    }
