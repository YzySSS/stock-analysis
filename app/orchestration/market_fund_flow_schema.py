from __future__ import annotations

from app.shared.db import mysql_conn

DDL = [
    """
    CREATE TABLE IF NOT EXISTS market_sector_fund_flow_snapshot (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        sector_type VARCHAR(32) NOT NULL,
        sector_name VARCHAR(128) NOT NULL,
        trade_date DATE NOT NULL,
        quote_time DATETIME NOT NULL,
        rank_no INT DEFAULT NULL,
        sector_index DECIMAL(20,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        inflow_amount DECIMAL(20,4) DEFAULT NULL,
        outflow_amount DECIMAL(20,4) DEFAULT NULL,
        net_amount DECIMAL(20,4) DEFAULT NULL,
        company_count INT DEFAULT NULL,
        leading_stock VARCHAR(64) DEFAULT NULL,
        leading_stock_pct_chg DECIMAL(12,4) DEFAULT NULL,
        leading_stock_price DECIMAL(12,4) DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'akshare_stock_fund_flow',
        source_unit VARCHAR(16) NOT NULL DEFAULT '亿元',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_sector_fund_flow_snapshot (sector_type, sector_name),
        KEY idx_market_sector_fund_flow_snapshot_type (sector_type),
        KEY idx_market_sector_fund_flow_snapshot_trade_date (trade_date),
        KEY idx_market_sector_fund_flow_snapshot_net (sector_type, net_amount),
        KEY idx_market_sector_fund_flow_snapshot_quote_time (quote_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS market_sector_fund_flow_intraday (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        sector_type VARCHAR(32) NOT NULL,
        sector_name VARCHAR(128) NOT NULL,
        trade_date DATE NOT NULL,
        quote_time DATETIME NOT NULL,
        quote_minute DATETIME NOT NULL,
        rank_no INT DEFAULT NULL,
        sector_index DECIMAL(20,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        inflow_amount DECIMAL(20,4) DEFAULT NULL,
        outflow_amount DECIMAL(20,4) DEFAULT NULL,
        net_amount DECIMAL(20,4) DEFAULT NULL,
        company_count INT DEFAULT NULL,
        leading_stock VARCHAR(64) DEFAULT NULL,
        leading_stock_pct_chg DECIMAL(12,4) DEFAULT NULL,
        leading_stock_price DECIMAL(12,4) DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'akshare_stock_fund_flow',
        source_unit VARCHAR(16) NOT NULL DEFAULT '亿元',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_market_sector_fund_flow_intraday (sector_type, sector_name, quote_minute),
        KEY idx_market_sector_fund_flow_intraday_type_time (sector_type, quote_minute),
        KEY idx_market_sector_fund_flow_intraday_trade_date (trade_date),
        KEY idx_market_sector_fund_flow_intraday_net (sector_type, trade_date, net_amount)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_market_fund_flow_schema() -> dict:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in DDL:
                cursor.execute(ddl)
    return {"status": "ok", "tables": ["market_sector_fund_flow_snapshot", "market_sector_fund_flow_intraday"]}
