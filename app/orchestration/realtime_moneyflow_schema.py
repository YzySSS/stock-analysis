from __future__ import annotations

from app.orchestration.init_project import init_mysql_schema
from app.shared.db import mysql_conn

DDL = [
    """
    CREATE TABLE IF NOT EXISTS stock_realtime_moneyflow_snapshot (
        code VARCHAR(16) NOT NULL PRIMARY KEY,
        source_code VARCHAR(16) DEFAULT NULL,
        name VARCHAR(64) DEFAULT NULL,
        trade_date DATE NOT NULL,
        quote_time DATETIME NOT NULL,
        latest_price DECIMAL(12,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        turnover_rate DECIMAL(12,4) DEFAULT NULL,
        inflow_amount DECIMAL(20,2) DEFAULT NULL,
        outflow_amount DECIMAL(20,2) DEFAULT NULL,
        net_amount DECIMAL(20,2) DEFAULT NULL,
        amount DECIMAL(20,2) DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'akshare_ths_stock_fund_flow_individual',
        source_unit VARCHAR(16) NOT NULL DEFAULT '元',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_realtime_moneyflow_trade_date (trade_date),
        KEY idx_realtime_moneyflow_quote_time (quote_time),
        KEY idx_realtime_moneyflow_net (net_amount)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_realtime_moneyflow_intraday (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        source_code VARCHAR(16) DEFAULT NULL,
        name VARCHAR(64) DEFAULT NULL,
        trade_date DATE NOT NULL,
        quote_time DATETIME NOT NULL,
        quote_minute DATETIME NOT NULL,
        latest_price DECIMAL(12,4) DEFAULT NULL,
        pct_chg DECIMAL(12,4) DEFAULT NULL,
        turnover_rate DECIMAL(12,4) DEFAULT NULL,
        inflow_amount DECIMAL(20,2) DEFAULT NULL,
        outflow_amount DECIMAL(20,2) DEFAULT NULL,
        net_amount DECIMAL(20,2) DEFAULT NULL,
        amount DECIMAL(20,2) DEFAULT NULL,
        source VARCHAR(64) NOT NULL DEFAULT 'akshare_ths_stock_fund_flow_individual',
        source_unit VARCHAR(16) NOT NULL DEFAULT '元',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_realtime_moneyflow_intraday_minute (trade_date, quote_minute, code),
        KEY idx_realtime_moneyflow_intraday_code_time (code, quote_minute),
        KEY idx_realtime_moneyflow_intraday_trade_date (trade_date),
        KEY idx_realtime_moneyflow_intraday_net (trade_date, net_amount)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_realtime_moneyflow_schema() -> dict:
    init_mysql_schema()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in DDL:
                cursor.execute(ddl)
    return {
        "status": "ok",
        "tables": ["stock_realtime_moneyflow_snapshot", "stock_realtime_moneyflow_intraday"],
    }


if __name__ == "__main__":
    print(ensure_realtime_moneyflow_schema())
