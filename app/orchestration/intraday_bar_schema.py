from __future__ import annotations

from app.orchestration.init_project import init_mysql_schema
from app.shared.db import mysql_conn

DDL = [
    """
    CREATE TABLE IF NOT EXISTS stock_intraday_bar (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        minute_time DATETIME NOT NULL,
        open DECIMAL(12,4) DEFAULT NULL,
        high DECIMAL(12,4) DEFAULT NULL,
        low DECIMAL(12,4) DEFAULT NULL,
        close DECIMAL(12,4) DEFAULT NULL,
        avg_price DECIMAL(12,4) DEFAULT NULL,
        volume BIGINT DEFAULT NULL,
        amount DECIMAL(20,2) DEFAULT NULL,
        source VARCHAR(48) NOT NULL DEFAULT 'akshare_stock_zh_a_hist_min_em',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_intraday_bar_minute (code, trade_date, minute_time),
        KEY idx_stock_intraday_bar_date (trade_date),
        KEY idx_stock_intraday_bar_code_date (code, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


def ensure_intraday_bar_schema() -> dict:
    init_mysql_schema()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for ddl in DDL:
                cursor.execute(ddl)
    return {"status": "ok", "tables": ["stock_intraday_bar"]}


if __name__ == "__main__":
    print(ensure_intraday_bar_schema())
