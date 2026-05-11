from __future__ import annotations

from app.orchestration.init_project import init_mysql_schema
from app.shared.db import mysql_conn


FACTOR_INPUT_DAILY_COLUMNS: dict[str, str] = {
    "turnover_rate": "ALTER TABLE factor_input_daily ADD COLUMN turnover_rate DECIMAL(12,4) DEFAULT NULL AFTER pb_tushare",
    "turnover_rate_f": "ALTER TABLE factor_input_daily ADD COLUMN turnover_rate_f DECIMAL(12,4) DEFAULT NULL AFTER turnover_rate",
    "volume_ratio": "ALTER TABLE factor_input_daily ADD COLUMN volume_ratio DECIMAL(12,4) DEFAULT NULL AFTER turnover_rate_f",
    "total_mv": "ALTER TABLE factor_input_daily ADD COLUMN total_mv DECIMAL(20,4) DEFAULT NULL AFTER volume_ratio",
    "circ_mv": "ALTER TABLE factor_input_daily ADD COLUMN circ_mv DECIMAL(20,4) DEFAULT NULL AFTER total_mv",
    "fundamental_publish_date": "ALTER TABLE factor_input_daily ADD COLUMN fundamental_publish_date DATE DEFAULT NULL AFTER fundamental_period",
    "valuation_source": "ALTER TABLE factor_input_daily ADD COLUMN valuation_source VARCHAR(32) DEFAULT NULL AFTER fundamental_publish_date",
    "fundamental_source": "ALTER TABLE factor_input_daily ADD COLUMN fundamental_source VARCHAR(32) DEFAULT NULL AFTER valuation_source",
    "valuation_updated_at": "ALTER TABLE factor_input_daily ADD COLUMN valuation_updated_at DATETIME DEFAULT NULL AFTER fundamental_source",
    "fundamental_updated_at": "ALTER TABLE factor_input_daily ADD COLUMN fundamental_updated_at DATETIME DEFAULT NULL AFTER valuation_updated_at",
    "completeness_score": "ALTER TABLE factor_input_daily ADD COLUMN completeness_score DECIMAL(8,4) DEFAULT NULL AFTER fundamental_updated_at",
}

FACTOR_INPUT_DAILY_INDEXES: dict[str, str] = {
    "idx_factor_input_period": "ALTER TABLE factor_input_daily ADD KEY idx_factor_input_period (fundamental_period)",
}

BACKTEST_RUN_COLUMNS: dict[str, str] = {
    "progress_total_days": "ALTER TABLE backtest_run ADD COLUMN progress_total_days INT DEFAULT 0 AFTER total_trades",
    "progress_done_days": "ALTER TABLE backtest_run ADD COLUMN progress_done_days INT DEFAULT 0 AFTER progress_total_days",
    "progress_pct": "ALTER TABLE backtest_run ADD COLUMN progress_pct DECIMAL(8,4) DEFAULT 0 AFTER progress_done_days",
    "current_trade_date": "ALTER TABLE backtest_run ADD COLUMN current_trade_date DATE DEFAULT NULL AFTER progress_pct",
    "estimated_seconds_left": "ALTER TABLE backtest_run ADD COLUMN estimated_seconds_left INT DEFAULT NULL AFTER current_trade_date",
    "total_return_pct": "ALTER TABLE backtest_run ADD COLUMN total_return_pct DECIMAL(12,4) DEFAULT NULL AFTER estimated_seconds_left",
    "avg_return_pct": "ALTER TABLE backtest_run ADD COLUMN avg_return_pct DECIMAL(12,4) DEFAULT NULL AFTER total_return_pct",
    "max_drawdown_pct": "ALTER TABLE backtest_run ADD COLUMN max_drawdown_pct DECIMAL(12,4) DEFAULT NULL AFTER avg_return_pct",
    "win_rate_pct": "ALTER TABLE backtest_run ADD COLUMN win_rate_pct DECIMAL(12,4) DEFAULT NULL AFTER max_drawdown_pct",
    "worker_id": "ALTER TABLE backtest_run ADD COLUMN worker_id VARCHAR(128) DEFAULT NULL AFTER status",
    "locked_at": "ALTER TABLE backtest_run ADD COLUMN locked_at DATETIME DEFAULT NULL AFTER worker_id",
    "worker_heartbeat_at": "ALTER TABLE backtest_run ADD COLUMN worker_heartbeat_at DATETIME DEFAULT NULL AFTER locked_at",
    "cancel_requested": "ALTER TABLE backtest_run ADD COLUMN cancel_requested TINYINT(1) NOT NULL DEFAULT 0 AFTER worker_heartbeat_at",
    "is_system_test": "ALTER TABLE backtest_run ADD COLUMN is_system_test TINYINT(1) NOT NULL DEFAULT 0 AFTER cancel_requested",
}

V21_TABLE_DDL: dict[str, str] = {
    "adj_factor_daily": """
    CREATE TABLE IF NOT EXISTS adj_factor_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        adj_factor DECIMAL(20,8) NOT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare_adj_factor',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_adj_factor_daily (code, trade_date),
        KEY idx_adj_factor_trade_date (trade_date),
        KEY idx_adj_factor_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "stock_moneyflow_daily": """
    CREATE TABLE IF NOT EXISTS stock_moneyflow_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        buy_sm_vol BIGINT DEFAULT NULL,
        buy_sm_amount DECIMAL(20,4) DEFAULT NULL,
        sell_sm_vol BIGINT DEFAULT NULL,
        sell_sm_amount DECIMAL(20,4) DEFAULT NULL,
        buy_md_vol BIGINT DEFAULT NULL,
        buy_md_amount DECIMAL(20,4) DEFAULT NULL,
        sell_md_vol BIGINT DEFAULT NULL,
        sell_md_amount DECIMAL(20,4) DEFAULT NULL,
        buy_lg_vol BIGINT DEFAULT NULL,
        buy_lg_amount DECIMAL(20,4) DEFAULT NULL,
        sell_lg_vol BIGINT DEFAULT NULL,
        sell_lg_amount DECIMAL(20,4) DEFAULT NULL,
        buy_elg_vol BIGINT DEFAULT NULL,
        buy_elg_amount DECIMAL(20,4) DEFAULT NULL,
        sell_elg_vol BIGINT DEFAULT NULL,
        sell_elg_amount DECIMAL(20,4) DEFAULT NULL,
        net_mf_vol BIGINT DEFAULT NULL,
        net_mf_amount DECIMAL(20,4) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare_moneyflow',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_moneyflow_daily (code, trade_date),
        KEY idx_moneyflow_trade_date (trade_date),
        KEY idx_moneyflow_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "stock_chip_daily": """
    CREATE TABLE IF NOT EXISTS stock_chip_daily (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        his_low DECIMAL(12,4) DEFAULT NULL,
        his_high DECIMAL(12,4) DEFAULT NULL,
        cost_5pct DECIMAL(12,4) DEFAULT NULL,
        cost_15pct DECIMAL(12,4) DEFAULT NULL,
        cost_50pct DECIMAL(12,4) DEFAULT NULL,
        cost_85pct DECIMAL(12,4) DEFAULT NULL,
        cost_95pct DECIMAL(12,4) DEFAULT NULL,
        weight_avg DECIMAL(12,4) DEFAULT NULL,
        winner_rate DECIMAL(12,4) DEFAULT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'tushare_cyq_perf',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_stock_chip_daily (code, trade_date),
        KEY idx_chip_trade_date (trade_date),
        KEY idx_chip_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
}


def _existing_columns(table: str) -> set[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM " + table)
            return {row["Field"] for row in cursor.fetchall()}


def _existing_indexes(table: str) -> set[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW INDEX FROM " + table)
            return {row["Key_name"] for row in cursor.fetchall()}


def ensure_v2_schema() -> dict:
    init_mysql_schema()
    applied: list[str] = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for table_name, ddl in V21_TABLE_DDL.items():
                cursor.execute(ddl)
                applied.append(table_name)
    columns = _existing_columns("factor_input_daily")
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for column, sql in FACTOR_INPUT_DAILY_COLUMNS.items():
                if column not in columns:
                    cursor.execute(sql)
                    applied.append(column)
    indexes = _existing_indexes("factor_input_daily")
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for index_name, sql in FACTOR_INPUT_DAILY_INDEXES.items():
                if index_name not in indexes:
                    cursor.execute(sql)
                    applied.append(index_name)
    backtest_columns = _existing_columns("backtest_run")
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for column, sql in BACKTEST_RUN_COLUMNS.items():
                if column not in backtest_columns:
                    cursor.execute(sql)
                    applied.append(column)
    return {"status": "ok", "applied": applied}


if __name__ == "__main__":
    print(ensure_v2_schema())
