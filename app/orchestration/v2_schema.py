from __future__ import annotations

from app.shared.db import mysql_conn
import json


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
    "source": "ALTER TABLE factor_input_daily ADD COLUMN source VARCHAR(32) DEFAULT 'tushare_daily_basic' AFTER completeness_score",
}

FACTOR_INPUT_DAILY_INDEXES: dict[str, str] = {
    "idx_factor_input_period": "ALTER TABLE factor_input_daily ADD KEY idx_factor_input_period (fundamental_period)",
}

BACKTEST_RUN_COLUMNS: dict[str, str] = {
    "trade_strategy_id": "ALTER TABLE backtest_run ADD COLUMN trade_strategy_id VARCHAR(64) DEFAULT NULL AFTER strategy_id",
    "evaluation_mode": "ALTER TABLE backtest_run ADD COLUMN evaluation_mode VARCHAR(32) NOT NULL DEFAULT 'research' AFTER return_mode",
    "methodology_version": "ALTER TABLE backtest_run ADD COLUMN methodology_version VARCHAR(64) NOT NULL DEFAULT 'legacy_pre_point_in_time_v1' AFTER evaluation_mode",
    "data_cutoff_date": "ALTER TABLE backtest_run ADD COLUMN data_cutoff_date DATE DEFAULT NULL AFTER methodology_version",
    "strategy_config_hash": "ALTER TABLE backtest_run ADD COLUMN strategy_config_hash CHAR(64) DEFAULT NULL AFTER data_cutoff_date",
    "methodology_json": "ALTER TABLE backtest_run ADD COLUMN methodology_json JSON DEFAULT NULL AFTER strategy_config_hash",
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
    "idempotency_key": "ALTER TABLE backtest_run ADD COLUMN idempotency_key CHAR(64) DEFAULT NULL AFTER status",
    "active_idempotency_key": "ALTER TABLE backtest_run ADD COLUMN active_idempotency_key CHAR(64) DEFAULT NULL AFTER idempotency_key",
    "attempt_count": "ALTER TABLE backtest_run ADD COLUMN attempt_count INT NOT NULL DEFAULT 0 AFTER cancel_requested",
    "max_attempts": "ALTER TABLE backtest_run ADD COLUMN max_attempts INT NOT NULL DEFAULT 2 AFTER attempt_count",
    "phase": "ALTER TABLE backtest_run ADD COLUMN phase VARCHAR(64) DEFAULT NULL AFTER max_attempts",
    "error_code": "ALTER TABLE backtest_run ADD COLUMN error_code VARCHAR(64) DEFAULT NULL AFTER summary_json",
    "is_system_test": "ALTER TABLE backtest_run ADD COLUMN is_system_test TINYINT(1) NOT NULL DEFAULT 0 AFTER cancel_requested",
    "validation_baseline_id": "ALTER TABLE backtest_run ADD COLUMN validation_baseline_id VARCHAR(80) DEFAULT NULL AFTER is_system_test",
    "commission_bps": "ALTER TABLE backtest_run ADD COLUMN commission_bps DECIMAL(10,4) NOT NULL DEFAULT 0 AFTER use_adjusted_price",
    "stamp_tax_bps": "ALTER TABLE backtest_run ADD COLUMN stamp_tax_bps DECIMAL(10,4) NOT NULL DEFAULT 0 AFTER commission_bps",
    "slippage_bps": "ALTER TABLE backtest_run ADD COLUMN slippage_bps DECIMAL(10,4) NOT NULL DEFAULT 0 AFTER stamp_tax_bps",
    "execution_constraints_enabled": "ALTER TABLE backtest_run ADD COLUMN execution_constraints_enabled TINYINT(1) NOT NULL DEFAULT 0 AFTER slippage_bps",
}

BACKTEST_RUN_INDEXES: dict[str, str] = {
    "idx_backtest_validation_baseline": "ALTER TABLE backtest_run ADD KEY idx_backtest_validation_baseline (validation_baseline_id, strategy_id, id)",
    "uniq_backtest_active_idempotency": "ALTER TABLE backtest_run ADD UNIQUE KEY uniq_backtest_active_idempotency (active_idempotency_key)",
    "idx_backtest_claim": "ALTER TABLE backtest_run ADD KEY idx_backtest_claim (status, cancel_requested, id)",
    "idx_backtest_stale": "ALTER TABLE backtest_run ADD KEY idx_backtest_stale (status, worker_heartbeat_at)",
    "idx_backtest_idempotency": "ALTER TABLE backtest_run ADD KEY idx_backtest_idempotency (idempotency_key)",
}

V21_TABLE_DDL: dict[str, str] = {
    "trade_strategy": """
    CREATE TABLE IF NOT EXISTS trade_strategy (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        strategy_id VARCHAR(64) NOT NULL,
        display_name VARCHAR(128) NOT NULL,
        version VARCHAR(32) NOT NULL DEFAULT 'v1',
        status VARCHAR(32) NOT NULL DEFAULT 'active',
        is_builtin TINYINT(1) NOT NULL DEFAULT 0,
        description TEXT DEFAULT NULL,
        buy_rule_json JSON NOT NULL,
        sell_rule_json JSON NOT NULL,
        risk_rule_json JSON DEFAULT NULL,
        cost_rule_json JSON DEFAULT NULL,
        execution_rule_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_trade_strategy_version (strategy_id, version),
        KEY idx_trade_strategy_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "backtest_trade_order": """
    CREATE TABLE IF NOT EXISTS backtest_trade_order (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(128) NOT NULL,
        selection_strategy_id VARCHAR(64) NOT NULL,
        trade_strategy_id VARCHAR(64) NOT NULL,
        trade_date DATE NOT NULL,
        code VARCHAR(16) NOT NULL,
        name VARCHAR(64) DEFAULT NULL,
        rank_no INT DEFAULT NULL,
        score DECIMAL(12,4) DEFAULT NULL,
        order_side VARCHAR(16) NOT NULL,
        order_status VARCHAR(32) NOT NULL DEFAULT 'planned',
        failure_reason VARCHAR(128) DEFAULT NULL,
        planned_trade_date DATE DEFAULT NULL,
        planned_price_type VARCHAR(32) DEFAULT NULL,
        planned_price DECIMAL(14,4) DEFAULT NULL,
        executed_trade_date DATE DEFAULT NULL,
        executed_price_type VARCHAR(32) DEFAULT NULL,
        executed_price DECIMAL(14,4) DEFAULT NULL,
        executed_quantity DECIMAL(20,4) DEFAULT NULL,
        executed_amount DECIMAL(20,4) DEFAULT NULL,
        fee_amount DECIMAL(20,4) DEFAULT NULL,
        stamp_tax_amount DECIMAL(20,4) DEFAULT NULL,
        slippage_amount DECIMAL(20,4) DEFAULT NULL,
        rule_snapshot_json JSON DEFAULT NULL,
        decision_reason_json JSON DEFAULT NULL,
        market_context_json JSON DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_bto_run (run_id),
        KEY idx_bto_code_date (code, trade_date),
        KEY idx_bto_status (order_status),
        KEY idx_bto_strategy (selection_strategy_id, trade_strategy_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "backtest_trade_analysis": """
    CREATE TABLE IF NOT EXISTS backtest_trade_analysis (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(128) NOT NULL,
        analysis_scope VARCHAR(32) NOT NULL DEFAULT 'run',
        code VARCHAR(16) DEFAULT NULL,
        trade_date DATE DEFAULT NULL,
        summary TEXT DEFAULT NULL,
        buy_reason TEXT DEFAULT NULL,
        sell_reason TEXT DEFAULT NULL,
        failure_analysis TEXT DEFAULT NULL,
        optimization_suggestion TEXT DEFAULT NULL,
        metrics_json JSON DEFAULT NULL,
        reason_json JSON DEFAULT NULL,
        generated_by VARCHAR(32) NOT NULL DEFAULT 'rule',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_bta_run (run_id),
        KEY idx_bta_code_date (code, trade_date),
        KEY idx_bta_scope (analysis_scope)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
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


def seed_trade_strategies() -> int:
    strategies = [
        {
            "strategy_id": "next_open_1d",
            "display_name": "隔日开盘卖出",
            "version": "v1",
            "description": "T 日收盘形成信号，T+1 开盘买入，再下一交易日开盘卖出。",
            "buy_rule_json": {"entry_day": "next_trading_day", "entry_price": "open"},
            "sell_rule_json": {"exit_day_offset": 1, "exit_price": "open"},
        },
        {
            "strategy_id": "hold_3d_close",
            "display_name": "持有 3 日收盘卖出",
            "version": "v1",
            "description": "T 日收盘形成信号，T+1 开盘买入，含入场日在内持有 3 个交易日后收盘卖出。",
            "buy_rule_json": {"entry_day": "next_trading_day", "entry_price": "open"},
            "sell_rule_json": {"exit_day_offset": 2, "exit_price": "close"},
        },
        {
            "strategy_id": "triple_barrier_5d",
            "display_name": "五日止盈止损",
            "version": "v1",
            "description": "T 日收盘形成信号，T+1 开盘买入，最多持有 5 个交易日；触及 +6% 止盈、-3% 止损或到期收盘，谁先发生就卖出。",
            "buy_rule_json": {"entry_day": "next_trading_day", "entry_price": "open"},
            "sell_rule_json": {"take_profit_pct": 6, "stop_loss_pct": -3, "max_holding_days": 5, "time_exit_price": "close"},
        },
        {
            "strategy_id": "observe_t3_daily",
            "display_name": "T+3 每日观察回测",
            "version": "v1",
            "description": "T 日收盘形成信号，T+1 开盘买入；观察入场日、入场+1、入场+2 的收盘价、最大浮盈和最大回撤。",
            "buy_rule_json": {"entry_day": "next_trading_day", "entry_price": "open"},
            "sell_rule_json": {"observe_days": [0, 1, 2], "summary_exit_day_offset": 2, "summary_exit_price": "close", "purpose": "selection_strategy_diagnostics"},
        },
    ]
    sql = """
    INSERT INTO trade_strategy (
        strategy_id, display_name, version, status, is_builtin, description,
        buy_rule_json, sell_rule_json, risk_rule_json, cost_rule_json, execution_rule_json
    ) VALUES (%s, %s, %s, 'active', 1, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        display_name=VALUES(display_name), status='active', is_builtin=1, description=VALUES(description),
        buy_rule_json=VALUES(buy_rule_json), sell_rule_json=VALUES(sell_rule_json),
        risk_rule_json=VALUES(risk_rule_json), cost_rule_json=VALUES(cost_rule_json), execution_rule_json=VALUES(execution_rule_json)
    """
    rows = [
        (
            item["strategy_id"],
            item["display_name"],
            item["version"],
            item["description"],
            json.dumps(item["buy_rule_json"], ensure_ascii=False),
            json.dumps(item["sell_rule_json"], ensure_ascii=False),
            json.dumps({}, ensure_ascii=False),
            json.dumps({"enabled": False}, ensure_ascii=False),
            json.dumps({"enabled": False}, ensure_ascii=False),
        )
        for item in strategies
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
            return cursor.rowcount


def ensure_v2_schema() -> dict:
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
    backtest_indexes = _existing_indexes("backtest_run")
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for index_name, sql in BACKTEST_RUN_INDEXES.items():
                if index_name not in backtest_indexes:
                    cursor.execute(sql)
                    applied.append(index_name)
    seed_trade_strategies()
    return {"status": "ok", "applied": applied}
