from __future__ import annotations

import json

from app.shared.db import mysql_conn
from app.stock_selection.turtle_trade_plan import (
    load_turtle_trade_plan_spec,
    turtle_trade_plan_spec_hash,
)


SENTIMENT_CANDIDATE_COLUMN_MIGRATIONS = {
    "industry": (
        "ALTER TABLE sentiment_candidate_snapshot "
        "ADD COLUMN industry VARCHAR(128) DEFAULT NULL AFTER name"
    ),
}

IMMUTABLE_INDUSTRY_BACKFILL_SQL = """
UPDATE sentiment_candidate_snapshot
SET industry = NULLIF(
    TRIM(
        JSON_UNQUOTE(
            JSON_EXTRACT(explain_json, '$.raw_metrics.industry')
        )
    ),
    ''
)
WHERE industry IS NULL
  AND JSON_TYPE(
      JSON_EXTRACT(explain_json, '$.raw_metrics.industry')
  ) = 'STRING'
"""

SELECTION_TRADE_PLAN_EVENT_DDL = """
CREATE TABLE IF NOT EXISTS selection_trade_plan_event (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    plan_id VARCHAR(128) NOT NULL,
    selection_result_id BIGINT DEFAULT NULL,
    snapshot_id VARCHAR(96) DEFAULT NULL,
    code VARCHAR(16) NOT NULL,
    trade_plan_version VARCHAR(64) NOT NULL,
    spec_hash CHAR(64) DEFAULT NULL,
    event_time DATETIME NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    planned_price DECIMAL(16,4) DEFAULT NULL,
    observed_price DECIMAL(16,4) DEFAULT NULL,
    executable TINYINT(1) NOT NULL DEFAULT 0,
    block_reason VARCHAR(160) DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_selection_trade_plan_event (
        plan_id, event_time, event_type
    ),
    KEY idx_selection_trade_plan_event_result (
        selection_result_id, event_time
    ),
    KEY idx_selection_trade_plan_event_snapshot (
        snapshot_id, code, event_time
    ),
    KEY idx_selection_trade_plan_event_type (
        event_type, executable, event_time
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _existing_columns(table: str) -> set[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS FROM {table}")
            return {str(row["Field"]) for row in cursor.fetchall()}


def _seed_turtle_trade_strategy() -> None:
    spec = load_turtle_trade_plan_spec()
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO trade_strategy (
                    strategy_id, display_name, version, status, is_builtin,
                    description, buy_rule_json, sell_rule_json, risk_rule_json,
                    cost_rule_json, execution_rule_json
                ) VALUES (
                    %s, %s, %s, 'research', 1,
                    %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    display_name=VALUES(display_name),
                    status='research',
                    is_builtin=1,
                    description=VALUES(description),
                    buy_rule_json=VALUES(buy_rule_json),
                    sell_rule_json=VALUES(sell_rule_json),
                    risk_rule_json=VALUES(risk_rule_json),
                    cost_rule_json=VALUES(cost_rule_json),
                    execution_rule_json=VALUES(execution_rule_json)
                """,
                (
                    spec["spec_id"],
                    "海龟V4研究回测",
                    "v1",
                    "选股后等待20日高点突破/回踩触发，以N20控制止损、单位风险和盈利加仓；五日无0.5N进展退出，盈利趋势继续跟踪；仅研究，不自动交易。",
                    json.dumps(spec["entry"], ensure_ascii=False),
                    json.dumps(spec["exit"], ensure_ascii=False),
                    json.dumps(
                        {
                            **spec["risk"],
                            **spec["portfolio"],
                            "account_drawdown": spec["account_drawdown"],
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps({"use_request_costs": True}, ensure_ascii=False),
                    json.dumps(
                        {
                            **spec["decision_contract"],
                            "spec_hash": turtle_trade_plan_spec_hash(),
                        },
                        ensure_ascii=False,
                    ),
                ),
            )


def ensure_selection_trade_plan_v4_schema() -> dict:
    applied: list[str] = []
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(SELECTION_TRADE_PLAN_EVENT_DDL)
    applied.append("selection_trade_plan_event")

    columns = _existing_columns("sentiment_candidate_snapshot")
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            for column, ddl in SENTIMENT_CANDIDATE_COLUMN_MIGRATIONS.items():
                if column not in columns:
                    cursor.execute(ddl)
                    applied.append(
                        f"sentiment_candidate_snapshot.{column}"
                    )
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            industry_backfilled = int(
                cursor.execute(IMMUTABLE_INDUSTRY_BACKFILL_SQL) or 0
            )
    _seed_turtle_trade_strategy()
    return {
        "status": "ok",
        "applied": applied,
        "industry_backfilled_from_immutable_snapshot_json": industry_backfilled,
        "seeded_trade_strategy": "turtle_selection_risk_v1",
    }
