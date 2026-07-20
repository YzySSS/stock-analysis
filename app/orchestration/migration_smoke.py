from __future__ import annotations

import argparse
import json

from app.orchestration.migrate import apply_migrations, migration_plan
from app.shared.db import mysql_conn, ping_mysql


SMOKE_DATABASE_BASENAME = "stock_migration_smoke"
REQUIRED_TABLES = {
    "schema_migration",
    "stock_basic",
    "daily_kline",
    "factor_input_daily",
    "selection_result",
    "selection_run",
    "backtest_run",
    "portfolio_position",
    "portfolio_advice_run",
    "market_context_daily",
    "stock_news",
    "stock_instrument_lifecycle",
    "stock_name_history",
    "stock_suspension_daily",
    "stock_status_pit_manifest",
    "stock_fundamental_pit",
    "fundamental_pit_manifest",
    "index_constituent_pit",
    "index_constituent_pit_manifest",
    "strategy_validation_protocol",
    "strategy_forward_protocol",
    "strategy_forward_observation",
    "strategy_forward_pick",
    "strategy_forward_action",
    "sector_opinion_daily",
    "sector_opinion_stock",
    "market_timing_signal_daily",
    "stock_realtime_snapshot",
    "stock_realtime_intraday",
    "stock_realtime_bar_rollup",
    "stock_realtime_rollup_manifest",
    "lowvol_reversal_feature_daily",
    "worker_runtime_heartbeat",
    "task_run_daily_summary",
    "job_error_daily_summary",
}


def validate_smoke_database_name(actual: str | None, expected: str) -> None:
    has_safe_name = expected == SMOKE_DATABASE_BASENAME or (
        expected.startswith(f"{SMOKE_DATABASE_BASENAME}_")
        and len(expected) > len(SMOKE_DATABASE_BASENAME) + 1
    )
    if not has_safe_name:
        raise ValueError(
            f"smoke database must be {SMOKE_DATABASE_BASENAME} "
            f"or start with {SMOKE_DATABASE_BASENAME}_"
        )
    if actual != expected:
        raise RuntimeError(f"connected database {actual!r} does not match expected smoke database {expected!r}")


def _table_names() -> set[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE()
                """
            )
            return {str(row["TABLE_NAME"]) for row in (cursor.fetchall() or [])}


def run_empty_database_smoke(expected_database: str) -> dict:
    database_info = ping_mysql()
    actual_database = database_info.get("db")
    validate_smoke_database_name(actual_database, expected_database)
    before = _table_names()
    if before:
        raise RuntimeError(
            f"smoke database must be empty before first run; found {len(before)} tables"
        )

    first = apply_migrations()
    after = _table_names()
    missing = sorted(REQUIRED_TABLES - after)
    if missing:
        raise RuntimeError(f"empty database migration is missing required tables: {missing}")
    first_plan = migration_plan()
    if not first_plan.get("ready"):
        raise RuntimeError("schema is not ready after empty database migration")

    second = apply_migrations()
    if second.get("applied_now"):
        raise RuntimeError("second migration run was not idempotent")

    return {
        "status": "success",
        "database": actual_database,
        "server_version": database_info.get("version"),
        "table_count": len(after),
        "required_tables": len(REQUIRED_TABLES),
        "migration_total": first_plan.get("total"),
        "migration_applied": first_plan.get("applied"),
        "first_applied_now": len(first.get("applied_now") or []),
        "second_applied_now": len(second.get("applied_now") or []),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the unified migration twice against an explicitly provisioned empty smoke database"
    )
    parser.add_argument(
        "--database",
        required=True,
        help=(
            f"must be {SMOKE_DATABASE_BASENAME} "
            f"or start with {SMOKE_DATABASE_BASENAME}_"
        ),
    )
    args = parser.parse_args()
    try:
        print(json.dumps(run_empty_database_smoke(args.database), ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {"status": "failed", "error": f"{type(exc).__name__}: {str(exc)[:1000]}"},
                ensure_ascii=False,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
