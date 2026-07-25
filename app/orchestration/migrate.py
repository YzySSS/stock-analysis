from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from app.orchestration.adj_factor_schema import ensure_adj_factor_manifest_schema
from app.orchestration.automatic_observation_schema import ensure_automatic_observation_schema
from app.jobs.schema import ensure_job_ops_schema
from app.orchestration.durable_task_schema import ensure_durable_task_schema
from app.orchestration.etf_rotation_schema import ensure_etf_rotation_schema
from app.orchestration.backtest_validation_schema import ensure_backtest_validation_schema
from app.orchestration.feature_cache_schema import ensure_feature_cache_schema
from app.orchestration.forward_observation_schema import ensure_forward_observation_schema
from app.orchestration.fundamental_pit_schema import ensure_fundamental_pit_schema
from app.orchestration.init_project import init_mysql_schema
from app.orchestration.intraday_bar_schema import ensure_intraday_bar_schema
from app.orchestration.index_constituent_pit_schema import ensure_index_constituent_pit_schema
from app.orchestration.market_fund_flow_schema import ensure_market_fund_flow_schema
from app.orchestration.market_opinion_schema import ensure_market_opinion_schema
from app.orchestration.market_scenario_forecast_schema import (
    ensure_market_scenario_forecast_schema,
)
from app.orchestration.market_sentiment_schema import ensure_market_sentiment_schema
from app.orchestration.market_timing_schema import ensure_market_timing_schema
from app.orchestration.market_timing_v19_schema import ensure_market_timing_v19_schema
from app.orchestration.portfolio_schema import ensure_portfolio_schema
from app.orchestration.realtime_moneyflow_schema import ensure_realtime_moneyflow_schema
from app.orchestration.realtime_lifecycle_schema import ensure_realtime_lifecycle_schema
from app.orchestration.realtime_schema import ensure_realtime_schema
from app.orchestration.redundant_index_schema import drop_exact_duplicate_indexes
from app.orchestration.selection_result_schema import ensure_selection_result_version_schema
from app.orchestration.selection_run_schema import ensure_selection_run_schema
from app.orchestration.selection_trade_plan_v4_schema import (
    ensure_selection_trade_plan_v4_schema,
)
from app.orchestration.sentiment_consistency_schema import ensure_sentiment_consistency_schema
from app.orchestration.stock_popularity_schema import ensure_stock_popularity_schema
from app.orchestration.stock_status_pit_schema import ensure_stock_status_pit_schema
from app.orchestration.stock_technical_feature_schema import ensure_stock_technical_feature_schema
from app.orchestration.strategy_factor_ci_schema import ensure_strategy_factor_ci_schema
from app.orchestration.strategy_factor_evaluation_v2_schema import (
    ensure_strategy_factor_evaluation_v2_schema,
)
from app.orchestration.ths_concept_hot_schema import ensure_ths_concept_hot_schema
from app.orchestration.v2_schema import ensure_v2_schema
from app.shared.db import mysql_conn, ping_mysql
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock


MIGRATION_LOCK_NAME = "stock_analysis_schema_migration"

SCHEMA_MIGRATION_DDL = """
CREATE TABLE IF NOT EXISTS schema_migration (
    version VARCHAR(48) NOT NULL PRIMARY KEY,
    name VARCHAR(160) NOT NULL,
    checksum CHAR(64) NOT NULL,
    status VARCHAR(24) NOT NULL,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    execution_ms BIGINT DEFAULT NULL,
    details_json JSON DEFAULT NULL,
    error_message VARCHAR(1000) DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_schema_migration_status (status, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    runner: Callable[[], Any]

    @property
    def checksum(self) -> str:
        # `python -m app.orchestration.migrate` executes this module as
        # `__main__`, while API imports use the package path. Keep the local
        # core wrapper's identity stable across both loading modes. Imported
        # schema runners already retain their canonical module names.
        runner_module = self.runner.__module__
        if self.version == "0001" and self.runner.__name__ == "_run_core":
            runner_module = "__main__"
        payload = f"{self.version}\n{self.name}\n{runner_module}.{self.runner.__name__}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _run_core() -> dict[str, Any]:
    init_mysql_schema()
    return {"status": "ok", "schema": "core"}


MIGRATIONS: tuple[Migration, ...] = (
    Migration("0001", "core tables and stock_basic extensions", _run_core),
    Migration("0002", "V2 factor, trade and backtest schema", ensure_v2_schema),
    Migration("0003", "lowvol feature cache schema", ensure_feature_cache_schema),
    Migration("0004", "selection worker schema", ensure_selection_run_schema),
    Migration("0005", "portfolio and advice schema", ensure_portfolio_schema),
    Migration("0006", "market sentiment and stock news schema", ensure_market_sentiment_schema),
    Migration("0007", "market opinion normalized schema", ensure_market_opinion_schema),
    Migration("0008", "market timing schema", ensure_market_timing_schema),
    Migration("0009", "realtime snapshot, raw and rollup schema", ensure_realtime_schema),
    Migration("0010", "realtime moneyflow schema", ensure_realtime_moneyflow_schema),
    Migration("0011", "market sector fund flow schema", ensure_market_fund_flow_schema),
    Migration("0012", "stock popularity schema", ensure_stock_popularity_schema),
    Migration("0013", "THS concept hot schema", ensure_ths_concept_hot_schema),
    Migration("0014", "intraday bar schema", ensure_intraday_bar_schema),
    Migration("0015", "strategy factor CI schema", ensure_strategy_factor_ci_schema),
    Migration("0016", "worker runtime, job state and retention schema", ensure_job_ops_schema),
    Migration("0017", "point-in-time stock lifecycle and status schema", ensure_stock_status_pit_schema),
    Migration("0018", "point-in-time fundamental announcement schema", ensure_fundamental_pit_schema),
    Migration("0019", "point-in-time index constituent schema", ensure_index_constituent_pit_schema),
    Migration("0020", "frozen out-of-sample validation protocol schema", ensure_backtest_validation_schema),
    Migration("0021", "prospective strategy observation and action schema", ensure_forward_observation_schema),
    Migration("0022", "adjustment factor history manifest schema", ensure_adj_factor_manifest_schema),
    Migration("0023", "sentiment data consistency and operational snapshot schema", ensure_sentiment_consistency_schema),
    Migration("0024", "neutral daily stock technical feature read model", ensure_stock_technical_feature_schema),
    Migration("0025", "durable API asynchronous task queue", ensure_durable_task_schema),
    Migration("0026", "realtime lifecycle source fingerprint schema", ensure_realtime_lifecycle_schema),
    Migration("0027", "selection result strategy lineage and cross-version 14-day statistics", ensure_selection_result_version_schema),
    Migration("0028", "automatic paired strategy observation campaign schema", ensure_automatic_observation_schema),
    Migration("0029", "market timing V1.9 versioned indicator schema", ensure_market_timing_v19_schema),
    Migration("0030", "selection trade-plan V4 shadow and immutable industry snapshot schema", ensure_selection_trade_plan_v4_schema),
    Migration("0031", "point-in-time strategy factor evaluation V2 schema", ensure_strategy_factor_evaluation_v2_schema),
    Migration("0032", "market probability scenario and leadership state schema", ensure_market_scenario_forecast_schema),
    Migration("0033", "research-only industry ETF rotation shadow schema", ensure_etf_rotation_schema),
    Migration("0034", "drop exact duplicate secondary indexes", drop_exact_duplicate_indexes),
)


def _selected_migrations(target: str | None = None) -> tuple[Migration, ...]:
    versions = [migration.version for migration in MIGRATIONS]
    if len(versions) != len(set(versions)):
        raise RuntimeError("duplicate schema migration version")
    if versions != sorted(versions):
        raise RuntimeError("schema migrations must be ordered by version")
    if target is None:
        return MIGRATIONS
    if target not in versions:
        raise ValueError(f"unknown migration target: {target}")
    return tuple(migration for migration in MIGRATIONS if migration.version <= target)


def _migration_table_exists() -> bool:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='schema_migration'
                """
            )
            row = cursor.fetchone() or {}
            return bool(row.get("cnt"))


def _applied_rows() -> dict[str, dict[str, Any]]:
    if not _migration_table_exists():
        return {}
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT version, name, checksum, status, started_at, finished_at,
                       execution_ms, error_message
                FROM schema_migration
                ORDER BY version
                """
            )
            return {str(row["version"]): row for row in (cursor.fetchall() or [])}


def migration_plan(target: str | None = None) -> dict[str, Any]:
    selected = _selected_migrations(target)
    applied = _applied_rows()
    items = []
    for migration in selected:
        row = applied.get(migration.version)
        recorded_checksum = row.get("checksum") if row else None
        checksum_matches = recorded_checksum in {None, migration.checksum}
        status = "applied" if row and row.get("status") == "success" and checksum_matches else "pending"
        if row and row.get("status") == "success" and not checksum_matches:
            status = "checksum_mismatch"
        elif row and row.get("status") == "failed":
            status = "failed"
        items.append(
            {
                "version": migration.version,
                "name": migration.name,
                "status": status,
                "checksum": migration.checksum,
                "recorded_checksum": recorded_checksum,
                "error_message": row.get("error_message") if row else None,
            }
        )
    pending = [item for item in items if item["status"] != "applied"]
    return {
        "database": ping_mysql().get("db"),
        "target": target or selected[-1].version,
        "total": len(items),
        "applied": len(items) - len(pending),
        "pending": len(pending),
        "ready": not pending,
        "items": items,
    }


def _ensure_migration_table() -> None:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(SCHEMA_MIGRATION_DDL)


def _mark_started(migration: Migration) -> None:
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO schema_migration (
                    version, name, checksum, status, started_at, finished_at,
                    execution_ms, details_json, error_message
                ) VALUES (%s,%s,%s,'running',NOW(),NULL,NULL,NULL,NULL)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name), checksum=VALUES(checksum), status='running',
                    started_at=NOW(), finished_at=NULL, execution_ms=NULL,
                    details_json=NULL, error_message=NULL
                """,
                (migration.version, migration.name, migration.checksum),
            )


def _mark_finished(
    migration: Migration,
    *,
    status: str,
    execution_ms: int,
    details: Any = None,
    error_message: str | None = None,
) -> None:
    details_json = json.dumps(details, ensure_ascii=False, default=str) if details is not None else None
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE schema_migration
                SET status=%s, finished_at=NOW(), execution_ms=%s,
                    details_json=%s, error_message=%s
                WHERE version=%s AND checksum=%s
                """,
                (status, execution_ms, details_json, error_message, migration.version, migration.checksum),
            )


def apply_migrations(target: str | None = None) -> dict[str, Any]:
    selected = _selected_migrations(target)
    lock_handle = acquire_mysql_advisory_lock(MIGRATION_LOCK_NAME)
    if lock_handle is None:
        raise RuntimeError("another schema migration is already running")
    started_at = datetime.now()
    applied_now: list[dict[str, Any]] = []
    try:
        _ensure_migration_table()
        existing = _applied_rows()
        for migration in selected:
            row = existing.get(migration.version)
            if row and row.get("status") == "success":
                if row.get("checksum") != migration.checksum:
                    raise RuntimeError(f"checksum mismatch for applied migration {migration.version}")
                continue

            _mark_started(migration)
            step_started = time.monotonic()
            try:
                details = migration.runner()
            except Exception as exc:
                execution_ms = int((time.monotonic() - step_started) * 1000)
                _mark_finished(
                    migration,
                    status="failed",
                    execution_ms=execution_ms,
                    error_message=f"{type(exc).__name__}: {str(exc)[:900]}",
                )
                raise
            execution_ms = int((time.monotonic() - step_started) * 1000)
            _mark_finished(
                migration,
                status="success",
                execution_ms=execution_ms,
                details=details,
            )
            applied_now.append(
                {"version": migration.version, "name": migration.name, "execution_ms": execution_ms}
            )
            existing[migration.version] = {"status": "success", "checksum": migration.checksum}

        plan = migration_plan(target)
        return {
            "status": "success",
            "started_at": started_at.isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "applied_now": applied_now,
            "plan": plan,
        }
    finally:
        release_mysql_advisory_lock(lock_handle)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan, apply or check stock-analysis schema migrations")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--apply", action="store_true", help="apply pending migrations; default is read-only plan")
    action.add_argument("--check", action="store_true", help="exit non-zero when migrations are pending")
    parser.add_argument("--target", help="stop at an exact migration version")
    parser.add_argument("--pretty", action="store_true", help="pretty-print JSON")
    args = parser.parse_args(argv)

    try:
        result = apply_migrations(args.target) if args.apply else migration_plan(args.target)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2 if args.pretty else None))
        if args.check and not result.get("ready"):
            return 1
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {"status": "failed", "error": f"{type(exc).__name__}: {str(exc)[:1000]}"},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
