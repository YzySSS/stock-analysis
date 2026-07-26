from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.market_timing.scenario_forecast import MarketScenarioForecastRepository
from app.shared.db import mysql_read_conn
from app.shared.mysql_lock import (
    acquire_mysql_advisory_lock,
    release_mysql_advisory_lock,
)
from app.shared.task_log import TaskRunLogger


TASK_NAME = "market_scenario_forecast_shadow_update"
LOCK_NAME = "stock_analysis_market_scenario_forecast_shadow_update"


def _latest_timing_date() -> date:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(trade_date) AS trade_date
                FROM market_timing_signal_daily
                WHERE model_id='market_timing_v20'
                """
            )
            value = (cursor.fetchone() or {}).get("trade_date")
    if value is None:
        raise RuntimeError("market timing V2.0 has no materialized date")
    return value


def run(
    *,
    trade_date: date | str,
    horizons: list[int],
    outcome_limit: int,
) -> dict[str, Any]:
    repository = MarketScenarioForecastRepository()
    outcomes = repository.refresh_outcomes(limit=outcome_limit)
    materialized = repository.materialize(trade_date, horizons=horizons)
    return {
        "status": "success",
        "outcomes": outcomes,
        "materialized": {
            "trade_date": materialized["trade_date"],
            "forecast_count": materialized["forecast_count"],
            "created_forecast_count": materialized[
                "created_forecast_count"
            ],
            "reused_forecast_count": materialized[
                "reused_forecast_count"
            ],
            "leadership_count": materialized["leadership_count"],
            "validation": [
                {
                    "horizon_days": item["horizon_days"],
                    "status": item["validation_status"],
                    "probability_display_allowed": item[
                        "probability_display_allowed"
                    ],
                }
                for item in materialized["forecasts"]
            ],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Materialize research-only market scenario probabilities"
    )
    parser.add_argument("--trade-date")
    parser.add_argument("--horizons", default="1,5,20")
    parser.add_argument("--outcome-limit", type=int, default=200)
    args = parser.parse_args()
    horizons = sorted(
        {
            int(value.strip())
            for value in str(args.horizons).split(",")
            if value.strip()
        }
    )
    if not horizons or min(horizons) <= 0:
        raise ValueError("horizons must contain positive integers")
    trade_date = (
        date.fromisoformat(args.trade_date)
        if args.trade_date
        else _latest_timing_date()
    )
    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {
        "trade_date": str(trade_date),
        "horizons": horizons,
        "outcome_limit": args.outcome_limit,
        "research_only": True,
    }
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, metadata)
    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        result = {
            "status": "skipped",
            "reason": "another scenario forecast run owns the lock",
            "run_id": run_id,
        }
        logger.finish(TASK_NAME, run_id, "success", None, result)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    try:
        result = run(
            trade_date=trade_date,
            horizons=horizons,
            outcome_limit=max(1, args.outcome_limit),
        )
        result["run_id"] = run_id
        logger.finish(TASK_NAME, run_id, "success", None, result)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        error = f"{type(exc).__name__}: {str(exc)[:1000]}"
        logger.finish(TASK_NAME, run_id, "failed", error, metadata)
        print(
            json.dumps(
                {"status": "failed", "run_id": run_id, "error": error},
                ensure_ascii=False,
            )
        )
        return 1
    finally:
        release_mysql_advisory_lock(lock_handle)


if __name__ == "__main__":
    raise SystemExit(main())
