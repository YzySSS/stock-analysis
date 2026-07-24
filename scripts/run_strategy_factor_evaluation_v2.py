from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.mysql_lock import (
    acquire_mysql_advisory_lock,
    release_mysql_advisory_lock,
)
from app.shared.task_log import TaskRunLogger
from app.stock_selection.factor_evaluation_v2 import (
    StrategyFactorEvaluationRepository,
)
from app.strategies.service import StrategyService


TASK_NAME = "strategy_factor_evaluation_v2"
LOCK_NAME = "stock_analysis_strategy_factor_evaluation_v2"


def build_run_id() -> str:
    return f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def run(
    *,
    strategy_id: str,
    horizons: list[int],
    backfill_selected_history: bool,
    history_limit: int,
    manifest_limit: int,
) -> dict[str, Any]:
    meta = StrategyService().get_strategy_meta(strategy_id)
    strategy_version = str(meta.get("version") or "")
    if not strategy_version:
        raise ValueError(f"strategy version is missing: {strategy_id}")
    repository = StrategyFactorEvaluationRepository()
    history = (
        repository.backfill_selected_only_history(
            strategy_id=strategy_id,
            limit=history_limit,
        )
        if backfill_selected_history
        else {"status": "skipped", "reason": "disabled"}
    )
    outcomes = repository.refresh_outcomes(
        horizons=horizons,
        manifest_limit=manifest_limit,
    )
    evaluations = repository.run_evaluations(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        horizons=horizons,
    )
    summary = repository.latest_summary(
        strategy_id,
        strategy_version=strategy_version,
        horizon_days=5 if 5 in horizons else horizons[0],
    )
    return {
        "status": "success",
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "history": history,
        "outcomes": outcomes,
        "evaluations": evaluations,
        "latest_summary": {
            "status": summary.get("status"),
            "trace_summary": summary.get("trace_summary"),
            "evaluation_count": len(summary.get("evaluations") or []),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh immutable strategy factor labels and V2 research evaluation"
    )
    parser.add_argument(
        "--strategy-id",
        default="a_share_sentiment_v05",
    )
    parser.add_argument(
        "--horizons",
        default="1,3,5,10,20",
        help="comma-separated market trading-day horizons",
    )
    parser.add_argument(
        "--skip-selected-history-backfill",
        action="store_true",
    )
    parser.add_argument("--history-limit", type=int, default=1000)
    parser.add_argument("--manifest-limit", type=int, default=60)
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

    run_id = build_run_id()
    logger = TaskRunLogger()
    metadata = {
        "strategy_id": args.strategy_id,
        "horizons": horizons,
        "backfill_selected_history": not args.skip_selected_history_backfill,
        "history_limit": args.history_limit,
        "manifest_limit": args.manifest_limit,
    }
    logger.start(TASK_NAME, run_id, metadata)
    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        result = {
            "status": "skipped",
            "reason": "another factor evaluation run owns the lock",
            "run_id": run_id,
        }
        logger.finish(TASK_NAME, run_id, "success", None, result)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    try:
        result = run(
            strategy_id=args.strategy_id,
            horizons=horizons,
            backfill_selected_history=not args.skip_selected_history_backfill,
            history_limit=max(1, args.history_limit),
            manifest_limit=max(1, args.manifest_limit),
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
