from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.market_opinion_lifecycle import (
    MarketOpinionLifecyclePolicy,
    build_market_opinion_lifecycle_plan,
    run_market_opinion_lifecycle,
)
from app.shared.task_log import TaskRunLogger

TASK_NAME = "market_opinion_lifecycle"


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize and retain sector opinion snapshots")
    parser.add_argument("--apply", action="store_true", help="write normalized payloads and prune; default is dry-run")
    parser.add_argument("--intraday-trade-days", type=int, default=5)
    parser.add_argument("--daily-trade-days", type=int, default=90)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--normalize-only", action="store_true")
    parser.add_argument("--retention-only", action="store_true")
    args = parser.parse_args()
    if args.normalize_only and args.retention_only:
        parser.error("--normalize-only and --retention-only are mutually exclusive")

    policy = MarketOpinionLifecyclePolicy(
        intraday_trade_days=args.intraday_trade_days,
        daily_trade_days=args.daily_trade_days,
        batch_size=args.batch_size,
    ).validate()
    if not args.apply:
        print(json.dumps({"status": "dry_run", **build_market_opinion_lifecycle_plan(policy)}, ensure_ascii=False, default=str))
        return

    run_id = f"market_opinion_lifecycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {"policy": policy.__dict__})

    def progress(item: dict) -> None:
        print(json.dumps({"run_id": run_id, **item}, ensure_ascii=False), flush=True)

    try:
        result = run_market_opinion_lifecycle(
            policy,
            normalize=not args.retention_only,
            prune=not args.normalize_only,
            progress=progress,
        )
        logger.finish(
            TASK_NAME,
            run_id,
            result.get("status", "success"),
            f"market opinion lifecycle finished, normalized={result.get('normalization', {}).get('snapshots', 0)}, deleted={result.get('pruning', {}).get('deleted_snapshots', 0)}",
            result,
        )
        print(json.dumps({"run_id": run_id, **result}, ensure_ascii=False, default=str))
    except Exception as exc:
        payload = {"error": f"{type(exc).__name__}: {str(exc)[:500]}"}
        logger.finish(TASK_NAME, run_id, "failed", payload["error"], payload, error_code="market_opinion_lifecycle_failed")
        print(json.dumps({"run_id": run_id, "status": "failed", **payload}, ensure_ascii=False), file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
