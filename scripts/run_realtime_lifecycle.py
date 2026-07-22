from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.realtime_lifecycle import (
    RealtimeLifecyclePolicy,
    build_lifecycle_plan,
    run_lifecycle,
)
from app.shared.task_log import TaskRunLogger

TASK_NAME = "stock_realtime_lifecycle"


def main() -> None:
    parser = argparse.ArgumentParser(description="Roll up and retain full-market realtime minute snapshots")
    parser.add_argument("--apply", action="store_true", help="write rollups and apply retention; default is dry-run")
    parser.add_argument("--raw-trade-days", type=int, default=2)
    parser.add_argument("--rollup-trade-days", type=int, default=90)
    parser.add_argument("--tracked-trade-days", type=int, default=90)
    args = parser.parse_args()

    policy = RealtimeLifecyclePolicy(
        raw_trade_days=args.raw_trade_days,
        rollup_trade_days=args.rollup_trade_days,
        tracked_trade_days=args.tracked_trade_days,
    ).validate()
    if not args.apply:
        print(json.dumps({"status": "dry_run", **build_lifecycle_plan(policy)}, ensure_ascii=False, default=str))
        return

    run_id = f"realtime_lifecycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {"policy": policy.__dict__})
    try:
        result = run_lifecycle(policy)
        processed_count = sum(1 for item in result.get("rollups", []) if item.get("status") in {"success", "partial"})
        skipped_count = sum(1 for item in result.get("rollups", []) if item.get("status") == "skipped")
        problem_count = sum(1 for item in result.get("rollups", []) if item.get("status") in {"partial", "failed"})
        failure_count = len(result.get("failures", []))
        task_status = result.get("status", "success")
        logger.finish(
            TASK_NAME,
            run_id,
            task_status,
            (
                "realtime lifecycle finished, "
                f"processed={processed_count}, skipped={skipped_count}, "
                f"rollup_problems={problem_count}, failures={failure_count}"
            ),
            result,
        )
        print(json.dumps({"run_id": run_id, **result}, ensure_ascii=False, default=str))
    except Exception as exc:
        payload = {"error": f"{type(exc).__name__}: {str(exc)[:500]}"}
        logger.finish(TASK_NAME, run_id, "failed", payload["error"], payload, error_code="realtime_lifecycle_failed")
        print(json.dumps({"run_id": run_id, "status": "failed", **payload}, ensure_ascii=False))
        raise


if __name__ == "__main__":
    main()
