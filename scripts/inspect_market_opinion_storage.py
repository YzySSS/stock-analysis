from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.market_opinion_storage import (
    analyze_market_opinion_statistics,
    build_market_opinion_storage_report,
)
from app.shared.task_log import TaskRunLogger


TASK_NAME = "market_opinion_storage_maintenance"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect market-opinion storage; optionally refresh optimizer statistics without rebuilding tables"
    )
    parser.add_argument(
        "--analyze-statistics",
        action="store_true",
        help="run ANALYZE TABLE under the market-opinion advisory lock; never runs OPTIMIZE or a rebuild",
    )
    parser.add_argument("--lock-wait-seconds", type=int, default=5)
    args = parser.parse_args()

    before = build_market_opinion_storage_report()
    if not args.analyze_statistics:
        print(json.dumps({"status": "inspection_only", "report": before}, ensure_ascii=False, default=str))
        return

    run_id = f"market_opinion_storage_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(
        TASK_NAME,
        run_id,
        {"operation": "analyze_statistics_only", "lock_wait_seconds": args.lock_wait_seconds},
    )
    try:
        analysis = analyze_market_opinion_statistics(lock_wait_seconds=args.lock_wait_seconds)
        after = build_market_opinion_storage_report()
        result = {"run_id": run_id, "status": analysis["status"], "before": before, "analysis": analysis, "after": after}
        logger.finish(
            TASK_NAME,
            run_id,
            analysis["status"],
            f"market opinion storage maintenance finished: {analysis['status']}",
            result,
        )
        print(json.dumps(result, ensure_ascii=False, default=str))
    except Exception as exc:
        payload = {"error": f"{type(exc).__name__}: {str(exc)[:500]}"}
        logger.finish(
            TASK_NAME,
            run_id,
            "failed",
            payload["error"],
            payload,
            error_code="market_opinion_storage_maintenance_failed",
        )
        print(json.dumps({"run_id": run_id, "status": "failed", **payload}, ensure_ascii=False), file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
