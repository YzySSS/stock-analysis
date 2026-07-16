from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.jobs.retention import JobRetentionPolicy, JobRetentionService  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402


TASK_NAME = "job_retention"


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate and retain Stock Analysis job/task history")
    parser.add_argument("--apply", action="store_true", help="apply retention; default is dry-run")
    parser.add_argument("--task-detail-days", type=int, default=90)
    parser.add_argument("--selection-task-days", type=int, default=90)
    parser.add_argument("--backtest-system-test-days", type=int, default=90)
    parser.add_argument("--portfolio-raw-response-days", type=int, default=30)
    parser.add_argument("--portfolio-snapshot-days", type=int, default=90)
    parser.add_argument("--error-summary-days", type=int, default=365)
    parser.add_argument("--abandoned-task-hours", type=int, default=24)
    args = parser.parse_args()

    policy = JobRetentionPolicy(
        task_detail_days=args.task_detail_days,
        selection_task_days=args.selection_task_days,
        backtest_system_test_days=args.backtest_system_test_days,
        portfolio_raw_response_days=args.portfolio_raw_response_days,
        portfolio_snapshot_days=args.portfolio_snapshot_days,
        error_summary_days=args.error_summary_days,
        abandoned_task_hours=args.abandoned_task_hours,
    )
    service = JobRetentionService(policy)
    if not args.apply:
        print(json.dumps(service.preview(), ensure_ascii=False, default=str, indent=2))
        return

    logger = TaskRunLogger()
    run_id = f"job_retention_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.start(TASK_NAME, run_id, {"policy": policy.__dict__, "mode": "apply"})
    try:
        result = service.apply()
        logger.finish(TASK_NAME, run_id, "success", "job retention completed", result)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], {"policy": policy.__dict__})
        raise


if __name__ == "__main__":
    main()
