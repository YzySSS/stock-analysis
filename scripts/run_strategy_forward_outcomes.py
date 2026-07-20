from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.shared.task_log import TaskRunLogger
from app.stock_selection.forward_observation import ForwardObservationService


TASK_NAME = "strategy_forward_outcome_update"
LOCK_NAME = "strategy_forward_outcome_update_lock"


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile forward observations and update matured outcomes.")
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be greater than 0")

    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock_handle is None:
        print(json.dumps({"status": "skipped", "reason": "lock_unavailable"}, ensure_ascii=False))
        return
    logger = TaskRunLogger()
    run_id = f"strategy_forward_outcome_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {"limit": args.limit}
    try:
        logger.start(TASK_NAME, run_id, metadata)
        service = ForwardObservationService()
        result = {
            "status": "success",
            "reconciled": service.reconcile_open_observations(),
            "outcomes": service.refresh_outcomes(limit=args.limit),
        }
        logger.finish(TASK_NAME, run_id, "success", "forward outcomes updated", result)
        print(json.dumps(result, ensure_ascii=False, default=str))
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc), metadata)
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock_handle)
        if release_error:
            print(f"warning: failed to release {LOCK_NAME}: {release_error}", file=sys.stderr)


if __name__ == "__main__":
    main()
