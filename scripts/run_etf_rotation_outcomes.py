from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.etf_rotation.outcomes import EtfRotationOutcomeService
from app.shared.mysql_lock import (
    acquire_mysql_advisory_lock,
    release_mysql_advisory_lock,
)
from app.shared.task_log import TaskRunLogger


TASK_NAME = "etf_rotation_forward_outcomes"
LOCK_NAME = "stock_analysis_etf_rotation_forward_outcomes"


def main() -> int:
    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {"research_only": True}
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, metadata)
    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        result = {
            "status": "skipped",
            "reason": "another ETF outcome updater owns the lock",
            "run_id": run_id,
        }
        logger.finish(TASK_NAME, run_id, "success", None, result)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    try:
        result = EtfRotationOutcomeService().update()
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
