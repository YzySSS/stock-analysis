from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_quality.service import DataQualityAuditService  # noqa: E402
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402


TASK_NAME = "data_quality_audit"
LOCK_NAME = "stock_analysis_data_quality_audit"


def main() -> None:
    lock = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock is None:
        print(json.dumps({"status": "skipped", "reason": "audit already running"}, ensure_ascii=False))
        return

    logger = TaskRunLogger()
    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.start(TASK_NAME, run_id, {"mode": "read_only_bounded_audit"})
    try:
        result = DataQualityAuditService().run()
        task_status = "success" if result.get("health") == "healthy" else "partial_success"
        counts = result.get("counts") or {}
        message = (
            f"data quality audit completed: pass={counts.get('pass', 0)}, "
            f"warn={counts.get('warn', 0)}, fail={counts.get('fail', 0)}"
        )
        logger.finish(TASK_NAME, run_id, task_status, message, result)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], {"mode": "read_only_bounded_audit"})
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock)
        if release_error:
            print(json.dumps({"lock_release_warning": release_error}, ensure_ascii=False), file=sys.stderr)


if __name__ == "__main__":
    main()
