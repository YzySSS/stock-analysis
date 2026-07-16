from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.stock_basic_sync import StockBasicSync
from app.shared.task_log import TaskRunLogger


TASK_NAME = "stock_basic_sync"


def main() -> None:
    run_id = f"stock_basic_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, {})
    try:
        sync = StockBasicSync()
        count = sync.run()
        payload = {"run_id": run_id, "status": "success", "updated": count}
        logger.finish(TASK_NAME, run_id, "success", f"stock basic updated={count}", payload)
        print(payload)
    except Exception as exc:
        payload = {"run_id": run_id, "status": "failed", "error_type": type(exc).__name__, "error": str(exc)[:500]}
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], payload)
        print(payload)
        raise


if __name__ == "__main__":
    main()
