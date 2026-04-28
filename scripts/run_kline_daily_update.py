from __future__ import annotations

import json
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import date, timedelta

from app.data_ingestion.daily_kline_sync import DailyKlineSync
from app.shared.task_log import TaskRunLogger


def build_run_id() -> str:
    return f"daily_kline_increment_{date.today().strftime('%Y%m%d')}"


def main() -> None:
    logger = TaskRunLogger()
    run_id = build_run_id()
    yesterday = date.today() - timedelta(days=1)
    trade_date = yesterday.isoformat()
    metadata = {
        "mode": "incremental_daily",
        "trade_date": trade_date,
        "start_date": trade_date,
        "end_date": trade_date,
        "limit": None,
    }
    logger.start(task_name="daily_kline_increment", run_id=run_id, metadata=metadata)
    try:
        sync = DailyKlineSync()
        result = sync.run(
            start_date=trade_date,
            end_date=trade_date,
            limit=None,
            instrument_type="stock",
            pause_seconds=0.05,
            relogin_every=20,
        )
        payload = {**metadata, **result}
        logger.finish(
            task_name="daily_kline_increment",
            run_id=run_id,
            status="success",
            message=f"daily kline incremental sync completed, rows={result.get('rows_synced', 0)}",
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(
            task_name="daily_kline_increment",
            run_id=run_id,
            status="failed",
            message=str(exc)[:500],
            metadata=metadata,
        )
        raise


if __name__ == "__main__":
    main()
