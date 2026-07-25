from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import tushare as ts

from app.etf_rotation.data_sync import latest_market_trade_date, sync_etf_rotation_data
from app.shared.mysql_lock import (
    acquire_mysql_advisory_lock,
    release_mysql_advisory_lock,
)
from app.shared.task_log import TaskRunLogger


TASK_NAME = "etf_rotation_data_sync"
LOCK_NAME = "stock_analysis_etf_rotation_data_sync"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync the frozen research-only ETF rotation universe"
    )
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", help="YYYY-MM-DD")
    parser.add_argument("--lookback-calendar-days", type=int, default=180)
    args = parser.parse_args()
    if args.lookback_calendar_days < 30:
        parser.error("--lookback-calendar-days must be at least 30")

    end_date = (
        date.fromisoformat(args.end_date)
        if args.end_date
        else latest_market_trade_date()
    )
    start_date = (
        date.fromisoformat(args.start_date)
        if args.start_date
        else end_date - timedelta(days=args.lookback_calendar_days)
    )
    if start_date > end_date:
        parser.error("--start-date must not be after --end-date")

    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "research_only": True,
    }
    logger = TaskRunLogger()
    logger.start(TASK_NAME, run_id, metadata)
    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        result = {
            "status": "skipped",
            "reason": "another ETF rotation data sync owns the lock",
            "run_id": run_id,
        }
        logger.finish(TASK_NAME, run_id, "success", None, result)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    try:
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        result = sync_etf_rotation_data(
            pro=ts.pro_api(token),
            start_date=start_date,
            end_date=end_date,
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
