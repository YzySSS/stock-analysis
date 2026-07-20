from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.adj_factor_history import (
    DEFAULT_MINIMUM_COVERAGE_RATIO,
    AdjFactorHistoryBackfill,
)
from app.shared.db import mysql_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.shared.task_log import TaskRunLogger


LOCK_NAME = "stock_analysis_adj_factor_sync"


def fetch_recent_trade_dates(days: int) -> list[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT trade_date FROM daily_kline ORDER BY trade_date DESC LIMIT %s", (days,))
            return sorted(str(row["trade_date"]) for row in cursor.fetchall())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily update Tushare adj_factor into adj_factor_daily")
    parser.add_argument("--recent-trade-days", type=int, default=5)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--pause-seconds", type=float, default=0.5)
    parser.add_argument(
        "--minimum-coverage-ratio",
        type=float,
        default=DEFAULT_MINIMUM_COVERAGE_RATIO,
    )
    parser.add_argument("--max-failures", type=int, default=5)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    trade_dates = fetch_recent_trade_dates(args.recent_trade_days) if not (args.start_date and args.end_date) else []
    start_date = args.start_date or (trade_dates[0] if trade_dates else None)
    end_date = args.end_date or (trade_dates[-1] if trade_dates else None)
    if not start_date or not end_date:
        raise RuntimeError("no trade dates found for adj factor update")
    lock = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock is None:
        print(json.dumps({"status": "skipped", "reason": "adjustment factor sync already running"}))
        return
    logger = TaskRunLogger()
    run_id = f"adj_factor_daily_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {
        "start_date": start_date,
        "end_date": end_date,
        "recent_trade_days": args.recent_trade_days,
        "minimum_coverage_ratio": args.minimum_coverage_ratio,
        "pause_seconds": max(args.pause_seconds, 0.0),
    }
    logger.start("adj_factor_daily_update", run_id, metadata)
    try:
        service = AdjFactorHistoryBackfill()
        result = service.run(
            run_id,
            start_date,
            end_date,
            pending_only=False,
            pause_seconds=max(args.pause_seconds, 0.0),
            minimum_coverage_ratio=args.minimum_coverage_ratio,
            max_failures=args.max_failures,
        )
        payload = {**metadata, **result, "audit": service.audit(
            start_date,
            end_date,
            minimum_coverage_ratio=args.minimum_coverage_ratio,
        )}
        logger.finish(
            "adj_factor_daily_update",
            run_id,
            result["status"],
            (
                "adj factor update completed; "
                f"success={result['success_trade_days']}, "
                f"partial={result['partial_trade_days']}, "
                f"empty={result['empty_trade_days']}, "
                f"failed={result['failed_trade_days']}"
            ),
            payload,
        )
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish("adj_factor_daily_update", run_id, "failed", str(exc)[:500], metadata)
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock)
        if release_error:
            print(json.dumps({"lock_release_warning": release_error}), file=sys.stderr)


if __name__ == "__main__":
    main()
