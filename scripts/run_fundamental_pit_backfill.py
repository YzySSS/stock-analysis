from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.fundamental_pit_sync import (  # noqa: E402
    FundamentalPitSync,
    quarter_end_periods,
)
from app.shared.db import mysql_conn  # noqa: E402
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402


TASK_NAME = "fundamental_pit_backfill"
LOCK_NAME = "stock_analysis_fundamental_pit_backfill"


def to_json_safe(value):
    if isinstance(value, dict):
        return {key: to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_safe(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _latest_quarter_end(value: date) -> str:
    endings = ((3, "0331"), (6, "0630"), (9, "0930"), (12, "1231"))
    eligible = [suffix for month, suffix in endings if month <= value.month]
    if eligible:
        return f"{value.year}{eligible[-1]}"
    return f"{value.year - 1}1231"


def default_period_range() -> tuple[str, str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT MIN(trade_date) AS min_trade_date,
                       MAX(trade_date) AS max_trade_date
                FROM factor_input_daily
                """
            )
            row = cursor.fetchone() or {}
    min_trade_date = row.get("min_trade_date")
    max_trade_date = row.get("max_trade_date")
    if not min_trade_date or not max_trade_date:
        raise RuntimeError("factor_input_daily 没有可用于公告日 PIT 的历史区间")
    if not isinstance(min_trade_date, date):
        min_trade_date = date.fromisoformat(str(min_trade_date)[:10])
    if not isinstance(max_trade_date, date):
        max_trade_date = date.fromisoformat(str(max_trade_date)[:10])
    return f"{min_trade_date.year - 2}1231", _latest_quarter_end(max_trade_date)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill point-in-time financial indicators by source announcement date"
    )
    parser.add_argument("--start-period", help="Quarter end in YYYYMMDD format")
    parser.add_argument("--end-period", help="Quarter end in YYYYMMDD format")
    parser.add_argument(
        "--recent-periods",
        type=int,
        help="Only refresh the latest N periods inside the selected range",
    )
    parser.add_argument("--pending-only", action="store_true")
    parser.add_argument("--pause-seconds", type=float, default=0.1)
    parser.add_argument("--page-size", type=int, default=5000)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    default_start, default_end = default_period_range()
    start_period = args.start_period or default_start
    end_period = args.end_period or default_end
    periods = quarter_end_periods(start_period, end_period)
    if args.recent_periods is not None:
        recent_count = max(int(args.recent_periods), 0)
        periods = periods[-recent_count:] if recent_count else []
    if not periods:
        raise ValueError("at least one report period is required")
    if args.page_size <= 0 or args.page_size > 5000:
        raise ValueError("page_size must be between 1 and 5000")

    lock = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock is None:
        print(json.dumps({"status": "skipped", "reason": "fundamental PIT backfill already running"}))
        return

    run_id = f"fundamental_pit_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    metadata = {
        "start_period": periods[0],
        "end_period": periods[-1],
        "periods": periods,
        "pending_only": args.pending_only,
        "pause_seconds": max(args.pause_seconds, 0.0),
        "page_size": args.page_size,
    }
    logger.start(TASK_NAME, run_id, metadata)
    try:
        payload = FundamentalPitSync().run(
            run_id,
            periods,
            pending_only=args.pending_only,
            pause_seconds=max(args.pause_seconds, 0.0),
            page_size=args.page_size,
        )
        status = "partial_success" if payload["failed_periods"] or payload["partial_periods"] else "success"
        payload["status"] = status
        safe_payload = to_json_safe(payload)
        logger.finish(
            TASK_NAME,
            run_id,
            status,
            (
                "fundamental PIT backfill completed; "
                f"success={len(payload['success_periods'])}, "
                f"partial={len(payload['partial_periods'])}, "
                f"failed={len(payload['failed_periods'])}"
            ),
            safe_payload,
        )
        print(json.dumps(safe_payload, ensure_ascii=False), flush=True)
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], metadata)
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock)
        if release_error:
            print(json.dumps({"lock_release_warning": release_error}), file=sys.stderr)


if __name__ == "__main__":
    main()
