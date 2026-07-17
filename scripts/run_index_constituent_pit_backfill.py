from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.index_constituent_pit_sync import (  # noqa: E402
    IndexConstituentPitSync,
    month_periods,
    normalize_month,
)
from app.shared.db import mysql_conn  # noqa: E402
from app.shared.index_universe import INDEX_UNIVERSE_DEFINITIONS  # noqa: E402
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402


TASK_NAME = "index_constituent_pit_backfill"
LOCK_NAME = "stock_analysis_index_constituent_pit_backfill"


def to_json_safe(value):
    if isinstance(value, dict):
        return {key: to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_safe(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _shift_month(month: str, offset: int) -> str:
    normalized = normalize_month(month)
    absolute = int(normalized[:4]) * 12 + int(normalized[4:]) - 1 + offset
    return f"{absolute // 12:04d}{absolute % 12 + 1:02d}"


def default_month_range(today: date | None = None) -> tuple[str, str]:
    today = today or date.today()
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
        raise RuntimeError("factor_input_daily 没有可用于指数成分 PIT 的历史区间")
    if not isinstance(min_trade_date, date):
        min_trade_date = date.fromisoformat(str(min_trade_date)[:10])
    if not isinstance(max_trade_date, date):
        max_trade_date = date.fromisoformat(str(max_trade_date)[:10])
    history_start = _shift_month(min_trade_date.strftime("%Y%m"), -1)
    latest_completed = _shift_month(today.strftime("%Y%m"), -1)
    history_end = min(max_trade_date.strftime("%Y%m"), latest_completed)
    if history_start > history_end:
        raise RuntimeError("指数成分 PIT 默认区间没有已完成月份")
    return history_start, history_end


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill point-in-time monthly index constituent weights"
    )
    parser.add_argument("--start-month", help="Month in YYYYMM format")
    parser.add_argument("--end-month", help="Month in YYYYMM format")
    parser.add_argument(
        "--index-codes",
        default=",".join(INDEX_UNIVERSE_DEFINITIONS),
        help="Comma-separated supported index codes",
    )
    parser.add_argument("--recent-months", type=int)
    parser.add_argument("--pending-only", action="store_true")
    parser.add_argument("--pause-seconds", type=float, default=0.2)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    default_start, default_end = default_month_range()
    months = month_periods(args.start_month or default_start, args.end_month or default_end)
    if args.recent_months is not None:
        recent_count = max(int(args.recent_months), 0)
        months = months[-recent_count:] if recent_count else []
    if not months:
        raise ValueError("at least one completed month is required")
    index_codes = [item.strip().upper() for item in args.index_codes.split(",") if item.strip()]
    if not index_codes:
        raise ValueError("at least one index code is required")

    lock = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock is None:
        print(json.dumps({"status": "skipped", "reason": "index constituent PIT backfill already running"}))
        return

    run_id = f"index_constituent_pit_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    metadata = {
        "index_codes": index_codes,
        "start_month": months[0],
        "end_month": months[-1],
        "months": months,
        "pending_only": args.pending_only,
        "pause_seconds": max(args.pause_seconds, 0.0),
    }
    logger.start(TASK_NAME, run_id, metadata)
    try:
        payload = IndexConstituentPitSync().run(
            run_id,
            index_codes=index_codes,
            months=months,
            pending_only=args.pending_only,
            pause_seconds=max(args.pause_seconds, 0.0),
        )
        status = (
            "partial_success"
            if payload["failed_partitions"] or payload["partial_partitions"]
            else "success"
        )
        payload["status"] = status
        safe_payload = to_json_safe(payload)
        logger.finish(
            TASK_NAME,
            run_id,
            status,
            (
                "index constituent PIT backfill completed; "
                f"success={len(payload['success_partitions'])}, "
                f"partial={len(payload['partial_partitions'])}, "
                f"failed={len(payload['failed_partitions'])}"
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
