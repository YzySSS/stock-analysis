from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.stock_status_pit_sync import StockStatusPitSync  # noqa: E402
from app.shared.db import mysql_conn  # noqa: E402
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402


TASK_NAME = "stock_status_pit_backfill"
LOCK_NAME = "stock_analysis_stock_status_pit_backfill"
ALL_STAGES = ("lifecycle", "names", "suspensions", "market-data")


def to_json_safe(value):
    if isinstance(value, dict):
        return {key: to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_safe(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def default_history_range() -> tuple[str, str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT MIN(trade_date) AS start_date, MAX(trade_date) AS end_date
                FROM factor_input_daily
                """
            )
            row = cursor.fetchone() or {}
    if not row.get("start_date") or not row.get("end_date"):
        raise RuntimeError("factor_input_daily 没有可用于 PIT 回填的历史区间")
    return str(row["start_date"]), str(row["end_date"])


def parse_stages(raw: str) -> list[str]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if "all" in values:
        return list(ALL_STAGES)
    unknown = sorted(set(values) - set(ALL_STAGES))
    if unknown:
        raise ValueError(f"unsupported PIT stages: {', '.join(unknown)}")
    if not values:
        raise ValueError("at least one PIT stage is required")
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill point-in-time stock lifecycle, ST/name, suspension and historical market data"
    )
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--stages", default="all", help="all or comma-separated lifecycle,names,suspensions,market-data")
    parser.add_argument("--pause-seconds", type=float, default=0.05)
    parser.add_argument(
        "--suspension-recent-trade-days",
        type=int,
        help="Only refresh the latest N factor trade dates instead of the full suspension range",
    )
    parser.add_argument(
        "--pending-market-only",
        action="store_true",
        help="Only backfill delisted codes without a successful historical-market manifest",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    default_start, default_end = default_history_range()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end
    stages = parse_stages(args.stages)
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    lock = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock is None:
        print(json.dumps({"status": "skipped", "reason": "PIT backfill already running"}, ensure_ascii=False))
        return

    run_id = f"stock_status_pit_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    metadata = {
        "start_date": start_date,
        "end_date": end_date,
        "stages": stages,
        "pause_seconds": args.pause_seconds,
        "suspension_recent_trade_days": args.suspension_recent_trade_days,
        "pending_market_only": args.pending_market_only,
    }
    logger.start(TASK_NAME, run_id, metadata)
    try:
        payload = StockStatusPitSync().run(
            run_id,
            start_date,
            end_date,
            stages=stages,
            pause_seconds=max(args.pause_seconds, 0.0),
            suspension_recent_trade_days=args.suspension_recent_trade_days,
            pending_market_only=args.pending_market_only,
        )
        market_result = payload.get("market_data") or {}
        suspension_failures = len((payload.get("suspensions") or {}).get("failed_dates") or [])
        market_failures = (
            len(market_result.get("failed_codes") or [])
            + len(market_result.get("incomplete_codes") or [])
        )
        status = "partial_success" if suspension_failures or market_failures else "success"
        payload["status"] = status
        safe_payload = to_json_safe(payload)
        logger.finish(
            TASK_NAME,
            run_id,
            status,
            (
                "point-in-time stock status backfill completed; "
                f"suspension_failures={suspension_failures}, market_failures={market_failures}"
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
            print(json.dumps({"lock_release_warning": release_error}, ensure_ascii=False), file=sys.stderr)


if __name__ == "__main__":
    main()
