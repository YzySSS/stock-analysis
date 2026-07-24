from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.market_timing.v20 import INDEX_CODE, MarketTimingV20Repository
from app.shared.db import mysql_read_conn
from app.shared.mysql_lock import (
    acquire_mysql_advisory_lock,
    release_mysql_advisory_lock,
)
from app.shared.task_log import TaskRunLogger


TASK_NAME = "market_timing_v20_shadow_update"
LOCK_NAME = "stock_analysis_market_timing_v20_shadow_update"


def _latest_trade_date() -> date:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(trade_date) AS trade_date
                FROM daily_kline
                WHERE close IS NOT NULL
                """
            )
            trade_date = (cursor.fetchone() or {}).get("trade_date")
    if trade_date is None:
        raise RuntimeError("daily_kline has no trade date")
    return trade_date


def _trade_dates(
    *,
    trade_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> list[date]:
    if trade_date:
        return [date.fromisoformat(trade_date)]
    if start_date or end_date:
        start_value = date.fromisoformat(start_date) if start_date else date(2000, 1, 1)
        end_value = date.fromisoformat(end_date) if end_date else _latest_trade_date()
        if start_value > end_value:
            raise ValueError("start-date must not be after end-date")
        with mysql_read_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM daily_kline
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                    """,
                    (start_value, end_value),
                )
                return [row["trade_date"] for row in (cursor.fetchall() or [])]
    return [_latest_trade_date()]


def run(
    *,
    trade_dates: list[date],
    index_code: str = INDEX_CODE,
    overlay_points: float = 0.0,
) -> dict[str, Any]:
    repository = MarketTimingV20Repository()
    rows = [
        repository.materialize(
            trade_date,
            index_code=index_code,
            overlay_points=overlay_points,
        )
        for trade_date in sorted(trade_dates)
    ]
    return {
        "status": "success",
        "model_id": "market_timing_v20",
        "index_code": index_code,
        "processed": len(rows),
        "start_date": str(rows[0]["trade_date"]) if rows else None,
        "end_date": str(rows[-1]["trade_date"]) if rows else None,
        "latest": rows[-1] if rows else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Materialize the research-only market timing V2.0 shadow signal"
    )
    parser.add_argument("--trade-date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--index-code", default=INDEX_CODE)
    parser.add_argument("--overlay-points", type=float, default=0.0)
    args = parser.parse_args()
    if args.trade_date and (args.start_date or args.end_date):
        raise ValueError("--trade-date cannot be combined with a date range")

    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = TaskRunLogger()
    metadata = {
        "trade_date": args.trade_date,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "index_code": args.index_code,
        "overlay_points": args.overlay_points,
        "research_only": True,
    }
    logger.start(TASK_NAME, run_id, metadata)
    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME)
    if lock_handle is None:
        result = {
            "status": "skipped",
            "reason": "another market timing V2.0 run owns the lock",
            "run_id": run_id,
        }
        logger.finish(TASK_NAME, run_id, "success", None, result)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    try:
        result = run(
            trade_dates=_trade_dates(
                trade_date=args.trade_date,
                start_date=args.start_date,
                end_date=args.end_date,
            ),
            index_code=args.index_code,
            overlay_points=args.overlay_points,
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
