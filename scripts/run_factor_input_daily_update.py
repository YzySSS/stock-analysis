from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.factor_input_history_sync import FactorInputHistorySync
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


def build_run_id() -> str:
    return f"factor_input_daily_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def fetch_recent_trade_dates(days: int) -> list[str]:
    sql = """
    SELECT DISTINCT trade_date
    FROM daily_kline
    ORDER BY trade_date DESC
    LIMIT %s
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (days,))
            rows = cursor.fetchall() or []
    return sorted(str(row["trade_date"]) for row in rows)


def count_active_stocks() -> int:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS c FROM stock_basic WHERE instrument_type='stock' AND is_delisted=0")
            return int((cursor.fetchone() or {}).get("c") or 0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily update factor_input_daily for recent trade dates")
    parser.add_argument("--recent-trade-days", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--pause-seconds", type=float, default=3.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logger = TaskRunLogger()
    run_id = build_run_id()
    trade_dates = fetch_recent_trade_dates(args.recent_trade_days)
    total_codes = count_active_stocks()
    metadata = {
        "recent_trade_days": args.recent_trade_days,
        "batch_size": args.batch_size,
        "trade_dates": trade_dates,
        "total_codes": total_codes,
    }
    logger.start(task_name="factor_input_daily_update", run_id=run_id, metadata=metadata)
    try:
        if not trade_dates:
            payload = {**metadata, "rows_synced": 0, "batches": 0, "message": "no trade dates"}
            logger.finish("factor_input_daily_update", run_id, "success", "no trade dates for factor input update", payload)
            print(json.dumps(payload, ensure_ascii=False))
            return

        import time

        sync = FactorInputHistorySync()
        rows_synced = 0
        batches = 0
        start_date = trade_dates[0]
        end_date = trade_dates[-1]
        for offset in range(0, total_codes, args.batch_size):
            result = sync.run(
                start_date=start_date,
                end_date=end_date,
                limit_per_day=args.batch_size,
                offset=offset,
            )
            rows_synced += int(result.get("rows_synced") or 0)
            batches += 1
            if args.pause_seconds > 0:
                time.sleep(args.pause_seconds)
        payload = {
            **metadata,
            "start_date": start_date,
            "end_date": end_date,
            "rows_synced": rows_synced,
            "batches": batches,
        }
        logger.finish(
            task_name="factor_input_daily_update",
            run_id=run_id,
            status="success",
            message=f"factor input daily update completed, rows_synced={rows_synced}",
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(
            task_name="factor_input_daily_update",
            run_id=run_id,
            status="failed",
            message=str(exc)[:500],
            metadata=metadata,
        )
        raise


if __name__ == "__main__":
    main()
