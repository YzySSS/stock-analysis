from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.daily_kline_sync import DailyKlineSync
from app.data_ingestion.factor_input_history_sync import FactorInputHistorySync
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

TASK_NAME = "backtest_data_backfill"


def count_active_stocks() -> int:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS c FROM stock_basic WHERE instrument_type='stock' AND is_delisted=0")
            return int((cursor.fetchone() or {}).get("c") or 0)


def coverage(table: str, trade_date: str) -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(DISTINCT code) AS codes, COUNT(*) AS rows_n FROM {table} WHERE trade_date=%s", (trade_date,))
            return cursor.fetchone() or {}


def run_kline_batches(trade_date: str, batch_size: int, pause_seconds: float) -> dict:
    total_codes = count_active_stocks()
    sync = DailyKlineSync()
    batches: list[dict] = []
    total_rows = 0
    total_success = 0
    failed_codes: list[dict] = []
    for offset in range(0, total_codes, batch_size):
        result = sync.run(
            start_date=trade_date,
            end_date=trade_date,
            limit=batch_size,
            offset=offset,
            instrument_type="stock",
            pause_seconds=0.03,
            relogin_every=20,
        )
        batch = {
            "offset": offset,
            "limit": batch_size,
            "rows_synced": int(result.get("rows_synced") or 0),
            "success_codes": int(result.get("success_codes") or 0),
            "failed_count": len(result.get("failed_codes") or []),
        }
        batches.append(batch)
        total_rows += batch["rows_synced"]
        total_success += batch["success_codes"]
        failed_codes.extend(result.get("failed_codes") or [])
        print(json.dumps({"stage": "daily_kline", **batch}, ensure_ascii=False), flush=True)
        if pause_seconds > 0:
            time.sleep(pause_seconds)
    return {
        "total_codes": total_codes,
        "batches": len(batches),
        "rows_synced": total_rows,
        "success_codes": total_success,
        "failed_codes": failed_codes[:100],
        "failed_count": len(failed_codes),
    }


def run_factor_batches(trade_date: str, batch_size: int, pause_seconds: float) -> dict:
    total_codes = count_active_stocks()
    sync = FactorInputHistorySync()
    batches: list[dict] = []
    rows_synced = 0
    for offset in range(0, total_codes, batch_size):
        result = sync.run(
            start_date=trade_date,
            end_date=trade_date,
            limit_per_day=batch_size,
            offset=offset,
        )
        batch = {
            "offset": offset,
            "limit": batch_size,
            "rows_synced": int(result.get("rows_synced") or 0),
        }
        batches.append(batch)
        rows_synced += batch["rows_synced"]
        print(json.dumps({"stage": "factor_input", **batch}, ensure_ascii=False), flush=True)
        if pause_seconds > 0:
            time.sleep(pause_seconds)
    return {
        "total_codes": total_codes,
        "batches": len(batches),
        "rows_synced": rows_synced,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill daily_kline and factor_input_daily for backtest readiness")
    parser.add_argument("--trade-date", required=True, help="Trade date to backfill, YYYY-MM-DD")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--pause-seconds", type=float, default=1.0)
    parser.add_argument("--skip-kline", action="store_true")
    parser.add_argument("--skip-factor", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_id = f"backtest_data_backfill_{args.trade_date.replace('-', '')}_{datetime.now().strftime('%H%M%S')}"
    logger = TaskRunLogger()
    metadata = {
        "trade_date": args.trade_date,
        "batch_size": args.batch_size,
        "pause_seconds": args.pause_seconds,
        "skip_kline": args.skip_kline,
        "skip_factor": args.skip_factor,
    }
    logger.start(TASK_NAME, run_id, metadata)
    started = datetime.now()
    try:
        payload: dict = {**metadata, "run_id": run_id}
        if not args.skip_kline:
            payload["kline"] = run_kline_batches(args.trade_date, args.batch_size, args.pause_seconds)
            payload["kline_coverage"] = coverage("daily_kline", args.trade_date)
        if not args.skip_factor:
            payload["factor_input"] = run_factor_batches(args.trade_date, args.batch_size, args.pause_seconds)
            payload["factor_coverage"] = coverage("factor_input_daily", args.trade_date)
        payload["elapsed_seconds"] = round((datetime.now() - started).total_seconds(), 2)
        logger.finish(TASK_NAME, run_id, "success", "backtest data backfill completed", payload)
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], metadata)
        raise


if __name__ == "__main__":
    main()
