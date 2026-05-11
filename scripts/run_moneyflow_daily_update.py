from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.moneyflow_sync import MoneyflowSync
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


def fetch_recent_trade_dates(days: int) -> list[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT trade_date FROM daily_kline ORDER BY trade_date DESC LIMIT %s", (days,))
            return sorted(str(row["trade_date"]) for row in cursor.fetchall())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily update Tushare moneyflow into stock_moneyflow_daily")
    parser.add_argument("--recent-trade-days", type=int, default=5)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--pause-seconds", type=float, default=0.5)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    trade_dates = fetch_recent_trade_dates(args.recent_trade_days) if not (args.start_date and args.end_date) else []
    start_date = args.start_date or (trade_dates[0] if trade_dates else None)
    end_date = args.end_date or (trade_dates[-1] if trade_dates else None)
    if not start_date or not end_date:
        raise RuntimeError("no trade dates found for moneyflow update")
    logger = TaskRunLogger()
    run_id = f"moneyflow_daily_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {"start_date": start_date, "end_date": end_date, "recent_trade_days": args.recent_trade_days}
    logger.start("moneyflow_daily_update", run_id, metadata)
    try:
        result = MoneyflowSync().run(start_date, end_date, pause_seconds=args.pause_seconds)
        payload = {**metadata, **result}
        logger.finish("moneyflow_daily_update", run_id, "success", f"moneyflow update completed, rows={result.get('rows_synced', 0)}", payload)
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish("moneyflow_daily_update", run_id, "failed", str(exc)[:500], metadata)
        raise


if __name__ == "__main__":
    main()
