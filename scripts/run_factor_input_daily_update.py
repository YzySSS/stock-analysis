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
from app.shared.instrument_policy import (
    STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS,
    STOCK_DAILY_COMPLETENESS_RATIO,
    STOCK_INSTRUMENT_TYPE,
)
from app.shared.task_log import TaskRunLogger


DAILY_BASIC_MIN_COVERAGE_RATIO = 0.80


def build_run_id() -> str:
    return f"factor_input_daily_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def fetch_recent_trade_dates(days: int) -> list[str]:
    sql = f"""
    SELECT k.trade_date
    FROM daily_kline k
    WHERE k.trade_date >= DATE_SUB(
        (SELECT MAX(trade_date) FROM daily_kline),
        INTERVAL {STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS} DAY
    )
    GROUP BY k.trade_date
    HAVING COUNT(*) >= (
        SELECT COUNT(*) * {STOCK_DAILY_COMPLETENESS_RATIO}
        FROM stock_basic
        WHERE instrument_type='{STOCK_INSTRUMENT_TYPE}'
    )
    ORDER BY k.trade_date DESC
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


def prefetch_daily_basic_maps(
    sync: FactorInputHistorySync,
    trade_dates: list[str],
    total_codes: int,
    min_coverage_ratio: float = DAILY_BASIC_MIN_COVERAGE_RATIO,
) -> tuple[dict[str, dict], dict[str, dict]]:
    minimum_rows = max(1, int(total_codes * min_coverage_ratio))
    available_maps: dict[str, dict] = {}
    coverage: dict[str, dict] = {}
    for trade_date in trade_dates:
        daily_basic_map = sync.fetch_daily_basic_map(trade_date)
        row_count = len(daily_basic_map)
        available = row_count >= minimum_rows
        coverage[trade_date] = {
            "rows": row_count,
            "coverage_ratio": round(row_count / total_codes, 4) if total_codes else 0.0,
            "minimum_rows": minimum_rows,
            "available": available,
        }
        if available:
            available_maps[trade_date] = daily_basic_map
    return available_maps, coverage


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

        sync = FactorInputHistorySync()
        daily_basic_maps, source_coverage = prefetch_daily_basic_maps(sync, trade_dates, total_codes)
        available_trade_dates = [date for date in trade_dates if date in daily_basic_maps]
        unavailable_trade_dates = [date for date in trade_dates if date not in daily_basic_maps]
        metadata.update(
            {
                "available_trade_dates": available_trade_dates,
                "unavailable_trade_dates": unavailable_trade_dates,
                "daily_basic_coverage": source_coverage,
                "daily_basic_min_coverage_ratio": DAILY_BASIC_MIN_COVERAGE_RATIO,
            }
        )
        if not available_trade_dates:
            payload = {**metadata, "rows_synced": 0, "batches": 0}
            logger.finish(
                "factor_input_daily_update",
                run_id,
                "partial_success",
                "daily_basic source is not published or below coverage threshold",
                payload,
            )
            print(json.dumps(payload, ensure_ascii=False))
            return

        import time

        rows_synced = 0
        batches = 0
        start_date = available_trade_dates[0]
        end_date = available_trade_dates[-1]
        for offset in range(0, total_codes, args.batch_size):
            result = sync.run(
                start_date=start_date,
                end_date=end_date,
                limit_per_day=args.batch_size,
                offset=offset,
                trade_dates_override=available_trade_dates,
                daily_basic_maps=daily_basic_maps,
            )
            rows_synced += int(result.get("rows_synced") or 0)
            batches += 1
            if args.pause_seconds > 0 and offset + args.batch_size < total_codes:
                time.sleep(args.pause_seconds)
        payload = {
            **metadata,
            "start_date": start_date,
            "end_date": end_date,
            "rows_synced": rows_synced,
            "batches": batches,
        }
        final_status = "partial_success" if unavailable_trade_dates else "success"
        logger.finish(
            task_name="factor_input_daily_update",
            run_id=run_id,
            status=final_status,
            message=(
                f"factor input daily update completed, rows_synced={rows_synced}, "
                f"unavailable_trade_dates={len(unavailable_trade_dates)}"
            ),
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
