from __future__ import annotations

import argparse
import json
from datetime import date
from datetime import datetime
from datetime import time as dtime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


def _parse_min_quote_time(value: str) -> dtime:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError:
        return datetime.strptime(value, "%H:%M:%S").time()


def backfill(
    trade_date: str | None = None,
    min_valid_rows: int = 4500,
    min_latest_quote_time: str = "14:55",
    max_duplicate_prev_ratio: float = 0.8,
    allow_overwrite_tushare: bool = False,
) -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            if not trade_date:
                cursor.execute("SELECT MAX(trade_date) AS trade_date FROM stock_realtime_snapshot")
                trade_date = str((cursor.fetchone() or {}).get("trade_date") or "")
            if not trade_date:
                raise RuntimeError("stock_realtime_snapshot has no trade_date")

            cursor.execute(
                """
                SELECT COUNT(*) AS count
                FROM stock_realtime_snapshot r
                INNER JOIN stock_basic sb ON sb.code = r.code AND sb.instrument_type = 'stock'
                WHERE r.trade_date = %s
                  AND r.latest_price IS NOT NULL AND r.latest_price > 0
                  AND r.open_price IS NOT NULL AND r.open_price > 0
                  AND r.high_price IS NOT NULL AND r.high_price > 0
                  AND r.low_price IS NOT NULL AND r.low_price > 0
                """,
                (trade_date,),
            )
            valid_rows = int((cursor.fetchone() or {}).get("count") or 0)
            if valid_rows < min_valid_rows:
                raise RuntimeError(f"valid realtime rows too low for {trade_date}: {valid_rows} < {min_valid_rows}")

            min_quote_time = _parse_min_quote_time(min_latest_quote_time)
            cursor.execute(
                """
                SELECT MAX(quote_time) AS latest_quote_time
                FROM stock_realtime_snapshot r
                INNER JOIN stock_basic sb ON sb.code = r.code AND sb.instrument_type = 'stock'
                WHERE r.trade_date = %s
                  AND r.latest_price IS NOT NULL AND r.latest_price > 0
                  AND r.open_price IS NOT NULL AND r.open_price > 0
                  AND r.high_price IS NOT NULL AND r.high_price > 0
                  AND r.low_price IS NOT NULL AND r.low_price > 0
                """,
                (trade_date,),
            )
            latest_quote_time = (cursor.fetchone() or {}).get("latest_quote_time")
            if not latest_quote_time:
                raise RuntimeError(f"no latest realtime quote_time for {trade_date}")
            if latest_quote_time.date().isoformat() != trade_date or latest_quote_time.time() < min_quote_time:
                raise RuntimeError(
                    f"latest realtime quote_time too early for EOD backfill: {latest_quote_time} < {trade_date} {min_latest_quote_time}"
                )

            cursor.execute(
                "SELECT MAX(trade_date) AS prev_trade_date FROM daily_kline WHERE trade_date < %s",
                (trade_date,),
            )
            prev_trade_date = (cursor.fetchone() or {}).get("prev_trade_date")
            duplicate_prev_ratio = None
            duplicate_prev_count = 0
            comparable_prev_count = 0
            if prev_trade_date:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS comparable_count,
                        SUM(
                            r.open_price = p.open
                            AND r.high_price = p.high
                            AND r.low_price = p.low
                            AND r.latest_price = p.close
                            AND COALESCE(r.volume, -1) = COALESCE(p.volume, -1)
                        ) AS duplicate_count
                    FROM stock_realtime_snapshot r
                    INNER JOIN stock_basic sb ON sb.code = r.code AND sb.instrument_type = 'stock'
                    INNER JOIN daily_kline p ON p.code = r.code AND p.trade_date = %s
                    WHERE r.trade_date = %s
                      AND r.latest_price IS NOT NULL AND r.latest_price > 0
                      AND r.open_price IS NOT NULL AND r.open_price > 0
                      AND r.high_price IS NOT NULL AND r.high_price > 0
                      AND r.low_price IS NOT NULL AND r.low_price > 0
                    """,
                    (prev_trade_date, trade_date),
                )
                row = cursor.fetchone() or {}
                comparable_prev_count = int(row.get("comparable_count") or 0)
                duplicate_prev_count = int(row.get("duplicate_count") or 0)
                duplicate_prev_ratio = (duplicate_prev_count / comparable_prev_count) if comparable_prev_count else None
                if (
                    duplicate_prev_ratio is not None
                    and comparable_prev_count >= min_valid_rows
                    and duplicate_prev_ratio >= max_duplicate_prev_ratio
                ):
                    raise RuntimeError(
                        "realtime EOD bars look stale: "
                        f"{duplicate_prev_count}/{comparable_prev_count} rows equal previous trade date {prev_trade_date} "
                        f"(ratio={duplicate_prev_ratio:.2%})"
                    )

            sql = """
            INSERT INTO daily_kline (code, trade_date, open, high, low, close, volume, amount, source)
            SELECT
                r.code,
                r.trade_date,
                r.open_price,
                r.high_price,
                r.low_price,
                r.latest_price,
                r.volume,
                r.amount,
                'akshare_realtime_eod'
            FROM stock_realtime_snapshot r
            INNER JOIN stock_basic sb ON sb.code = r.code AND sb.instrument_type = 'stock'
            WHERE r.trade_date = %s
              AND r.latest_price IS NOT NULL AND r.latest_price > 0
              AND r.open_price IS NOT NULL AND r.open_price > 0
              AND r.high_price IS NOT NULL AND r.high_price > 0
              AND r.low_price IS NOT NULL AND r.low_price > 0
            ON DUPLICATE KEY UPDATE
                open = IF(%s OR daily_kline.source <> 'tushare_daily', r.open_price, daily_kline.open),
                high = IF(%s OR daily_kline.source <> 'tushare_daily', r.high_price, daily_kline.high),
                low = IF(%s OR daily_kline.source <> 'tushare_daily', r.low_price, daily_kline.low),
                close = IF(%s OR daily_kline.source <> 'tushare_daily', r.latest_price, daily_kline.close),
                volume = IF(%s OR daily_kline.source <> 'tushare_daily', r.volume, daily_kline.volume),
                amount = IF(%s OR daily_kline.source <> 'tushare_daily', r.amount, daily_kline.amount),
                source = IF(%s OR daily_kline.source <> 'tushare_daily', 'akshare_realtime_eod', daily_kline.source)
            """
            overwrite_flag = 1 if allow_overwrite_tushare else 0
            affected = cursor.execute(sql, (trade_date, *([overwrite_flag] * 7)))
            cursor.execute("SELECT COUNT(*) AS count FROM daily_kline WHERE trade_date = %s", (trade_date,))
            final_count = int((cursor.fetchone() or {}).get("count") or 0)

    return {
        "trade_date": trade_date,
        "valid_realtime_rows": valid_rows,
        "affected_rows": int(affected or 0),
        "daily_kline_count": final_count,
        "source": "akshare_realtime_eod",
        "latest_quote_time": str(latest_quote_time),
        "min_latest_quote_time": min_latest_quote_time,
        "prev_trade_date": str(prev_trade_date) if prev_trade_date else None,
        "duplicate_prev_count": duplicate_prev_count,
        "comparable_prev_count": comparable_prev_count,
        "duplicate_prev_ratio": round(duplicate_prev_ratio, 6) if duplicate_prev_ratio is not None else None,
        "max_duplicate_prev_ratio": max_duplicate_prev_ratio,
        "allow_overwrite_tushare": allow_overwrite_tushare,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill daily_kline EOD bars from cached realtime snapshot")
    parser.add_argument("--trade-date", help="Trade date to backfill, defaults to latest realtime snapshot date")
    parser.add_argument("--min-valid-rows", type=int, default=4500)
    parser.add_argument("--min-latest-quote-time", default="14:55", help="Require latest realtime quote_time at or after HH:MM before writing EOD bars.")
    parser.add_argument("--max-duplicate-prev-ratio", type=float, default=0.8, help="Reject writes if too many rows equal the previous trade date.")
    parser.add_argument("--allow-overwrite-tushare", action="store_true", help="Allow realtime EOD bars to overwrite existing tushare_daily rows.")
    parser.add_argument("--log-task", action="store_true")
    args = parser.parse_args()

    run_id = f"kline_realtime_eod_backfill_{date.today().strftime('%Y%m%d')}"
    logger = TaskRunLogger() if args.log_task else None
    metadata = {
        "trade_date": args.trade_date,
        "min_valid_rows": args.min_valid_rows,
        "min_latest_quote_time": args.min_latest_quote_time,
        "max_duplicate_prev_ratio": args.max_duplicate_prev_ratio,
        "allow_overwrite_tushare": args.allow_overwrite_tushare,
    }
    if logger:
        logger.start("daily_kline_realtime_eod_backfill", run_id, metadata=metadata)
    try:
        result = backfill(
            args.trade_date,
            args.min_valid_rows,
            args.min_latest_quote_time,
            args.max_duplicate_prev_ratio,
            args.allow_overwrite_tushare,
        )
        if logger:
            logger.finish(
                "daily_kline_realtime_eod_backfill",
                run_id,
                status="success",
                message=f"daily kline realtime EOD backfill completed, rows={result['daily_kline_count']}",
                metadata={**metadata, **result},
            )
        print(json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        if logger:
            logger.finish("daily_kline_realtime_eod_backfill", run_id, status="failed", message=str(exc)[:500], metadata=metadata)
        raise


if __name__ == "__main__":
    main()
