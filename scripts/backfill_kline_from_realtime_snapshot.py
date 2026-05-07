from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


def backfill(trade_date: str | None = None, min_valid_rows: int = 4500) -> dict:
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
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                amount = VALUES(amount),
                source = VALUES(source)
            """
            affected = cursor.execute(sql, (trade_date,))
            cursor.execute("SELECT COUNT(*) AS count FROM daily_kline WHERE trade_date = %s", (trade_date,))
            final_count = int((cursor.fetchone() or {}).get("count") or 0)

    return {
        "trade_date": trade_date,
        "valid_realtime_rows": valid_rows,
        "affected_rows": int(affected or 0),
        "daily_kline_count": final_count,
        "source": "akshare_realtime_eod",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill daily_kline EOD bars from cached realtime snapshot")
    parser.add_argument("--trade-date", help="Trade date to backfill, defaults to latest realtime snapshot date")
    parser.add_argument("--min-valid-rows", type=int, default=4500)
    parser.add_argument("--log-task", action="store_true")
    args = parser.parse_args()

    run_id = f"kline_realtime_eod_backfill_{date.today().strftime('%Y%m%d')}"
    logger = TaskRunLogger() if args.log_task else None
    metadata = {"trade_date": args.trade_date, "min_valid_rows": args.min_valid_rows}
    if logger:
        logger.start("daily_kline_realtime_eod_backfill", run_id, metadata=metadata)
    try:
        result = backfill(args.trade_date, args.min_valid_rows)
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
