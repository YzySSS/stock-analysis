from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import date, datetime, time, timedelta

from app.data_ingestion.daily_kline_sync import DailyKlineSync
from app.shared.task_log import TaskRunLogger


TUSHARE_DAILY_READY_TIME = time(18, 0)


def completed_market_date_cutoff(now: datetime | None = None) -> str:
    """Return the latest calendar date whose daily bar can be considered complete."""

    current = now or datetime.now()
    cutoff = current.date()
    if current.time() < TUSHARE_DAILY_READY_TIME:
        cutoff -= timedelta(days=1)
    return cutoff.isoformat()


def resolve_target_trade_date(
    sync: DailyKlineSync,
    *,
    requested_trade_date: str | None = None,
    now: datetime | None = None,
) -> str:
    if requested_trade_date:
        return date.fromisoformat(requested_trade_date).isoformat()
    return sync.latest_open_trade_date(end_date=completed_market_date_cutoff(now))


def build_run_id(trade_date: str, now: datetime | None = None) -> str:
    current = now or datetime.now()
    return (
        f"daily_kline_increment_{trade_date.replace('-', '')}_"
        f"{current.strftime('%Y%m%d_%H%M%S')}"
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Calibrate the latest completed A-share daily bars from Tushare"
    )
    parser.add_argument(
        "--trade-date",
        help="explicit completed trade date (YYYY-MM-DD); defaults to the latest completed open date",
    )
    args = parser.parse_args(argv)

    logger = TaskRunLogger()
    sync = DailyKlineSync()
    trade_date = resolve_target_trade_date(sync, requested_trade_date=args.trade_date)
    run_id = build_run_id(trade_date)
    metadata = {
        "mode": "incremental_daily",
        "trade_date": trade_date,
        "start_date": trade_date,
        "end_date": trade_date,
        "limit": None,
        "source": "tushare_daily",
    }
    logger.start(task_name="daily_kline_increment", run_id=run_id, metadata=metadata)
    try:
        result = sync.run(
            start_date=trade_date,
            end_date=trade_date,
            limit=None,
            instrument_type="stock",
            pause_seconds=0.05,
            relogin_every=20,
        )
        payload = {**metadata, **result}
        rows_synced = int(result.get("rows_synced") or 0)
        if rows_synced <= 0:
            raise RuntimeError(
                f"Tushare daily returned zero rows for completed trade date {trade_date}"
            )
        logger.finish(
            task_name="daily_kline_increment",
            run_id=run_id,
            status="success",
            message=f"daily kline incremental sync completed, rows={rows_synced}",
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(
            task_name="daily_kline_increment",
            run_id=run_id,
            status="failed",
            message=str(exc)[:500],
            metadata={**metadata, **(result if "result" in locals() else {})},
        )
        raise


if __name__ == "__main__":
    main()
