from __future__ import annotations

import json
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime

from app.data_ingestion.daily_kline_sync import DailyKlineSync
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

BATCH_SIZE = 50
START_DATE = '2025-01-01'
END_DATE = '2026-04-27'


def build_run_id() -> str:
    return f"daily_kline_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def fetch_missing_codes(limit: int = BATCH_SIZE) -> list[str]:
    sql = """
    SELECT code
    FROM stock_basic
    WHERE instrument_type = 'stock'
      AND code NOT IN (SELECT DISTINCT code FROM daily_kline)
    ORDER BY code
    LIMIT %s
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (limit,))
            return [row['code'] for row in cursor.fetchall()]


def main() -> None:
    logger = TaskRunLogger()
    run_id = build_run_id()
    codes = fetch_missing_codes()
    metadata = {
        'mode': 'historical_backfill',
        'start_date': START_DATE,
        'end_date': END_DATE,
        'batch_size': BATCH_SIZE,
        'requested_codes': len(codes),
        'codes': codes,
    }
    logger.start(task_name='daily_kline_backfill', run_id=run_id, metadata=metadata)

    if not codes:
        payload = {**metadata, 'message': 'no missing codes'}
        logger.finish(
            task_name='daily_kline_backfill',
            run_id=run_id,
            status='success',
            message='no missing codes left for backfill',
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return

    try:
        sync = DailyKlineSync()
        result = sync.run(
            start_date=START_DATE,
            end_date=END_DATE,
            codes=codes,
            pause_seconds=0.1,
            relogin_every=5,
        )
        payload = {**metadata, **result}
        logger.finish(
            task_name='daily_kline_backfill',
            run_id=run_id,
            status='success',
            message=f"daily kline backfill completed, success_codes={result.get('success_codes', 0)}",
            metadata=payload,
        )
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(
            task_name='daily_kline_backfill',
            run_id=run_id,
            status='failed',
            message=str(exc)[:500],
            metadata=metadata,
        )
        raise


if __name__ == '__main__':
    main()
