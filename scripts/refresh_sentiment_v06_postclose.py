"""Refresh real daily dependencies before publishing the v0.6 evening snapshot.

This is an operational refresh, not a forward observation or trading action.
The existing overnight updates remain as independent recovery paths.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import sys
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn


class DailyDependencyRepository:
    def calendar_day(self, day: date) -> dict:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""SELECT cal_date,is_open,source
                    FROM etf_rotation_trade_calendar
                    WHERE exchange_code='SSE' AND cal_date=%s""", (day,))
                return cursor.fetchone() or {}

    def coverage(self, day: date) -> dict:
        # Only this target partition, with the provider-supported SH/SZ universe.
        # Negative and zero net flow are valid; missing net flow is not zero.
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""SELECT COUNT(*) AS expected_rows,
                    COALESCE(SUM(k.code IS NOT NULL AND k.open>0 AND k.close>0
                        AND m.net_mf_amount IS NOT NULL
                        AND c.cost_50pct>0 AND c.weight_avg>0
                        AND c.winner_rate BETWEEN 0 AND 100),0) AS covered_rows
                    FROM stock_basic b
                    LEFT JOIN daily_kline k ON k.code=b.code AND k.trade_date=%s
                    LEFT JOIN stock_moneyflow_daily m ON m.code=b.code AND m.trade_date=%s
                    LEFT JOIN stock_chip_daily c ON c.code=b.code AND c.trade_date=%s
                    WHERE b.instrument_type='stock' AND COALESCE(b.is_delisted,0)=0
                      AND (b.code LIKE 'sh.%%' OR b.code LIKE 'sz.%%')""", (day, day, day))
                return dict(cursor.fetchone() or {})


def refresh_postclose(*, now: datetime | None = None, dry_run: bool = False,
                      repository=None, moneyflow_factory=None, chip_factory=None,
                      materializer_factory=None) -> dict:
    current = now or datetime.now(ZoneInfo('Asia/Shanghai'))
    if current.tzinfo is None:
        raise ValueError('refresh clock must be timezone-aware')
    current = current.astimezone(ZoneInfo('Asia/Shanghai'))
    day = current.date()
    repo = repository or DailyDependencyRepository()
    calendar = repo.calendar_day(day)
    if (str(calendar.get('cal_date')) != str(day)
            or calendar.get('source') != 'tushare.trade_cal'
            or calendar.get('is_open') not in (0, 1)):
        raise ValueError('independent current-day calendar missing or invalid')
    if not calendar['is_open'] or current.time() < time(18, 45):
        return {'status': 'skipped', 'trade_date': str(day),
                'reason': 'not_open_day_or_before_1845', 'external_provider_calls': False}
    if dry_run:
        return {'status': 'dry_run', 'trade_date': str(day),
                'coverage': repo.coverage(day), 'external_provider_calls': False}
    if moneyflow_factory is None:
        from app.data_ingestion.moneyflow_sync import MoneyflowSync
        moneyflow_factory = MoneyflowSync
    if chip_factory is None:
        from app.data_ingestion.chip_sync import ChipSync
        chip_factory = ChipSync
    archive_enabled = materializer_factory is None
    if materializer_factory is None:
        from app.backtest.sentiment_evidence import archiving_materializer
        materializer_factory = archiving_materializer
    dependencies = {}
    for name, factory in [('moneyflow', moneyflow_factory), ('chip', chip_factory)]:
        result = factory().run(str(day), str(day), pause_seconds=0)
        dependencies[name] = result
        if (result.get('trade_dates') != 1 or result.get('processed_days') != 1
                or int(result.get('rows_synced') or 0) <= 0):
            raise ValueError(f'{name}: target day returned no data; snapshot not published')
    coverage = repo.coverage(day)
    expected = int(coverage.get('expected_rows') or 0)
    covered = int(coverage.get('covered_rows') or 0)
    if expected <= 0 or covered / expected < 0.98:
        raise ValueError(f'target-day valid daily dependency coverage {covered}/{expected} below 98%')
    materializer = materializer_factory()
    snapshot = materializer.materialize(
        strategy_id='a_share_sentiment_v06', allow_shadow=True)
    if (snapshot.get('snapshot_status') != 'ready' or snapshot.get('quality_status') != 'passed'
            or str(snapshot.get('trade_date')) != str(day)):
        raise ValueError('materializer did not return a ready/passed target-day snapshot')
    if archive_enabled:
        from app.backtest.sentiment_evidence import seal_published_result
        snapshot['evidence_archive'] = seal_published_result(
            snapshot, input_events=materializer.evidence_archive_events)
    return {'status': 'success', 'trade_date': str(day), 'dependencies': dependencies,
            'coverage': coverage, 'snapshot': snapshot, 'external_provider_calls': True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    from app.display_cache.events import TaskRunLogger
    task_name = 'sentiment_v06_postclose_refresh'
    run_id = f'{task_name}_{datetime.now():%Y%m%d_%H%M%S_%f}'
    logger = TaskRunLogger()
    # The lock is local and never held while waiting for another process.
    with (PROJECT_ROOT / 'logs' / '.sentiment_v06_postclose.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'status': 'skipped', 'reason': 'refresh_already_running'}))
            return 0
        if not args.dry_run:
            logger.start(task_name, run_id, {'strategy_id': 'a_share_sentiment_v06'})
        try:
            result = refresh_postclose(dry_run=args.dry_run)
            if not args.dry_run:
                archive_warning = result.get('snapshot', {}).get('evidence_archive', {}).get('status') == 'warning'
                message = result['status'] + (' 证据封存告警' if archive_warning else '')
                logger.finish(task_name, run_id, 'success', message, result)
            print(json.dumps(result, ensure_ascii=False, default=str))
            return 0
        except Exception as exc:
            if not args.dry_run:
                logger.finish(task_name, run_id, 'failed', str(exc)[:500],
                              {'error_code': type(exc).__name__})
            print(json.dumps({'status': 'failed', 'error': str(exc)[:1000]}, ensure_ascii=False),
                  file=sys.stderr)
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
