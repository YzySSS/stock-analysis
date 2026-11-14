from __future__ import annotations
import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch
from pymysql import OperationalError
from app.tracking import minute_daily
from app.tracking.repository import TrackingRepository
from app.api.routes import tracking
from tests.test_tracking_repository import FakeTracker


class Cursor:
    def __init__(self, *, dates=(), missing=False):
        self.dates, self.missing, self.sql = dates, missing, []
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, params=None):
        self.sql.append((' '.join(sql.split()), params))
        if self.missing and 'SELECT trade_date FROM tracking_minute_daily_manifest' in sql:
            raise OperationalError(1146, 'missing')
    def fetchall(self):
        return [{'trade_date': day} for day in self.dates] if 'SELECT trade_date FROM tracking_minute_daily_manifest' in self.sql[-1][0] else []
    def fetchone(self): return {'status': 'ready'}


class DailyModelTests(unittest.TestCase):
    def test_ready_dates_use_one_snapshot_and_constant_partition_exclusion(self):
        cur = Cursor(dates=[date(2026, 11, 13)])
        @contextmanager
        def connect(**kwargs):
            class Conn:
                def cursor(self): return cur
            yield Conn()
        TrackingRepository(connection_factory=connect).list_selection_result_rows(limit=10, instrument_type='stock')
        self.assertEqual(cur.sql[0][0], 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        sql, params = cur.sql[-1]
        self.assertIn('m.source_revision=d.source_revision', sql)
        self.assertIn('ri.trade_date NOT IN (%s)', sql)
        self.assertIn('COUNT(DISTINCT trade_date)', sql)
        self.assertEqual(params[-1], date(2026, 11, 13))
        self.assertIn('ri.quote_minute >= TIMESTAMP(DATE_ADD(sr_inner.trade_date, INTERVAL 1 DAY))', sql)

    def test_missing_migration_falls_back_without_dropping_source(self):
        cur = Cursor(missing=True)
        self.assertEqual(minute_daily.ready_dates(cur), [])
        cur = Cursor(dates=[])
        @contextmanager
        def connect(**kwargs):
            class Conn:
                def cursor(self): return cur
            yield Conn()
        TrackingRepository(connection_factory=connect).list_selection_result_rows(limit=10, instrument_type='stock')
        self.assertIn('stock_realtime_intraday_tracked ri', cur.sql[-1][0])
        self.assertNotIn('tracking_minute_daily d', cur.sql[-1][0])

    def test_retiring_day_cannot_be_republished_after_partition_withdrawal(self):
        cur = Cursor()
        cur.fetchone = lambda: {'status': 'retiring'}
        with self.assertRaisesRegex(RuntimeError, 'retiring'):
            minute_daily.lock_day(cur, date(2026, 1, 1))
        minute_daily.lock_day(cur, date(2026, 1, 1), allow_retiring=True)

    def test_failed_tracked_copy_protects_raw_source_from_retention(self):
        from app.data_ingestion import realtime_lifecycle as lifecycle
        days = [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]
        with patch.object(lifecycle, '_date_rows', return_value=days), \
             patch.object(lifecycle, '_rollup_complete', return_value=True), \
             patch.object(lifecycle, '_delete_trade_date') as delete:
            result = lifecycle.apply_retention(protected_raw_dates=[days[0]])
        self.assertEqual(result['raw'][0]['method'], 'protected_failed_tracked_copy')
        delete.assert_not_called()

    def test_split_list_never_computes_global_summary(self):
        fake = FakeTracker()
        with patch.object(tracking, 'SelectionResultTracker', return_value=fake), \
             patch.object(tracking, '_count_tracking_items', return_value=23), \
             patch.object(tracking, '_full_tracking_summary') as summary:
            result = tracking._tracking_payload(limit=10, offset=10, include_runs=False, include_summary=False)
        summary.assert_not_called()
        self.assertEqual(len(fake.calls), 1)
        self.assertEqual(fake.calls[0]['limit'], 10)
        self.assertIsNone(result['filtered_summary'])
        self.assertEqual(result['pagination']['total'], 23)

    def test_independent_summary_still_uses_entire_scope_and_exclusions(self):
        fake = FakeTracker()
        with patch.object(tracking, 'SelectionResultTracker', return_value=fake), \
             patch.object(tracking, '_count_tracking_items', return_value=23), \
             patch.object(tracking, '_get_cached_tracking_summary', return_value=None), \
             patch.object(tracking, '_cache_tracking_summary'):
            result = tracking.get_tracking_summary(run_id=None, strategy_id='s', selection_date=None, instrument_type='stock', latest_only=False)
        self.assertEqual(fake.calls[0]['limit'], 23)
        self.assertTrue(fake.calls[0]['include_in_stats_only'])
        self.assertEqual(result['filtered_summary']['excluded_count'], 22)
        self.assertEqual(result['filtered_summary']['count'], 1)


if __name__ == '__main__': unittest.main()

class TrackingIndependentFrontendTests(unittest.TestCase):
    def test_table_is_rendered_while_global_summary_is_pending_or_failed(self):
        import subprocess
        from pathlib import Path
        source = Path('app/api/web/js/tracking.js').read_text()
        section = source[source.index('let trackingReadRequest'):source.index('function waitForMilliseconds')]
        harness = r'''
const vm = require('vm');
const assert = require('assert');
let resolveSummary; let renderCount = 0;
const elements = new Map();
const ctx = {window:{addEventListener(){}}, URLSearchParams, Date,
  getCookieValue:()=> 'identity-1',
  qs:(s)=> { if (!elements.has(s)) elements.set(s,{textContent:''}); return elements.get(s); },
  loadTrackingFilters:async()=>({strategy_options:[],selection_dates:[]}),
  renderStrategyOptions(){},renderSelectionDateOptions(){},updateTrackingStats(){},
  renderTrackingTable(){renderCount++},renderReviewSummary(){},renderReviewNotes(){},renderPagination(){},
  lastTrackingState:{deepReviewAvailable:true},
  fetchJson: (url) => url.startsWith('/api/tracking/summary') ? new Promise((resolve,reject)=>{resolveSummary=()=>reject(new Error('injected failure'))}) : Promise.resolve({items:[{code:'a'}],summary:{benchmark_return_pct:0},pagination:{total:99},stats_retention:{},summary_revision:'r1'}),
};
vm.createContext(ctx);
vm.runInContext(SOURCE,ctx);
(async()=>{
 const pending=ctx.loadTrackingData();
 await new Promise(r=>setImmediate(r));
 assert.equal(renderCount,1,'table must not wait for global summary');
 assert(ctx.qs('#tracking-review-summary').textContent.includes('统计计算中'));
 resolveSummary();await pending;
 assert.equal(renderCount,1,'summary failure must not erase or rerender table');
 assert(ctx.qs('#tracking-review-summary').textContent.includes('统计加载失败'));
})().catch(e=>{console.error(e);process.exitCode=1});
'''.replace('SOURCE', __import__('json').dumps(section))
        subprocess.run(['node','-e',harness],check=True,capture_output=True,text=True)
