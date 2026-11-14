from __future__ import annotations

import json
import subprocess
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from app.api.routes import dashboard, stocks, tracking

ROOT = Path(__file__).resolve().parents[1]


class ReadSplittingTests(unittest.TestCase):
    def test_risk_projection_preserves_alert_and_delegates_cached_contract(self):
        alert = {'active': True, 'blocking': False, 'selection_allowed': True, 'evidence': ['red']}
        with patch('app.display_cache.service.entry', return_value={'payload': {'market_overview': {'risk_alert': alert}}}) as get:
            self.assertEqual(dashboard.dashboard_risk_alert(), {'risk_alert': alert})
        get.assert_called_once_with('dashboard')

    def test_risk_cache_miss_does_not_build_homepage(self):
        with patch('app.display_cache.service.entry', return_value=None), patch.object(dashboard, '_dashboard_market_overview', return_value={'risk_alert': {'active': False}}), patch.object(dashboard, '_build_dashboard_summary') as full:
            self.assertEqual(dashboard.dashboard_risk_alert(), {'risk_alert': {'active': False}})
        full.assert_not_called()

    def test_compact_filters_does_not_mutate_shared_payload(self):
        original = {'available_runs': [1], 'selection_dates': ['2026-11-13'], 'strategy_options': []}
        with patch('app.display_cache.service.get', return_value=original):
            result = tracking.get_tracking_filters('stock', None, False)
        self.assertNotIn('available_runs', result)
        self.assertEqual(original['available_runs'], [1])

    def test_dynamic_does_not_load_news_history_selections_chips_or_minutes(self):
        statements = []
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql, params=()): statements.append(sql)
            def fetchone(self):
                if 'FROM stock_basic' in statements[-1]: return {'code': 'sh.600519'}
                return {}
            def fetchall(self): return []
        class Conn:
            def cursor(self): return Cursor()
        @contextmanager
        def conn(): yield Conn()
        with patch.object(stocks, 'mysql_read_conn', conn), patch.object(stocks, '_display_revision', return_value='r'), patch.object(stocks, 'cached_bars') as bars:
            payload = stocks.stock_detail('sh.600519', 600, 12, 240, 'dynamic')
        text = '\n'.join(statements)
        self.assertNotIn('stock_news', text)
        self.assertNotIn('selection_result', text)
        self.assertNotIn('stock_realtime_intraday', text)
        self.assertNotIn('SELECT\n                    trade_date,\n                    his_low', text)
        self.assertNotIn('price_history', payload)
        self.assertNotIn('fundamentals', payload)
        bars.assert_not_called()

    def test_history_query_binds_range_and_preserves_chronological_output(self):
        queries = []
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql, params=()): queries.append((sql, params))
            def fetchone(self): return {'min_date':'2024-05-01', 'max_date':'2026-11-13', 'revision':'r'}
            def fetchall(self): return [{'trade_date':'2026-11-13', 'close':10}, {'trade_date':'2026-11-12','close':9}]
        class Conn:
            def cursor(self): return Cursor()
        @contextmanager
        def conn(): yield Conn()
        with patch.object(stocks, 'mysql_read_conn', conn):
            result = stocks.stock_price_history('sh.600519', 35, '2026-11-01', '2026-11-13')
        self.assertEqual(queries[-1][1], ('sh.600519','2024-05-01','2026-11-01','2026-11-13',35))
        self.assertIn('LIMIT %s', queries[-1][0])
        self.assertEqual([r['trade_date'] for r in result['items']], ['2026-11-12','2026-11-13'])


class BrowserReadCacheTests(unittest.TestCase):
    def run_js(self, body):
        source = str(ROOT/'app/api/web/js/common.js')
        code = f"""
const fs=require('fs'),vm=require('vm');
let identity='alice',calls=0,fail=false,release;
const c={{Date,URL,Map,Promise,Headers,window:{{fetch:async()=>{{}},addEventListener:()=>{{}}, location:{{href:'https://example.test',pathname:'/',search:'',hash:'',origin:'https://example.test'}}}},document:{{cookie:'',addEventListener:()=>{{}}}}}};
vm.createContext(c);
vm.runInContext(fs.readFileSync({json.dumps(source)},'utf8').split('function qs(')[0],c);
vm.runInContext(`getCookieValue=()=>identity; fetchJson=async()=>{{ calls++; if (fail) throw new Error('offline'); return {{calls}}; }};`,c);
c.identity='alice';c.calls=0;c.fail=false;
(async()=>{{const result=await vm.runInContext(`(async()=>{{{body}}})()`,c);process.stdout.write(JSON.stringify(result));}})().catch(e=>{{console.error(e);process.exit(1)}});
"""
        r = subprocess.run(['node','-e',code],check=True,capture_output=True,text=True)
        return json.loads(r.stdout)

    def test_deduplicates_inflight_and_invalidates_revision_identity_and_mutation(self):
        result = self.run_js("""
const [a,b]=await Promise.all([fetchReadJson('/api/stocks/test/history'),fetchReadJson('/api/stocks/test/history')]);
await fetchReadJson('/api/stocks/test/history');const before=calls;
await fetchReadJson('/api/stocks/test/history',{revision:'new-source'});const revision=calls;
identity='bob';await fetchReadJson('/api/stocks/test/history');const user=calls;
clearReadCache();await fetchReadJson('/api/stocks/test/history');return {before,revision,user,after:calls,a,b};
""")
        self.assertEqual([result[k] for k in ['before','revision','user','after']], [1,2,3,4])
        self.assertEqual(result['a'], result['b'])

    def test_failed_response_not_cached_and_ttl_expires(self):
        result = self.run_js("""
fail=true;try{await fetchReadJson('/api/stocks/test/history');}catch(e){}
fail=false;await fetchReadJson('/api/stocks/test/history',{ttl:0});await fetchReadJson('/api/stocks/test/history');return {calls};
""")
        self.assertEqual(result['calls'],3)

    def test_inflight_after_mutation_cannot_publish_stale_result(self):
        result = self.run_js("""
let resolve;fetchJson=()=>new Promise(r=>{resolve=r;});
const pending=fetchReadJson('/api/stocks/test/history');clearReadCache();resolve({old:true});
let rejected=false;try{await pending;}catch(e){rejected=true;}return {rejected,size:readCache.size};
""")
        self.assertEqual(result, {'rejected': True, 'size': 0})


if __name__ == '__main__': unittest.main()
