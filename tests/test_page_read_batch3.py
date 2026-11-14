from __future__ import annotations
import json,subprocess,unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch
from app.backtest.repository import BacktestRepository
from app.display_reads.forward_evidence import _rounded_average
from tests.test_backtest_repository import FakeCursor,connection_factory

class BatchedReadsTests(unittest.TestCase):
    def test_same_entry_deduplicated_and_distinct_windows_keep_limit_before_union(self):
        rows=[{'code':'x','trade_date':'2026-01-01','entry_date':'2026-01-02'},
              {'code':'x','trade_date':'2026-01-02','entry_date':'2026-01-02'},
              {'code':'x','trade_date':'2026-01-03','entry_date':'2026-01-04'}]
        bars=[{'horizon_key':1,'trade_date':'2026-01-05','close':2},
              {'horizon_key':0,'trade_date':'2026-01-03','close':1},
              {'horizon_key':0,'trade_date':'2026-01-02','close':0}]
        cursor=FakeCursor(fetchone_values=[{}, {'total':3}],fetchall_values=[rows,bars])
        result=BacktestRepository(connection_factory(cursor)).load_trade_page(run_id='r',limit=3,offset=0,trade_date=None,code=None,return_mode='1d')
        self.assertEqual(len(cursor.executed),4)
        self.assertEqual(cursor.executed[-1][0].count('LIMIT 5'),2)
        self.assertEqual(result['horizon_bars'][('2026-01-01','x')],result['horizon_bars'][('2026-01-02','x')])
        self.assertEqual(result['horizon_bars'][('2026-01-03','x')][0]['close'],2)
        self.assertEqual(result['horizon_bars'][('2026-01-01','x')][0]['trade_date'],'2026-01-02')

    def test_empty_trade_page_has_no_horizon_query(self):
        cur=FakeCursor(fetchone_values=[{},{}],fetchall_values=[[]])
        BacktestRepository(connection_factory(cur)).load_trade_page(run_id='r',limit=10,offset=99,trade_date=None,code=None,return_mode='1d')
        self.assertEqual(len(cur.executed),3)

    def test_compact_runs_do_not_read_full_summary_or_methodology(self):
        cur=FakeCursor(fetchall_values=[[]])
        BacktestRepository(connection_factory(cur)).list_runs(limit=20,include_system_tests=False,compact=True,display=True)
        sql=cur.executed[0][0]
        self.assertNotIn('SELECT *',sql)
        self.assertNotIn('summary_json',sql)
        self.assertNotIn('methodology_json',sql)
        self.assertIn('JSON_EXTRACT(request_json',sql)
        self.assertIn('LIMIT %s',sql)

    def test_exact_float_mean_rounding_at_decimal_half_steps(self):
        from statistics import mean
        cur=FakeCursor(fetchall_values=[[{'v':.0001,'n':1},{'v':.0002,'n':1}]])
        self.assertEqual(_rounded_average(cur,.00015,'scalar query',()),round(mean([.0001,.0002]),4))
        self.assertEqual(len(cur.executed),1)

class BacktestFrontendReadsTests(unittest.TestCase):
    def test_unchanged_poll_retries_failure_and_rejects_out_of_order_run(self):
        source=Path('app/api/web/js/backtest.js').read_text()
        state=source[:source.index('const BACKTEST_STRATEGY_LABELS')]
        loaders=source[source.index('function pickDefaultBacktestRun'):source.index('async function runBacktest')]
        harness=r'''
const vm=require('vm'),assert=require('assert');
const calls=[],renders=[],elements=new Map();let pendingA,failTrades=false;
const ctx={console,Map,JSON,Promise,Date,URLSearchParams,window:{addEventListener(){}},document:{hidden:false},
 getCookieValue:()=> 'identity',qs:(s)=>{if(!elements.has(s))elements.set(s,{value:'1d',textContent:'',innerHTML:'',scrollIntoView(){}});return elements.get(s)},
 renderRuns(){},updatePolling(){},setBacktestStats:(d)=>renders.push(d.run_id),renderCurve(){},
 renderTrades:(items,mode,meta)=>{renders.push(meta.run_id||'clear');vm.runInContext('currentBacktestTradesPage='+Number(meta.page||1),ctx)},
 renderError:(el,msg)=>{el.textContent=msg},renderEmptyRow:(n,msg)=>msg,
 loadBacktestStrategies:async()=>{},loadFactorStatus:async()=>{},
 fetchJson:async(url)=>{calls.push(url);if(url.includes('/runs?'))return {items:[]};
 if(url.includes('run_id=A')&&url.includes('/results'))return new Promise(r=>{pendingA=r});
 if(url.includes('/trades')){if(failTrades)throw Error('injected');return {run_id:url.includes('run_id=B')?'B':'A',items:[],page:1};}
 return {run_id:'B',return_mode:'1d',curve:[]};}
};vm.createContext(ctx);vm.runInContext(STATE+LOADERS,ctx);
(async()=>{
 vm.runInContext("backtestRead.items.set('A',{run_id:'A',status:'success',progress_done_days:2,return_mode:'1d'});backtestRead.items.set('B',{run_id:'B',status:'success',progress_done_days:2,return_mode:'1d'})",ctx);
 const a=ctx.loadBacktestResult('A',{scroll:false});await new Promise(r=>setImmediate(r));
 await ctx.loadBacktestResult('B',{scroll:false});pendingA({run_id:'A',return_mode:'1d',curve:[]});await a;
 assert.equal(vm.runInContext('currentBacktestRunId',ctx),'B');assert.equal(renders.at(-1),'B');
 const oldCount=calls.length;await ctx.loadBacktestResult('B',{refreshChanged:true,keepTradePage:true,scroll:false});assert.equal(calls.length,oldCount,'unchanged state must skip both full reads');
 vm.runInContext('backtestRead.tradeKey=null',ctx);failTrades=true;await ctx.loadBacktestResult('B',{refreshChanged:true,keepTradePage:true,scroll:false});
 assert(ctx.qs('#backtest-trades-body').innerHTML.includes('injected'));assert.equal(vm.runInContext('backtestRead.tradeKey',ctx),null);
 failTrades=false;await ctx.loadBacktestResult('B',{refreshChanged:true,keepTradePage:true,scroll:false});assert(vm.runInContext('backtestRead.tradeKey',ctx));
 assert.equal(vm.runInContext('backtestRead.pending',ctx),0);
 vm.runInContext("backtestRead.items.get('B').progress_done_days=3",ctx);const before=calls.length;
 await ctx.loadBacktestResult('B',{refreshChanged:true,keepTradePage:true,scroll:false});assert.equal(calls.length,before+2);
})().catch(e=>{console.error(e);process.exitCode=1});
'''.replace('STATE',json.dumps(state)).replace('LOADERS',json.dumps(loaders))
        subprocess.run(['node','-e',harness],check=True,capture_output=True,text=True)
