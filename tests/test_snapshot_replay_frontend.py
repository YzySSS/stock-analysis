from __future__ import annotations

import json
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("node"), "node is required for frontend request-race checks")
class SnapshotReplayFrontendTests(unittest.TestCase):
    def run_js(self, expression):
        bootstrap = r"""
const fs=require('fs'),vm=require('vm');
const source=fs.readFileSync(SOURCE,'utf8').split("qs('#backtest-form')?.addEventListener")[0];
class Element {
  constructor(value='') { this.value=value;this.open=false;this.disabled=false;this.listeners={};this.textContent='';this._html=''; }
  addEventListener(type,callback) { this.listeners[type]=callback; }
  set textContent(value) { this._text=value;this._html=''; }
  get textContent() { return this._text; }
  set innerHTML(value) { this._html=value;this._text='';const option=value.match(/<option value="([^"]*)"/);if(option)this.value=option[1]; }
  get innerHTML() { return this._html; }
}
const elements={};
['diagnosis','strategy','id','holding','view','result'].forEach(key=>elements['#recorded-snapshot-'+key]=new Element());
elements['#recorded-snapshot-strategy'].value='a_share_sentiment_v06';
elements['#recorded-snapshot-holding'].value='3';
const calls=[];
const context={console,Map,Number,String,JSON,URLSearchParams,encodeURIComponent,currentIdentity:'user1',elements,calls,
  qs:key=>elements[key],qsa:()=>[],
  fetchJson:async url=>{calls.push(url);return {snapshots:[]};},
  formatPercent:value=>value==null||Number.isNaN(Number(value))?'-':Number(value).toFixed(2)+'%',
  escapeHtml:value=>String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;'),
};
context.getCookieValue=()=>context.currentIdentity;
vm.createContext(context);vm.runInContext(source,context);
(async()=>{const result=await vm.runInContext('(async()=>{'+EXPRESSION+'})()',context);process.stdout.write(JSON.stringify(result));})().catch(error=>{console.error(error);process.exit(1);});
""".replace("SOURCE", json.dumps(str(ROOT / "app/api/web/js/backtest.js"))).replace(
            "EXPRESSION", json.dumps(expression)
        )
        result = subprocess.run(["node", "-e", bootstrap], cwd=ROOT,
                                capture_output=True, check=True, text=True)
        return json.loads(result.stdout)

    def test_closed_section_is_lazy_and_reopening_loaded_list_does_not_refetch(self):
        result = self.run_js("""
initRecordedSnapshotDiagnosis();
await loadRecordedSnapshots();const initial=calls.length;
elements['#recorded-snapshot-diagnosis'].open=true;
await loadRecordedSnapshots();await loadRecordedSnapshots();
return {initial,calls};
""")
        self.assertEqual(result["initial"], 0)
        self.assertEqual(len(result["calls"]), 1)
        self.assertIn("strategy_id=a_share_sentiment_v06&limit=50", result["calls"][0])
        page = (ROOT / "app/api/web/pages/backtest.html").read_text()
        tag = next(part.split(">", 1)[0] for part in page.split("<details ")
                   if 'id="recorded-snapshot-diagnosis"' in part.split(">", 1)[0])
        self.assertNotIn("open", tag)

    def test_out_of_order_strategy_lists_do_not_overwrite_new_selection(self):
        result = self.run_js("""
const pending=[];fetchJson=url=>new Promise(resolve=>pending.push({url,resolve}));
elements['#recorded-snapshot-diagnosis'].open=true;
const old=loadRecordedSnapshots();
elements['#recorded-snapshot-strategy'].value='a_share_sentiment_v05';
const fresh=loadRecordedSnapshots();
pending[1].resolve({snapshots:[{snapshot_id:'new',published_at:'2026-11-17 09:32:00',strategy_version:'0.5.1',candidate_count:2}]});
await fresh;
pending[0].resolve({snapshots:[{snapshot_id:'old',published_at:'2026-11-16 09:32:00',strategy_version:'0.6.0',candidate_count:3}]});
await old;return {html:elements['#recorded-snapshot-id'].innerHTML};
""")
        self.assertIn('value="new"', result["html"])
        self.assertNotIn('value="old"', result["html"])

    def test_late_diagnosis_cannot_replace_new_snapshot_result(self):
        result = self.run_js("""
const pending=[];fetchJson=url=>new Promise(resolve=>pending.push(resolve));
elements['#recorded-snapshot-diagnosis'].open=true;
elements['#recorded-snapshot-id'].value='old';const old=viewRecordedSnapshotDiagnosis();
elements['#recorded-snapshot-id'].value='new';invalidateRecordedSnapshot({keepOptions:true});
const fresh=viewRecordedSnapshotDiagnosis();
pending[1]({ready_count:1,labels:[{name:'最新记录',status:'ready',original_trade_grade:'watch',return_pct:1}]});await fresh;
pending[0]({ready_count:1,labels:[{name:'过期记录',status:'ready',original_trade_grade:'watch',return_pct:9}]});await old;
return {html:elements['#recorded-snapshot-result'].innerHTML,disabled:elements['#recorded-snapshot-view'].disabled};
""")
        self.assertIn("最新记录", result["html"])
        self.assertNotIn("过期记录", result["html"])
        self.assertFalse(result["disabled"])

    def test_changed_login_identity_discards_inflight_result(self):
        result = self.run_js("""
let resolve;fetchJson=()=>new Promise(done=>resolve=done);
elements['#recorded-snapshot-diagnosis'].open=true;
elements['#recorded-snapshot-id'].value='old';const old=viewRecordedSnapshotDiagnosis();
currentIdentity='user2';
resolve({ready_count:1,labels:[{name:'旧身份结果',status:'ready'}]});await old;
backtestIdentity();
return {html:elements['#recorded-snapshot-result'].innerHTML,text:elements['#recorded-snapshot-result'].textContent,snapshot:elements['#recorded-snapshot-id'].value};
""")
        self.assertNotIn("旧身份结果", result["html"])
        self.assertEqual(result["text"], "")
        self.assertEqual(result["snapshot"], "")

    def test_candidate_names_grades_and_reasons_are_escaped(self):
        result = self.run_js("""
const html=renderRecordedSnapshotDiagnosis({ready_count:0,pending_count:0,excluded_count:1,labels:[{
name:'<img src=x onerror=alert(1)>',original_trade_grade:'<b>grade</b>',status:'excluded',reason:'<script>oops</script>',
entry_date:'2026-11-03',exit_date:'2026-11-05',return_pct:null}]});return {html};
""")
        self.assertNotIn("<img", result["html"])
        self.assertNotIn("<script>", result["html"])
        self.assertIn("&lt;img", result["html"])
        self.assertIn("&lt;b&gt;grade", result["html"])


if __name__ == "__main__":
    unittest.main()
