from __future__ import annotations

import json
import shutil
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "app" / "api" / "web" / "js" / "stock-detail.js"


@unittest.skipUnless(shutil.which("node"), "node is required for stock detail JavaScript tests")
class StockDetailIntradayFrontendTests(unittest.TestCase):
    def _run_javascript(self, expression: str) -> dict:
        bootstrap = f"""
const fs = require('fs');
const vm = require('vm');
const source = fs.readFileSync({json.dumps(str(SCRIPT_PATH))}, 'utf8')
  .split("document.addEventListener('DOMContentLoaded'")[0];
const context = {{
  console,
  Date,
  URLSearchParams,
  fetchJson: async () => {{ throw new Error('fetchJson stub missing'); }},
  window: {{
    setTimeout: (callback) => callback(),
    setInterval: () => 0,
  }},
}};
vm.createContext(context);
vm.runInContext(source, context);
(async () => {{
  const result = await vm.runInContext(`(async () => {{ {expression} }})()`, context);
  process.stdout.write(JSON.stringify(result));
}})().catch((error) => {{
  console.error(error);
  process.exit(1);
}});
"""
        completed = subprocess.run(
            ["node", "-e", bootstrap],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    def test_morning_cache_is_stale_and_realtime_tail_is_merged(self):
        result = self._run_javascript(
            """
const cached = [
  { minute_time: '2026-07-22 09:31:00', open: 10.0, close: 10.1 },
  { minute_time: '2026-07-22 11:30:00', open: 10.1, close: 10.2 },
];
const realtime = [
  { quote_minute: '2026-07-22 11:30:00', latest_price: 9.8 },
  { quote_minute: '2026-07-22 13:00:00', latest_price: 10.3 },
  { quote_minute: '2026-07-22 15:00:00', latest_price: 10.5 },
];
const merged = mergeIntradayChartPoints(cached, realtime, { prevClose: 10.0 });
const overlap = merged.find((item) => intradayPointMinuteKey(item) === '2026-07-22 11:30');
return {
  stale: isIntradayCacheStale(cached, realtime),
  latest: latestIntradayMinuteKey(merged),
  overlapPrice: overlap.latest_price,
  mergedCount: merged.length,
};
"""
        )

        self.assertTrue(result["stale"])
        self.assertEqual(result["latest"], "2026-07-22 15:00")
        self.assertEqual(result["overlapPrice"], 10.2)
        self.assertEqual(result["mergedCount"], 5)

    def test_cache_with_same_latest_minute_is_fresh(self):
        result = self._run_javascript(
            """
const cached = [
  { minute_time: '2026-07-22 09:31:00' },
  { minute_time: '2026-07-22 15:00:00' },
];
const realtime = [{ quote_minute: '2026-07-22 15:00:00' }];
return { stale: isIntradayCacheStale(cached, realtime) };
"""
        )

        self.assertFalse(result["stale"])

    def test_refresh_has_session_cooldown_but_cached_get_still_runs(self):
        result = self._run_javascript(
            """
let posts = 0;
let gets = 0;
fetchJson = async (_url, options = {}) => {
  if (options.method === 'POST') {
    posts += 1;
    return { status: 'queued' };
  }
  gets += 1;
  return {
    count: 2,
    source_status: 'cached',
    items: [
      { minute_time: '2026-07-22 09:31:00' },
      { minute_time: '2026-07-22 15:00:00' },
    ],
  };
};
await refreshAndLoadIntradayBars('sz.000977', '2026-07-22', '2026-07-22 15:00');
await refreshAndLoadIntradayBars('sz.000977', '2026-07-22', '2026-07-22 15:00');
return { posts, gets };
"""
        )

        self.assertEqual(result["posts"], 1)
        self.assertEqual(result["gets"], 2)

    def test_cached_chip_reference_tracks_live_price_without_mutating_source(self):
        result = self._run_javascript(
            """
const original={weight_avg:10,winner_rate:50,label:'中性',price_vs_weight_avg_pct:0};
const live=chipWithPrice(original,11);
return {live,original};
"""
        )
        self.assertEqual(result['live']['price_vs_weight_avg_pct'], 10)
        self.assertEqual(result['live']['label'], '价格高于平均成本')
        self.assertEqual(result['original']['price_vs_weight_avg_pct'], 0)

    def test_initial_history_keeps_previous_visible_month_window_and_full_bounds(self):
        result = self._run_javascript(
            """
stockPage.code='sz.000725';
fetchReadJson=async()=>({min_date:'2024-05-21',max_date:'2026-11-13',items:[{trade_date:'2026-11-13'}]});
qs=()=>({});renderKlineState=()=>{};
await loadStockHistory({initial:true});
const date=new Date('2026-11-13T00:00:00');date.setMonth(date.getMonth()-1);
return {start:klineState.startDate,legacyStart:date.toISOString().slice(0,10),min:klineState.minDate,end:klineState.endDate};
"""
        )
        self.assertEqual(result['start'], result['legacyStart'])
        self.assertEqual(result['min'], '2024-05-21')
        self.assertEqual(result['end'], '2026-11-13')

    def test_page_uses_new_stock_detail_cache_buster(self):
        page = (PROJECT_ROOT / "app" / "api" / "web" / "pages" / "stock-detail.html").read_text(
            encoding="utf-8"
        )

        self.assertIn("stock-detail.js?v=20260915sentimentv06", page)


if __name__ == "__main__":
    unittest.main()
