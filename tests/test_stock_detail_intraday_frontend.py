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

    def test_page_uses_new_stock_detail_cache_buster(self):
        page = (PROJECT_ROOT / "app" / "api" / "web" / "pages" / "stock-detail.html").read_text(
            encoding="utf-8"
        )

        self.assertIn("stock-detail.js?v=20260722intradayfix1", page)


if __name__ == "__main__":
    unittest.main()
