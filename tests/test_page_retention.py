from __future__ import annotations

import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ExistingPageRetentionTests(unittest.TestCase):
    def test_all_existing_product_pages_are_retained(self):
        pages = PROJECT_ROOT / "app" / "api" / "web" / "pages"
        expected = {
            "home.html",
            "selection.html",
            "tracking.html",
            "portfolio.html",
            "backtest.html",
            "strategies.html",
            "trade-strategies.html",
            "stock-detail.html",
            "system.html",
        }

        self.assertEqual(expected - {path.name for path in pages.glob("*.html")}, set())

    def test_all_existing_product_routers_remain_mounted(self):
        source = (PROJECT_ROOT / "app" / "api" / "main.py").read_text(encoding="utf-8")
        expected_router_names = {
            "web_router",
            "health_router",
            "dashboard_router",
            "system_router",
            "strategies_router",
            "selection_router",
            "tracking_router",
            "portfolio_router",
            "backtest_router",
            "trade_strategies_router",
            "stocks_router",
        }

        for router_name in expected_router_names:
            self.assertIn(f"app.include_router({router_name}", source)

    def test_stock_detail_uses_async_intraday_refresh_then_cached_get(self):
        source = (PROJECT_ROOT / "app" / "api" / "web" / "js" / "stock-detail.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("/intraday-bars/refresh?trade_date=", source)
        self.assertIn("{ method: 'POST' }", source)
        self.assertIn("refreshAndLoadIntradayBars(", source)


if __name__ == "__main__":
    unittest.main()
