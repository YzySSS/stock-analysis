from __future__ import annotations

import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class MarketRiskAlertUiTests(unittest.TestCase):
    def test_home_and_selection_render_the_same_non_blocking_alert(self):
        pages = PROJECT_ROOT / "app" / "api" / "web" / "pages"
        js = PROJECT_ROOT / "app" / "api" / "web" / "js"
        css = PROJECT_ROOT / "app" / "api" / "web" / "css" / "pages.css"

        home_html = (pages / "home.html").read_text(encoding="utf-8")
        selection_html = (pages / "selection.html").read_text(encoding="utf-8")
        home_js = (js / "home.js").read_text(encoding="utf-8")
        selection_js = (js / "selection.js").read_text(encoding="utf-8")
        common_js = (js / "common.js").read_text(encoding="utf-8")
        pages_css = css.read_text(encoding="utf-8")

        self.assertIn('id="home-market-risk-alert"', home_html)
        self.assertIn('id="selection-market-risk-alert"', selection_html)
        self.assertIn("renderMarketRiskAlert('#home-market-risk-alert'", home_js)
        self.assertIn("/api/dashboard/summary?limit=8&compact=true", selection_js)
        self.assertIn("renderMarketRiskAlert(", common_js)
        self.assertIn("选股功能仍可正常使用", common_js)
        self.assertIn(".market-risk-alert.red", pages_css)
        self.assertNotIn("selection-run-submit').disabled", selection_js)

    def test_changed_assets_have_a_shared_cachebuster(self):
        pages = PROJECT_ROOT / "app" / "api" / "web" / "pages"

        home = (pages / "home.html").read_text(encoding="utf-8")
        selection = (pages / "selection.html").read_text(encoding="utf-8")

        self.assertIn("pages.css?v=20260819marketalert1", home)
        self.assertIn("pages.css?v=20260819marketalert1", selection)
        self.assertIn("common.js?v=20260819marketalert1", home)
        self.assertIn("common.js?v=20260819marketalert1", selection)
        self.assertIn("home.js?v=20260819marketalert1", home)
        self.assertIn("selection.js?v=20260819marketalert1", selection)


if __name__ == "__main__":
    unittest.main()
