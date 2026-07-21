from __future__ import annotations

import json
import unittest

from app.api.routes.backtest import normalize_run_row
from app.api.routes.dashboard import _compact_dashboard_payload
from app.api.routes.tracking import _compact_tracking_item


class PagePayloadCompactionTests(unittest.TestCase):
    def test_tracking_compact_item_keeps_ui_fields_and_drops_raw_context(self):
        raw = {
            "code": "sh.600000",
            "name": "浦发银行",
            "strategy_id": "test_strategy",
            "selection_date": "2026-07-01",
            "current_price": 10.5,
            "factor_scores": {"large": "x" * 8000},
            "sentiment_context": {"large": "y" * 7000},
            "trade_plan": {
                "entry_price": 10,
                "entry_zone": {"low": 9.9, "high": 10.1, "unused": "z" * 500},
                "stop_loss": {"price": 9.5, "reason": "unused"},
                "take_profit": [{"price": 11}, {"price": 12}],
            },
            "trade_plan_status": {"status": "tracking", "status_label": "跟踪中", "completed": False, "unused": "x"},
        }

        compact = _compact_tracking_item(raw)

        self.assertEqual(compact["code"], "sh.600000")
        self.assertEqual(compact["trade_plan"]["take_profit"], [{"price": 11}])
        self.assertNotIn("factor_scores", compact)
        self.assertNotIn("sentiment_context", compact)
        self.assertLess(len(json.dumps(compact)), len(json.dumps(raw)) / 10)

    def test_dashboard_compact_payload_drops_large_tracking_context(self):
        raw = {
            "latest_trade_date": "2026-07-15",
            "market_overview": {"strong_sectors": [], "weak_sectors": []},
            "market_timing": {"signals": [], "article_factor_coverage": []},
            "hot_themes": {"items": [], "as_of": "2026-07-15"},
            "emotion_board": {"limit_up_pool": [], "hot_limit_watch_pool": [], "reversal_watch_pool": []},
            "latest_tracking_count": 1,
            "latest_tracking_avg_price_change_pct": 1.2,
            "latest_tracking_preview": [
                {"code": "sh.600000", "name": "浦发银行", "factor_scores": {"large": "x" * 8000}}
            ],
            "latest_selection_summary": None,
        }

        compact = _compact_dashboard_payload(raw)

        self.assertEqual(compact["latest_tracking_preview"][0]["code"], "sh.600000")
        self.assertNotIn("factor_scores", compact["latest_tracking_preview"][0])

    def test_compact_backtest_run_omits_full_summary(self):
        row = {
            "run_id": "run",
            "strategy_id": "test_strategy",
            "methodology_version": "close_signal_next_open_v2",
            "summary_json": {"equity_curve": [{"value": "x" * 5000}]},
            "request_json": {"max_picks": 3},
        }

        compact = normalize_run_row(row, include_details=False)
        full = normalize_run_row(row, include_details=True)

        self.assertNotIn("summary", compact)
        self.assertIn("summary", full)
        self.assertEqual(compact["request"]["max_picks"], 3)


if __name__ == "__main__":
    unittest.main()
