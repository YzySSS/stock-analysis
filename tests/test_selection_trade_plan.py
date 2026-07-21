from __future__ import annotations

import unittest
from unittest.mock import patch

from app.stock_selection.trade_plan import build_selection_trade_plan


class SelectionTradePlanRiskControlTests(unittest.TestCase):
    @patch("app.stock_selection.trade_plan._technical_context")
    def test_sentiment_plan_uses_about_five_percent_stop_and_minimum_rr(self, technical_context):
        technical_context.return_value = {
            "ma5": 99.0,
            "ma10": 97.0,
            "ma20": 94.0,
            "high20": 102.0,
            "low20": 90.0,
            "atr14": 3.0,
            "trade_date": "2026-07-17",
            "source": "test",
        }

        plan = build_selection_trade_plan(
            {"code": "sh.600000", "close": 100.0},
            strategy_id="a_share_sentiment",
            raw_metrics={"selected_price": 100.0, "trade_signal_state": "tradable"},
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["version"], "selection_trade_plan_v3_risk_control")
        self.assertAlmostEqual(plan["risk_control"]["actual_stop_loss_pct"], 5.0, places=2)
        self.assertGreaterEqual(plan["risk_control"]["take_profit_1_risk_reward"], 1.2)
        self.assertGreaterEqual(plan["risk_control"]["take_profit_1_risk_reward_at_entry_high"], 1.2)
        self.assertGreaterEqual(plan["take_profit"][0]["price"], 106.0)
        self.assertLessEqual(plan["entry_zone"]["high"], 100.0)
        self.assertTrue(plan["risk_control"]["compliant"])

    @patch("app.stock_selection.trade_plan._technical_context")
    def test_non_sentiment_plan_keeps_technical_stop_policy(self, technical_context):
        technical_context.return_value = {
            "ma5": 99.0,
            "ma10": 98.0,
            "ma20": 97.0,
            "high20": 108.0,
            "low20": 90.0,
            "atr14": 2.0,
            "trade_date": "2026-07-17",
            "source": "test",
        }

        plan = build_selection_trade_plan(
            {"code": "sh.600000", "close": 100.0},
            strategy_id="test_strategy",
            raw_metrics={"selected_price": 100.0},
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["version"], "selection_trade_plan_v2_technical")
        self.assertIsNone(plan["risk_control"]["target_stop_loss_pct"])
        self.assertIsNone(plan["risk_control"]["min_take_profit_1_risk_reward"])
        self.assertNotAlmostEqual(plan["risk_control"]["actual_stop_loss_pct"], 5.0, places=1)


if __name__ == "__main__":
    unittest.main()
