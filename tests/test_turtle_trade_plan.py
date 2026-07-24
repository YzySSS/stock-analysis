from __future__ import annotations

import unittest

from app.stock_selection.turtle_trade_plan import (
    EXPECTED_SPEC_SHA256,
    attach_turtle_research_shadow,
    build_turtle_selection_trade_plan,
    calculate_wilder_n,
    constrain_turtle_plan_to_selection_grade,
    load_turtle_trade_plan_spec,
    turtle_trade_plan_spec_hash,
)


def technical_rows(count: int = 25) -> list[dict]:
    rows = []
    for index in range(count):
        close = 10.0 + index * 0.02
        rows.append(
            {
                "trade_date": f"2026-06-{index + 1:02d}",
                "open": close - 0.02,
                "high": close + 0.10,
                "low": close - 0.10,
                "close": close,
            }
        )
    return rows


class TurtleTradePlanTests(unittest.TestCase):
    def test_frozen_machine_spec_hash_and_identity(self):
        spec = load_turtle_trade_plan_spec()

        self.assertEqual(turtle_trade_plan_spec_hash(), EXPECTED_SPEC_SHA256)
        self.assertEqual(spec["spec_id"], "turtle_selection_risk_v1")
        self.assertEqual(
            spec["trade_plan_version"],
            "selection_trade_plan_v4_turtle_risk",
        )
        self.assertEqual(spec["status"], "research_only_shadow")
        self.assertEqual(spec["exit"]["time_exit_trade_days"], 5)
        self.assertEqual(spec["exit"]["minimum_progress_n"], 0.5)
        self.assertEqual(spec["exit"]["evaluation_censor_trade_days"], 20)

    def test_wilder_n_uses_completed_history(self):
        n20 = calculate_wilder_n(technical_rows())

        self.assertIsNotNone(n20)
        self.assertAlmostEqual(n20, 0.2, places=6)

    def test_builds_breakout_plan_with_risk_sizing_and_profit_only_add(self):
        plan = build_turtle_selection_trade_plan(
            {
                "code": "sh.600000",
                "name": "浦发银行",
                "industry": "银行",
                "close": 10.50,
            },
            strategy_id="a_share_sentiment",
            raw_metrics={
                "selected_price": 10.50,
                "selection_clock_mode": "postclose",
                "account_drawdown_pct": 0,
                "market_timing_state": "cautious",
                "market_target_position_pct": 35,
                "market_position_upper_pct": 45,
                "industry_state": "confirmed",
            },
            technical_rows=technical_rows(),
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["state"], "breakout_ready")
        self.assertEqual(plan["earliest_execution_policy"], "next_trading_day_open")
        self.assertAlmostEqual(plan["n20"], 0.2, places=3)
        self.assertEqual(plan["entry"]["setup"], "breakout_20d")
        self.assertTrue(plan["entry"]["trigger_required"])
        self.assertAlmostEqual(plan["entry"]["trigger"], 10.59, places=3)
        self.assertAlmostEqual(plan["risk"]["initial_stop"], 10.19, places=3)
        self.assertAlmostEqual(plan["risk"]["risk_per_share"], 0.4, places=3)
        self.assertEqual(plan["risk"]["shares_per_reference_equity"], 800)
        self.assertIsNone(plan["risk"]["unit_shares"])
        self.assertEqual(plan["add_levels"], [10.69])
        self.assertEqual(plan["account_guard"]["status"], "normal")
        self.assertEqual(plan["market_constraint"]["position_upper_pct"], 45)
        self.assertEqual(plan["industry_constraint"]["industry"], "银行")

    def test_account_drawdown_circuit_breaker_blocks_new_entry(self):
        plan = build_turtle_selection_trade_plan(
            {"code": "sh.600000", "name": "浦发银行", "close": 10.50},
            strategy_id="a_share_sentiment",
            raw_metrics={
                "selected_price": 10.50,
                "account_drawdown_pct": 8.0,
            },
            technical_rows=technical_rows(),
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["state"], "no_trade")
        self.assertEqual(plan["account_guard"]["status"], "cooldown")
        self.assertFalse(plan["account_guard"]["allow_new_entry"])
        self.assertEqual(plan["add_levels"], [])

    def test_v05_watch_grade_cannot_be_upgraded_by_execution_layer(self):
        plan = build_turtle_selection_trade_plan(
            {
                "code": "sh.600000",
                "name": "浦发银行",
                "close": 10.50,
                "grade_state": "watch",
            },
            strategy_id="a_share_sentiment_v05",
            raw_metrics={"selected_price": 10.50},
            technical_rows=technical_rows(),
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["state"], "watch")
        self.assertIn("观察级", plan["state_reason"])

    def test_final_selection_grade_can_only_downgrade_shadow_plan(self):
        trade_plan = {
            "research_shadow": {
                "state": "breakout_ready",
                "state_label": "突破待确认",
                "state_reason": "接近突破",
                "reasons": ["接近突破"],
            }
        }

        constrained = constrain_turtle_plan_to_selection_grade(
            trade_plan,
            grade_state="watch",
            grade_reason="duplicate_tradable_theme",
        )

        self.assertEqual(
            constrained["research_shadow"]["state"],
            "watch",
        )
        self.assertIn(
            "duplicate_tradable_theme",
            constrained["research_shadow"]["state_reason"],
        )

    def test_missing_history_fails_closed(self):
        plan = build_turtle_selection_trade_plan(
            {"code": "sh.600000", "close": 10.50},
            strategy_id="a_share_sentiment",
            raw_metrics={"selected_price": 10.50},
            technical_rows=technical_rows(10),
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["state"], "no_trade")
        self.assertIsNone(plan["n20"])
        self.assertIn("日线不足", plan["reasons"][0])

    def test_downstream_attachment_keeps_frozen_active_plan_unchanged(self):
        active_plan = {
            "version": "selection_trade_plan_v3_risk_control",
            "strategy_id": "a_share_sentiment",
            "risk_control": {"actual_stop_loss_pct": 5.0},
        }
        item = {
            "code": "sh.600000",
            "name": "浦发银行",
            "close": 10.50,
            "trade_grade_state": "tradable",
            "trade_plan": active_plan,
        }

        plan = attach_turtle_research_shadow(
            item,
            strategy_id="a_share_sentiment",
            raw_metrics={"selected_price": 10.50},
            technical_rows=technical_rows(),
        )

        self.assertNotIn("research_shadow", active_plan)
        self.assertEqual(plan["version"], "selection_trade_plan_v3_risk_control")
        self.assertEqual(
            plan["research_shadow"]["version"],
            "selection_trade_plan_v4_turtle_risk",
        )
        self.assertEqual(plan["risk_control"]["actual_stop_loss_pct"], 5.0)


if __name__ == "__main__":
    unittest.main()
