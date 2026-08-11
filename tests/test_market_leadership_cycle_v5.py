from __future__ import annotations

import unittest
from pathlib import Path

from app.market_timing.leadership_cycle_v5 import (
    BASE_SPEC_HASH,
    LeadershipCycleBuilder,
    build_strength_confirmation_checks,
    classify_cycle,
    classify_strength,
    leadership_cycle_spec_hash,
    load_leadership_cycle_spec,
    resolve_capital_evidence,
)
from app.market_timing.scenario_forecast import LEADERSHIP_MODEL_ID


class MarketLeadershipCycleV5Tests(unittest.TestCase):
    @staticmethod
    def _row(**overrides) -> dict:
        row = {
            "leadership_state": "confirmed",
            "leadership_score": 70.0,
            "confidence": 0.9,
            "heat_score": 65.0,
            "capital_score": 60.0,
            "breadth_score": 70.0,
            "price_score": 75.0,
            "crowding_score": 55.0,
            "price_evidence_status": "ready",
            "cycle_state": "first_impulse",
            "price_metrics": {"distance_ma60_pct": 2.0},
            "breadth_metrics": {"status": "ready"},
        }
        row.update(overrides)
        return row

    def test_spec_freezes_confirmation_hierarchy(self) -> None:
        spec = load_leadership_cycle_spec()
        thresholds = spec["strength_confirmation_thresholds"]

        self.assertEqual("market_leadership_cycle_v5", spec["model_id"])
        self.assertEqual("5.0.0", spec["version"])
        self.assertEqual(BASE_SPEC_HASH, spec["base_spec_hash"])
        self.assertEqual(65.0, thresholds["minimum_leadership_score"])
        self.assertEqual(0.6, thresholds["minimum_capital_component_coverage"])
        self.assertEqual(2, thresholds["maximum_market_branches"])
        self.assertEqual(64, len(leadership_cycle_spec_hash()))

    def test_scenario_repository_uses_v5_builder(self) -> None:
        self.assertEqual("market_leadership_cycle_v5", LEADERSHIP_MODEL_ID)
        self.assertEqual("market_leadership_cycle_v5", LeadershipCycleBuilder.model_id)

    def test_theme_capital_uses_observed_mapped_industry_sum(self) -> None:
        flow_latest = {
            ("industry", "化学制药"): {
                "net_amount": 12.0,
                "quote_time": "2026-08-11 15:01:00",
            },
            ("industry", "医疗服务"): {
                "net_amount": 8.0,
                "quote_time": "2026-08-11 15:01:01",
            },
        }

        result = resolve_capital_evidence(
            sector_type="theme",
            sector_name="医药",
            price_industries=["化学制药", "医疗服务", "中药"],
            flow_latest=flow_latest,
            minimum_component_coverage=0.6,
        )

        self.assertEqual("observed", result["status"])
        self.assertEqual("mapped_industry_sum", result["mode"])
        self.assertEqual(20.0, result["net_amount"])
        self.assertEqual(0.6667, result["coverage"])

    def test_theme_capital_fails_closed_below_coverage_threshold(self) -> None:
        result = resolve_capital_evidence(
            sector_type="theme",
            sector_name="消费",
            price_industries=["白酒", "食品", "零售"],
            flow_latest={
                ("industry", "白酒"): {
                    "net_amount": 10.0,
                    "quote_time": "2026-08-11 15:01:00",
                }
            },
            minimum_component_coverage=0.6,
        )

        self.assertEqual("incomplete", result["status"])
        self.assertIsNone(result["net_amount"])

    def test_missing_capital_cannot_confirm_strength(self) -> None:
        spec = load_leadership_cycle_spec()
        row = self._row()
        checks = build_strength_confirmation_checks(
            row,
            capital_evidence={"status": "missing"},
            source_count=3,
            positive_news_count=4,
            negative_news_count=1,
            base_opinion_confirmed=False,
            thresholds=spec["strength_confirmation_thresholds"],
        )

        self.assertFalse(checks["资金证据真实可用"])
        self.assertEqual("watch", classify_strength(row, checks=checks))

    def test_price_below_ma60_cannot_confirm_strength(self) -> None:
        spec = load_leadership_cycle_spec()
        row = self._row(price_metrics={"distance_ma60_pct": -0.1})
        checks = build_strength_confirmation_checks(
            row,
            capital_evidence={"status": "observed"},
            source_count=3,
            positive_news_count=4,
            negative_news_count=1,
            base_opinion_confirmed=False,
            thresholds=spec["strength_confirmation_thresholds"],
        )

        self.assertFalse(checks["价格站上MA60"])
        self.assertEqual("watch", classify_strength(row, checks=checks))

    def test_complete_evidence_can_reach_strength_threshold(self) -> None:
        spec = load_leadership_cycle_spec()
        row = self._row()
        checks = build_strength_confirmation_checks(
            row,
            capital_evidence={"status": "observed"},
            source_count=3,
            positive_news_count=4,
            negative_news_count=1,
            base_opinion_confirmed=False,
            thresholds=spec["strength_confirmation_thresholds"],
        )

        self.assertTrue(all(checks.values()))
        self.assertEqual("confirmed", classify_strength(row, checks=checks))

    def test_price_cycle_labels_no_longer_claim_market_confirmation(self) -> None:
        cycle = classify_cycle(
            {
                "status": "ready",
                "trade_date": "2026-08-11",
                "distance_ma20_pct": 3.0,
                "distance_ma60_pct": 1.0,
                "ma20_slope_5_pct": 1.0,
                "ma60_slope_5_pct": -0.2,
                "ma20_slope_10_pct": 1.5,
                "return_5d_pct": 1.0,
                "return_10d_pct": 6.0,
                "return_20d_pct": 7.0,
                "return_20d_exact_pct": 7.0,
                "above_ma20_days_10": 8,
                "consecutive_above_ma20_days_10": 5,
                "drawdown_from_high_60_pct": -7.0,
                "prior_runup_to_high_pct": 10.0,
                "post_high_drawdown_pct": -5.0,
                "rebound_from_post_high_low_pct": 4.0,
                "days_since_high_60": 12,
                "days_since_post_high_low": 5,
            },
            {
                "status": "ready",
                "score": 68.0,
                "above_ma20_pct": 75.0,
                "above_ma60_pct": 55.0,
            },
        )

        self.assertEqual("first_impulse", cycle["cycle_state"])
        self.assertEqual("多周期转强", cycle["cycle_label"])
        self.assertNotIn("确认", cycle["cycle_label"])

    def test_home_reserves_confirmed_word_for_market_mainline(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        home_js = (project_root / "app/api/web/js/home.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("市场主线 · 已确认", home_js)
        self.assertIn("行业雷达 · ${radarStrengthLabel(item)}", home_js)
        self.assertIn("first_impulse: '多周期转强'", home_js)
        self.assertIn(
            "hasExplicitMainlineDecision ? null : fallbackMainline",
            home_js,
        )
        self.assertNotIn("first_impulse: '多周期启动确认'", home_js)


if __name__ == "__main__":
    unittest.main()
