from __future__ import annotations

import unittest
from datetime import date, timedelta

from app.market_timing.leadership_cycle_v4 import (
    BASE_SPEC_HASH,
    LeadershipCycleBuilder,
    classify_cycle,
    compute_price_metrics,
    leadership_cycle_spec_hash,
    load_leadership_cycle_spec,
)
from app.market_timing.scenario_forecast import LEADERSHIP_MODEL_ID


class MarketLeadershipCycleV4Tests(unittest.TestCase):
    @staticmethod
    def _metrics(**overrides) -> dict:
        metrics = {
            "status": "ready",
            "trade_date": "2026-08-10",
            "distance_ma20_pct": 3.0,
            "distance_ma60_pct": -1.0,
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
        }
        metrics.update(overrides)
        return metrics

    @staticmethod
    def _breadth(**overrides) -> dict:
        breadth = {
            "status": "ready",
            "score": 68.0,
            "above_ma20_pct": 75.0,
            "above_ma60_pct": 55.0,
        }
        breadth.update(overrides)
        return breadth

    def test_spec_freezes_multi_horizon_startup_confirmation(self) -> None:
        spec = load_leadership_cycle_spec()
        thresholds = spec["startup_confirmation_thresholds"]

        self.assertEqual("market_leadership_cycle_v4", spec["model_id"])
        self.assertEqual("4.0.0", spec["version"])
        self.assertEqual(BASE_SPEC_HASH, spec["base_spec_hash"])
        self.assertEqual(3.0, thresholds["minimum_return_10d_pct"])
        self.assertEqual(6, thresholds["minimum_above_ma20_days_10"])
        self.assertEqual(55.0, thresholds["minimum_breadth_score"])
        self.assertEqual(64, len(leadership_cycle_spec_hash()))

    def test_scenario_repository_uses_v4_builder(self) -> None:
        self.assertEqual("market_leadership_cycle_v4", LEADERSHIP_MODEL_ID)
        self.assertEqual(
            "market_leadership_cycle_v4",
            LeadershipCycleBuilder.model_id,
        )

    def test_price_metrics_add_exact_ten_day_stability_evidence(self) -> None:
        start = date(2026, 1, 1)
        series = [
            {
                "trade_date": str(start + timedelta(days=index)),
                "value": index + 1,
            }
            for index in range(70)
        ]

        metrics = compute_price_metrics(series)

        self.assertEqual("ready", metrics["status"])
        self.assertAlmostEqual(16.6667, metrics["return_10d_pct"], places=4)
        self.assertAlmostEqual(40.0, metrics["return_20d_exact_pct"], places=4)
        self.assertEqual(10, metrics["above_ma20_days_10"])
        self.assertEqual(10, metrics["consecutive_above_ma20_days_10"])
        self.assertGreater(metrics["ma20_slope_10_pct"], 0)

    def test_short_price_bounce_is_only_startup_watch(self) -> None:
        cycle = classify_cycle(
            self._metrics(
                return_10d_pct=1.0,
                above_ma20_days_10=3,
                consecutive_above_ma20_days_10=2,
            ),
            self._breadth(score=48.0, above_ma20_pct=52.0),
        )

        self.assertEqual("impulse_watch", cycle["cycle_state"])
        self.assertEqual("短线转强·启动待确认", cycle["cycle_label"])
        self.assertTrue(
            any("尚未满足多周期启动确认" in item for item in cycle["reasons"])
        )

    def test_confirmed_startup_requires_price_persistence_and_breadth(self) -> None:
        cycle = classify_cycle(self._metrics(), self._breadth())

        self.assertEqual("first_impulse", cycle["cycle_state"])
        self.assertEqual("多周期启动确认", cycle["cycle_label"])
        self.assertTrue(any("近10日收益" in item for item in cycle["reasons"]))
        self.assertTrue(any("近10日有 8 日" in item for item in cycle["reasons"]))

    def test_recent_mild_pullback_does_not_override_medium_term_confirmation(self) -> None:
        cycle = classify_cycle(
            self._metrics(return_5d_pct=-2.5),
            self._breadth(),
        )

        self.assertEqual("first_impulse", cycle["cycle_state"])

    def test_strong_ten_day_return_can_override_lagging_ma20_ten_day_slope(self) -> None:
        cycle = classify_cycle(
            self._metrics(return_10d_pct=9.0, ma20_slope_10_pct=-1.0),
            self._breadth(),
        )

        self.assertEqual("first_impulse", cycle["cycle_state"])

    def test_missing_real_breadth_fails_closed_as_startup_watch(self) -> None:
        cycle = classify_cycle(
            self._metrics(),
            {"status": "insufficient_coverage", "score": 68.0},
        )

        self.assertEqual("impulse_watch", cycle["cycle_state"])


if __name__ == "__main__":
    unittest.main()
