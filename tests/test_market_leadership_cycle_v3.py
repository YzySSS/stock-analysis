from __future__ import annotations

import unittest

from app.market_timing.leadership_cycle_v3 import (
    BASE_SPEC_HASH,
    build_evidence_alignment,
    classify_cycle,
    leadership_cycle_spec_hash,
    load_leadership_cycle_spec,
)


class MarketLeadershipCycleV3Tests(unittest.TestCase):
    @staticmethod
    def _metrics(**overrides) -> dict:
        metrics = {
            "status": "ready",
            "trade_date": "2026-08-04",
            "distance_ma20_pct": -12.3113,
            "distance_ma60_pct": -17.8422,
            "ma20_slope_5_pct": -9.2639,
            "ma60_slope_5_pct": -0.6951,
            "return_5d_pct": -1.694,
            "return_20d_pct": -28.6343,
            "drawdown_from_high_60_pct": -34.9279,
            "prior_runup_to_high_pct": 35.0,
            "post_high_drawdown_pct": -39.001,
            "rebound_from_post_high_low_pct": 6.6774,
            "days_since_high_60": 24,
            "days_since_post_high_low": 3,
        }
        metrics.update(overrides)
        return metrics

    @staticmethod
    def _breadth(**overrides) -> dict:
        breadth = {
            "status": "ready",
            "score": 23.2642,
            "above_ma20_pct": 5.6995,
            "above_ma60_pct": 4.6632,
        }
        breadth.update(overrides)
        return breadth

    def test_spec_freezes_stricter_b_wave_confirmation(self) -> None:
        spec = load_leadership_cycle_spec()

        self.assertEqual("market_leadership_cycle_v3", spec["model_id"])
        self.assertEqual("3.0.0", spec["version"])
        self.assertEqual(BASE_SPEC_HASH, spec["base_spec_hash"])
        self.assertEqual(8.0, spec["cycle_thresholds"]["b_wave_min_rebound_pct"])
        self.assertEqual(40.0, spec["cycle_thresholds"]["b_wave_min_breadth_score"])
        self.assertEqual(64, len(leadership_cycle_spec_hash()))

    def test_semiconductor_like_one_day_bounce_is_only_oversold_rebound(self) -> None:
        cycle = classify_cycle(self._metrics(), self._breadth())

        self.assertEqual("oversold_rebound", cycle["cycle_state"])
        self.assertEqual("超跌反弹·趋势未确认", cycle["cycle_label"])
        self.assertTrue(any("尚未满足B浪确认" in item for item in cycle["reasons"]))

    def test_b_wave_candidate_requires_price_and_breadth_confirmation(self) -> None:
        cycle = classify_cycle(
            self._metrics(
                distance_ma20_pct=-2.0,
                ma20_slope_5_pct=1.2,
                return_5d_pct=4.5,
                rebound_from_post_high_low_pct=12.0,
                days_since_post_high_low=5,
            ),
            self._breadth(score=52.0, above_ma20_pct=36.0),
        )

        self.assertEqual("rebound_candidate", cycle["cycle_state"])
        self.assertEqual("持续修复·B浪候选", cycle["cycle_label"])

    def test_missing_breadth_cannot_be_b_wave_candidate(self) -> None:
        cycle = classify_cycle(
            self._metrics(
                distance_ma20_pct=-2.0,
                ma20_slope_5_pct=1.2,
                return_5d_pct=4.5,
                rebound_from_post_high_low_pct=12.0,
                days_since_post_high_low=5,
            ),
            {"status": "insufficient_coverage", "score": 50.0},
        )

        self.assertEqual("oversold_rebound", cycle["cycle_state"])

    def test_secondary_decline_takes_priority_over_oversold_rebound(self) -> None:
        cycle = classify_cycle(
            self._metrics(return_5d_pct=-5.0),
            self._breadth(),
        )

        self.assertEqual("secondary_decline_risk", cycle["cycle_state"])

    def test_misaligned_evidence_fails_closed(self) -> None:
        metrics = self._metrics()
        alignment = build_evidence_alignment("2026-08-05", metrics, "2026-08-04")
        cycle = classify_cycle(
            metrics,
            self._breadth(),
            evidence_alignment=alignment,
        )

        self.assertFalse(alignment["evidence_aligned"])
        self.assertEqual("stale_data", cycle["cycle_state"])
        self.assertEqual("数据待对齐", cycle["cycle_label"])


if __name__ == "__main__":
    unittest.main()
