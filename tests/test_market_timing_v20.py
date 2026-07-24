from __future__ import annotations

import unittest

from app.market_timing.v20 import compose_market_timing_v20


def _dimensions(score: float = 50.0) -> dict:
    return {
        name: {"score": score, "source_status": "ready"}
        for name in ("trend", "breadth", "capital", "tail_risk", "leadership")
    }


class MarketTimingV20CompositionTest(unittest.TestCase):
    def test_missing_dimension_is_neutral_without_weight_redistribution(self) -> None:
        dimensions = _dimensions(80)
        dimensions["capital"] = {"score": None, "source_status": "missing"}

        result = compose_market_timing_v20(dimensions)

        self.assertAlmostEqual(result["timing_score"], 74.0)
        self.assertAlmostEqual(result["confidence"], 0.8)
        self.assertEqual(result["dimensions"]["capital"]["score"], 50.0)
        self.assertFalse(result["dimensions"]["capital"]["available"])

    def test_upgrade_requires_two_observations_and_advances_one_state(self) -> None:
        previous = {
            "state": "neutral",
            "coverage_json": {
                "position_target": 0.55,
                "upgrade_candidate_state": "strong_risk_on",
                "upgrade_streak": 1,
            },
        }

        result = compose_market_timing_v20(_dimensions(82), previous_signal=previous)

        self.assertEqual(result["state"], "risk_on")
        self.assertEqual(result["hysteresis_action"], "confirmed_upgrade_one_step")
        self.assertLessEqual(result["position_target"], 0.75)

    def test_first_upgrade_observation_holds_previous_state(self) -> None:
        result = compose_market_timing_v20(
            _dimensions(82),
            previous_signal={
                "state": "neutral",
                "coverage_json": {"position_target": 0.55},
            },
        )

        self.assertEqual(result["state"], "neutral")
        self.assertEqual(result["upgrade_streak"], 1)
        self.assertEqual(result["hysteresis_action"], "upgrade_waiting_confirmation")

    def test_downgrade_is_same_day(self) -> None:
        result = compose_market_timing_v20(
            _dimensions(42),
            previous_signal={
                "state": "risk_on",
                "coverage_json": {"position_target": 0.75},
            },
        )

        self.assertEqual(result["state"], "cautious")
        self.assertEqual(result["hysteresis_action"], "same_day_downgrade")

    def test_emergency_breadth_can_jump_to_cash(self) -> None:
        dimensions = _dimensions(80)
        dimensions["breadth"]["score"] = 16

        result = compose_market_timing_v20(
            dimensions,
            overlay_points=10,
            previous_signal={
                "state": "strong_risk_on",
                "coverage_json": {"position_target": 0.9},
            },
        )

        self.assertEqual(result["state"], "cash")
        self.assertTrue(result["emergency"])
        self.assertEqual(result["hysteresis_action"], "emergency_downgrade")


if __name__ == "__main__":
    unittest.main()
