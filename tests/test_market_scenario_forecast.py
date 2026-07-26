from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from app.market_timing.scenario_forecast import (
    MarketScenarioForecastRepository,
    classify_scenario,
    fit_multinomial_logistic,
    multiclass_brier,
    predict_multinomial_logistic,
    validate_probability_model,
)


class MarketScenarioForecastTest(unittest.TestCase):
    def test_volatility_standardized_scenario_labels(self) -> None:
        self.assertEqual(classify_scenario(1.01, 2.0), "up")
        self.assertEqual(classify_scenario(-1.01, 2.0), "down")
        self.assertEqual(classify_scenario(1.0, 2.0), "range")

    def test_multinomial_probabilities_sum_to_one(self) -> None:
        features = [[20, 25], [30, 20], [50, 50], [55, 48], [80, 75], [85, 90]]
        labels = [0, 0, 1, 1, 2, 2]
        model = fit_multinomial_logistic(
            features * 12,
            labels * 12,
            iterations=120,
        )

        probabilities = predict_multinomial_logistic(model, [84, 82])

        self.assertAlmostEqual(sum(probabilities), 1.0)
        self.assertEqual(max(range(3), key=lambda index: probabilities[index]), 2)

    def test_evidence_is_insufficient_before_minimum_sample(self) -> None:
        result = validate_probability_model(
            [
                {"features": [50, 50], "label_index": 1}
                for _ in range(149)
            ]
        )

        self.assertEqual(result["status"], "insufficient_evidence")
        self.assertFalse(result["beats_both_baselines"])

    def test_brier_rewards_correct_probability(self) -> None:
        good = multiclass_brier([[0.05, 0.10, 0.85]], [2])
        bad = multiclass_brier([[0.85, 0.10, 0.05]], [2])

        self.assertLess(good, bad)

    def test_materialize_reuses_immutable_forecast_without_rebuilding(self) -> None:
        write_factory = MagicMock()
        cursor = (
            write_factory.return_value.__enter__.return_value
            .cursor.return_value.__enter__.return_value
        )
        cursor.fetchone.return_value = None
        repository = MarketScenarioForecastRepository(
            connection_factory=write_factory,
        )
        stored = {
            "forecast_id": "msfv1_20260724_000300SH_h1",
            "horizon_days": 1,
            "validation_status": "insufficient_evidence",
            "probability_display_allowed": False,
            "materialization_status": "reused",
        }
        repository._existing_forecasts = MagicMock(  # type: ignore[method-assign]
            return_value={1: stored}
        )
        repository._leadership_rows = MagicMock(  # type: ignore[method-assign]
            return_value=[]
        )
        repository.build_forecast = MagicMock()  # type: ignore[method-assign]

        result = repository.materialize("2026-07-24", horizons=(1,))

        repository.build_forecast.assert_not_called()
        self.assertEqual(result["forecast_count"], 1)
        self.assertEqual(result["created_forecast_count"], 0)
        self.assertEqual(result["reused_forecast_count"], 1)
        self.assertEqual(result["forecasts"], [stored])

if __name__ == "__main__":
    unittest.main()
