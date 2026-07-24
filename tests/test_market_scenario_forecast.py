from __future__ import annotations

import unittest

from app.market_timing.scenario_forecast import (
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


if __name__ == "__main__":
    unittest.main()
