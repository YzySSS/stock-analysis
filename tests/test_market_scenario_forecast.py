from __future__ import annotations

import sys
import unittest
from unittest.mock import MagicMock, patch

from app.market_timing.scenario_forecast import (
    MarketScenarioForecastRepository,
    classify_scenario,
    fit_multinomial_logistic,
    multiclass_brier,
    predict_multinomial_logistic,
    summarize_market_mainline,
    validate_probability_model,
)
from scripts import run_market_scenario_forecast as scenario_script


class MarketScenarioForecastTest(unittest.TestCase):
    @staticmethod
    def _leadership_row(
        name: str,
        *,
        strength: str,
        cycle: str,
        score: float,
        breadth_status: str = "ready",
        confidence: float = 0.9,
    ) -> dict:
        return {
            "sector_type": "theme",
            "sector_name": name,
            "leadership_state": strength,
            "state_label": strength,
            "cycle_state": cycle,
            "cycle_label": cycle,
            "leadership_score": score,
            "confidence": confidence,
            "price_evidence_status": "ready",
            "breadth_metrics": {"status": breadth_status},
        }

    def test_market_mainline_is_single_and_fail_closed(self) -> None:
        rows = [
            self._leadership_row(
                "热度观察板块",
                strength="watch",
                cycle="first_impulse",
                score=95,
            ),
            self._leadership_row(
                "证据缺失板块",
                strength="core",
                cycle="main_up",
                score=90,
                breadth_status="insufficient_coverage",
            ),
            self._leadership_row(
                "第二候选",
                strength="confirmed",
                cycle="first_impulse",
                score=72,
            ),
            self._leadership_row(
                "唯一市场主线",
                strength="core",
                cycle="main_up",
                score=78,
            ),
        ]

        result = summarize_market_mainline(rows)

        self.assertEqual(result["status"], "present")
        self.assertEqual(result["sector"]["sector_name"], "唯一市场主线")
        self.assertEqual(result["fully_qualified_count"], 2)
        self.assertEqual(result["strength_qualified_count"], 3)
        self.assertEqual(result["price_strengthening_count"], 4)
        self.assertEqual(result["selection_policy"], "single_primary_or_none")
        self.assertIn("多周期启动确认", result["qualification_note"])

    def test_market_mainline_allows_explicit_empty_state(self) -> None:
        result = summarize_market_mainline(
            [
                self._leadership_row(
                    "仅价格启动",
                    strength="watch",
                    cycle="first_impulse",
                    score=88,
                )
            ]
        )

        self.assertEqual(result["status"], "none")
        self.assertIsNone(result["sector"])
        self.assertEqual(result["label"], "暂无已确认市场主线")
        self.assertEqual(result["price_strengthening_count"], 1)

    def test_startup_watch_cannot_be_promoted_to_market_mainline(self) -> None:
        result = summarize_market_mainline(
            [
                self._leadership_row(
                    "两日反弹板块",
                    strength="core",
                    cycle="impulse_watch",
                    score=95,
                )
            ]
        )

        self.assertEqual("none", result["status"])
        self.assertIsNone(result["sector"])
        self.assertEqual(0, result["price_strengthening_count"])

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

    def test_materialize_defers_stale_leadership_until_sources_align(self) -> None:
        write_factory = MagicMock()
        repository = MarketScenarioForecastRepository(
            connection_factory=write_factory,
        )
        stored = {
            "forecast_id": "msfv1_20260805_000300SH_h1",
            "horizon_days": 1,
            "validation_status": "insufficient_evidence",
            "probability_display_allowed": False,
            "materialization_status": "reused",
        }
        repository._existing_forecasts = MagicMock(  # type: ignore[method-assign]
            return_value={1: stored}
        )
        repository._leadership_rows = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "sector_name": "半导体",
                    "price_evidence_status": "stale_data",
                }
            ]
        )
        repository.build_forecast = MagicMock()  # type: ignore[method-assign]

        result = repository.materialize("2026-08-05", horizons=(1,))

        cursor = (
            write_factory.return_value.__enter__.return_value
            .cursor.return_value.__enter__.return_value
        )
        cursor.execute.assert_not_called()
        self.assertEqual("partial_success", result["status"])
        self.assertEqual(0, result["leadership_count"])
        self.assertEqual(1, result["leadership_built_count"])
        self.assertEqual(1, result["leadership_stale_count"])
        self.assertEqual(1, result["leadership_deferred_count"])

    @patch("scripts.run_market_scenario_forecast.release_mysql_advisory_lock")
    @patch(
        "scripts.run_market_scenario_forecast.acquire_mysql_advisory_lock",
        return_value=object(),
    )
    @patch("scripts.run_market_scenario_forecast.TaskRunLogger")
    @patch(
        "scripts.run_market_scenario_forecast.run",
        return_value={"status": "partial_success"},
    )
    def test_script_persists_partial_success_status(
        self,
        run_mock,
        logger_class,
        acquire_lock,
        release_lock,
    ) -> None:
        del run_mock, acquire_lock, release_lock
        with patch.object(
            sys,
            "argv",
            ["run_market_scenario_forecast.py", "--trade-date", "2026-08-05"],
        ), patch("builtins.print"):
            exit_code = scenario_script.main()

        self.assertEqual(0, exit_code)
        finish_call = logger_class.return_value.finish.call_args
        self.assertEqual("partial_success", finish_call.args[2])

if __name__ == "__main__":
    unittest.main()
