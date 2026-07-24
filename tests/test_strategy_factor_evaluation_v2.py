from __future__ import annotations

import unittest

from app.stock_selection.factor_evaluation_v2 import (
    benjamini_hochberg,
    evaluate_factor_records,
    maturity_state,
    pearson,
    spearman,
)


class StrategyFactorEvaluationV2Test(unittest.TestCase):
    def test_correlation_helpers(self) -> None:
        self.assertAlmostEqual(pearson([1, 2, 3], [2, 4, 6]), 1.0)
        self.assertAlmostEqual(spearman([10, 20, 30], [3, 2, 1]), -1.0)

    def test_maturity_needs_time_rows_and_market_states(self) -> None:
        self.assertEqual(
            maturity_state(observation_days=60, valid_rows=2000, market_states=1),
            "provisional",
        )
        self.assertEqual(
            maturity_state(observation_days=60, valid_rows=2000, market_states=2),
            "research_candidate",
        )

    def test_selected_only_history_never_emits_cross_sectional_ic(self) -> None:
        rows = []
        for day in range(1, 4):
            for index in range(100):
                rows.append(
                    {
                        "trade_date": f"2026-07-{day:02d}",
                        "forward_return_pct": index / 100,
                        "factor_json": {"factor_a": index},
                        "contribution_json": {"factor_a": index / 10},
                        "score": index,
                        "market_state": "risk_on",
                    }
                )

        result = evaluate_factor_records(
            rows,
            strategy_id="example",
            strategy_version="1",
            scope_name="selected_top_k",
            horizon_days=5,
            factor_keys=["factor_a"],
        )

        evaluation = result["evaluations"][0]
        self.assertIsNone(evaluation["rank_ic_mean"])
        self.assertIsNone(evaluation["pearson_ic_mean"])
        self.assertEqual(evaluation["maturity_state"], "data_only")
        self.assertEqual(result["groups"], [])
        self.assertEqual(result["ablations"], [])

    def test_full_pool_emits_monotonic_groups(self) -> None:
        rows = []
        for day in range(1, 11):
            for index in range(100):
                rows.append(
                    {
                        "trade_date": f"2026-06-{day:02d}",
                        "forward_return_pct": index / 10,
                        "factor_json": {"factor_a": index},
                        "contribution_json": {"factor_a": index / 10},
                        "score": index,
                        "market_state": "risk_on",
                    }
                )

        result = evaluate_factor_records(
            rows,
            strategy_id="example",
            strategy_version="1",
            scope_name="eligible_pool",
            horizon_days=5,
            factor_keys=["factor_a"],
        )

        evaluation = result["evaluations"][0]
        self.assertAlmostEqual(evaluation["rank_ic_mean"], 1.0)
        self.assertAlmostEqual(evaluation["monotonicity_score"], 1.0)
        self.assertEqual(evaluation["group_count"], 5)

    def test_benjamini_hochberg_is_monotone(self) -> None:
        adjusted = benjamini_hochberg({"a": 0.01, "b": 0.04, "c": 0.03})
        self.assertLessEqual(adjusted["a"], adjusted["c"])
        self.assertLessEqual(adjusted["c"], adjusted["b"])


if __name__ == "__main__":
    unittest.main()
