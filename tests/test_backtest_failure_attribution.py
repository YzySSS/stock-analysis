from __future__ import annotations

import unittest
from datetime import date, timedelta

from app.backtest.failure_attribution import (
    StrategyFailureAttributionService,
    TransactionCosts,
    non_overlapping_cohorts,
    retention_diagnostics,
    spearman_correlation,
    summarize_values,
)


def trade_row(signal_date: date, code: str, gross_pct: float, rank: int = 1) -> dict:
    entry = 100.0
    exit_price = entry * (1 + gross_pct / 100)
    return {
        "run_id": "run-test",
        "trade_date": signal_date,
        "code": code,
        "rank_no": rank,
        "score": 80 - rank,
        "factor_json": {
            "factors": {"turnover": 70, "lowvol": 80, "reversal": 60},
            "raw_metrics": {
                "base_score_before_price_preference": 77 - rank,
                "industry": "测试行业",
            },
        },
        "entry_date": signal_date + timedelta(days=1),
        "entry_price": entry,
        "exit_date_1d": signal_date + timedelta(days=2),
        "exit_price_1d": exit_price,
        "exit_price_3d": exit_price,
        "exit_price_5d": exit_price,
        "exit_price_10d": exit_price,
    }


class FailureAttributionPureFunctionTests(unittest.TestCase):
    def setUp(self):
        self.costs = TransactionCosts(commission_bps=3, stamp_tax_bps=5, slippage_bps=5)

    def test_transaction_costs_match_realistic_round_trip_and_keep_missing(self):
        self.assertAlmostEqual(self.costs.round_trip_drag_pct, 0.2098, places=4)
        self.assertAlmostEqual(self.costs.gross_return_pct(100, 101), 1.0, places=10)
        self.assertLess(self.costs.net_return_pct(100, 101), 1.0)
        self.assertIsNone(self.costs.net_return_pct(100, None))

    def test_missing_forward_returns_are_excluded_not_zero_imputed(self):
        summary = summarize_values([1.0, None, -1.0])

        self.assertEqual(summary["sample_count"], 2)
        self.assertEqual(summary["missing_count"], 1)
        self.assertEqual(summary["mean_pct"], 0.0)

    def test_spearman_handles_ties_and_missing(self):
        self.assertEqual(spearman_correlation([(1, 10), (2, 20), (3, 30), (None, 40)]), 1.0)
        self.assertEqual(spearman_correlation([(1, 30), (2, 20), (3, 10)]), -1.0)

    def test_non_overlapping_cohorts_reports_every_offset(self):
        start = date(2026, 1, 1)
        signal_dates = [start + timedelta(days=index) for index in range(6)]
        returns = [1, -1, 1, 1, -1, 1]
        rows = [trade_row(signal_date, f"code-{index}", value) for index, (signal_date, value) in enumerate(zip(signal_dates, returns))]

        cohorts = non_overlapping_cohorts(signal_dates, rows, 3, self.costs)

        self.assertEqual([row["offset"] for row in cohorts], [0, 1, 2])
        self.assertEqual(cohorts[0]["scheduled_rebalance_count"], 2)
        self.assertEqual(cohorts[0]["valid_rebalance_count"], 2)
        self.assertGreater(cohorts[0]["gross_compound_pct"], 0)
        self.assertLess(cohorts[1]["gross_compound_pct"], 0)
        self.assertGreater(cohorts[2]["gross_compound_pct"], 0)

    def test_retention_counts_replacement_cycles(self):
        start = date(2026, 1, 1)
        signal_dates = [start + timedelta(days=index) for index in range(3)]
        code_sets = [("a", "b", "c"), ("a", "b", "d"), ("e", "f", "g")]
        rows = [
            trade_row(signal_date, code, 0.1, rank)
            for signal_date, codes in zip(signal_dates, code_sets)
            for rank, code in enumerate(codes, start=1)
        ]

        diagnostics = retention_diagnostics(signal_dates, rows, self.costs)

        self.assertEqual(diagnostics["full_churn_round_trips"], 9)
        self.assertEqual(diagnostics["replacement_round_trips"], 7)
        self.assertAlmostEqual(diagnostics["average_retention_pct"], 33.3333, places=4)


class FakeRepository:
    def __init__(self):
        self.start = date(2026, 3, 1)
        self.signal_dates = [self.start + timedelta(days=index) for index in range(6)]
        self.rows = [
            trade_row(signal_date, f"code-{index}", 0.05 if index % 2 == 0 else -0.05)
            for index, signal_date in enumerate(self.signal_dates)
        ]
        self.rows[-1]["exit_price_10d"] = None

    def load_run(self, _run_id):
        return {
            "run_id": "run-test",
            "strategy_id": "v13_three_factor",
            "strategy_version": "0.1.0",
            "status": "success",
            "start_date": self.signal_dates[0],
            "end_date": self.signal_dates[-1],
            "request_json": {"commission_bps": 3, "stamp_tax_bps": 5, "slippage_bps": 5},
        }

    def load_signal_dates(self, _run_id):
        return [{"trade_date": value, "pick_count": 1} for value in self.signal_dates]

    def load_trade_rows(self, _run_id):
        return self.rows

    def load_market_rows(self, _index_code, _end_date):
        market_start = self.start - timedelta(days=70)
        return [
            {"trade_date": market_start + timedelta(days=index), "open": 100 + index, "close": 100 + index}
            for index in range(77)
        ]


class FailureAttributionServiceTests(unittest.TestCase):
    def test_report_is_research_only_and_missing_horizon_stays_visible(self):
        report = StrategyFailureAttributionService(repository=FakeRepository()).build_report("run-test")

        self.assertTrue(report["run"]["research_only"])
        self.assertEqual(report["trade_horizons"]["10d"]["gross"]["missing_count"], 1)
        self.assertTrue(
            any(row["incomplete_rebalance_count"] == 1 for row in report["non_overlapping_cohorts"]["10d"])
        )
        self.assertEqual(report["report_version"], "strategy_failure_attribution_v1")
        self.assertEqual(report["classification"]["current_version_decision"], "reject_as_effective_strategy")


if __name__ == "__main__":
    unittest.main()
