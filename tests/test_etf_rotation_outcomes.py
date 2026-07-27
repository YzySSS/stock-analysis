from __future__ import annotations

import unittest

from app.etf_rotation.outcomes import compute_forward_outcome


class EtfRotationOutcomeTests(unittest.TestCase):
    def test_entry_is_next_open_and_exit_is_horizon_close(self) -> None:
        rows = [
            {
                "trade_date": "2026-07-27",
                "open": 1.0,
                "high": 1.08,
                "low": 0.98,
                "close": 1.05,
            },
            {
                "trade_date": "2026-07-28",
                "open": 1.05,
                "high": 1.12,
                "low": 1.01,
                "close": 1.1,
            },
            {
                "trade_date": "2026-07-29",
                "open": 1.09,
                "high": 1.15,
                "low": 1.04,
                "close": 1.12,
            },
        ]

        outcome = compute_forward_outcome(
            signal_trade_date="2026-07-24",
            horizon_days=3,
            future_rows=rows,
        )

        self.assertEqual("mature", outcome["outcome_status"])
        self.assertEqual("2026-07-27", outcome["entry_trade_date"])
        self.assertEqual(1.0, outcome["entry_price"])
        self.assertEqual("2026-07-29", outcome["exit_trade_date"])
        self.assertAlmostEqual(12.0, outcome["gross_return_pct"])
        self.assertAlmostEqual(15.0, outcome["maximum_favorable_excursion_pct"])
        self.assertAlmostEqual(-2.0, outcome["maximum_adverse_excursion_pct"])

    def test_entry_observed_is_not_mature_result(self) -> None:
        outcome = compute_forward_outcome(
            signal_trade_date="2026-07-24",
            horizon_days=5,
            future_rows=[
                {
                    "trade_date": "2026-07-27",
                    "open": 1.0,
                    "high": 1.02,
                    "low": 0.99,
                    "close": 1.01,
                }
            ],
        )

        self.assertEqual("entry_observed", outcome["outcome_status"])
        self.assertEqual("horizon_not_mature", outcome["block_reason"])
        self.assertNotIn("gross_return_pct", outcome)

    def test_missing_next_open_blocks_without_guessing(self) -> None:
        outcome = compute_forward_outcome(
            signal_trade_date="2026-07-24",
            horizon_days=1,
            future_rows=[
                {
                    "trade_date": "2026-07-27",
                    "open": None,
                    "close": 1.0,
                }
            ],
        )

        self.assertEqual("blocked", outcome["outcome_status"])
        self.assertEqual("next_trade_day_open_missing", outcome["block_reason"])

    def test_unit_split_inside_horizon_uses_provider_return(self) -> None:
        outcome = compute_forward_outcome(
            signal_trade_date="2026-07-01",
            horizon_days=3,
            future_rows=[
                {
                    "trade_date": "2026-07-02",
                    "open": 2.0,
                    "high": 2.04,
                    "low": 1.98,
                    "close": 2.0,
                    "pct_chg": 0.0,
                },
                {
                    "trade_date": "2026-07-03",
                    "open": 1.0,
                    "high": 1.02,
                    "low": 0.99,
                    "close": 1.0,
                    "pct_chg": 0.0,
                },
                {
                    "trade_date": "2026-07-06",
                    "open": 1.0,
                    "high": 1.03,
                    "low": 0.99,
                    "close": 1.01,
                    "pct_chg": 1.0,
                },
            ],
        )

        self.assertEqual("mature", outcome["outcome_status"])
        self.assertAlmostEqual(1.0, outcome["gross_return_pct"])
        self.assertEqual(
            "entry_open_then_provider_pct_chg_compounded",
            outcome["metadata"]["return_basis"],
        )
        self.assertEqual(1, len(outcome["metadata"]["unit_adjustments"]))


if __name__ == "__main__":
    unittest.main()
