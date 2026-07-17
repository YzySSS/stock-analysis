from __future__ import annotations

import unittest
from unittest.mock import patch

from app.backtest.validation_baseline import (
    BacktestValidationBaseline,
    VALIDATION_KNOWN_LIMITATIONS,
    build_run_comparison,
    validate_baseline_id,
)


def run_row(**overrides):
    row = {
        "id": 100,
        "run_id": "current",
        "strategy_id": "v13_three_factor",
        "strategy_version": "0.1.0",
        "strategy_config_hash": "abc",
        "methodology_version": "close_signal_next_open_v2",
        "start_date": "2026-04-24",
        "end_date": "2026-04-27",
        "return_mode": "1d",
        "sample_days": 2,
        "total_picks": 6,
        "total_trades": 6,
        "total_return_pct": -1.0,
        "avg_return_pct": -0.5,
        "max_drawdown_pct": -1.2,
        "win_rate_pct": 33.3333,
        "request_json": {
            "max_picks": 3,
            "score_threshold": 65.0,
            "use_adjusted_price": False,
        },
    }
    row.update(overrides)
    return row


class BacktestValidationBaselineTests(unittest.TestCase):
    def test_known_limitations_match_dq5_truth_layer(self):
        joined = " ".join(VALIDATION_KNOWN_LIMITATIONS)
        self.assertIn("sh.689009", joined)
        self.assertIn("月度权重快照", joined)
        self.assertNotIn("指数成分变更历史尚未建模", joined)
        self.assertNotIn("历史 ST、退市和成分变更数据仍不完整", joined)

    def test_baseline_id_rejects_whitespace_and_shell_chars(self):
        self.assertEqual(validate_baseline_id("b3_20260716-1d"), "b3_20260716-1d")
        with self.assertRaises(ValueError):
            validate_baseline_id("bad baseline; rm")

    def test_old_run_without_hash_is_directional_even_when_version_matches(self):
        legacy = run_row(
            id=10,
            run_id="legacy",
            strategy_config_hash=None,
            methodology_version="legacy_pre_point_in_time_v1",
            total_return_pct=-4.0,
        )

        comparison = build_run_comparison(run_row(), legacy)

        self.assertEqual(comparison["comparison_level"], "directional_same_version_unverifiable_config")
        self.assertEqual(comparison["metrics"]["total_return_pct"]["delta"], 3.0)
        self.assertFalse(comparison["checks"]["config_hash_verifiable"])

    def test_matching_hash_and_request_is_controlled_comparison(self):
        legacy = run_row(
            id=10,
            run_id="legacy",
            methodology_version="legacy_pre_point_in_time_v1",
            total_return_pct=-4.0,
        )

        comparison = build_run_comparison(run_row(), legacy)

        self.assertEqual(comparison["comparison_level"], "controlled_methodology_comparison")
        self.assertTrue(comparison["checks"]["config_hash_match"])

    def test_strategy_version_change_forces_directional_only(self):
        legacy = run_row(
            id=10,
            run_id="legacy",
            strategy_version="v1",
            strategy_config_hash=None,
            methodology_version="legacy_pre_point_in_time_v1",
        )

        comparison = build_run_comparison(run_row(strategy_id="lowvol_reversal", strategy_version="v2.1"), legacy)

        self.assertEqual(comparison["comparison_level"], "directional_only")
        self.assertFalse(comparison["checks"]["strategy_version_match"])

    def test_plan_rejects_window_over_trade_day_guardrail(self):
        class FakeService:
            @staticmethod
            def _fetch_trade_dates(_start, _end):
                return [f"2026-07-{day:02d}" for day in range(1, 12)]

        baseline = BacktestValidationBaseline(service=FakeService())
        with patch.object(BacktestValidationBaseline, "_active_runs", return_value=[]), patch(
            "app.backtest.validation_baseline.system_memory_snapshot",
            return_value={"available_mb": 2048, "swap_used_mb": 0},
        ):
            with self.assertRaises(ValueError):
                baseline.plan(
                    baseline_id="b3",
                    strategies=["lowvol_reversal"],
                    start_date="2026-07-01",
                    end_date="2026-07-15",
                    max_trade_days=10,
                )


if __name__ == "__main__":
    unittest.main()
