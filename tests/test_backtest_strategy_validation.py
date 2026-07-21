from __future__ import annotations

import unittest
from datetime import date, timedelta
from unittest.mock import patch

from app.backtest.service import BacktestRequest, BacktestService
from app.backtest.strategy_validation import (
    HISTORICAL_HOLDOUT,
    PROSPECTIVE_OOS,
    StrategyValidationRequest,
    StrategyValidationService,
    build_validation_report,
    normalize_protocol_row,
    validate_protocol_id,
)


def protocol_row(mode: str = HISTORICAL_HOLDOUT) -> dict:
    request = {
        "strategy_id": "lowvol_reversal",
        "start_date": "2025-07-01",
        "end_date": "2025-12-31",
        "return_mode": "1d",
        "instrument_type": "stock",
        "universe_code": "ALL_A",
        "use_adjusted_price": False,
        "commission_bps": 3.0,
        "stamp_tax_bps": 5.0,
        "slippage_bps": 5.0,
        "apply_execution_constraints": True,
        "max_picks": 3,
        "score_threshold": 60.0,
        "is_system_test": True,
        "validation_baseline_id": "oos-lowvol",
        "validation_implementation_hash": "1" * 64,
    }
    return {
        "protocol_id": "oos-lowvol",
        "protocol_version": "frozen_oos_protocol_v2",
        "strategy_id": "lowvol_reversal",
        "strategy_version": "v2.1-risk-filtered",
        "strategy_config_hash": "frozen-hash",
        "methodology_version": "close_signal_next_open_pit_index_universe_v5",
        "validation_mode": mode,
        "eligible_for_validation": mode == PROSPECTIVE_OOS,
        "freeze_data_cutoff_date": "2025-06-30",
        "start_date": "2025-07-01",
        "end_date": "2025-12-31",
        "universe_code": "ALL_A",
        "benchmark_index_code": "000300.SH",
        "strategy_snapshot_json": {
            "implementation_fingerprint": {"sha256": "1" * 64, "files": []},
        },
        "request_json": request,
        "criteria_json": {
            "minimum_trade_days": 120,
            "minimum_trades": 120,
            "minimum_benchmark_coverage_pct": 98,
            "minimum_total_return_pct": 0,
            "minimum_excess_return_pct": 0,
            "minimum_sharpe_ratio": 0.5,
            "maximum_drawdown_floor_pct": -20,
            "minimum_positive_excess_period_ratio": 0.5,
            "minimum_bootstrap_mean_excess_ci_low_pct": 0,
        },
    }


def run_row(protocol: dict, *, total_trades: int = 390) -> dict:
    return {
        "run_id": "run-oos",
        "status": "success",
        "methodology_version": protocol["methodology_version"],
        "strategy_config_hash": protocol["strategy_config_hash"],
        "total_trades": total_trades,
        "request_json": protocol["request_json"],
    }


def positive_series(days: int = 130) -> tuple[list[dict], list[dict]]:
    start = date(2025, 7, 1)
    daily = [
        {
            "trade_date": start + timedelta(days=index),
            "pick_count": 3,
            "avg_return_1d_pct": 0.18 if index % 2 == 0 else 0.22,
            "win_rate_1d_pct": 66.67,
        }
        for index in range(days)
    ]
    benchmark = [
        {
            "trade_date": start + timedelta(days=index),
            "open": 1000 * (1.0005 ** index),
        }
        for index in range(days + 3)
    ]
    return daily, benchmark


class StrategyValidationReportTests(unittest.TestCase):
    def test_protocol_id_rejects_shell_or_whitespace(self):
        self.assertEqual(validate_protocol_id("oos-lowvol_20260717"), "oos-lowvol_20260717")
        with self.assertRaises(ValueError):
            validate_protocol_id("bad protocol; rm")

    def test_historical_holdout_can_pass_diagnostics_but_never_validation(self):
        protocol = protocol_row(HISTORICAL_HOLDOUT)
        daily, benchmark = positive_series()

        report = build_validation_report(
            protocol=protocol,
            run=run_row(protocol),
            daily_rows=daily,
            benchmark_rows=benchmark,
        )

        self.assertEqual(report["verdict"], "historical_diagnostic_pass")
        self.assertEqual(report["validation_status"], "validation_pending")
        self.assertFalse(report["auto_promoted"])
        self.assertTrue(all(report["checks"]["structural"].values()))

    def test_prospective_pass_is_candidate_and_not_auto_promoted(self):
        protocol = protocol_row(PROSPECTIVE_OOS)
        daily, benchmark = positive_series()

        report = build_validation_report(
            protocol=protocol,
            run=run_row(protocol),
            daily_rows=daily,
            benchmark_rows=benchmark,
        )

        self.assertEqual(report["verdict"], "prospective_oos_pass")
        self.assertEqual(report["validation_status"], "oos_pass_candidate")
        self.assertFalse(report["auto_promoted"])

    def test_no_trade_day_is_zero_return_instead_of_dropped(self):
        protocol = protocol_row(HISTORICAL_HOLDOUT)
        daily, benchmark = positive_series()
        daily[0]["avg_return_1d_pct"] = None
        daily[0]["pick_count"] = 0

        report = build_validation_report(
            protocol=protocol,
            run=run_row(protocol),
            daily_rows=daily,
            benchmark_rows=benchmark,
        )

        self.assertEqual(report["metrics"]["sample_days"], 130)
        self.assertEqual(report["metrics"]["no_trade_days"], 1)
        self.assertEqual(report["metrics"]["missing_return_days"], 0)
        self.assertEqual(report["metrics"]["trade_days"], 129)

    def test_trade_without_return_makes_report_inconclusive(self):
        protocol = protocol_row(HISTORICAL_HOLDOUT)
        daily, benchmark = positive_series()
        daily[0]["avg_return_1d_pct"] = None
        daily[0]["pick_count"] = 3

        report = build_validation_report(
            protocol=protocol,
            run=run_row(protocol),
            daily_rows=daily,
            benchmark_rows=benchmark,
        )

        self.assertEqual(report["verdict"], "inconclusive")
        self.assertEqual(report["metrics"]["missing_return_days"], 1)
        self.assertFalse(report["checks"]["structural"]["complete_return_series"])

    def test_config_drift_makes_report_inconclusive(self):
        protocol = protocol_row(HISTORICAL_HOLDOUT)
        daily, benchmark = positive_series()
        run = run_row(protocol)
        run["strategy_config_hash"] = "changed"

        report = build_validation_report(
            protocol=protocol,
            run=run,
            daily_rows=daily,
            benchmark_rows=benchmark,
        )

        self.assertEqual(report["verdict"], "inconclusive")
        self.assertFalse(report["checks"]["structural"]["strategy_config_hash_frozen"])

    def test_implementation_drift_makes_report_inconclusive(self):
        protocol = protocol_row(HISTORICAL_HOLDOUT)
        daily, benchmark = positive_series()
        run = run_row(protocol)
        run["request_json"] = {
            **run["request_json"],
            "validation_implementation_hash": "2" * 64,
        }

        report = build_validation_report(
            protocol=protocol,
            run=run,
            daily_rows=daily,
            benchmark_rows=benchmark,
        )

        self.assertEqual(report["verdict"], "inconclusive")
        self.assertFalse(report["checks"]["structural"]["implementation_code_frozen"])

    def test_compact_protocol_projection_omits_frozen_snapshots(self):
        row = {
            **protocol_row(HISTORICAL_HOLDOUT),
            "strategy_snapshot_json": {"large": "snapshot"},
            "report_json": {"metrics": {"sample_days": 120}, "checks": {"large": True}},
        }

        compact = normalize_protocol_row(row, include_details=False)

        self.assertNotIn("strategy_snapshot_json", compact)
        self.assertNotIn("request_json", compact)
        self.assertNotIn("criteria_json", compact)
        self.assertEqual(compact["report_json"]["metrics"]["sample_days"], 120)
        self.assertNotIn("checks", compact["report_json"])


class StrategyValidationPlanningTests(unittest.TestCase):
    class FakeValidationRepository:
        @staticmethod
        def current_data_cutoff():
            return "2026-07-17"

    class FakeBacktestService:
        MAX_BACKTEST_DAYS = 260
        _stable_hash = staticmethod(BacktestService._stable_hash)
        _strategy_config_hash = staticmethod(BacktestService._strategy_config_hash)

        @staticmethod
        def _fetch_trade_dates(_start: str, _end: str) -> list[str]:
            return [f"day-{index}" for index in range(120)]

    def service(self) -> StrategyValidationService:
        return StrategyValidationService(
            backtest_service=self.FakeBacktestService(),
            backtest_repository=object(),
            repository=self.FakeValidationRepository(),
        )

    def test_prospective_window_must_start_after_freeze_cutoff(self):
        request = StrategyValidationRequest(
            protocol_id="future-lowvol",
            strategy_id="lowvol_reversal",
            validation_mode=PROSPECTIVE_OOS,
            start_date="2026-07-17",
            end_date="2027-01-31",
        )

        with self.assertRaisesRegex(ValueError, "晚于冻结数据截止日"):
            self.service().plan(request)

    def test_historical_plan_is_explicitly_diagnostic_only(self):
        request = StrategyValidationRequest(
            protocol_id="history-lowvol",
            strategy_id="lowvol_reversal",
            validation_mode=HISTORICAL_HOLDOUT,
            start_date="2025-07-01",
            end_date="2026-06-30",
        )

        plan = self.service().plan(request)

        self.assertFalse(plan["eligible_for_validation"])
        self.assertEqual(plan["interpretation"], "historical_diagnostic_only")
        self.assertEqual(plan["planned_trade_days"], 120)
        fingerprint = plan["strategy_snapshot_json"]["implementation_fingerprint"]
        self.assertRegex(fingerprint["sha256"], r"^[0-9a-f]{64}$")
        self.assertTrue(any(item["path"].endswith("service.py") for item in fingerprint["files"]))
        self.assertEqual(
            plan["request_json"]["validation_implementation_hash"],
            fingerprint["sha256"],
        )

    def test_implementation_hash_is_restricted_to_grouped_system_runs(self):
        request = BacktestRequest(
            strategy_id="lowvol_reversal",
            start_date="2026-07-01",
            end_date="2026-07-02",
            validation_implementation_hash="1" * 64,
        )

        with patch("app.backtest.service.StrategyService.require_backtest_ready", return_value={"runtime_ready": True}):
            with self.assertRaisesRegex(ValueError, "已分组的系统验证任务"):
                BacktestService()._validate_request(request)


if __name__ == "__main__":
    unittest.main()
