from __future__ import annotations

import copy
import unittest

from app.data_quality.repository import gap_persistence
from app.data_quality.service import evaluate_data_quality


def healthy_snapshot() -> dict:
    return {
        "audit_version": "dq4",
        "generated_at": "2026-07-17 04:05:00",
        "history_lookback_trade_days": 60,
        "upstream_attempts": {
            "daily_kline": {
                "task_name": "daily_kline_increment",
                "status": "success",
                "last_attempt_at": "2026-07-17 02:00:02",
            },
            "factor_input_daily": {
                "task_name": "factor_input_daily_update",
                "status": "success",
                "last_attempt_at": "2026-07-17 03:20:48",
            },
            "fundamental_pit": {
                "task_name": "fundamental_pit_backfill",
                "status": "success",
                "last_attempt_at": "2026-07-17 04:40:48",
            },
        },
        "dates": {
            "daily_kline_trade_date": "2026-07-16",
            "factor_input_trade_date": "2026-07-16",
            "status_snapshot_trade_date": "2026-07-16",
            "factor_status_snapshot_trade_date": "2026-07-16",
        },
        "stock_basic": {
            "active_stock_rows": 100,
            "missing_instrument_type": 0,
            "invalid_code": 0,
            "missing_name": 0,
            "missing_market": 0,
            "market_code_mismatch": 0,
            "missing_listing_date": 0,
            "missing_industry": 0,
            "suspected_delisted_active": 0,
        },
        "daily_kline": {
            "metrics": {
                "rows_count": 100,
                "duplicate_rows": 0,
                "orphan_rows": 0,
                "null_ohlc": 0,
                "nonpositive_ohlc": 0,
                "invalid_ohlc_order": 0,
                "invalid_volume": 0,
                "invalid_amount": 0,
                "missing_source": 0,
            },
            "gaps": {
                "missing_total": 2,
                "expected_non_trading": 2,
                "new_listing_pending": 0,
                "actionable_missing": 0,
            },
            "samples": [],
        },
        "factor_input_daily": {
            "metrics": {
                "duplicate_rows": 0,
                "orphan_rows": 0,
                "null_completeness": 0,
                "missing_provenance_rows": 0,
                "low_completeness_rows": 20,
                "missing_pe_rows": 50,
                "missing_pb_rows": 0,
                "missing_all_fundamental_rows": 0,
                "avg_completeness": 0.9,
            },
            "coverage_gaps": {
                "missing_total": 0,
                "expected_non_trading": 0,
                "new_listing_pending": 0,
                "actionable_missing": 0,
            },
            "coverage_samples": [],
            "market_field_gaps": {
                "missing_total": 2,
                "expected_non_trading": 2,
                "new_listing_pending": 0,
                "actionable_missing": 0,
            },
            "samples": [],
        },
        "point_in_time_status": {
            "history_start_date": "2024-01-02",
            "history_end_date": "2026-07-16",
            "lifecycle": {
                "lifecycle_rows": 120,
                "active_rows": 100,
                "delisted_rows": 20,
                "invalid_lifecycle_rows": 0,
            },
            "name_history": {
                "rows_count": 300,
                "relevant_codes": 120,
                "name_covered_codes": 120,
                "invalid_intervals": 0,
                "st_intervals": 25,
                "delisting_intervals": 20,
            },
            "suspension": {
                "expected_trade_days": 600,
                "successful_days": 600,
                "failed_days": 0,
            },
            "manifest": {
                "lifecycle_status": "success",
                "name_history_status": "success",
            },
            "historical_market_data": {
                "historical_delisted_codes": 20,
                "kline_covered_codes": 20,
                "factor_covered_codes": 20,
                "missing_market_data_codes": 0,
                "historical_factor_rows": 1000,
                "historical_market_field_rows": 980,
                "market_field_coverage_ratio": 0.98,
            },
            "samples": [],
        },
        "point_in_time_fundamentals": {
            "history_start_date": "2024-01-02",
            "history_end_date": "2026-07-16",
            "table": {
                "rows_count": 80000,
                "distinct_codes": 5800,
                "min_announcement_date": "2023-03-01",
                "max_announcement_date": "2026-07-16",
                "min_period_end_date": "2022-12-31",
                "max_period_end_date": "2026-06-30",
                "invalid_reporting_order_rows": 0,
                "future_announcement_rows": 0,
                "future_period_rows": 0,
                "empty_indicator_rows": 0,
            },
            "manifest": {
                "manifest_periods": 15,
                "successful_periods": 15,
                "partial_periods": 0,
                "failed_periods": 0,
            },
            "coverage": {
                "sample_dates": 12,
                "expected_rows": 60000,
                "covered_rows": 59000,
                "missing_rows": 1000,
                "coverage_ratio": 0.983333,
                "worst_trade_date": "2024-01-02",
                "worst_coverage_ratio": 0.97,
            },
            "samples": [],
        },
        "future_rows": {
            "daily_kline": 0,
            "factor_input_daily": 0,
            "stock_status_snapshot": 0,
        },
    }


class DataQualityEvaluationTests(unittest.TestCase):
    def test_gap_persistence_uses_market_trade_dates_and_marks_bounded_history(self):
        reference_dates = ["2026-07-10", "2026-07-13", "2026-07-14", "2026-07-15", "2026-07-16"]

        persistent = gap_persistence(reference_dates, "2026-07-13", "2020-01-01")
        capped = gap_persistence(reference_dates, "2026-07-01", "2020-01-01")
        new_listing = gap_persistence(reference_dates, None, "2026-07-15")

        self.assertEqual(persistent["consecutive_missing_trade_days"], 3)
        self.assertEqual(persistent["persistence_level"], "persistent")
        self.assertFalse(persistent["persistence_capped"])
        self.assertEqual(capped["consecutive_missing_trade_days"], 5)
        self.assertEqual(capped["persistence_level"], "long_running")
        self.assertTrue(capped["persistence_capped"])
        self.assertEqual(new_listing["consecutive_missing_trade_days"], 2)
        self.assertFalse(new_listing["persistence_capped"])

    def test_expected_non_trading_and_missing_pe_are_not_failures(self):
        result = evaluate_data_quality(healthy_snapshot())

        self.assertEqual(result["health"], "healthy")
        self.assertEqual(result["counts"], {"pass": 14, "warn": 0, "fail": 0})

    def test_actionable_gaps_are_warnings_with_small_bounded_counts(self):
        snapshot = healthy_snapshot()
        snapshot["daily_kline"]["gaps"].update(
            {"missing_total": 5, "expected_non_trading": 2, "actionable_missing": 3}
        )
        snapshot["factor_input_daily"]["market_field_gaps"].update(
            {"missing_total": 4, "expected_non_trading": 1, "actionable_missing": 3}
        )

        result = evaluate_data_quality(snapshot)
        status_by_id = {item["check_id"]: item["status"] for item in result["checks"]}

        self.assertEqual(result["health"], "warning")
        self.assertEqual(status_by_id["daily_kline_coverage"], "warn")
        self.assertEqual(status_by_id["factor_input_market_fields"], "warn")

    def test_persistent_gap_trace_is_kept_in_check_metrics_and_samples(self):
        snapshot = healthy_snapshot()
        sample = {
            "code": "sh.689009",
            "classification": "actionable_missing",
            "consecutive_missing_trade_days": 5,
            "persistence_level": "long_running",
            "last_success_trade_date": "2026-07-10",
            "last_success_source": "tushare_daily",
            "last_attempt_at": "2026-07-17 02:00:02",
            "last_attempt_status": "success",
        }
        snapshot["daily_kline"]["gaps"].update({"missing_total": 1, "actionable_missing": 1})
        snapshot["daily_kline"]["samples"] = [sample]

        result = evaluate_data_quality(snapshot)
        check = next(item for item in result["checks"] if item["check_id"] == "daily_kline_coverage")

        self.assertEqual(result["audit_version"], "dq4")
        self.assertEqual(check["metrics"]["persistent_samples"], 1)
        self.assertEqual(check["metrics"]["long_running_samples"], 1)
        self.assertEqual(check["metrics"]["max_consecutive_missing_trade_days"], 5)
        self.assertEqual(check["metrics"]["upstream_attempt"]["status"], "success")
        self.assertEqual(check["samples"][0]["last_success_source"], "tushare_daily")

    def test_invalid_ohlc_or_future_rows_fail_the_audit(self):
        snapshot = copy.deepcopy(healthy_snapshot())
        snapshot["daily_kline"]["metrics"]["invalid_ohlc_order"] = 1
        snapshot["future_rows"]["factor_input_daily"] = 2

        result = evaluate_data_quality(snapshot)
        status_by_id = {item["check_id"]: item["status"] for item in result["checks"]}

        self.assertEqual(result["health"], "error")
        self.assertEqual(status_by_id["daily_kline_integrity"], "fail")
        self.assertEqual(status_by_id["future_trade_dates"], "fail")

    def test_incomplete_historical_delisted_market_data_stays_research_warning(self):
        snapshot = healthy_snapshot()
        snapshot["point_in_time_status"]["historical_market_data"].update(
            {
                "kline_covered_codes": 14,
                "factor_covered_codes": 14,
                "missing_market_data_codes": 6,
            }
        )
        snapshot["point_in_time_status"]["samples"] = [
            {
                "code": "sz.000005",
                "classification": "historical_universe_missing",
                "missing_kline": 1,
                "missing_factor_input": 1,
            }
        ]

        result = evaluate_data_quality(snapshot)
        check = next(
            item for item in result["checks"]
            if item["check_id"] == "point_in_time_historical_universe_data"
        )

        self.assertEqual(result["health"], "warning")
        self.assertEqual(check["status"], "warn")
        self.assertFalse(check["metrics"]["backtest_universe_ready"])

    def test_complete_historical_delisted_market_data_reports_zero_gap(self):
        snapshot = healthy_snapshot()
        snapshot["point_in_time_status"]["historical_market_data"].update(
            {
                "historical_delisted_codes": 3,
                "kline_covered_codes": 2,
                "factor_covered_codes": 2,
                "no_market_activity_codes": 1,
                "missing_market_data_codes": 0,
            }
        )

        result = evaluate_data_quality(snapshot)
        check = next(
            item for item in result["checks"]
            if item["check_id"] == "point_in_time_historical_universe_data"
        )

        self.assertEqual(check["status"], "pass")
        self.assertTrue(check["metrics"]["backtest_universe_ready"])
        self.assertIn("缺口已归零", check["message"])
        self.assertNotIn("未归零前", check["message"])

    def test_historical_rows_without_market_fields_are_not_marked_ready(self):
        snapshot = healthy_snapshot()
        snapshot["point_in_time_status"]["historical_market_data"].update(
            {
                "historical_factor_rows": 1000,
                "historical_market_field_rows": 900,
                "market_field_coverage_ratio": 0.9,
            }
        )

        result = evaluate_data_quality(snapshot)
        check = next(
            item for item in result["checks"]
            if item["check_id"] == "point_in_time_historical_universe_data"
        )

        self.assertEqual(check["status"], "warn")
        self.assertFalse(check["metrics"]["backtest_universe_ready"])
        self.assertEqual(check["metrics"]["market_field_min_coverage"], 0.95)

    def test_low_point_in_time_fundamental_coverage_stays_research_warning(self):
        snapshot = healthy_snapshot()
        snapshot["point_in_time_fundamentals"]["coverage"].update(
            {
                "expected_rows": 60000,
                "covered_rows": 54000,
                "missing_rows": 6000,
                "coverage_ratio": 0.9,
                "worst_coverage_ratio": 0.85,
            }
        )

        result = evaluate_data_quality(snapshot)
        check = next(
            item for item in result["checks"]
            if item["check_id"] == "point_in_time_fundamental_truth"
        )

        self.assertEqual(result["health"], "warning")
        self.assertEqual(check["status"], "warn")
        self.assertFalse(check["metrics"]["backtest_fundamental_ready"])
        self.assertEqual(check["metrics"]["minimum_coverage_ratio"], 0.95)


if __name__ == "__main__":
    unittest.main()
