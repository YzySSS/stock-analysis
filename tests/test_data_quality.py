from __future__ import annotations

import copy
import unittest

from app.data_quality.service import evaluate_data_quality


def healthy_snapshot() -> dict:
    return {
        "generated_at": "2026-07-17 04:05:00",
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
            "market_field_gaps": {
                "missing_total": 2,
                "expected_non_trading": 2,
                "new_listing_pending": 0,
                "actionable_missing": 0,
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
    def test_expected_non_trading_and_missing_pe_are_not_failures(self):
        result = evaluate_data_quality(healthy_snapshot())

        self.assertEqual(result["health"], "healthy")
        self.assertEqual(result["counts"], {"pass": 11, "warn": 0, "fail": 0})

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

    def test_invalid_ohlc_or_future_rows_fail_the_audit(self):
        snapshot = copy.deepcopy(healthy_snapshot())
        snapshot["daily_kline"]["metrics"]["invalid_ohlc_order"] = 1
        snapshot["future_rows"]["factor_input_daily"] = 2

        result = evaluate_data_quality(snapshot)
        status_by_id = {item["check_id"]: item["status"] for item in result["checks"]}

        self.assertEqual(result["health"], "error")
        self.assertEqual(status_by_id["daily_kline_integrity"], "fail")
        self.assertEqual(status_by_id["future_trade_dates"], "fail")


if __name__ == "__main__":
    unittest.main()
