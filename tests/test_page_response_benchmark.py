from __future__ import annotations

import unittest

from scripts.benchmark_page_responses import (
    PROFILE_TARGETS,
    Target,
    summarize_samples,
    targets_for_profiles,
    validate_base_url,
)
from unittest.mock import patch


class PageResponseBenchmarkTests(unittest.TestCase):
    def test_profiles_only_contain_read_only_get_targets(self):
        targets = targets_for_profiles(PROFILE_TARGETS)

        self.assertGreaterEqual(len(targets), 10)
        self.assertTrue(all(target.path.startswith(("/", "direct:")) for target in targets))
        self.assertFalse(any("/run" in target.path and "runs?" not in target.path for target in targets))
        self.assertFalse(any("/refresh" in target.path for target in targets))

    def test_duplicate_shared_targets_are_measured_once(self):
        targets = targets_for_profiles(["portfolio", "selection", "backtest"])
        strategy_targets = [target for target in targets if target.path == "/api/strategies?instrument_type=stock"]

        self.assertEqual(len(strategy_targets), 1)

    def test_budget_summary_uses_first_and_warm_contracts(self):
        result = summarize_samples(
            Target("sample", "/api/sample", "api"),
            [700.0, 120.0, 180.0],
            status_code=200,
            response_bytes=100,
        )

        self.assertTrue(result["first_budget_pass"])
        self.assertTrue(result["warm_budget_pass"])
        self.assertEqual(result["warm_median_ms"], 150.0)

    def test_base_url_validation_rejects_non_http_and_query(self):
        self.assertEqual(validate_base_url("http://127.0.0.1:8000/"), "http://127.0.0.1:8000")
        with self.assertRaises(ValueError):
            validate_base_url("file:///tmp/stock")
        with self.assertRaises(ValueError):
            validate_base_url("http://127.0.0.1:8000/?token=secret")

    def test_dashboard_cache_miss_probe_only_uses_cold_budget(self):
        from scripts.benchmark_page_responses import measure_dashboard_local_cache_miss

        with patch(
            "app.api.routes.dashboard.dashboard_summary",
            return_value={"status": "ok"},
        ), patch(
            "scripts.benchmark_page_responses.time.perf_counter",
            side_effect=[0.0, 0.4, 1.0, 1.45],
        ):
            result = measure_dashboard_local_cache_miss(2)

        self.assertTrue(result["first_budget_pass"])
        self.assertIsNone(result["warm_budget_ms"])
        self.assertIsNone(result["warm_budget_pass"])
        self.assertEqual(result["cache_miss_median_ms"], 425.0)


if __name__ == "__main__":
    unittest.main()
