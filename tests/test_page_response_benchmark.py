from __future__ import annotations

import unittest

from scripts.benchmark_page_responses import (
    PROFILE_TARGETS,
    Target,
    measure_backtest_first_screen,
    pick_default_backtest_run,
    summarize_samples,
    targets_for_profiles,
    validate_base_url,
)
from unittest.mock import Mock, patch


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

    def test_backtest_default_run_matches_frontend_priority(self):
        items = [
            {"run_id": "success", "status": "success", "progress_pct": 100},
            {"run_id": "queued", "status": "queued", "progress_pct": 0},
            {"run_id": "running-low", "status": "running", "progress_pct": 20},
            {"run_id": "running-high", "status": "running", "progress_pct": 80},
        ]

        self.assertEqual(pick_default_backtest_run(items)["run_id"], "running-high")
        self.assertEqual(
            pick_default_backtest_run([item for item in items if item["status"] != "running"])["run_id"],
            "queued",
        )
        self.assertEqual(pick_default_backtest_run([items[0]])["run_id"], "success")
        self.assertIsNone(pick_default_backtest_run([]))

    def test_backtest_first_screen_includes_latest_result_and_trades(self):
        requested_urls = []
        session = Mock()

        def fake_get(url, timeout):
            requested_urls.append(url)
            response = Mock(status_code=200, content=b"{}")
            response.raise_for_status.return_value = None
            if "/api/backtest/runs?" in url:
                response.json.return_value = {
                    "items": [{"run_id": "run-1", "status": "success", "return_mode": "1d"}]
                }
            elif "/api/backtest/results?" in url:
                response.json.return_value = {"run_id": "run-1", "return_mode": "3d"}
            else:
                response.json.return_value = {}
            return response

        session.get.side_effect = fake_get
        with patch(
            "scripts.benchmark_page_responses.time.perf_counter",
            side_effect=[0.0, 0.1],
        ):
            result = measure_backtest_first_screen(
                session,
                "http://127.0.0.1:8000",
                repetitions=1,
                timeout_seconds=10,
            )

        self.assertEqual(result["request_count"], 6)
        self.assertEqual(result["selected_run_id"], "run-1")
        self.assertTrue(any("/api/backtest/results?run_id=run-1" in url for url in requested_urls))
        self.assertTrue(any("return_mode=3d" in url for url in requested_urls))

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
