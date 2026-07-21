from __future__ import annotations

import unittest

from app.shared.observability import DatabaseMetrics, RequestMetrics


class RequestMetricsTests(unittest.TestCase):
    def test_snapshot_exposes_bounded_latency_percentiles_and_errors(self):
        metrics = RequestMetrics(max_endpoints=2)
        for duration in (10, 20, 30, 40):
            metrics.observe("GET", "/api/health", 200, duration)
        metrics.observe("POST", "/api/selection/run", 503, 100)
        metrics.observe("GET", "/api/another", 200, 5)

        snapshot = metrics.snapshot()

        self.assertEqual(snapshot["request_count"], 6)
        self.assertEqual(snapshot["error_count"], 1)
        health = next(item for item in snapshot["items"] if item["endpoint"] == "GET /api/health")
        self.assertEqual(health["p50_ms"], 20)
        self.assertEqual(health["p95_ms"], 40)
        self.assertTrue(any(item["endpoint"] == "OTHER" for item in snapshot["items"]))

    def test_reset_discards_current_process_window(self):
        metrics = RequestMetrics()
        metrics.observe("GET", "/api/health", 200, 1)

        metrics.reset()

        self.assertEqual(metrics.snapshot()["request_count"], 0)


class DatabaseMetricsTests(unittest.TestCase):
    def test_exposes_pool_wait_and_transaction_percentiles(self):
        metrics = DatabaseMetrics(window_size=4)
        metrics.observe(checkout_ms=2, transaction_ms=20, success=True)
        metrics.observe(checkout_ms=8, transaction_ms=80, success=False)

        snapshot = metrics.snapshot()

        self.assertEqual(snapshot["transaction_count"], 2)
        self.assertEqual(snapshot["error_count"], 1)
        self.assertEqual(snapshot["pool_checkout_p95_ms"], 8.0)
        self.assertEqual(snapshot["transaction_p95_ms"], 80.0)


if __name__ == "__main__":
    unittest.main()
