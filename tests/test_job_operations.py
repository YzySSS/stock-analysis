from __future__ import annotations

import unittest

from app.api.routes.system import LATEST_DATES_SQL
from app.jobs.errors import error_fingerprint, infer_error_code, sanitize_error_message
from app.jobs.readiness import (
    DATA_SNAPSHOT_SQL,
    WORKER_STALE_SECONDS,
    _serialize_data_snapshot,
    classify_worker_snapshot,
)
from app.jobs.retention import JobRetentionPolicy, JobRetentionService
from app.jobs.worker_runtime import WorkerRuntimeHeartbeat


class WorkerReadinessTests(unittest.TestCase):
    def test_process_heartbeat_distinguishes_idle_from_missing_or_stale(self):
        self.assertEqual(classify_worker_snapshot(None), "missing")
        self.assertEqual(
            classify_worker_snapshot({"status": "idle", "heartbeat_age_seconds": 2}),
            "healthy",
        )
        self.assertEqual(
            classify_worker_snapshot(
                {"status": "running", "heartbeat_age_seconds": WORKER_STALE_SECONDS + 1}
            ),
            "stale",
        )
        self.assertEqual(
            classify_worker_snapshot({"status": "stopped", "heartbeat_age_seconds": 0}),
            "stopped",
        )

    def test_runtime_lease_reports_idle_running_idle_and_stop(self):
        class FakeRepository:
            def __init__(self):
                self.registered = []
                self.heartbeats = []
                self.stopped = []

            def register(self, worker_type, worker_id, metadata=None):
                self.registered.append((worker_type, worker_id, metadata))

            def heartbeat(self, worker_type, worker_id, status, current_job_id):
                self.heartbeats.append((worker_type, worker_id, status, current_job_id))
                return True

            def stop(self, worker_type, worker_id):
                self.stopped.append((worker_type, worker_id))

        repository = FakeRepository()
        runtime = WorkerRuntimeHeartbeat(
            "selection",
            "host:123",
            interval_seconds=60,
            repository=repository,
        )
        runtime.start()
        runtime.set_running("selection-1")
        runtime.set_idle()
        runtime.stop()

        self.assertEqual(repository.registered[0][:2], ("selection", "host:123"))
        self.assertEqual(
            [item[2:] for item in repository.heartbeats],
            [("idle", None), ("running", "selection-1"), ("idle", None)],
        )
        self.assertEqual(repository.stopped, [("selection", "host:123")])

    def test_data_readiness_compares_factor_input_with_complete_stock_kline_day(self):
        snapshot = _serialize_data_snapshot(
            {
                "daily_kline_latest_available_trade_date": "2026-07-16",
                "daily_kline_latest_complete_trade_date": "2026-07-15",
                "factor_input_latest_trade_date": "2026-07-15",
                "stock_basic_latest_updated_at": "2026-07-16 02:41:29",
            }
        )

        self.assertEqual(snapshot["health"], "healthy")
        self.assertFalse(snapshot["factor_input_lags_daily_kline"])
        self.assertTrue(snapshot["daily_kline_latest_is_partial"])
        self.assertEqual(snapshot["daily_kline_latest_trade_date"], "2026-07-15")
        self.assertEqual(snapshot["daily_kline_latest_available_trade_date"], "2026-07-16")

    def test_data_readiness_query_uses_stock_universe_completeness_threshold(self):
        normalized = " ".join(DATA_SNAPSHOT_SQL.split())

        self.assertIn("HAVING COUNT(*) >=", normalized)
        self.assertIn("WHERE instrument_type='stock'", normalized)
        self.assertIn("daily_kline_latest_complete_trade_date", normalized)

    def test_system_latest_dates_sql_has_rendered_completeness_policy(self):
        self.assertNotIn("{STOCK_", LATEST_DATES_SQL)
        self.assertIn("COUNT(*) * 0.95", LATEST_DATES_SQL)
        self.assertIn("WHERE instrument_type='stock'", LATEST_DATES_SQL)


class StructuredErrorTests(unittest.TestCase):
    def test_fingerprint_groups_messages_that_only_differ_by_numbers(self):
        first = "HTTP timeout after 30 seconds for batch 120"
        second = "HTTP timeout after 60 seconds for batch 999"
        self.assertEqual(error_fingerprint(first), error_fingerprint(second))
        self.assertEqual(infer_error_code(first), "upstream_timeout")

    def test_error_message_redacts_runtime_secrets(self):
        redacted = sanitize_error_message("token=private-value Bearer abc.def password=hunter2")
        self.assertNotIn("private-value", redacted)
        self.assertNotIn("abc.def", redacted)
        self.assertNotIn("hunter2", redacted)


class RetentionSafetyTests(unittest.TestCase):
    def test_policy_rejects_non_positive_retention(self):
        with self.assertRaises(ValueError):
            JobRetentionPolicy(task_detail_days=0).validate()

    def test_backtest_cleanup_only_targets_non_baseline_system_tests(self):
        statements = []

        class CapturingService(JobRetentionService):
            def _execute(self, sql, params=()):
                statements.append((sql, params))
                return 0

        CapturingService()._delete_old_backtest_system_tests()

        self.assertGreaterEqual(len(statements), 2)
        for sql, _params in statements:
            normalized = " ".join(sql.split())
            self.assertIn("is_system_test=1", normalized)
            self.assertIn("validation_baseline_id IS NULL", normalized)
            self.assertIn("status IN ('success','failed','cancelled')", normalized)

    def test_tracking_stats_retention_updates_flag_without_deleting_results(self):
        statements = []

        class CapturingService(JobRetentionService):
            def _execute(self, sql, params=()):
                statements.append((" ".join(sql.split()), params))
                return 0

        CapturingService()._exclude_expired_tracking_stats()

        self.assertEqual(len(statements), 1)
        sql, params = statements[0]
        self.assertIn("UPDATE selection_result", sql)
        self.assertIn("SET include_in_stats=0", sql)
        self.assertNotIn("DELETE", sql.upper())
        self.assertEqual(params, (14,))

    def test_durable_task_retention_only_deletes_terminal_rows(self):
        statements = []

        class CapturingService(JobRetentionService):
            def _execute(self, sql, params=()):
                statements.append((" ".join(sql.split()), params))
                return 0

        CapturingService()._delete_old_durable_tasks()

        self.assertEqual(len(statements), 1)
        sql, params = statements[0]
        self.assertIn("FROM durable_task", sql)
        self.assertIn("status IN ('success','failed','cancelled')", sql)
        self.assertNotIn("status='queued'", sql)
        self.assertEqual(params, (30,))


if __name__ == "__main__":
    unittest.main()
