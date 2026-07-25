from __future__ import annotations

import json
import unittest
from datetime import date, datetime
from unittest.mock import patch

from app.api.routes.system import LATEST_DATES_SQL, LATEST_KLINE_COUNTS_SQL, _latest_dates
from app.jobs.errors import error_fingerprint, infer_error_code, sanitize_error_message
from app.jobs.readiness import (
    DATA_SNAPSHOT_SQL,
    WORKER_STALE_SECONDS,
    _serialize_data_snapshot,
    classify_worker_snapshot,
)
from app.jobs.retention import JobRetentionPolicy, JobRetentionService
from app.jobs.task_log_compaction import (
    TaskRunMetadataCompactionService,
    prepare_market_opinion_metadata_compaction,
)
from app.jobs.worker_runtime import WorkerRuntimeHeartbeat
from app.shared.task_log import (
    TASK_RUN_METADATA_MARKER,
    TASK_RUN_METADATA_MAX_BYTES,
    _serialize_metadata,
)


class TaskRunLoggerSerializationTests(unittest.TestCase):
    def test_metadata_serialization_normalizes_date_and_datetime_values(self):
        payload = json.loads(
            _serialize_metadata(
                {
                    "summary_date": date(2026, 7, 21),
                    "captured_at": datetime(2026, 7, 21, 22, 0, 1),
                }
            )
        )

        self.assertEqual(payload["summary_date"], "2026-07-21")
        self.assertEqual(payload["captured_at"], "2026-07-21 22:00:01")

    def test_oversized_metadata_is_valid_bounded_json_with_scalar_lineage(self):
        payload = json.loads(
            _serialize_metadata(
                {
                    "run_id": "market_opinion_20260725_150000",
                    "status": "success",
                    "top_sectors": [
                        {
                            "sector_name": f"行业{index}",
                            "top_stocks": [
                                {"code": f"sh.{stock:06d}", "evidence": "证据" * 2000}
                                for stock in range(30)
                            ],
                        }
                        for index in range(8)
                    ],
                }
            )
        )

        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertLessEqual(len(encoded), TASK_RUN_METADATA_MAX_BYTES)
        self.assertEqual(payload["run_id"], "market_opinion_20260725_150000")
        self.assertEqual(payload["status"], "success")
        self.assertIsInstance(payload["top_sectors"], list)
        self.assertTrue(payload[TASK_RUN_METADATA_MARKER]["truncated"])
        self.assertGreater(
            payload[TASK_RUN_METADATA_MARKER]["original_bytes"],
            TASK_RUN_METADATA_MAX_BYTES,
        )

    def test_market_opinion_compaction_prepares_small_summary(self):
        serialized, serialized_bytes = prepare_market_opinion_metadata_compaction(
            {
                "run_id": "market_opinion_1",
                "status": "success",
                "top_sectors": [
                    {
                        "sector_name": "银行",
                        "sector_type": "industry",
                        "sector_score": 88.2,
                        "news_count": 12,
                        "source_count": 5,
                        "top_stocks": [{"blob": "x" * 100_000}],
                        "top_news": [{"blob": "y" * 100_000}],
                    }
                ],
            }
        )
        payload = json.loads(serialized)

        self.assertLess(serialized_bytes, TASK_RUN_METADATA_MAX_BYTES)
        self.assertEqual(payload["top_sectors"][0]["sector_name"], "银行")
        self.assertNotIn("top_stocks", payload["top_sectors"][0])
        self.assertNotIn("top_news", payload["top_sectors"][0])
        self.assertEqual(payload["detail_storage"], "normalized_market_opinion_tables")

    def test_compaction_service_rejects_unbounded_batch_size(self):
        with self.assertRaises(ValueError):
            TaskRunMetadataCompactionService(batch_size=501)

    def test_compaction_service_releases_lock_after_success(self):
        service = TaskRunMetadataCompactionService()
        preview = {
            "task_name": "market_opinion_update",
            "max_bytes": TASK_RUN_METADATA_MAX_BYTES,
            "total_rows": 0,
            "total_bytes": 0,
            "oversized_rows": 0,
            "oversized_bytes": 0,
            "first_started_at": None,
            "last_started_at": None,
        }
        lock_handle = object()

        with (
            patch(
                "app.jobs.task_log_compaction.acquire_mysql_advisory_lock",
                return_value=lock_handle,
            ),
            patch(
                "app.jobs.task_log_compaction.release_mysql_advisory_lock",
                return_value=None,
            ) as release_lock,
            patch.object(service, "preview", side_effect=[preview, preview]),
            patch.object(service, "_upper_bound_id", return_value=0),
            patch.object(service, "_fetch_batch", return_value=[]),
        ):
            result = service.apply()

        self.assertEqual(result["status"], "success")
        release_lock.assert_called_once_with(lock_handle)


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
        self.assertIn("WHERE instrument_type='stock'", LATEST_DATES_SQL)
        self.assertIn("FORCE INDEX (idx_trade_date)", LATEST_KLINE_COUNTS_SQL)
        self.assertIn("ORDER BY trade_date DESC", LATEST_KLINE_COUNTS_SQL)
        self.assertIn("LIMIT 20", LATEST_KLINE_COUNTS_SQL)
        self.assertIn("STOCK_DAILY_COMPLETENESS_RATIO", __import__("inspect").getsource(_latest_dates))


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
