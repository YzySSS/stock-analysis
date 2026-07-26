from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from app.jobs.readiness import _classify_error_recovery, recent_error_summaries


class JobErrorRecoveryTests(unittest.TestCase):
    def test_successful_later_run_marks_scheduled_error_recovered(self):
        result = _classify_error_recovery(
            {
                "source_kind": "scheduled_task",
                "last_seen_at": "2026-07-26 03:47:03",
            },
            {
                "status": "success",
                "started_at": datetime(2026, 7, 26, 14, 55, 3),
                "finished_at": datetime(2026, 7, 26, 14, 55, 4),
                "latest_success_at": datetime(2026, 7, 26, 14, 55, 4),
                "latest_partial_success_at": None,
            },
        )

        self.assertEqual(result["recovery_status"], "recovered")
        self.assertEqual(result["recovery_label"], "已恢复")
        self.assertEqual(result["latest_run_status"], "success")
        self.assertEqual(result["latest_run_at"], "2026-07-26 14:55:04")
        self.assertEqual(result["recovery_run_status"], "success")

    def test_later_partial_and_running_runs_are_not_reported_as_fully_recovered(self):
        summary = {
            "source_kind": "scheduled_task",
            "last_seen_at": "2026-07-26 03:47:03",
        }

        partial = _classify_error_recovery(
            summary,
            {
                "status": "partial_success",
                "finished_at": "2026-07-26 05:00:00",
                "latest_partial_success_at": "2026-07-26 05:00:00",
            },
        )
        running = _classify_error_recovery(
            summary,
            {"status": "running", "started_at": "2026-07-26 05:01:00"},
        )

        self.assertEqual(partial["recovery_status"], "partially_recovered")
        self.assertEqual(running["recovery_status"], "running_after_error")

    def test_new_running_row_does_not_hide_prior_successful_recovery(self):
        result = _classify_error_recovery(
            {
                "source_kind": "scheduled_task",
                "last_seen_at": "2026-07-26 03:47:03",
            },
            {
                "status": "running",
                "started_at": "2026-07-26 15:00:00",
                "latest_success_at": "2026-07-26 14:55:04",
            },
        )

        self.assertEqual(result["recovery_status"], "recovered")
        self.assertEqual(result["latest_run_status"], "running")
        self.assertEqual(result["recovery_run_at"], "2026-07-26 14:55:04")

    def test_older_success_and_non_scheduled_errors_remain_conservative(self):
        unresolved = _classify_error_recovery(
            {
                "source_kind": "scheduled_task",
                "last_seen_at": "2026-07-26 03:47:03",
            },
            {
                "status": "success",
                "finished_at": "2026-07-26 03:40:00",
                "latest_success_at": "2026-07-26 03:40:00",
            },
        )
        historical = _classify_error_recovery(
            {
                "source_kind": "durable_task",
                "last_seen_at": "2026-07-26 03:47:03",
            },
            None,
        )

        self.assertEqual(unresolved["recovery_status"], "unresolved")
        self.assertEqual(historical["recovery_status"], "historical")

    def test_recent_summaries_join_latest_task_run_without_dropping_history(self):
        conn_context = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        conn_context.__enter__.return_value = conn
        conn.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchall.side_effect = [
            [
                {
                    "source_kind": "scheduled_task",
                    "job_type": "market_scenario_forecast_shadow_update",
                    "error_code": "task_failed",
                    "occurrence_count": 1,
                    "first_seen_at": datetime(2026, 7, 26, 3, 47, 2),
                    "last_seen_at": datetime(2026, 7, 26, 3, 47, 3),
                    "last_message": "immutable forecast payload mismatch",
                },
                {
                    "source_kind": "durable_task",
                    "job_type": "durable_task",
                    "error_code": "upstream_timeout",
                    "occurrence_count": 2,
                    "first_seen_at": datetime(2026, 7, 25, 3, 0, 0),
                    "last_seen_at": datetime(2026, 7, 25, 3, 1, 0),
                    "last_message": "timeout",
                },
            ],
            [
                {
                    "task_name": "market_scenario_forecast_shadow_update",
                    "status": "success",
                    "started_at": datetime(2026, 7, 26, 14, 55, 3),
                    "finished_at": datetime(2026, 7, 26, 14, 55, 4),
                    "latest_success_at": datetime(2026, 7, 26, 14, 55, 4),
                    "latest_partial_success_at": None,
                }
            ],
        ]

        with patch("app.jobs.readiness.mysql_read_conn", return_value=conn_context):
            items = recent_error_summaries()

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["recovery_status"], "recovered")
        self.assertEqual(items[1]["recovery_status"], "historical")
        self.assertEqual(cursor.execute.call_count, 2)


if __name__ == "__main__":
    unittest.main()
