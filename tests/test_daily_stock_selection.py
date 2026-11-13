from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from scripts import run_daily_stock_selection as daily


def readiness(*, opened=True, healthy=True, accepting=True, day="2026-11-16"):
    return {"status": "ready" if accepting else "not_ready", "accepting_jobs": accepting,
            "data": {"independent_calendar": {
                "health": "healthy" if healthy else "error",
                "current_date": day, "current_date_is_open": opened,
            }}}


class DailySelectionTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 16, 9, 35, tzinfo=daily.TIMEZONE)
        self.service = MagicMock()
        self.service.existing.return_value = None
        self.service.submit.return_value = {
            "run_id": "scheduled-run", "status": "queued", "input_snapshot_id": "new-snapshot",
        }
        self.strategy = MagicMock()
        self.strategy.get_strategy_meta.return_value = {"version": "0.4.4"}
        self.strategy._sentiment_read_model_enabled.return_value = True
        self.materializer = MagicMock()
        self.materializer.materialize.return_value = {
            "snapshot_id": "new-snapshot", "snapshot_status": "ready", "quality_status": "passed",
            "strategy_id": daily.STRATEGY_ID, "strategy_version": "0.4.4",
            "decision_as_of": "2026-11-16 09:35:01",
        }
        self.patches = [
            patch.object(daily, "local_now", return_value=self.now),
            patch.object(daily, "DailySelectionRunService", return_value=self.service),
            patch.object(daily, "StrategyService", return_value=self.strategy),
            patch.object(daily, "SentimentSnapshotMaterializationService", return_value=self.materializer),
            patch.object(daily, "build_operational_readiness", return_value=readiness()),
            patch.object(daily, "acquire_mysql_advisory_lock", return_value="lock"),
            patch.object(daily, "release_mysql_advisory_lock", return_value=None),
            patch.object(daily, "next_scheduled_at", return_value="2026-11-16T09:35:00+08:00"),
        ]
        self.mocks = [p.start() for p in self.patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(self.patches)])

    def test_submit_uses_current_snapshot_version_and_at_most_three(self):
        result = daily.run()
        self.assertEqual(result["status"], "submitted")
        self.materializer.materialize.assert_called_once_with(strategy_id="a_share_sentiment", max_picks=3)
        request = self.service.submit.call_args.args[0]
        self.assertEqual((request["strategy_id"], request["max_picks"], request["limit"]),
                         ("a_share_sentiment", 3, 3))
        self.assertIs(request["save"], False)
        self.assertEqual(self.service.submission_snapshot["snapshot_id"], "new-snapshot")
        self.mocks[6].assert_called_once_with("lock")

    def test_holiday_on_weekday_skips_without_materializing_or_submitting(self):
        self.mocks[4].return_value = readiness(opened=False)
        self.assertEqual(daily.run()["reason"], "market_closed")
        self.materializer.materialize.assert_not_called()
        self.service.submit.assert_not_called()

    def test_missing_calendar_fails_closed(self):
        self.mocks[4].return_value = readiness(healthy=False)
        self.assertEqual(daily.run()["status"], "blocked")
        self.service.submit.assert_not_called()

    def test_stale_calendar_date_fails_closed(self):
        self.mocks[4].return_value = readiness(day="2026-11-13")
        self.assertEqual(daily.run()["reason"], "independent_calendar_not_ready")
        self.service.submit.assert_not_called()

    def test_unready_data_or_worker_does_not_submit(self):
        self.mocks[4].return_value = readiness(accepting=False)
        self.assertEqual(daily.run()["reason"], "operational_readiness_not_ready")
        self.service.submit.assert_not_called()

    def test_before_time_and_deadline_do_not_backfill(self):
        for hour, minute in ((9, 34), (9, 40), (14, 35)):
            self.mocks[0].return_value = self.now.replace(hour=hour, minute=minute)
            self.assertEqual(daily.run()["reason"], "outside_0935_submission_window")
        self.service.submit.assert_not_called()

    def test_completed_failed_cancelled_and_empty_runs_are_not_repeated(self):
        for status in ("success", "failed", "cancelled", "no_data", "queued", "running"):
            self.service.existing.return_value = {"run_id": "existing", "status": status}
            self.assertEqual(daily.run()["status"], "deduplicated")
        self.materializer.materialize.assert_not_called()
        self.service.submit.assert_not_called()

    def test_concurrent_invocation_skips(self):
        self.mocks[5].return_value = None
        self.assertEqual(daily.run()["reason"], "already_running")
        self.service.existing.assert_not_called()
        self.service.submit.assert_not_called()

    def test_dry_run_never_locks_materializes_or_submits(self):
        result = daily.run(dry_run=True)
        self.assertEqual(result["registered_strategy_version"], "0.4.4")
        self.assertIs(result["selection_task_submitted"], False)
        self.mocks[5].assert_not_called()
        self.materializer.materialize.assert_not_called()
        self.service.submit.assert_not_called()

    def test_version_drift_is_rejected(self):
        self.strategy.get_strategy_meta.return_value = {"version": "0.5.1"}
        with self.assertRaisesRegex(RuntimeError, "0.4.4"):
            daily.run()
        self.service.submit.assert_not_called()
        self.mocks[6].assert_called_once()

    def test_snapshot_failure_is_not_fallback_to_old_snapshot(self):
        self.materializer.materialize.side_effect = RuntimeError("source gap")
        with self.assertRaisesRegex(RuntimeError, "source gap"):
            daily.run()
        self.service.submit.assert_not_called()
        self.mocks[6].assert_called_once()

    def test_old_rejected_or_wrong_version_snapshots_are_rejected(self):
        original = self.materializer.materialize.return_value
        for changes in ({"decision_as_of": "2026-11-13 09:35:01"},
                        {"decision_as_of": "2026-11-16 09:22:01"},
                        {"snapshot_status": "rejected"}, {"quality_status": "failed"},
                        {"strategy_version": "0.5.1"}):
            self.materializer.materialize.return_value = {**original, **changes}
            with self.assertRaisesRegex(RuntimeError, "snapshot is not ready"):
                daily.run()
        self.service.submit.assert_not_called()

    def test_slow_materialization_does_not_submit_after_deadline(self):
        self.mocks[0].side_effect = [self.now, self.now.replace(minute=40)]
        self.assertEqual(daily.run()["reason"], "submission_deadline_elapsed")
        self.service.submit.assert_not_called()


class DailySelectionIdentityTests(unittest.TestCase):
    def test_key_is_independent_of_data_date_snapshot_and_manual_namespace(self):
        service = daily.DailySelectionRunService(date(2026, 11, 16))
        key = service._idempotency_key({"input_snapshot_id": "first"}, date(2026, 11, 13))
        self.assertEqual(key, service._idempotency_key({"input_snapshot_id": "second"}, date(2026, 11, 16)))
        self.assertNotEqual(key, daily.daily_key(date(2026, 11, 17)))
        self.assertNotEqual(key, daily.SelectionRunService._idempotency_key({}, date(2026, 11, 16)))

    def test_lookup_reuses_terminal_rows_using_indexed_schedule_key(self):
        service = daily.DailySelectionRunService(date(2026, 11, 16))
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {"run_id": "finished", "status": "success"}
        with patch.object(daily, "mysql_read_conn", return_value=connection):
            row = service._get_active_by_idempotency("schedule-key")
        self.assertEqual(row["status"], "success")
        sql, params = cursor.execute.call_args.args
        self.assertNotIn("status IN", sql)
        self.assertEqual(params, ("schedule-key",))

    def test_pinned_snapshot_cannot_silently_resolve_latest(self):
        service = daily.DailySelectionRunService(date(2026, 11, 16))
        with self.assertRaisesRegex(RuntimeError, "pin"):
            service._resolve_submission_snapshot_meta()
        service.submission_snapshot = {"snapshot_id": "new"}
        self.assertEqual(service._resolve_submission_snapshot_meta()["snapshot_id"], "new")

    def test_timezone_is_shanghai_not_process_or_utc(self):
        utc_now = datetime(2026, 11, 16, 1, 35, tzinfo=timezone.utc)
        self.assertEqual(daily.preflight(utc_now, readiness()), ("ready", None))


if __name__ == "__main__":
    unittest.main()
