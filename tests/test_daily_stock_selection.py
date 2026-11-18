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
        self.service.get_run.return_value = {
            "run_id": "scheduled-run", "status": "success", "strategy_id": daily.STRATEGY_ID,
            "request": {"strategy_version": "0.4.4"},
            "result": {"count": 1, "results": [{"code": "sh.600000", "trade_date": "2026-11-13"}]},
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
            patch.object(daily, "save_tracking_item", return_value={"intraday_cache": {"status": "queued"}}),
        ]
        self.mocks = [p.start() for p in self.patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(self.patches)])

    def test_submit_uses_current_snapshot_version_and_at_most_three(self):
        result = daily.run()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["tracking_status"], "saved")
        self.assertEqual(result["saved_codes"], ["sh.600000"])
        self.materializer.materialize.assert_called_once_with(strategy_id="a_share_sentiment", max_picks=3)
        request = self.service.submit.call_args.args[0]
        self.assertEqual((request["strategy_id"], request["max_picks"], request["limit"]),
                         ("a_share_sentiment", 3, 3))
        self.assertIs(request["save"], False)
        self.assertEqual(self.service.submission_snapshot["snapshot_id"], "new-snapshot")
        self.mocks[8].assert_called_once_with(
            run_id="scheduled-run", item={"code": "sh.600000", "trade_date": "2026-11-13"},
            score_threshold=None,
        )
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

    def test_existing_runs_are_never_reselected_even_outside_submission_window(self):
        self.mocks[0].return_value = self.now.replace(hour=10)
        for status in ("success", "failed", "cancelled", "no_data", "queued", "running"):
            self.service.existing.return_value = {"run_id": "existing", "status": status}
            self.assertFalse(daily.run()["selection_task_submitted"])
        self.materializer.materialize.assert_not_called()
        self.service.submit.assert_not_called()

    def test_save_only_never_creates_missing_daily_run_even_in_submission_window(self):
        self.assertEqual(daily.run(save_only=True)["reason"], "no_daily_selection_run")
        self.service.submit.assert_not_called()
        self.materializer.materialize.assert_not_called()
        self.mocks[4].assert_not_called()
        self.mocks[8].assert_not_called()

    def test_dry_run_existing_task_never_saves_or_waits(self):
        self.service.existing.return_value = {"run_id": "existing", "status": "success"}
        self.assertEqual(daily.run(dry_run=True)["status"], "deduplicated")
        self.service.get_run.assert_not_called()
        self.mocks[8].assert_not_called()

    def test_save_only_reuses_existing_result_without_readiness_or_new_snapshot(self):
        self.service.existing.return_value = {"run_id": "existing", "status": "success"}
        self.mocks[4].side_effect = AssertionError("must not block saving an existing result")
        self.assertEqual(daily.run(save_only=True)["tracking_status"], "saved")
        self.service.submit.assert_not_called()
        self.materializer.materialize.assert_not_called()
        self.service.get_run.assert_called_once_with("existing")

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
        self.mocks[8].assert_not_called()

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


class DailyTrackingSaveTests(unittest.TestCase):
    def setUp(self):
        self.item = {"code": "sh.600000", "trade_date": "2026-11-13", "selected_price": 12.34,
                     "selected_price_type": "realtime", "selected_price_trade_date": "2026-11-16",
                     "selected_price_quote_time": "09:35:01", "rank_no": 2}
        self.run = {"status": "success", "strategy_id": daily.STRATEGY_ID,
                    "request": {"strategy_version": "0.4.4"},
                    "result": {"count": 1, "results": [self.item], "score_threshold": 60}}
        self.service = MagicMock()
        self.service.get_run.return_value = self.run
        self.save = patch.object(daily, "save_tracking_item", return_value={
            "intraday_cache": {"status": "queued"}}).start()
        self.addCleanup(patch.stopall)

    def test_original_result_run_id_price_time_rank_and_threshold_are_preserved(self):
        result = daily.save_completed_run(self.service, "daily-run")
        self.assertEqual(result["tracking_status"], "saved")
        self.save.assert_called_once_with(run_id="daily-run", item=self.item, score_threshold=60)
        self.assertEqual(self.item["selected_price_trade_date"], "2026-11-16")

    def test_failed_cancelled_and_empty_tasks_do_not_create_tracking(self):
        for status in ("failed", "cancelled", "no_data", "success"):
            self.run.update(status=status, result={"count": 0, "results": []})
            result = daily.save_completed_run(self.service, "daily-run")
            self.assertEqual(result["tracking_status"], "empty" if status in {"success", "no_data"} else "not_saved")
        self.save.assert_not_called()

    def test_pending_run_timeout_does_not_save_and_is_not_success(self):
        self.run["status"] = "running"
        result = daily.save_completed_run(self.service, "daily-run", wait_seconds=0)
        self.assertEqual(result["status"], "pending")
        self.save.assert_not_called()

    def test_waits_for_worker_before_saving(self):
        self.service.get_run.side_effect = [{**self.run, "status": "queued"}, self.run]
        with patch.object(daily.clock, "sleep") as sleep:
            result = daily.save_completed_run(self.service, "daily-run")
        sleep.assert_called_once()
        self.assertEqual(result["saved_count"], 1)

    def test_partial_save_failure_is_explicit_and_remaining_items_are_saved(self):
        self.run["result"] = {"count": 2, "results": [self.item, {**self.item, "code": "sz.000001"}]}
        self.save.side_effect = [RuntimeError("database unavailable"), {"intraday_cache": {"status": "queued"}}]
        result = daily.save_completed_run(self.service, "daily-run")
        self.assertEqual((result["status"], result["saved_count"]), ("failed", 1))
        self.assertEqual(result["tracking_errors"][0]["code"], "sh.600000")
        self.assertEqual(result["saved_codes"], ["sz.000001"])

    def test_saved_but_enrichment_enqueue_failure_does_not_report_full_success(self):
        self.save.return_value = {"intraday_cache": {"status": "degraded"}}
        result = daily.save_completed_run(self.service, "daily-run")
        self.assertEqual((result["status"], result["saved_count"]), ("failed", 1))
        self.assertEqual(result["reason"], "tracking_save_failed")

    def test_malformed_result_is_not_misreported_as_successful_empty_selection(self):
        for bad_result in (None, {}, {"results": [self.item], "count": 2},
                           {"results": [self.item, self.item], "count": 2}):
            self.run["result"] = bad_result
            with self.assertRaises(ValueError):
                daily.save_completed_run(self.service, "daily-run")
        self.save.assert_not_called()

    def test_research_cohort_and_wrong_version_are_rejected_without_saving(self):
        for request in ({"strategy_version": "0.5.1"},
                        {"strategy_version": "0.4.4", "forward_observation_id": "09:25"}):
            self.run["request"] = request
            with self.assertRaisesRegex(ValueError, "not a research task"):
                daily.save_completed_run(self.service, "daily-run")
        self.save.assert_not_called()

    def test_save_uses_existing_manual_contract_not_an_alternative_writer(self):
        patch.stopall()
        with patch("app.api.routes.selection.save_selection_item", return_value={"code": "sh.600000"}) as save:
            daily.save_tracking_item(run_id="daily-run", item=self.item, score_threshold=60)
        payload = save.call_args.args[0]
        self.assertEqual((payload.run_id, payload.strategy_id), ("daily-run", daily.STRATEGY_ID))
        self.assertEqual(payload.item["trade_date"], "2026-11-16")
        self.assertEqual(payload.item["selected_price"], self.item["selected_price"])

    def test_published_snapshot_pins_recorded_intraday_quote_not_yesterdays_close(self):
        item = {"code": "sh.600206", "selected_price": "39.3300", "selected_price_source": "daily_kline",
                "explain": {"raw_metrics": {"trade_date": "2026-11-17", "close": 39.33,
                    "realtime_price": 39.74, "realtime_trade_date": "2026-11-18",
                    "realtime_quote_time": "2026-11-18 09:33:54"}}}
        normalized = daily.tracking_item(item)
        self.assertEqual((normalized["selected_price"], normalized["selected_price_type"],
                          normalized["trade_date"], normalized["selected_price_quote_time"]),
                         (39.74, "realtime", "2026-11-18", "2026-11-18 09:33:54"))
        self.assertNotIn("trade_date", item)
        self.assertEqual(item["selected_price"], "39.3300")
        # Existing frozen writer honors the pinned quote even after the close.
        from app.stock_selection.selector import StockSelector
        selector = object.__new__(StockSelector)
        snapshot = selector._selection_price_snapshot(normalized, clock_mode="postclose")
        self.assertEqual(snapshot["selected_price"], 39.74)
        self.assertEqual(snapshot["selected_price_trade_date"], "2026-11-18")

    def test_missing_recorded_quote_is_an_explicit_gap_not_save_time_or_current_market_data(self):
        with self.assertRaisesRegex(ValueError, "recorded realtime"):
            daily.tracking_item({"code": "sh.600000", "selected_price": 10, "trade_date": "2026-11-13"})


class DailySelectionCliTests(unittest.TestCase):
    def test_pending_or_failed_save_returns_nonzero_and_marks_task_log_failed(self):
        for status in ("pending", "failed"):
            with self.subTest(status=status), patch.object(daily, "TaskRunLogger") as logger, \
                    patch.object(daily, "run", return_value={"status": status, "reason": "save_incomplete"}), \
                    patch("builtins.print"):
                self.assertEqual(daily.main(["--save-only", "--wait-seconds", "0"]), 2)
            self.assertEqual(logger.return_value.finish.call_args.args[2], "failed")

    def test_dry_run_never_starts_or_finishes_task_log(self):
        with patch.object(daily, "TaskRunLogger") as logger, \
                patch.object(daily, "run", return_value={"status": "deduplicated"}) as run, \
                patch("builtins.print"):
            self.assertEqual(daily.main(["--save-only", "--dry-run"]), 0)
        run.assert_called_once_with(dry_run=True, save_only=True, wait_seconds=300)
        logger.return_value.start.assert_not_called()
        logger.return_value.finish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
