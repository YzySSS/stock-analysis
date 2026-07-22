from __future__ import annotations

import unittest
from io import StringIO
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import date, datetime
from unittest.mock import patch

from app.data_ingestion import realtime_lifecycle
from app.data_ingestion.realtime_lifecycle import (
    RealtimeLifecyclePolicy,
    _chunks,
    _manifest_matches_legacy_source,
    _rollup_sql,
    expired_trade_dates,
    partition_name_for_date,
    retained_trade_dates,
)
from scripts.run_realtime_snapshot_update import (
    RealtimeFetchTimeout,
    RealtimeRow,
    fetch_deadline,
    fetch_spot_rows,
    is_trading_time,
    save_rows,
)


class RealtimeLifecyclePolicyTests(unittest.TestCase):
    def test_policy_preserves_two_raw_trade_days_and_longer_rollups(self):
        policy = RealtimeLifecyclePolicy().validate()
        self.assertEqual(policy.raw_trade_days, 2)
        self.assertEqual(policy.rollup_trade_days, 90)
        self.assertEqual(policy.tracked_trade_days, 90)
        with self.assertRaises(ValueError):
            RealtimeLifecyclePolicy(raw_trade_days=1).validate()
        with self.assertRaises(ValueError):
            RealtimeLifecyclePolicy(raw_trade_days=3, rollup_trade_days=2).validate()

    def test_retention_counts_distinct_trade_dates_not_calendar_days(self):
        values = ["2026-07-10", "2026-07-13", "2026-07-14", "2026-07-14"]
        self.assertEqual(
            retained_trade_dates(values, 2),
            [date(2026, 7, 14), date(2026, 7, 13)],
        )
        self.assertEqual(expired_trade_dates(values, 2), [date(2026, 7, 10)])
        self.assertEqual(partition_name_for_date("2026-07-14"), "p20260714")

    def test_rollup_uses_observed_prices_and_cumulative_deltas(self):
        sql = " ".join(_rollup_sql(5).split())
        self.assertIn("MAX(latest_price) AS high_price", sql)
        self.assertIn("MIN(latest_price) AS low_price", sql)
        self.assertIn("LAG(cumulative_volume)", sql)
        self.assertNotIn("MAX(high_price)", sql)
        with self.assertRaises(ValueError):
            _rollup_sql(30)

    def test_rollup_supports_small_code_batches(self):
        sql = " ".join(_rollup_sql(5, 2).split())
        self.assertIn("code IN (%s,%s)", sql)
        self.assertEqual(list(_chunks(["a", "b", "c"], 2)), [["a", "b"], ["c"]])

    def test_legacy_manifest_backfill_requires_matching_complete_source(self):
        source = {
            "source_rows": 100,
            "source_codes": 2,
            "first_quote_minute": datetime(2026, 7, 22, 9, 15),
            "last_quote_minute": datetime(2026, 7, 22, 15, 0),
        }
        manifest = {
            **source,
            "status": "success",
            "rollup_rows": 20,
            "rollup_codes": 2,
        }
        self.assertTrue(_manifest_matches_legacy_source(manifest, source))
        self.assertFalse(_manifest_matches_legacy_source({**manifest, "status": "failed"}, source))
        self.assertFalse(_manifest_matches_legacy_source({**manifest, "source_rows": 99}, source))


class RealtimeLifecycleExecutionTests(unittest.TestCase):
    def test_latest_date_runs_first_and_one_interval_failure_does_not_abort_later_work(self):
        dates = [date(2026, 7, 20), date(2026, 7, 22), date(2026, 7, 21)]
        aggregate_calls = []

        def aggregate(target, interval, **_kwargs):
            aggregate_calls.append((target, interval))
            if target == date(2026, 7, 22) and interval == 5:
                raise RuntimeError("bounded batch failed")
            return {"trade_date": target.isoformat(), "interval_minutes": interval, "status": "success"}

        source = {
            "source_rows": 10,
            "source_codes": 1,
            "first_quote_minute": datetime(2026, 7, 22, 9, 15),
            "last_quote_minute": datetime(2026, 7, 22, 15, 0),
        }
        with patch.object(realtime_lifecycle, "acquire_mysql_advisory_lock", side_effect=["lifecycle", "writer"]), patch.object(
            realtime_lifecycle, "release_mysql_advisory_lock"
        ) as release_lock, patch.object(realtime_lifecycle, "_date_rows", return_value=dates), patch.object(
            realtime_lifecycle, "ensure_daily_partition", return_value=False
        ), patch.object(realtime_lifecycle, "_source_revision", side_effect=lambda value: {"source_fingerprint": f"fp-{value}"}), patch.object(
            realtime_lifecycle, "_manifest_rows_for_date", return_value={}
        ), patch.object(realtime_lifecycle, "_source_stats", return_value=source), patch.object(
            realtime_lifecycle, "_source_codes", return_value=["sh.600000"]
        ), patch.object(realtime_lifecycle, "aggregate_trade_date", side_effect=aggregate), patch.object(
            realtime_lifecycle,
            "copy_tracked_trade_date",
            side_effect=lambda value: {"trade_date": value.isoformat(), "rows": 1, "codes": 1},
        ), patch.object(realtime_lifecycle, "apply_retention", return_value={"raw": [], "rollup": [], "tracked": []}), patch.object(
            realtime_lifecycle, "build_lifecycle_plan", return_value={"status": "bounded"}
        ):
            result = realtime_lifecycle.run_lifecycle()

        self.assertEqual(result["processing_order"], ["2026-07-22", "2026-07-21", "2026-07-20"])
        self.assertEqual(aggregate_calls[0], (date(2026, 7, 22), 5))
        self.assertIn((date(2026, 7, 22), 15), aggregate_calls)
        self.assertIn((date(2026, 7, 20), 15), aggregate_calls)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["failures"][0]["stage"], "rollup_5m")
        self.assertEqual(release_lock.call_count, 2)

    def test_matching_fingerprint_skips_rollup_without_rescanning_source(self):
        target = date(2026, 7, 22)
        manifest = {
            "status": "success",
            "source_fingerprint": "same",
            "source_rows": 100,
            "source_codes": 2,
            "rollup_rows": 20,
            "rollup_codes": 2,
        }
        with patch.object(realtime_lifecycle, "acquire_mysql_advisory_lock", side_effect=["lifecycle", "writer"]), patch.object(
            realtime_lifecycle, "release_mysql_advisory_lock"
        ), patch.object(realtime_lifecycle, "_date_rows", return_value=[target]), patch.object(
            realtime_lifecycle, "ensure_daily_partition", return_value=False
        ), patch.object(realtime_lifecycle, "_source_revision", return_value={"source_fingerprint": "same"}), patch.object(
            realtime_lifecycle, "_manifest_rows_for_date", return_value={5: manifest, 15: manifest}
        ), patch.object(realtime_lifecycle, "_source_stats") as source_stats, patch.object(
            realtime_lifecycle, "aggregate_trade_date"
        ) as aggregate, patch.object(realtime_lifecycle, "copy_tracked_trade_date", return_value={"rows": 1}), patch.object(
            realtime_lifecycle, "apply_retention", return_value={}
        ), patch.object(realtime_lifecycle, "build_lifecycle_plan", return_value={}):
            result = realtime_lifecycle.run_lifecycle()

        self.assertEqual([item["status"] for item in result["rollups"]], ["skipped", "skipped"])
        source_stats.assert_not_called()
        aggregate.assert_not_called()


class RealtimeWriterTests(unittest.TestCase):
    def test_realtime_window_preserves_morning_close_through_lunch_recess(self):
        self.assertTrue(is_trading_time(datetime(2026, 7, 22, 11, 30)))
        self.assertFalse(is_trading_time(datetime(2026, 7, 22, 11, 31)))
        self.assertFalse(is_trading_time(datetime(2026, 7, 22, 12, 59)))
        self.assertTrue(is_trading_time(datetime(2026, 7, 22, 13, 0)))

    def test_source_progress_is_quiet_by_default(self):
        class FakeAkshare:
            @staticmethod
            def stock_zh_a_spot():
                print("noisy progress")
                print("noisy warning", file=__import__("sys").stderr)
                return ["ok"]

        stdout = StringIO()
        stderr = StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = fetch_spot_rows(FakeAkshare(), 1, 0, 0)

        self.assertEqual(result, ["ok"])
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "")

    def test_fetch_deadline_can_be_disabled_and_raises_stable_error(self):
        with fetch_deadline(0):
            pass
        with self.assertRaises(RealtimeFetchTimeout):
            with fetch_deadline(0.01):
                import time

                time.sleep(0.1)

    def test_hot_writer_does_not_run_retention_delete(self):
        statements = []

        class Cursor:
            rowcount = 1

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def executemany(self, sql, values):
                statements.append((" ".join(sql.split()), values))
                self.rowcount = len(values)

            def execute(self, sql, params=None):
                statements.append((" ".join(sql.split()), params))

        class Connection:
            def cursor(self):
                return Cursor()

        @contextmanager
        def fake_mysql_conn(*_args, **_kwargs):
            yield Connection()

        row = RealtimeRow(
            code="sh.600000",
            source_code="600000",
            name="浦发银行",
            trade_date="2026-07-16",
            quote_time="2026-07-16 10:00:01",
            quote_minute="2026-07-16 10:00:00",
            latest_price=10.0,
            change_amount=0.1,
            pct_chg=1.0,
            bid_price=9.99,
            ask_price=10.01,
            pre_close=9.9,
            open_price=9.95,
            high_price=10.1,
            low_price=9.9,
            volume=1000,
            amount=10000.0,
            received_at="2026-07-16 10:00:03",
            freshness_seconds=2,
            is_stale=0,
        )
        with patch("scripts.run_realtime_snapshot_update.mysql_conn", fake_mysql_conn), patch(
            "scripts.run_realtime_snapshot_update.ensure_intraday_hot_partition",
            return_value=["p20260716"],
        ) as ensure_partition:
            result = save_rows([row], batch_id="realtime_snapshot_test")

        self.assertEqual(len(statements), 2)
        ensure_partition.assert_called_once_with("2026-07-16")
        self.assertEqual(result["created_partitions"], ["p20260716"])
        self.assertTrue(result["retention_deferred"])
        self.assertFalse(any("DELETE FROM stock_realtime_intraday" in sql for sql, _ in statements))


if __name__ == "__main__":
    unittest.main()
