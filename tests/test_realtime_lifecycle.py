from __future__ import annotations

import unittest
from io import StringIO
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import date, datetime
from unittest.mock import patch

from app.data_ingestion.realtime_lifecycle import (
    RealtimeLifecyclePolicy,
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
        with patch("scripts.run_realtime_snapshot_update.mysql_conn", fake_mysql_conn):
            result = save_rows([row], batch_id="realtime_snapshot_test")

        self.assertEqual(len(statements), 2)
        self.assertTrue(result["retention_deferred"])
        self.assertFalse(any("DELETE FROM stock_realtime_intraday" in sql for sql, _ in statements))


if __name__ == "__main__":
    unittest.main()
