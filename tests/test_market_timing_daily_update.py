from __future__ import annotations

import io
import sys
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts.run_market_timing_daily_update import (
    MarketTimingSourceTimeout,
    MarketTimingTotalTimeout,
    _fetch_index_closes,
    _remaining_source_timeout,
    _safe_call,
    main,
)


class MarketTimingDeadlineTests(unittest.TestCase):
    def test_source_timeout_is_clamped_to_remaining_total_budget(self):
        with patch("scripts.run_market_timing_daily_update.time.monotonic", return_value=100.0):
            timeout = _remaining_source_timeout(45.0, 112.5)

        self.assertEqual(timeout, 12.5)

    def test_expired_total_budget_is_fatal(self):
        with patch("scripts.run_market_timing_daily_update.time.monotonic", return_value=100.0):
            with self.assertRaises(MarketTimingTotalTimeout):
                _remaining_source_timeout(45.0, 100.0)

    def test_safe_call_turns_a_single_source_timeout_into_degraded_error(self):
        result, error = _safe_call(
            "slow_source",
            lambda: time.sleep(0.05),
            source_timeout_seconds=0.01,
            total_deadline=None,
        )

        self.assertIsNone(result)
        self.assertIn("slow_source exceeded", error or "")

    def test_internal_best_effort_loop_cannot_swallow_hard_timeout(self):
        class FakePro:
            def index_daily(self, **_kwargs):
                raise MarketTimingSourceTimeout("index_closes exceeded hard timeout")

        with self.assertRaises(MarketTimingSourceTimeout):
            _fetch_index_closes(FakePro(), ["000300.SH"], "2026-07-20")


class MarketTimingLockTests(unittest.TestCase):
    def test_lock_contention_skips_without_starting_task(self):
        output = io.StringIO()
        argv = ["run_market_timing_daily_update.py", "--trade-date", "2026-07-20"]

        with (
            patch.object(sys, "argv", argv),
            patch("scripts.run_market_timing_daily_update.acquire_mysql_advisory_lock", return_value=None),
            patch("scripts.run_market_timing_daily_update.TaskRunLogger") as logger_class,
            redirect_stdout(output),
        ):
            main()

        logger_class.assert_not_called()
        self.assertIn('"status": "skipped"', output.getvalue())
        self.assertIn('"reason": "lock_unavailable"', output.getvalue())

    def test_lock_is_released_when_configuration_fails_before_task_start(self):
        handle = object()
        argv = ["run_market_timing_daily_update.py", "--trade-date", "2026-07-20"]

        with (
            patch.object(sys, "argv", argv),
            patch("scripts.run_market_timing_daily_update.acquire_mysql_advisory_lock", return_value=handle),
            patch("scripts.run_market_timing_daily_update.release_mysql_advisory_lock", return_value=None) as release,
            patch("scripts.run_market_timing_daily_update.os.getenv", return_value=None),
        ):
            with self.assertRaisesRegex(RuntimeError, "TUSHARE_TOKEN"):
                main()

        release.assert_called_once_with(handle)


if __name__ == "__main__":
    unittest.main()
