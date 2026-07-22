from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from scripts.run_kline_daily_update import (
    completed_market_date_cutoff,
    main,
    resolve_target_trade_date,
)


class KlineDailyUpdateTests(unittest.TestCase):
    def test_before_daily_ready_time_uses_previous_calendar_date_as_cutoff(self):
        self.assertEqual(
            completed_market_date_cutoff(datetime(2026, 7, 22, 2, 0)),
            "2026-07-21",
        )

    def test_after_daily_ready_time_allows_current_calendar_date(self):
        self.assertEqual(
            completed_market_date_cutoff(datetime(2026, 7, 22, 18, 1)),
            "2026-07-22",
        )

    def test_resolver_asks_tushare_for_latest_open_completed_date(self):
        sync = Mock()
        sync.latest_open_trade_date.return_value = "2026-07-21"

        result = resolve_target_trade_date(
            sync,
            now=datetime(2026, 7, 22, 2, 0),
        )

        self.assertEqual(result, "2026-07-21")
        sync.latest_open_trade_date.assert_called_once_with(end_date="2026-07-21")

    def test_zero_row_completed_date_is_logged_as_failure(self):
        sync = Mock()
        sync.latest_open_trade_date.return_value = "2026-07-21"
        sync.run.return_value = {"rows_synced": 0, "source": "tushare_daily"}
        logger = Mock()

        with patch("scripts.run_kline_daily_update.DailyKlineSync", return_value=sync), patch(
            "scripts.run_kline_daily_update.TaskRunLogger", return_value=logger
        ), self.assertRaisesRegex(RuntimeError, "returned zero rows"):
            main(["--trade-date", "2026-07-21"])

        self.assertEqual(logger.finish.call_args.kwargs["status"], "failed")
        self.assertEqual(logger.finish.call_args.kwargs["metadata"]["rows_synced"], 0)


if __name__ == "__main__":
    unittest.main()
