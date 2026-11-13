from __future__ import annotations

import re
import threading
import unittest
from unittest.mock import MagicMock, patch

from app.api import strategy_display as display
from app.strategies.capability import StrategyCapabilityService


class StrategyDisplayTests(unittest.TestCase):
    def test_concurrent_page_misses_share_one_live_snapshot(self):
        entered, release = threading.Event(), threading.Event()
        snapshot, results, errors = {"stock_count": 100}, [], []
        def query():
            entered.set()
            if not release.wait(2):
                raise AssertionError("query release timed out")
            return snapshot
        def read():
            try:
                results.append(display.get_display_dataset_snapshot())
            except BaseException as exc:
                errors.append(exc)
        with patch.object(display, "_DATASET_CACHE", None), patch.object(display, "_query_display_dataset_snapshot", side_effect=query) as live_query:
            first, second = threading.Thread(target=read), threading.Thread(target=read)
            first.start()
            self.assertTrue(entered.wait(2))
            second.start()
            release.set()
            first.join(2)
            second.join(2)
            self.assertFalse(first.is_alive() or second.is_alive())
            self.assertFalse(errors)
            self.assertEqual(results, [snapshot, snapshot])
            live_query.assert_called_once()

    def test_failed_live_read_never_reuses_expired_page_readiness(self):
        with patch.object(display, "_DATASET_CACHE", (0, {"stock_count": 1})), patch.object(display.time, "monotonic", return_value=1000), patch.object(display, "_query_display_dataset_snapshot", side_effect=RuntimeError("unavailable")):
            with self.assertRaises(RuntimeError):
                display.get_display_dataset_snapshot()
            self.assertEqual(display._DATASET_CACHE[0], 0)

    def test_display_query_matches_runtime_inputs_with_only_access_path_hints(self):
        def fixture():
            context, conn, cursor = MagicMock(), MagicMock(), MagicMock()
            context.__enter__.return_value = conn
            conn.cursor.return_value.__enter__.return_value = cursor
            cursor.fetchone.side_effect = [
                {"stock_count": 100, "reference_trade_date": "2026-11-12",
                 "factor_input_date": "2026-11-12", "moneyflow_date": "2026-11-12",
                 "chip_date": "2026-11-12", "sector_opinion_at": "2026-11-13 09:25:00",
                 "snapshot_now": "2026-11-13 09:35:00"},
                {"count": 99}, {"count": 100}, {"count": 90},
                {"count": 90}, {"count": 98}, {"count": 12},
            ]
            cursor.fetchall.return_value = [{"strategy_id": "a_share_sentiment_v06", "strategy_version": "0.6.0", "snapshot_id": "real-id"}]
            return context, cursor
        original_context, original_cursor = fixture()
        display_context, display_cursor = fixture()
        with patch("app.strategies.capability.mysql_conn", return_value=original_context):
            original = StrategyCapabilityService._query_dataset_snapshot()
        with patch.object(display, "mysql_read_conn", return_value=display_context):
            projected = display._query_display_dataset_snapshot()
        self.assertEqual(projected, original)
        calls = display_cursor.execute.call_args_list
        self.assertEqual(len(calls), len(original_cursor.execute.call_args_list))
        def normalize(sql):
            return " ".join(re.sub(r" FORCE INDEX \([a-z_]+\)", "", sql).replace("STRAIGHT_JOIN", "INNER JOIN").split())
        for current, prior in zip(calls, original_cursor.execute.call_args_list):
            self.assertEqual(normalize(current.args[0]), normalize(prior.args[0]))
            self.assertEqual(current.args[1:], prior.args[1:])
        self.assertEqual(sum("FORCE INDEX" in call.args[0] for call in calls), 5)
