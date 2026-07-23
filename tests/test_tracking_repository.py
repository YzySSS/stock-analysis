from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from app.api.routes import tracking as tracking_route
from app.error_learning.tracker import SelectionResultTracker
from app.tracking.repository import TrackingRepository


class RecordingCursor:
    def __init__(self, executions: list[tuple[str, Any]]) -> None:
        self.executions = executions
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=None) -> None:
        self.executions.append((" ".join(sql.split()), params))

    @staticmethod
    def fetchall():
        return []

    @staticmethod
    def fetchone():
        return {"count": 0}


class RecordingConnection:
    def __init__(self, executions: list[tuple[str, Any]]) -> None:
        self.executions = executions

    def cursor(self):
        return RecordingCursor(self.executions)


class RecordingConnectionFactory:
    def __init__(self) -> None:
        self.executions: list[tuple[str, Any]] = []

    @contextmanager
    def __call__(self, **_kwargs):
        yield RecordingConnection(self.executions)


class FakeTracker:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.retention_calls: list[dict[str, Any]] = []

    def enforce_stats_retention(self, **kwargs):
        self.retention_calls.append(dict(kwargs))
        return 0

    def build_latest_selection_snapshot(self, **kwargs):
        self.calls.append(dict(kwargs))
        if kwargs.get("include_in_stats_only"):
            return [
                {
                    "code": "sh.600000",
                    "name": "浦发银行",
                    "strategy_id": "strategy-a",
                    "strategy_display_name": "策略A",
                    "selection_date": "2026-07-15",
                    "include_in_stats": True,
                    "price_change_pct": 2.0,
                    "max_gain_pct": 3.0,
                    "max_drawdown_pct": -1.0,
                    "review_status": "tracking",
                }
            ]
        return [
            {
                "code": "sh.600000",
                "name": "浦发银行",
                "strategy_id": "strategy-a",
                "strategy_display_name": "策略A",
                "selection_date": "2026-07-15",
                "include_in_stats": True,
                "price_change_pct": 2.0,
                "max_gain_pct": 3.0,
                "max_drawdown_pct": -1.0,
                "review_status": "tracking",
            }
        ]

    @staticmethod
    def to_dict_list(records):
        return records


class TrackingRepositoryTests(unittest.TestCase):
    def test_compact_tracking_record_keeps_strategy_version(self):
        item = {
            "code": "sh.600000",
            "name": "浦发银行",
            "strategy_id": "a_share_sentiment",
            "strategy_version": "0.3.1",
            "strategy_display_name": "A股舆情",
        }

        compact = tracking_route._compact_tracking_item(item)

        self.assertEqual(compact["strategy_version"], "0.3.1")

    def test_cross_version_strategy_summary_keeps_one_aggregate_and_lineage(self):
        items = [
            {
                "code": "sh.600000",
                "strategy_id": "a_share_sentiment",
                "strategy_version": "0.3.1",
                "strategy_display_name": "A股舆情",
                "selection_date": "2026-07-21",
            },
            {
                "code": "sz.000001",
                "strategy_id": "a_share_sentiment",
                "strategy_version": "0.4.4",
                "strategy_display_name": "A股舆情",
                "selection_date": "2026-07-22",
            },
        ]

        summaries = tracking_route._build_strategy_summaries(items)

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["count"], 2)
        self.assertEqual(summaries[0]["strategy_versions"], ["0.3.1", "0.4.4"])

    def test_expired_stats_update_uses_strict_fourteen_day_cutoff(self):
        factory = RecordingConnectionFactory()
        repository = TrackingRepository(connection_factory=factory)
        as_of = datetime(2026, 7, 20, 12, 0, 0)

        changed = repository.exclude_expired_from_stats(
            instrument_type="stock",
            max_age_days=14,
            as_of_datetime=as_of,
        )

        self.assertEqual(changed, 0)
        self.assertEqual(len(factory.executions), 1)
        sql, params = factory.executions[0]
        self.assertIn("SET sr.include_in_stats = 0", sql)
        self.assertIn("sr.created_at < %s", sql)
        self.assertEqual(params, (datetime(2026, 7, 6, 12, 0, 0), "stock"))

    def test_stats_window_expires_only_after_full_fourteen_days(self):
        selected_at = datetime(2026, 7, 6, 12, 0, 0)

        at_boundary = SelectionResultTracker._stats_window_state(
            selected_at,
            as_of_datetime=selected_at + timedelta(days=14),
        )
        after_boundary = SelectionResultTracker._stats_window_state(
            selected_at,
            as_of_datetime=selected_at + timedelta(days=14, seconds=1),
        )

        self.assertFalse(at_boundary[0])
        self.assertTrue(after_boundary[0])
        self.assertIn("14 个自然日", after_boundary[2])

    def test_enrichment_query_scopes_expensive_joins_to_target_page(self):
        factory = RecordingConnectionFactory()
        repository = TrackingRepository(connection_factory=factory)

        rows = repository.list_selection_result_rows(
            limit=10,
            offset=20,
            instrument_type="stock",
            latest_only=False,
        )

        self.assertEqual(rows, [])
        self.assertEqual(len(factory.executions), 1)
        sql, params = factory.executions[0]
        self.assertIn("WITH target_selection AS", sql)
        self.assertIn("FROM target_selection target", sql)
        self.assertIn("LIMIT %s OFFSET %s", sql)
        self.assertIn("DAILY_KLINE DK FORCE INDEX (UNIQ_CODE_DATE)", sql.upper())
        self.assertIn("DK.TRADE_DATE > SR_INNER.TRADE_DATE", sql.upper())
        self.assertIn("STOCK_REALTIME_INTRADAY_TRACKED", sql.upper())
        self.assertIn("RI.QUOTE_MINUTE >= TIMESTAMP(DATE_ADD(SR_INNER.TRADE_DATE, INTERVAL 1 DAY))", sql.upper())
        self.assertNotIn("INNER JOIN STOCK_REALTIME_INTRADAY RI", sql.upper())
        self.assertEqual(params[-2:], [10, 20])

    def test_count_latest_scope_uses_one_query_without_recursive_run_lookup(self):
        factory = RecordingConnectionFactory()
        repository = TrackingRepository(connection_factory=factory)

        count = repository.count_items(
            instrument_type="stock",
            strategy_id="strategy-a",
            latest_only=True,
        )

        self.assertEqual(count, 0)
        self.assertEqual(len(factory.executions), 1)
        self.assertIn("SELECT MAX(sr2.trade_date)", factory.executions[0][0])

    def test_tracking_payload_pages_first_and_summarizes_only_statistical_rows(self):
        fake_tracker = FakeTracker()
        tracking_route._invalidate_tracking_summary_cache()
        with patch.object(tracking_route, "SelectionResultTracker", return_value=fake_tracker), patch.object(
            tracking_route,
            "_count_tracking_items",
            return_value=176,
        ):
            first = tracking_route._tracking_payload(
                limit=10,
                offset=20,
                include_runs=False,
            )
            second = tracking_route._tracking_payload(
                limit=10,
                offset=20,
                include_runs=False,
            )

        self.assertEqual(first["pagination"]["total"], 176)
        self.assertEqual(first["filtered_summary"]["total_count"], 176)
        self.assertEqual(first["filtered_summary"]["excluded_count"], 175)
        self.assertEqual(first["stats_retention"]["max_age_days"], 14)
        self.assertEqual(fake_tracker.retention_calls, [])
        self.assertEqual(fake_tracker.calls[0]["limit"], 10)
        self.assertEqual(fake_tracker.calls[0]["offset"], 20)
        self.assertFalse(fake_tracker.calls[0].get("include_in_stats_only", False))
        self.assertEqual(fake_tracker.calls[1]["limit"], 176)
        self.assertTrue(fake_tracker.calls[1]["include_in_stats_only"])
        self.assertEqual(fake_tracker.calls[2]["limit"], 10)
        self.assertEqual(len(fake_tracker.calls), 3)
        self.assertEqual(second["filtered_summary"], first["filtered_summary"])

    def test_expired_tracking_item_cannot_be_reincluded(self):
        request = tracking_route.TrackingStatsToggleRequest(include_in_stats=True)
        with patch.object(
            tracking_route._TRACKING_REPOSITORY,
            "is_stats_window_expired",
            return_value=True,
        ), patch.object(tracking_route, "_set_tracking_include_in_stats") as update_mock:
            with self.assertRaises(tracking_route.HTTPException) as context:
                tracking_route.update_tracking_item_stats(
                    request,
                    code="sh.600000",
                    selection_date="2026-07-01",
                    strategy_id="strategy-a",
                    instrument_type="stock",
                )

        self.assertEqual(context.exception.status_code, 409)
        update_mock.assert_not_called()

    def test_route_and_tracker_have_no_direct_sql_connection(self):
        for path in [
            Path("app/api/routes/tracking.py"),
            Path("app/error_learning/tracker.py"),
        ]:
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("mysql_conn", source)
            self.assertNotIn("SELECT ", source)


if __name__ == "__main__":
    unittest.main()
