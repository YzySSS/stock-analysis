from __future__ import annotations

import json
import unittest
from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import patch

from app.data_ingestion.market_opinion_lifecycle import (
    MarketOpinionLifecyclePolicy,
    normalize_retained_snapshots,
    retention_snapshot_ids,
)
from app.data_ingestion.market_opinion_repository import (
    normalized_payload_values,
    resolve_snapshot_news_direction,
    resolve_snapshot_news_event_type,
)


class MarketOpinionRetentionTests(unittest.TestCase):
    def test_recent_trade_days_keep_intraday_and_older_days_keep_only_latest_snapshot(self):
        rows = []
        row_id = 1
        for day in (date(2026, 7, 10), date(2026, 7, 13), date(2026, 7, 14)):
            for hour in (10, 15):
                rows.append(
                    {
                        "id": row_id,
                        "trade_date": day,
                        "as_of_datetime": datetime(day.year, day.month, day.day, hour, 0),
                    }
                )
                row_id += 1

        keep, prune, retained_dates = retention_snapshot_ids(
            rows,
            MarketOpinionLifecyclePolicy(intraday_trade_days=2, daily_trade_days=3),
        )

        self.assertEqual(retained_dates, [date(2026, 7, 14), date(2026, 7, 13), date(2026, 7, 10)])
        self.assertEqual(set(keep), {2, 3, 4, 5, 6})
        self.assertEqual(prune, [1])

    def test_policy_rejects_daily_retention_shorter_than_intraday(self):
        with self.assertRaises(ValueError):
            MarketOpinionLifecyclePolicy(intraday_trade_days=5, daily_trade_days=4).validate()


class MarketOpinionNormalizationTests(unittest.TestCase):
    def test_mixed_batch_updates_only_rows_still_requiring_normalization(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, sql, params=None):
                self.executed.append((sql, params))

            def fetchall(self):
                return [(2, None, None, None, 1)]

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor

            def cursor(self):
                return self._cursor

        cursor = FakeCursor()

        @contextmanager
        def fake_mysql_conn(*_args, **_kwargs):
            yield FakeConnection(cursor)

        with (
            patch("app.data_ingestion.market_opinion_lifecycle.mysql_conn", fake_mysql_conn),
            patch("app.data_ingestion.market_opinion_lifecycle.delete_snapshot_payloads"),
            patch("app.data_ingestion.market_opinion_lifecycle.insert_normalized_payload_values"),
            patch(
                "app.data_ingestion.market_opinion_lifecycle.normalized_payload_values",
                return_value=([], [], []),
            ),
        ):
            result = normalize_retained_snapshots([1, 2], batch_size=2)

        update_sql, update_params = cursor.executed[-1]
        self.assertIn("UPDATE sector_opinion_daily", update_sql)
        self.assertEqual(update_sql.count("%s"), 1)
        self.assertEqual(update_params, [2])
        self.assertEqual(result["snapshots"], 1)

    def test_news_payloads_are_replaced_by_raw_references(self):
        news = {
            "raw_id": 88,
            "title": "示例新闻",
            "source_id": "source-a",
            "impact_score": 72.5,
            "signed_score": 72.5,
            "timeliness_score": 90,
        }
        summary = {
            "top_stocks": [
                {
                    "code": "sh.600000",
                    "name": "浦发银行",
                    "score": 80,
                    "news_count": 1,
                    "matched_news": [news],
                    "custom_factor": 12.3,
                }
            ],
            "top_news": [news],
            "sources": ["source-a"],
        }

        stocks, news_refs, sources = normalized_payload_values(7, summary)

        self.assertEqual(len(stocks), 1)
        self.assertEqual(len(news_refs), 2)
        self.assertEqual({row[1] for row in news_refs}, {"stock", "sector"})
        self.assertTrue(all(row[4] == 88 for row in news_refs))
        self.assertEqual(sources, [(7, 1, "source-a")])
        stock_extra = json.loads(stocks[0][-1])
        self.assertEqual(stock_extra, {"custom_factor": 12.3})
        self.assertNotIn("matched_news", stock_extra)

    def test_local_direction_is_persisted_in_normalized_fallback(self):
        news = {
            "raw_id": 88,
            "title": "指数冲高回落，但示例股涨停",
            "direction": "positive",
            "article_direction": "negative",
            "event_type": "market_attention",
            "article_event_type": "earnings",
            "signed_score": 60,
        }
        summary = {"top_stocks": [{"code": "sh.600000", "matched_news": [news]}]}

        _, news_refs, _ = normalized_payload_values(7, summary)

        fallback = json.loads(news_refs[0][-1])
        self.assertEqual(fallback["direction"], "positive")
        self.assertEqual(fallback["article_direction"], "negative")
        self.assertEqual(fallback["event_type"], "market_attention")
        self.assertEqual(fallback["article_event_type"], "earnings")

    def test_old_snapshot_direction_is_recovered_from_signed_score(self):
        self.assertEqual(resolve_snapshot_news_direction({}, "negative", 53.0), "positive")
        self.assertEqual(resolve_snapshot_news_direction({}, "positive", -53.0), "negative")
        self.assertEqual(resolve_snapshot_news_direction({}, "positive", 0.0), "positive")

    def test_snapshot_event_type_prefers_local_value(self):
        self.assertEqual(
            resolve_snapshot_news_event_type({"event_type": "market_attention"}, "earnings"),
            "market_attention",
        )
        self.assertEqual(resolve_snapshot_news_event_type({}, "earnings"), "earnings")


if __name__ == "__main__":
    unittest.main()
