from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.orchestration.selection_trade_plan_v4_schema import (
    IMMUTABLE_INDUSTRY_BACKFILL_SQL,
    SELECTION_TRADE_PLAN_EVENT_DDL,
    SENTIMENT_CANDIDATE_COLUMN_MIGRATIONS,
    ensure_selection_trade_plan_v4_schema,
)


class RecordingCursor:
    def __init__(self, columns: set[str]) -> None:
        self.columns = columns
        self.execute_calls: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, _params=None):
        self.execute_calls.append(sql)

    def fetchall(self):
        return [{"Field": column} for column in sorted(self.columns)]


class RecordingConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class RecordingFactory:
    def __init__(self, cursor: RecordingCursor) -> None:
        self.cursor = cursor

    @contextmanager
    def __call__(self, **_kwargs):
        yield RecordingConnection(self.cursor)


class SelectionTradePlanV4SchemaTests(unittest.TestCase):
    def test_schema_adds_event_table_and_missing_industry_column(self):
        cursor = RecordingCursor({"id", "snapshot_id", "code", "name"})
        factory = RecordingFactory(cursor)

        with patch(
            "app.orchestration.selection_trade_plan_v4_schema.mysql_conn",
            factory,
        ):
            result = ensure_selection_trade_plan_v4_schema()

        self.assertIn("selection_trade_plan_event", result["applied"])
        self.assertIn(
            "sentiment_candidate_snapshot.industry",
            result["applied"],
        )
        self.assertIn(SELECTION_TRADE_PLAN_EVENT_DDL, cursor.execute_calls)
        self.assertIn(
            SENTIMENT_CANDIDATE_COLUMN_MIGRATIONS["industry"],
            cursor.execute_calls,
        )
        self.assertIn(
            IMMUTABLE_INDUSTRY_BACKFILL_SQL,
            cursor.execute_calls,
        )

    def test_existing_industry_column_is_idempotent(self):
        cursor = RecordingCursor(
            {"id", "snapshot_id", "code", "name", "industry"}
        )
        factory = RecordingFactory(cursor)

        with patch(
            "app.orchestration.selection_trade_plan_v4_schema.mysql_conn",
            factory,
        ):
            result = ensure_selection_trade_plan_v4_schema()

        self.assertEqual(result["applied"], ["selection_trade_plan_event"])
        self.assertNotIn(
            SENTIMENT_CANDIDATE_COLUMN_MIGRATIONS["industry"],
            cursor.execute_calls,
        )
        self.assertIn(
            IMMUTABLE_INDUSTRY_BACKFILL_SQL,
            cursor.execute_calls,
        )


if __name__ == "__main__":
    unittest.main()
