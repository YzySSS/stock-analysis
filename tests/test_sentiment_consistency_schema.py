from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.orchestration import migrate
from app.orchestration.sentiment_consistency_schema import (
    SENTIMENT_CONSISTENCY_DDL,
    ensure_sentiment_consistency_schema,
)


EXPECTED_TABLES = (
    "source_batch_manifest",
    "sentiment_candidate_snapshot_manifest",
    "sentiment_candidate_snapshot",
    "stock_realtime_rank_snapshot",
    "tracking_summary_daily",
    "operational_status_snapshot",
    "ai_advice_snapshot",
)


class FakeCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor


class SentimentConsistencySchemaTests(unittest.TestCase):
    def test_schema_contains_all_requested_tables_and_lineage_guards(self):
        combined = "\n".join(SENTIMENT_CONSISTENCY_DDL)
        self.assertEqual(len(SENTIMENT_CONSISTENCY_DDL), len(EXPECTED_TABLES))
        for table in EXPECTED_TABLES:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", combined)
        self.assertIn("source_event_time_max DATETIME", combined)
        self.assertIn("source_batch_set_hash CHAR(64) NOT NULL", combined)
        self.assertIn("source_lineage_json JSON NOT NULL", combined)
        self.assertIn("UNIQUE KEY uniq_ai_advice_cache_key (cache_key)", combined)

    def test_schema_runner_executes_every_statement_once(self):
        cursor = FakeCursor()

        @contextmanager
        def fake_mysql_conn(*_args, **_kwargs):
            yield FakeConnection(cursor)

        with patch(
            "app.orchestration.sentiment_consistency_schema.mysql_conn",
            fake_mysql_conn,
        ):
            result = ensure_sentiment_consistency_schema()

        self.assertEqual(len(cursor.statements), len(EXPECTED_TABLES))
        self.assertEqual(result, {"status": "ok", "tables": list(EXPECTED_TABLES)})

    def test_migration_is_appended_without_reordering_history(self):
        versions = [item.version for item in migrate.MIGRATIONS]
        self.assertEqual(versions[:22], [f"{value:04d}" for value in range(1, 23)])
        self.assertEqual(versions[22], "0023")
        self.assertIs(migrate.MIGRATIONS[22].runner, ensure_sentiment_consistency_schema)


if __name__ == "__main__":
    unittest.main()
