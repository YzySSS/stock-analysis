from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.orchestration.selection_result_schema import (
    LEGACY_INVALIDATION_REASONS,
    STATS_REINCLUDED_REASON,
    ensure_selection_result_version_schema,
)


class FakeCursor:
    def __init__(self, columns):
        self.columns = columns
        self.executed = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.executed.append((normalized, params))
        if "SET strategy_version = COALESCE" in normalized:
            self.rowcount = 189
        elif "SET include_in_stats = 1" in normalized:
            self.rowcount = 21
        else:
            self.rowcount = 0

    def fetchall(self):
        return self.columns


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class SelectionResultSchemaTests(unittest.TestCase):
    def test_migration_adds_lineage_backfills_and_restores_recoverable_window(self):
        cursor = FakeCursor(
            [
                ("id", "bigint", "NO", None),
                ("strategy_id", "varchar(64)", "NO", None),
            ]
        )

        @contextmanager
        def fake_mysql_conn(*_args, **_kwargs):
            yield FakeConnection(cursor)

        with patch(
            "app.orchestration.selection_result_schema.mysql_conn",
            fake_mysql_conn,
        ):
            result = ensure_selection_result_version_schema()

        statements = [sql for sql, _ in cursor.executed]
        self.assertTrue(any("ADD COLUMN strategy_version" in sql for sql in statements))
        self.assertTrue(any("MODIFY COLUMN strategy_version VARCHAR(32) NOT NULL" in sql for sql in statements))
        restore_sql, restore_params = next(
            (sql, params)
            for sql, params in cursor.executed
            if "SET include_in_stats = 1" in sql
        )
        self.assertIn("DATE_SUB(NOW(), INTERVAL 14 DAY)", restore_sql)
        self.assertIn("legacy_include_in_stats_before_invalidation", restore_sql)
        self.assertEqual(
            restore_params,
            (STATS_REINCLUDED_REASON, *LEGACY_INVALIDATION_REASONS),
        )
        self.assertEqual(result["strategy_versions_backfilled"], 189)
        self.assertEqual(result["stats_reincluded"], 21)

    def test_current_non_nullable_column_does_not_repeat_ddl(self):
        cursor = FakeCursor(
            [
                ("id", "bigint", "NO", None),
                ("strategy_version", "varchar(32)", "NO", None),
            ]
        )

        @contextmanager
        def fake_mysql_conn(*_args, **_kwargs):
            yield FakeConnection(cursor)

        with patch(
            "app.orchestration.selection_result_schema.mysql_conn",
            fake_mysql_conn,
        ):
            result = ensure_selection_result_version_schema()

        statements = [sql for sql, _ in cursor.executed]
        self.assertFalse(any("ADD COLUMN strategy_version" in sql for sql in statements))
        self.assertFalse(any("MODIFY COLUMN strategy_version" in sql for sql in statements))
        self.assertEqual(result["applied_columns"], [])


if __name__ == "__main__":
    unittest.main()
