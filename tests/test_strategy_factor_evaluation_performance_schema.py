from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.orchestration import strategy_factor_evaluation_performance_schema
from app.orchestration.strategy_factor_evaluation_v2_schema import DDL


def _index_rows(columns: tuple[str, ...]) -> list[dict]:
    return [
        {
            "index_name": (
                strategy_factor_evaluation_performance_schema.INDEX_NAME
            ),
            "non_unique": 1,
            "seq_in_index": position,
            "column_name": column,
            "sub_part": None,
            "index_type": "BTREE",
            "is_visible": "YES",
        }
        for position, column in enumerate(columns, start=1)
    ]


class _FakeCursor:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql: str, params=None):
        self.executed.append((" ".join(sql.split()), params))

    def fetchall(self):
        return list(self.rows)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _connection_factory(cursor: _FakeCursor):
    @contextmanager
    def connect(*args, **kwargs):
        yield _FakeConnection(cursor)

    return connect


class StrategyFactorEvaluationPerformanceSchemaTests(unittest.TestCase):
    def _run(self, rows: list[dict]):
        cursor = _FakeCursor(rows)
        with patch.object(
            strategy_factor_evaluation_performance_schema,
            "mysql_maintenance_conn",
            _connection_factory(cursor),
        ):
            result = (
                strategy_factor_evaluation_performance_schema
                .ensure_strategy_factor_evaluation_performance_index()
            )
        return result, cursor

    def test_adds_missing_index_online(self):
        result, cursor = self._run([])

        self.assertTrue(result["created"])
        alter_statements = [
            sql for sql, _ in cursor.executed if sql.startswith("ALTER TABLE")
        ]
        self.assertEqual(len(alter_statements), 1)
        self.assertIn("manifest_id", alter_statements[0])
        self.assertIn("in_eligible_pool", alter_statements[0])
        self.assertIn("code", alter_statements[0])
        self.assertIn("ALGORITHM=INPLACE", alter_statements[0])
        self.assertIn("LOCK=NONE", alter_statements[0])

    def test_accepts_exact_existing_index_without_ddl(self):
        result, cursor = self._run(
            _index_rows(
                strategy_factor_evaluation_performance_schema.INDEX_COLUMNS
            )
        )

        self.assertFalse(result["created"])
        self.assertFalse(
            any(sql.startswith("ALTER TABLE") for sql, _ in cursor.executed)
        )

    def test_fails_closed_for_wrong_existing_columns(self):
        with self.assertRaisesRegex(RuntimeError, "columns are"):
            self._run(_index_rows(("manifest_id", "code")))

    def test_fresh_schema_contains_manifest_scope_index(self):
        ddl = "\n".join(DDL)
        self.assertIn(
            "KEY idx_factor_snapshot_manifest_scope",
            ddl,
        )
        self.assertIn(
            "manifest_id, in_eligible_pool, code",
            " ".join(ddl.split()),
        )


if __name__ == "__main__":
    unittest.main()
