from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.orchestration import redundant_index_schema
from app.orchestration.forward_observation_schema import FORWARD_OBSERVATION_DDL
from app.orchestration.fundamental_pit_schema import FUNDAMENTAL_PIT_DDL
from app.orchestration.index_constituent_pit_schema import INDEX_CONSTITUENT_PIT_DDL


def _index_rows(
    index_name: str,
    columns: tuple[str, ...],
    *,
    non_unique: bool,
    visible: bool = True,
) -> list[dict]:
    return [
        {
            "index_name": index_name,
            "non_unique": int(non_unique),
            "seq_in_index": position,
            "column_name": column,
            "sub_part": None,
            "collation": "A",
            "index_type": "BTREE",
            "is_visible": "YES" if visible else "NO",
        }
        for position, column in enumerate(columns, start=1)
    ]


class _FakeCursor:
    def __init__(self, rows_by_table: dict[str, list[dict]]):
        self.rows_by_table = rows_by_table
        self.current_rows: list[dict] = []
        self.alter_statements: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql: str, params=None):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT"):
            self.current_rows = list(self.rows_by_table.get(str(params[0]), []))
            return
        if normalized.startswith("ALTER TABLE"):
            self.alter_statements.append(normalized)
            return
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchall(self):
        return list(self.current_rows)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _all_exact_rows() -> dict[str, list[dict]]:
    rows: dict[str, list[dict]] = {}
    definitions = {
        "factor_input_daily": ("code", "trade_date"),
        "index_constituent_pit": ("index_code", "effective_date", "code"),
        "stock_fundamental_pit": (
            "code",
            "period_end_date",
            "announcement_date",
            "update_flag",
        ),
        "strategy_forward_observation": ("protocol_id", "signal_trade_date"),
    }
    for candidate in redundant_index_schema.EXACT_DUPLICATE_INDEXES:
        columns = definitions[candidate.table_name]
        rows[candidate.table_name] = [
            *_index_rows(candidate.redundant_index, columns, non_unique=True),
            *_index_rows(candidate.replacement_index, columns, non_unique=False),
        ]
    return rows


class RedundantIndexSchemaTests(unittest.TestCase):
    def _run_with_rows(self, rows_by_table: dict[str, list[dict]]):
        cursor = _FakeCursor(rows_by_table)

        @contextmanager
        def fake_connection(*args, **kwargs):
            yield _FakeConnection(cursor)

        with patch.object(
            redundant_index_schema,
            "mysql_maintenance_conn",
            fake_connection,
        ):
            result = redundant_index_schema.drop_exact_duplicate_indexes()
        return result, cursor

    def test_drops_only_exact_nonunique_duplicates(self):
        result, cursor = self._run_with_rows(_all_exact_rows())

        self.assertEqual(result["candidate_count"], 4)
        self.assertEqual(len(result["dropped"]), 4)
        self.assertEqual(result["already_absent"], [])
        self.assertEqual(len(cursor.alter_statements), 4)
        for candidate, statement in zip(
            redundant_index_schema.EXACT_DUPLICATE_INDEXES,
            cursor.alter_statements,
        ):
            self.assertIn(f"`{candidate.table_name}`", statement)
            self.assertIn(f"`{candidate.redundant_index}`", statement)
            self.assertIn("ALGORITHM=INPLACE, LOCK=NONE", statement)

    def test_is_idempotent_when_redundant_indexes_are_absent(self):
        rows = _all_exact_rows()
        redundant_names = {
            candidate.redundant_index
            for candidate in redundant_index_schema.EXACT_DUPLICATE_INDEXES
        }
        for table_name in rows:
            rows[table_name] = [
                row for row in rows[table_name] if row["index_name"] not in redundant_names
            ]

        result, cursor = self._run_with_rows(rows)

        self.assertEqual(result["dropped"], [])
        self.assertEqual(len(result["already_absent"]), 4)
        self.assertEqual(cursor.alter_statements, [])

    def test_fails_closed_when_replacement_columns_differ(self):
        rows = _all_exact_rows()
        candidate = redundant_index_schema.EXACT_DUPLICATE_INDEXES[0]
        rows[candidate.table_name] = [
            *_index_rows(
                candidate.redundant_index,
                ("code", "trade_date"),
                non_unique=True,
            ),
            *_index_rows(
                candidate.replacement_index,
                ("trade_date", "code"),
                non_unique=False,
            ),
        ]

        with self.assertRaisesRegex(RuntimeError, "ordered index columns differ"):
            self._run_with_rows(rows)

    def test_fails_closed_when_replacement_is_missing_or_not_unique(self):
        candidate = redundant_index_schema.EXACT_DUPLICATE_INDEXES[0]
        rows = _all_exact_rows()
        rows[candidate.table_name] = [
            row
            for row in rows[candidate.table_name]
            if row["index_name"] != candidate.replacement_index
        ]
        with self.assertRaisesRegex(RuntimeError, "replacement index .* is missing"):
            self._run_with_rows(rows)

        rows = _all_exact_rows()
        for row in rows[candidate.table_name]:
            if row["index_name"] == candidate.replacement_index:
                row["non_unique"] = 1
        with self.assertRaisesRegex(RuntimeError, "replacement index .* is not unique"):
            self._run_with_rows(rows)

    def test_fresh_schema_ddl_does_not_recreate_redundant_indexes(self):
        ddl = "\n".join(
            (
                *FUNDAMENTAL_PIT_DDL,
                *INDEX_CONSTITUENT_PIT_DDL,
                *FORWARD_OBSERVATION_DDL,
            )
        )
        self.assertNotIn("idx_fundamental_pit_asof", ddl)
        self.assertNotIn("idx_index_constituent_pit_asof", ddl)
        self.assertNotIn("idx_strategy_forward_observation_protocol", ddl)


if __name__ == "__main__":
    unittest.main()
