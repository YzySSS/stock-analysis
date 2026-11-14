from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.orchestration import tracking_read_performance_schema as schema


class TrackingReadPerformanceSchemaTests(unittest.TestCase):
    def _run(self, rows, *, fail_ddl=False):
        executed = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, sql, params=None):
                executed.append(" ".join(sql.split()))
                if fail_ddl and sql.lstrip().startswith("ALTER"):
                    raise RuntimeError("online DDL unsupported")

            def fetchall(self):
                return rows

        @contextmanager
        def connection(**kwargs):
            class Connection:
                def cursor(self):
                    return Cursor()
            yield Connection()

        with patch.object(schema, "mysql_maintenance_conn", connection):
            if fail_ddl:
                with self.assertRaisesRegex(RuntimeError, "online DDL unsupported"):
                    schema.ensure_tracking_minute_covering_index()
                return None, executed
            return schema.ensure_tracking_minute_covering_index(), executed

    def _rows(self):
        return [
            dict(non_unique=1, seq_in_index=i, column_name=column,
                 sub_part=None, index_type="BTREE", is_visible="YES")
            for i, column in enumerate(schema.INDEX_COLUMNS, 1)
        ]

    def test_missing_index_requires_online_nonblocking_ddl_and_short_metadata_wait(self):
        result, executed = self._run([])
        self.assertTrue(result["created"])
        self.assertIn("SET SESSION lock_wait_timeout = 5", executed)
        alter = [sql for sql in executed if sql.startswith("ALTER")]
        self.assertEqual(len(alter), 1)
        self.assertIn("ALGORITHM=INPLACE", alter[0])
        self.assertIn("LOCK=NONE", alter[0])
        self.assertNotIn("DROP", " ".join(executed))

    def test_existing_exact_index_is_idempotent(self):
        result, executed = self._run(self._rows())
        self.assertFalse(result["created"])
        self.assertFalse(any(sql.startswith("ALTER") for sql in executed))

    def test_existing_incompatible_index_fails_closed(self):
        for key, value in [("column_name", "trade_date"), ("non_unique", 0),
                           ("sub_part", 8), ("index_type", "HASH"),
                           ("is_visible", "NO")]:
            with self.subTest(key=key):
                rows = self._rows()
                rows[0][key] = value
                with self.assertRaisesRegex(RuntimeError, "incompatible"):
                    self._run(rows)

    def test_online_ddl_failure_is_not_retried_with_blocking_fallback(self):
        _, executed = self._run([], fail_ddl=True)
        self.assertEqual(sum(sql.startswith("ALTER") for sql in executed), 1)


if __name__ == "__main__":
    unittest.main()
