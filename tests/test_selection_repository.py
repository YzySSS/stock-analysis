from __future__ import annotations

import unittest
from contextlib import contextmanager
from pathlib import Path

from app.stock_selection.repository import SelectionRepository


class FakeCursor:
    def __init__(self, *, fetchone_values=None, fetchall_values=None, rowcounts=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.rowcounts = list(rowcounts or [])
        self.rowcount = 0
        self.executed = []
        self.executed_many = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self.rowcounts:
            self.rowcount = self.rowcounts.pop(0)

    def executemany(self, sql, params):
        self.executed_many.append((sql, params))

    def fetchone(self):
        return self.fetchone_values.pop(0) if self.fetchone_values else None

    def fetchall(self):
        return self.fetchall_values.pop(0) if self.fetchall_values else []


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def connection_factory(cursor):
    @contextmanager
    def _connect(*_args, **_kwargs):
        yield FakeConnection(cursor)

    return _connect


class SelectionRepositoryTests(unittest.TestCase):
    def test_latest_result_meta_keeps_strategy_and_instrument_filters(self):
        expected = {"run_id": "run-1", "strategy_id": "lowvol_reversal"}
        cursor = FakeCursor(fetchone_values=[expected])
        repository = SelectionRepository(connection_factory(cursor))

        actual = repository.latest_result_run_meta(
            "stock",
            strategy_id="lowvol_reversal",
        )

        self.assertEqual(actual, expected)
        sql, params = cursor.executed[0]
        self.assertIn("sr2.strategy_id = %s", sql)
        self.assertEqual(params, ["stock", "stock", "lowvol_reversal"])

    def test_candidate_query_owns_cutoff_board_and_limit_sql(self):
        cursor = FakeCursor(fetchall_values=[[{"code": "sh.600000"}]])
        repository = SelectionRepository(connection_factory(cursor))

        rows = repository.load_candidate_rows(
            daily_kline_operator="<=",
            cutoff_date="2026-07-16",
            use_realtime=False,
            use_current_popularity=False,
            instrument_type="stock",
            market_board="main",
            candidate_limit=10,
        )

        self.assertEqual(rows, [{"code": "sh.600000"}])
        sql, params = cursor.executed[0]
        self.assertEqual(sql.count("trade_date <= %s"), 3)
        self.assertIn("AS ma5", sql)
        self.assertIn("AS ma10", sql)
        self.assertIn("AS ma30", sql)
        self.assertIn("AS avg_amount_5", sql)
        self.assertIn("WHERE rn <= 30", sql)
        self.assertIn("AVG(trend_score) AS market_index_trend_score", sql)
        self.assertIn("COUNT(DISTINCT index_code) AS market_index_count", sql)
        self.assertIn("'000300.SH', '000905.SH', '000852.SH'", sql)
        self.assertIn("AS csi1000_pct_chg", sql)
        self.assertIn("sb.code REGEXP '^sz\\.(000|001|002)'", sql)
        self.assertTrue(sql.rstrip().endswith("LIMIT %s"))
        self.assertEqual(
            params,
            ["2026-07-16", "2026-07-16", "2026-07-16", 0, 0, 0, "stock", 10],
        )

    def test_candidate_query_rejects_dynamic_operator(self):
        repository = SelectionRepository(connection_factory(FakeCursor()))

        with self.assertRaisesRegex(ValueError, "invalid daily kline cutoff"):
            repository.load_candidate_rows(
                daily_kline_operator=">= 0; DROP TABLE daily_kline; --",
                cutoff_date="2026-07-16",
                use_realtime=False,
                use_current_popularity=False,
                instrument_type="stock",
                market_board="all",
                candidate_limit=None,
            )

    def test_save_result_rows_preserves_both_dedupe_steps(self):
        cursor = FakeCursor()
        repository = SelectionRepository(connection_factory(cursor))
        payload = [
            ("run-1", "2026-07-16", "lowvol_reversal", "sh.600000", 80, 1, "{}"),
            ("run-1", "2026-07-16", "lowvol_reversal", "sz.000001", 79, 2, "{}"),
        ]

        repository.save_result_rows(payload=payload, run_id="run-1")

        self.assertEqual(len(cursor.executed_many), 1)
        self.assertEqual(cursor.executed_many[0][1], payload)
        self.assertEqual(cursor.executed[0][1], ("run-1",))
        self.assertEqual(
            [params for _, params in cursor.executed[1:]],
            [
                ("2026-07-16", "lowvol_reversal", "sh.600000"),
                ("2026-07-16", "lowvol_reversal", "sz.000001"),
            ],
        )


class SelectionPersistenceBoundaryTests(unittest.TestCase):
    def test_routes_and_domain_services_do_not_open_mysql_connections(self):
        project_root = Path(__file__).resolve().parents[1]
        relative_paths = [
            "app/api/routes/selection.py",
            "app/stock_selection/run_tasks.py",
            "app/stock_selection/selector.py",
        ]
        for relative_path in relative_paths:
            source = (project_root / relative_path).read_text(encoding="utf-8")
            with self.subTest(path=relative_path):
                self.assertNotIn("mysql_conn", source)
                self.assertNotIn("cursor.execute", source)


if __name__ == "__main__":
    unittest.main()
