from __future__ import annotations

import unittest
from contextlib import contextmanager
from pathlib import Path

from app.backtest.repository import BacktestRepository


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


class BacktestRepositoryTests(unittest.TestCase):
    def test_create_run_owns_complete_column_contract(self):
        cursor = FakeCursor()
        repository = BacktestRepository(connection_factory(cursor))
        values = {column: f"value-{index}" for index, column in enumerate(repository.RUN_COLUMNS)}

        repository.create_run(values)

        sql, params = cursor.executed[0]
        self.assertIn("INSERT INTO backtest_run", sql)
        self.assertEqual(sql.count("%s"), len(repository.RUN_COLUMNS))
        self.assertEqual(params, tuple(values[column] for column in repository.RUN_COLUMNS))

    def test_create_run_rejects_incomplete_payload(self):
        repository = BacktestRepository(connection_factory(FakeCursor()))

        with self.assertRaisesRegex(ValueError, "missing backtest run fields"):
            repository.create_run({"run_id": "run-1"})

    def test_trade_page_keeps_filters_order_and_horizon_rows(self):
        trade = {
            "trade_date": "2026-04-24",
            "entry_date": "2026-04-27",
            "code": "sh.600000",
        }
        bars = [{"trade_date": "2026-04-27", "close": 10}]
        cursor = FakeCursor(
            fetchone_values=[{"use_adjusted_price": 1}, {"total": 1}],
            fetchall_values=[[trade], bars],
        )
        repository = BacktestRepository(connection_factory(cursor))

        result = repository.load_trade_page(
            run_id="run-1",
            limit=10,
            offset=20,
            trade_date="2026-04-24",
            code="sh.600000",
            return_mode="3d",
        )

        self.assertTrue(result["use_adjusted_price"])
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["horizon_bars"][("2026-04-24", "sh.600000")], bars)
        trades_sql, trades_params = cursor.executed[2]
        self.assertIn("t.return_3d_pct DESC", trades_sql)
        self.assertEqual(
            trades_params,
            ["run-1", "2026-04-24", "sh.600000", 10, 20],
        )
        self.assertEqual(cursor.executed[3][1], ("sh.600000", "2026-04-27"))

    def test_window_start_rejects_dynamic_table_identifier(self):
        repository = BacktestRepository(connection_factory(FakeCursor()))

        with self.assertRaisesRegex(ValueError, "unsupported history table"):
            repository.fetch_window_start_date(
                "daily_kline; DROP TABLE backtest_run; --",
                "2026-07-16",
                90,
            )

    def test_candidate_sql_uses_point_in_time_lifecycle_and_name_intervals(self):
        cursor = FakeCursor(fetchall_values=[[]])
        repository = BacktestRepository(connection_factory(cursor))

        repository.load_feature_candidate_rows("2026-07-16", "stock")

        sql, params = cursor.executed[0]
        self.assertIn("stock_instrument_lifecycle", sql)
        self.assertIn("stock_name_history", sql)
        self.assertIn("f.trade_date < sil.delisting_date", sql)
        self.assertIn("nh.is_delisting_period", sql)
        self.assertIn("stock_fundamental_pit", sql)
        self.assertIn("fp2.announcement_date <= f.trade_date", sql)
        self.assertIn("fp2.period_end_date <= f.trade_date", sql)
        self.assertIn("fp2.period_end_date DESC", sql)
        self.assertIn("f.pe_tushare", sql)
        self.assertIn("pit_fundamental_available", sql)
        self.assertEqual(params, ("2026-07-16", "stock"))

    def test_fallback_candidate_sql_uses_the_same_fundamental_asof_boundary(self):
        cursor = FakeCursor(fetchall_values=[[]])
        repository = BacktestRepository(connection_factory(cursor))

        repository.load_candidate_rows(
            trade_date="2026-07-16",
            instrument_type="stock",
            kline_window_start="2026-04-01",
            factor_window_start="2026-07-01",
        )

        sql, params = cursor.executed[0]
        self.assertIn("stock_fundamental_pit", sql)
        self.assertIn("fp2.announcement_date <= f.trade_date", sql)
        self.assertIn("fp2.period_end_date <= f.trade_date", sql)
        self.assertIn("fp2.announcement_date DESC", sql)
        self.assertEqual(
            params,
            (
                "2026-04-01",
                "2026-07-16",
                "2026-07-01",
                "2026-07-16",
                "2026-07-16",
                "stock",
            ),
        )

    def test_save_results_preserves_three_short_transaction_batches(self):
        cursor = FakeCursor()
        repository = BacktestRepository(connection_factory(cursor))

        repository.save_results(
            run_id="run-1",
            strategy_id="lowvol_reversal",
            picks=[
                {
                    "trade_date": "2026-04-24",
                    "code": "sh.600000",
                    "rank_no": 1,
                    "score": 80,
                    "entry_price": 10,
                    "entry_price_type": "next_open",
                    "factor_json": {"a": 1},
                    "explain_json": {"b": 2},
                }
            ],
            trades=[
                {
                    "run_id": "run-1",
                    "strategy_id": "lowvol_reversal",
                    "trade_date": "2026-04-24",
                    "code": "sh.600000",
                    "entry_date": "2026-04-27",
                    "entry_price": 10,
                }
            ],
            daily=[
                {
                    "run_id": "run-1",
                    "strategy_id": "lowvol_reversal",
                    "trade_date": "2026-04-24",
                    "pick_count": 1,
                }
            ],
        )

        self.assertEqual(len(cursor.executed_many), 3)
        pick_params = cursor.executed_many[0][1][0]
        self.assertEqual(pick_params[0:4], ("run-1", "lowvol_reversal", "2026-04-24", "sh.600000"))
        self.assertEqual(pick_params[-2:], ('{"a": 1}', '{"b": 2}'))


class BacktestPersistenceBoundaryTests(unittest.TestCase):
    def test_route_service_and_validation_do_not_open_mysql_connections(self):
        project_root = Path(__file__).resolve().parents[1]
        relative_paths = [
            "app/api/routes/backtest.py",
            "app/backtest/service.py",
            "app/backtest/validation_baseline.py",
        ]
        for relative_path in relative_paths:
            source = (project_root / relative_path).read_text(encoding="utf-8")
            with self.subTest(path=relative_path):
                self.assertNotIn("mysql_conn", source)
                self.assertNotIn("cursor.execute", source)


if __name__ == "__main__":
    unittest.main()
