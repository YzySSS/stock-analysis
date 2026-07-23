from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

from pymysql.err import ProgrammingError

from app.stock_selection.repository import SelectionRepository
from app.stock_selection.selector import StockSelector


class FakeCursor:
    def __init__(self, *, fetchone_values=None, fetchall_values=None, rowcounts=None, execute_errors=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.rowcounts = list(rowcounts or [])
        self.execute_errors = list(execute_errors or [])
        self.rowcount = 0
        self.executed = []
        self.executed_many = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self.execute_errors:
            error = self.execute_errors.pop(0)
            if error is not None:
                raise error
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
        expected = {"run_id": "run-1", "strategy_id": "test_strategy"}
        cursor = FakeCursor(fetchone_values=[expected])
        repository = SelectionRepository(connection_factory(cursor))

        actual = repository.latest_result_run_meta(
            "stock",
            strategy_id="test_strategy",
        )

        self.assertEqual(actual, expected)
        sql, params = cursor.executed[0]
        self.assertIn("sr2.strategy_id = %s", sql)
        self.assertEqual(params, ["stock", "stock", "test_strategy"])

    def test_candidate_query_owns_cutoff_board_and_limit_sql(self):
        cursor = FakeCursor(fetchall_values=[[{"code": "sh.600000"}]])
        repository = SelectionRepository(connection_factory(cursor))

        rows = repository.load_candidate_rows(
            daily_kline_operator="<=",
            cutoff_date="2026-07-16",
            use_pit_fundamental=True,
            fundamental_date_operator="<=",
            fundamental_as_of_date="2026-07-16",
            use_realtime=False,
            use_current_popularity=False,
            instrument_type="stock",
            market_board="main",
            candidate_limit=10,
        )

        self.assertEqual(rows, [{"code": "sh.600000"}])
        sql, params = cursor.executed[0]
        self.assertEqual(sql.count("trade_date <= %s"), 2)
        self.assertIn("LEFT JOIN stock_technical_feature_daily ma", sql)
        self.assertIn("ma.ma5", sql)
        self.assertIn("ma.ma10", sql)
        self.assertIn("ma.ma30", sql)
        self.assertIn("ma.avg_amount_5", sql)
        self.assertIn("ma.median_amount_20", sql)
        self.assertNotIn("ROW_NUMBER()", sql)
        self.assertNotIn("STDDEV_SAMP", sql)
        self.assertNotIn("LIMIT 90", sql)
        self.assertIn("AVG(trend_score) AS market_index_trend_score", sql)
        self.assertIn("COUNT(DISTINCT index_code) AS market_index_count", sql)
        self.assertIn("'000300.SH', '000905.SH', '000852.SH'", sql)
        self.assertIn("AS csi1000_pct_chg", sql)
        self.assertNotIn("stock_fundamental_pit", sql)
        self.assertIn("LEFT JOIN stock_instrument_lifecycle lifecycle", sql)
        self.assertIn("LEFT JOIN stock_name_history name_state", sql)
        self.assertIn("LEFT JOIN stock_status_snapshot status_state", sql)
        self.assertIn("status_state.id = (", sql)
        self.assertIn("ss2.trade_date <= CURRENT_DATE", sql)
        self.assertNotIn("status_state.trade_date = dk.trade_date", sql)
        self.assertIn("AS is_suspended", sql)
        self.assertIn("ma.kline_count_60 AS listed_trade_days", sql)
        self.assertNotIn("lowvol_reversal_feature_daily", sql)
        self.assertIn("sb.code REGEXP '^sz\\.(000|001|002)'", sql)
        self.assertTrue(sql.rstrip().endswith("LIMIT %s"))
        self.assertEqual(
            params,
            ["2026-07-16", "2026-07-16", 0, 0, 0, "stock", 10],
        )

    def test_candidate_query_never_falls_back_to_retired_lowvol_table(self):
        cursor = FakeCursor(
            execute_errors=[ProgrammingError(1146, "stock_technical_feature_daily does not exist")],
        )
        repository = SelectionRepository(connection_factory(cursor))

        with self.assertRaises(ProgrammingError):
            repository.load_candidate_rows(
                daily_kline_operator="<=",
                cutoff_date="2026-07-16",
                use_pit_fundamental=False,
                fundamental_date_operator="<=",
                fundamental_as_of_date="2026-07-16",
                use_realtime=False,
                use_current_popularity=False,
                instrument_type="stock",
                market_board="all",
                candidate_limit=None,
            )

        self.assertEqual(len(cursor.executed), 1)
        self.assertIn("stock_technical_feature_daily", cursor.executed[0][0])
        self.assertNotIn("lowvol_reversal_feature_daily", cursor.executed[0][0])

    def test_candidate_query_binds_every_volatile_source_to_decision_as_of(self):
        decision = datetime(2026, 7, 21, 10, 15, 30)
        cursor = FakeCursor(fetchall_values=[[]])
        repository = SelectionRepository(connection_factory(cursor))

        repository.load_candidate_rows(
            daily_kline_operator="<",
            cutoff_date="2026-07-21",
            use_pit_fundamental=False,
            fundamental_date_operator="<",
            fundamental_as_of_date="2026-07-21",
            use_realtime=True,
            use_current_popularity=True,
            instrument_type="stock",
            market_board="all",
            candidate_limit=None,
            decision_as_of=decision,
            expected_realtime_batch_ids=["batch-b", "batch-a"],
        )

        sql, params = cursor.executed[0]
        self.assertEqual(sql.count("%s"), len(params))
        self.assertIn("computed_at <= %s", sql)
        self.assertIn("ss2.trade_date <= %s", sql)
        self.assertIn("ss2.created_at <= %s", sql)
        self.assertIn("realtime.trade_date = %s", sql)
        self.assertIn("realtime.quote_time <= %s", sql)
        self.assertIn("realtime.received_at <= %s", sql)
        self.assertIn("realtime.updated_at <= %s", sql)
        self.assertIn("COALESCE(realtime.is_stale, 0) = 0", sql)
        self.assertIn("realtime.batch_id IN (%s,%s)", sql)
        self.assertIn("realtime_mf.quote_time <= %s", sql)
        self.assertIn("pop.quote_time <= %s", sql)
        self.assertIn("pop.updated_at <= %s", sql)
        self.assertEqual(params[0], "2026-07-21")
        self.assertEqual(params[1:4], [date(2026, 7, 21), decision, decision])
        self.assertEqual(params[4:6], ["2026-07-21", decision])
        self.assertEqual(params[6], 1)
        self.assertEqual(params[11:13], ["batch-a", "batch-b"])
        self.assertEqual(params[-1], "stock")

    def test_candidate_query_rejects_dynamic_operator(self):
        repository = SelectionRepository(connection_factory(FakeCursor()))

        with self.assertRaisesRegex(ValueError, "invalid daily kline cutoff"):
            repository.load_candidate_rows(
                daily_kline_operator=">= 0; DROP TABLE daily_kline; --",
                cutoff_date="2026-07-16",
                use_pit_fundamental=True,
                fundamental_date_operator="<=",
                fundamental_as_of_date="2026-07-16",
                use_realtime=False,
                use_current_popularity=False,
                instrument_type="stock",
                market_board="all",
                candidate_limit=None,
            )

    def test_non_sentiment_candidate_query_skips_pit_lookup(self):
        cursor = FakeCursor(fetchall_values=[[]])
        repository = SelectionRepository(connection_factory(cursor))

        repository.load_candidate_rows(
            daily_kline_operator=None,
            cutoff_date=None,
            use_pit_fundamental=False,
            fundamental_date_operator="<=",
            fundamental_as_of_date="2026-07-21",
            use_realtime=False,
            use_current_popularity=False,
            instrument_type="stock",
            market_board="all",
            candidate_limit=None,
        )

        sql, params = cursor.executed[0]
        self.assertNotIn("stock_fundamental_pit", sql)
        self.assertEqual(params, [0, 0, 0, "stock"])

    def test_market_opinion_type_filter_is_applied_before_limit(self):
        cursor = FakeCursor(fetchall_values=[[{"sector_name": "医药"}], []])
        repository = SelectionRepository(connection_factory(cursor))

        sectors, _ = repository.load_market_opinion_rows(
            requested_as_of="2026-07-21 12:30:00",
            latest_candidate_trade_date="2026-07-20",
            allowed_sector_types=["theme"],
            excluded_sector_names=["金融"],
        )

        self.assertEqual(sectors, [{"sector_name": "医药"}])
        sql, params = cursor.executed[0]
        self.assertIn("sector_type IN (%s)", sql)
        self.assertIn("sector_name NOT IN (%s)", sql)
        self.assertLess(sql.index("sector_type IN"), sql.index("LIMIT 30"))
        self.assertEqual(
            params,
            (
                "2026-07-21 12:30:00",
                "2026-07-21 12:30:00",
                "2026-07-21 12:30:00",
                "theme",
                "金融",
            ),
        )
        fund_sql, fund_params = cursor.executed[1]
        self.assertIn("quote_time <= %s", fund_sql)
        self.assertIn("created_at <= %s", fund_sql)
        self.assertIn("updated_at <= %s", fund_sql)
        self.assertEqual(fund_params, ("2026-07-21 12:30:00",) * 9)

    def test_save_result_rows_preserves_both_dedupe_steps(self):
        cursor = FakeCursor()
        repository = SelectionRepository(connection_factory(cursor))
        payload = [
            ("run-1", "2026-07-16", "test_strategy", "2.0.0", "sh.600000", 80, 1, "{}"),
            ("run-1", "2026-07-16", "test_strategy", "2.0.0", "sz.000001", 79, 2, "{}"),
        ]

        repository.save_result_rows(payload=payload, run_id="run-1")

        self.assertEqual(len(cursor.executed_many), 1)
        self.assertEqual(cursor.executed_many[0][1], payload)
        self.assertIn("strategy_version", cursor.executed_many[0][0])
        self.assertEqual(cursor.executed[0][1], ("run-1",))
        self.assertEqual(
            [params for _, params in cursor.executed[1:]],
            [
                ("2026-07-16", "test_strategy", "sh.600000"),
                ("2026-07-16", "test_strategy", "sz.000001"),
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


class SelectorDecisionAsOfTests(unittest.TestCase):
    class CapturingRepository:
        def __init__(self, rows=None):
            self.rows = list(rows or [])
            self.candidate_kwargs = None

        def load_candidate_rows(self, **kwargs):
            self.candidate_kwargs = kwargs
            return list(self.rows)

        def load_market_opinion_rows(self, **_kwargs):
            return [], []

    def test_explicit_decision_as_of_reaches_repository_and_enables_safe_intraday(self):
        decision = datetime(2026, 7, 21, 10, 15, 30)
        repository = self.CapturingRepository()
        selector = StockSelector(
            "a_share_sentiment_v05",
            strategy_overrides={"decision_realtime_batch_ids": ["batch-1"]},
            repository=repository,  # type: ignore[arg-type]
        )

        selector.load_candidates_from_mysql(decision_as_of=decision)

        self.assertEqual(repository.candidate_kwargs["decision_as_of"], decision)
        self.assertTrue(repository.candidate_kwargs["use_realtime"])
        self.assertTrue(repository.candidate_kwargs["use_current_popularity"])
        self.assertEqual(
            repository.candidate_kwargs["expected_realtime_batch_ids"],
            ["batch-1"],
        )
        self.assertEqual(repository.candidate_kwargs["daily_kline_operator"], "<")

    def test_required_data_complete_is_candidate_specific(self):
        decision = datetime(2026, 7, 21, 10, 15, 30)
        base = {
            "code": "sh.600000",
            "name": "test",
            "trade_date": date(2026, 7, 20),
            "daily_data_available": 1,
            "technical_data_available": 1,
            "factor_data_available": 1,
            "daily_moneyflow_data_available": 1,
            "chip_data_available": 1,
            "realtime_data_available": 1,
            "realtime_quote_time": decision,
            "realtime_received_at": decision,
            "realtime_trade_date": decision.date(),
            "realtime_batch_id": "batch-1",
            "realtime_is_stale": 0,
        }
        repository = self.CapturingRepository([base, {**base, "code": "sh.600001", "chip_data_available": 0}])
        selector = StockSelector(
            "a_share_sentiment_v05",
            strategy_overrides={"decision_realtime_batch_ids": ["batch-1"]},
            repository=repository,  # type: ignore[arg-type]
        )

        bundle = selector.load_candidates_from_mysql(decision_as_of=decision)

        self.assertTrue(bundle["candidates"][0]["required_data_complete"])
        self.assertFalse(bundle["candidates"][1]["required_data_complete"])
        self.assertFalse(
            bundle["candidates"][1]["required_data_components"]["chip"]
        )
        self.assertEqual(bundle["candidates"][0]["decision_clock_mode"], "intraday")


if __name__ == "__main__":
    unittest.main()
