from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.orchestration.stock_technical_feature_schema import (
    SELECTION_RESULT_INDEX_MIGRATIONS,
    STOCK_TECHNICAL_FEATURE_DDL,
    ensure_stock_technical_feature_schema,
)
from app.stock_selection.technical_feature_daily import TechnicalFeatureDailyRefreshService


class FakeCursor:
    def __init__(self, fetchone_values=None, fetchall_values=None, rowcounts=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.rowcounts = list(rowcounts or [])
        self.rowcount = 0
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self.rowcounts:
            self.rowcount = self.rowcounts.pop(0)

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


class TechnicalFeatureSchemaTests(unittest.TestCase):
    def test_schema_has_neutral_features_and_read_indexes(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS stock_technical_feature_daily", STOCK_TECHNICAL_FEATURE_DDL)
        self.assertIn("median_amount_20", STOCK_TECHNICAL_FEATURE_DDL)
        self.assertIn("return_5d_pct", STOCK_TECHNICAL_FEATURE_DDL)
        self.assertIn("UNIQUE KEY uniq_stock_technical_feature_code_date", STOCK_TECHNICAL_FEATURE_DDL)
        self.assertIn("KEY idx_stock_technical_feature_date_code", STOCK_TECHNICAL_FEATURE_DDL)

    def test_migration_adds_only_missing_selection_result_indexes(self):
        existing_name = "idx_selection_result_strategy_trade_created"
        cursor = FakeCursor(fetchall_values=[[(None, None, existing_name)]])

        with patch(
            "app.orchestration.stock_technical_feature_schema.mysql_conn",
            connection_factory(cursor),
        ):
            result = ensure_stock_technical_feature_schema()

        executed_sql = [sql for sql, _ in cursor.executed]
        self.assertIn("SHOW INDEX FROM selection_result", executed_sql)
        self.assertNotIn(SELECTION_RESULT_INDEX_MIGRATIONS[existing_name], executed_sql)
        missing_name = "idx_selection_result_code_trade_strategy"
        self.assertIn(SELECTION_RESULT_INDEX_MIGRATIONS[missing_name], executed_sql)
        self.assertEqual(result["applied_indexes"], [missing_name])


class TechnicalFeatureRefreshTests(unittest.TestCase):
    def test_explicit_trade_date_builds_and_upserts_one_snapshot(self):
        cursor = FakeCursor(
            fetchone_values=[{"count": 2}],
            fetchall_values=[[{"code": "sh.600000"}, {"code": "sz.000001"}]],
            rowcounts=[0, 2, 0],
        )
        service = TechnicalFeatureDailyRefreshService(connection_factory(cursor))

        result = service.refresh("2026-07-21")

        refresh_sql, refresh_params = cursor.executed[1]
        self.assertIn("INSERT INTO stock_technical_feature_daily", refresh_sql)
        self.assertIn("FROM daily_kline", refresh_sql)
        self.assertIn("ROW_NUMBER() OVER", refresh_sql)
        self.assertIn("AVG(amount) AS median_amount_20", refresh_sql)
        self.assertIn("dk.code IN (%s,%s)", refresh_sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", refresh_sql)
        self.assertEqual(
            refresh_params,
            ("2026-07-21", "2026-07-21", "sh.600000", "sz.000001", "2026-07-21"),
        )
        self.assertEqual(result["published_rows"], 2)
        self.assertEqual(result["batches"], 1)
        self.assertEqual(result["source"], "daily_kline")

    def test_latest_trade_date_is_resolved_without_external_source(self):
        cursor = FakeCursor(
            fetchone_values=[{"trade_date": "2026-07-18"}, {"count": 4800}],
            fetchall_values=[[{"code": "sh.600000"}]],
            rowcounts=[0, 0, 4800, 0],
        )
        service = TechnicalFeatureDailyRefreshService(connection_factory(cursor))

        result = service.refresh()

        self.assertEqual(cursor.executed[0][0], "SELECT MAX(trade_date) AS trade_date FROM daily_kline")
        self.assertEqual(result["trade_date"], "2026-07-18")
        self.assertEqual(result["published_rows"], 4800)

    def test_refresh_splits_active_universe_into_bounded_batches(self):
        cursor = FakeCursor(
            fetchone_values=[{"count": 3}],
            fetchall_values=[[
                {"code": "sh.600000"},
                {"code": "sz.000001"},
                {"code": "sz.000002"},
            ]],
            rowcounts=[0, 2, 1, 0],
        )
        service = TechnicalFeatureDailyRefreshService(
            connection_factory(cursor),
            batch_size=2,
        )

        result = service.refresh("2026-07-21")

        refresh_calls = [item for item in cursor.executed if "INSERT INTO stock_technical_feature_daily" in item[0]]
        self.assertEqual(len(refresh_calls), 2)
        self.assertEqual(result["batches"], 2)
        self.assertEqual(result["batch_size"], 2)

    def test_no_daily_data_is_a_clean_noop(self):
        cursor = FakeCursor(fetchone_values=[{"trade_date": None}])
        service = TechnicalFeatureDailyRefreshService(connection_factory(cursor))

        result = service.refresh()

        self.assertEqual(result["status"], "no_data")
        self.assertEqual(result["published_rows"], 0)
        self.assertEqual(len(cursor.executed), 1)


if __name__ == "__main__":
    unittest.main()
