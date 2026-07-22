from __future__ import annotations

import inspect
import unittest
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

from app.api.routes import dashboard
from app.dashboard.repository import DashboardRepository


class ScriptedCursor:
    def __init__(self, executions: list[tuple[str, Any]]) -> None:
        self.executions = executions
        self._rows: list[dict[str, Any]] = []
        self._row: dict[str, Any] = {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=None) -> None:
        normalized = " ".join(sql.split())
        self.executions.append((normalized, params))
        self._rows = []
        self._row = {}
        if "r.latest_price >= ROUND" in normalized:
            self._rows = [
                {"code": "sh.600000", "name": "浦发银行", "trade_date": "2026-07-16"},
                {"code": "sz.000001", "name": "平安银行", "trade_date": "2026-07-16"},
            ]
        elif "FROM sector_opinion_daily" in normalized:
            self._rows = []
        elif "r.latest_price < ROUND" in normalized:
            self._rows = []
        elif normalized == "SELECT MAX(trade_date) AS latest_trade_date FROM daily_kline":
            self._row = {"latest_trade_date": "2026-07-15"}
        elif "COUNT(*) AS total" in normalized:
            self._row = {"total": 0, "trade_date": "2026-07-16"}
        elif "MAX(quote_time) AS latest_fund_flow_time" in normalized:
            self._row = {"latest_fund_flow_time": None, "fund_flow_rows": 0}
        elif "FROM market_context_daily" in normalized:
            self._row = {"trade_date": "2026-07-15", "market_strength": 50.0}
        elif "FROM market_sector_fund_flow_snapshot" in normalized:
            self._rows = []
        elif "GROUP BY sb.industry" in normalized:
            self._rows = []
        elif "FROM ths_concept_hot_snapshot" in normalized:
            self._rows = []
        elif "INNER JOIN daily_kline dk" in normalized:
            self._rows = [{"code": "sh.600000", "name": "浦发银行"}]
        elif "FROM stock_realtime_intraday FORCE INDEX" in normalized:
            self._rows = [
                {
                    "code": "sh.600000",
                    "trade_date": "2026-07-16",
                    "quote_minute": "2026-07-16 09:31:00",
                    "latest_price": 11.0,
                    "pre_close": 10.0,
                    "name": "浦发银行",
                },
                {
                    "code": "sh.600000",
                    "trade_date": "2026-07-16",
                    "quote_minute": "2026-07-16 10:02:00",
                    "latest_price": 10.8,
                    "pre_close": 10.0,
                    "name": "浦发银行",
                },
            ]
        elif "FROM daily_kline FORCE INDEX (uniq_code_date)" in normalized:
            self._rows = [
                {"code": "sh.600000", "trade_date": "2026-07-15", "close": 9.9},
                {"code": "sh.600000", "trade_date": "2026-07-14", "close": 9.0},
                {"code": "sz.000001", "trade_date": "2026-07-15", "close": 11.0},
            ]
        else:
            raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return dict(self._row)


class ScriptedConnection:
    def __init__(self, executions: list[tuple[str, Any]]) -> None:
        self.executions = executions

    def cursor(self):
        return ScriptedCursor(self.executions)


class ScriptedConnectionFactory:
    def __init__(self) -> None:
        self.executions: list[tuple[str, Any]] = []

    @contextmanager
    def __call__(self, **_kwargs):
        yield ScriptedConnection(self.executions)


class StubDashboardRepository:
    def __init__(self) -> None:
        self.calls: list[int] = []

    def load_emotion_board_inputs(self, limit: int):
        self.calls.append(limit)
        return {
            "limit_rows": [
                {
                    "code": "sh.600000",
                    "name": "浦发银行",
                    "industry": "银行",
                    "latest_price": 11.0,
                    "pct_chg": 10.0,
                    "realtime_amount": 100_000_000,
                }
            ],
            "theme_rows": [],
            "hot_limit_rows": [],
            "latest_kline_date": "2026-07-15",
            "reversal_rows": [],
            "open_board_rows": [
                {
                    "code": "sh.600000",
                    "trade_date": "2026-07-15",
                    "open_board_count": 1,
                    "first_limit_time": "2026-07-15 09:31:00",
                    "last_open_time": "2026-07-15 10:02:00",
                }
            ],
            "history_by_code": {
                "sh.600000": [
                    {"trade_date": "2026-07-14", "close": 9.0},
                    {"trade_date": "2026-07-15", "close": 9.9},
                ]
            },
        }


class DashboardRepositoryTests(unittest.TestCase):
    def test_dashboard_cache_warmer_builds_compact_shared_payload(self):
        with patch.object(
            dashboard,
            "_build_dashboard_summary",
            return_value={"latest_trade_date": "2026-07-21"},
        ) as build, patch.object(dashboard, "_cache_dashboard") as cache:
            result = dashboard.warm_dashboard_compact_cache(8)

        build.assert_called_once_with(8, compact=True)
        cache.assert_called_once_with(8, {"latest_trade_date": "2026-07-21"})
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["cache_key"], "dashboard:summary:v3:compact:8")

    def test_dashboard_cache_warmer_rejects_invalid_limit(self):
        with self.assertRaises(ValueError):
            dashboard.warm_dashboard_compact_cache(21)

    def test_compact_dashboard_uses_stale_while_refresh_cache(self):
        self.assertGreater(
            dashboard._DASHBOARD_CACHE_STORAGE_SECONDS,
            dashboard._DASHBOARD_CACHE_FRESH_SECONDS,
        )
        source = inspect.getsource(dashboard.dashboard_summary)
        self.assertIn("_start_dashboard_refresh", source)
        self.assertIn("with lock", source)

    def test_emotion_inputs_use_fixed_seven_queries_with_batch_history(self):
        factory = ScriptedConnectionFactory()
        repository = DashboardRepository(connection_factory=factory)

        result = repository.load_emotion_board_inputs(limit=10)

        self.assertEqual(len(factory.executions), 7)
        self.assertEqual(len(result["limit_rows"]), 2)
        self.assertEqual(len(result["reversal_rows"]), 1)
        self.assertEqual(len(result["history_by_code"]["sh.600000"]), 2)
        self.assertEqual(result["open_board_rows"][0]["open_board_count"], 1)
        self.assertNotIn("LAG(is_sealed)", factory.executions[-2][0])
        self.assertIn("ORDER BY code, quote_minute", factory.executions[-2][0])
        self.assertIn("FORCE INDEX (idx_realtime_intraday_code_time)", factory.executions[-2][0])
        self.assertIn("quote_minute >= %s", factory.executions[-2][0])
        self.assertEqual(
            factory.executions[-2][1][-3:],
            ["2026-07-16", "2026-07-16", "2026-07-16"],
        )
        self.assertNotIn("ROW_NUMBER() OVER", factory.executions[-1][0])
        self.assertIn("FORCE INDEX (uniq_code_date)", factory.executions[-1][0])
        self.assertEqual(result["open_board_rows"][0]["first_limit_time"], "2026-07-16 09:31:00")
        self.assertEqual(result["open_board_rows"][0]["last_open_time"], "2026-07-16 10:02:00")

    def test_emotion_board_calls_repository_once_without_per_stock_sql(self):
        repository = StubDashboardRepository()

        with patch.object(dashboard, "_DASHBOARD_REPOSITORY", repository):
            payload = dashboard._dashboard_emotion_board(limit=10)

        self.assertEqual(repository.calls, [10])
        self.assertEqual(len(payload["limit_up_pool"]), 1)
        self.assertEqual(payload["limit_up_pool"][0]["code"], "sh.600000")
        self.assertEqual(payload["limit_up_pool"][0]["open_board_count"], 1)
        self.assertEqual(payload["limit_up_pool"][0]["open_board_label"], "开板1次")
        source = inspect.getsource(dashboard._dashboard_emotion_board)
        self.assertNotIn("cursor", source)
        self.assertNotIn("mysql_conn", source)

    def test_market_overview_and_hot_themes_have_fixed_repository_queries(self):
        overview_factory = ScriptedConnectionFactory()
        overview_repository = DashboardRepository(connection_factory=overview_factory)

        overview = overview_repository.load_market_overview_inputs()

        self.assertEqual(len(overview_factory.executions), 7)
        self.assertEqual(overview["previous_strength"]["market_strength"], 50.0)

        themes_factory = ScriptedConnectionFactory()
        themes_repository = DashboardRepository(connection_factory=themes_factory)

        themes = themes_repository.load_hot_theme_inputs()

        self.assertEqual(len(themes_factory.executions), 3)
        self.assertEqual(themes, {"opinion_rows": [], "fund_rows": [], "ths_rows": []})

    def test_dashboard_route_has_no_direct_sql_connection(self):
        source = inspect.getsource(dashboard)
        self.assertNotIn("mysql_conn", source)
        self.assertNotIn("SELECT ", source)


if __name__ == "__main__":
    unittest.main()
