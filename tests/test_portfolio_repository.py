from __future__ import annotations

import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from app.portfolio.repository import PortfolioRepository
from app.portfolio.service import PortfolioService


class RecordingCursor:
    def __init__(self, executions: list[tuple[str, Any]]) -> None:
        self.executions = executions
        self._rows: list[dict[str, Any]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=None) -> None:
        normalized = " ".join(sql.split())
        self.executions.append((normalized, params))
        if "FROM stock_basic" in normalized:
            self._rows = [
                {"code": "sh.600000", "name": "浦发银行", "instrument_type": "stock"},
                {"code": "sz.000001", "name": "平安银行", "instrument_type": "stock"},
            ]
        elif "FROM stock_realtime_snapshot" in normalized:
            self._rows = [
                {"code": "sh.600000", "latest_price": 10.2},
                {"code": "sz.000001", "latest_price": 11.3},
            ]
        elif "ROW_NUMBER() OVER" in normalized:
            self._rows = [
                {"code": "sh.600000", "trade_date": "2026-07-14", "close": 10.0},
                {"code": "sh.600000", "trade_date": "2026-07-15", "close": 10.2},
                {"code": "sz.000001", "trade_date": "2026-07-15", "close": 11.3},
            ]
        elif "FROM stock_sentiment_daily" in normalized:
            self._rows = [{"code": "sh.600000", "sentiment_score": 62.0}]
        elif "FROM stock_realtime_moneyflow_snapshot" in normalized:
            self._rows = [{"code": "sh.600000", "net_amount": 123.0}]
        elif "FROM stock_chip_daily" in normalized:
            self._rows = [{"code": "sh.600000", "cost_50pct": 9.8}]
        else:
            raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchall(self):
        return list(self._rows)


class RecordingConnection:
    def __init__(self, executions: list[tuple[str, Any]]) -> None:
        self.executions = executions

    def cursor(self):
        return RecordingCursor(self.executions)


class RecordingConnectionFactory:
    def __init__(self) -> None:
        self.executions: list[tuple[str, Any]] = []

    @contextmanager
    def __call__(self, **_kwargs):
        yield RecordingConnection(self.executions)


class StubPortfolioRepository:
    def __init__(self) -> None:
        self.loaded_codes: list[str] | None = None

    @staticmethod
    def list_positions(_include_inactive: bool = False):
        return [
            {
                "id": 1,
                "code": "sh.600000",
                "name": "浦发银行",
                "strategy_id": "short_term",
                "cost_price": 10,
                "quantity": 100,
                "is_active": 1,
            },
            {
                "id": 2,
                "code": "sz.000001",
                "name": "平安银行",
                "strategy_id": "swing",
                "cost_price": 11,
                "quantity": 200,
                "is_active": 1,
            },
        ]

    def load_market_contexts(self, codes: list[str]):
        self.loaded_codes = list(codes)
        return {
            code: {
                "basic": {"code": code, "instrument_type": "stock"},
                "quote": {},
                "history": [],
                "sentiment": {},
                "moneyflow": {},
                "chip": {},
            }
            for code in codes
        }

    @staticmethod
    def latest_advice_runs(_position_ids: list[int]):
        return {}


class PortfolioRepositoryBatchTests(unittest.TestCase):
    def test_market_context_query_count_is_fixed_for_multiple_positions(self):
        factory = RecordingConnectionFactory()
        repository = PortfolioRepository(connection_factory=factory)

        contexts = repository.load_market_contexts(["sh.600000", "sz.000001"])

        self.assertEqual(len(factory.executions), 6)
        self.assertEqual(contexts["sh.600000"]["quote"]["latest_price"], 10.2)
        self.assertEqual(len(contexts["sh.600000"]["history"]), 2)
        self.assertEqual(contexts["sz.000001"]["history"][0]["close"], 11.3)

    def test_market_context_query_count_does_not_grow_with_position_count(self):
        factory = RecordingConnectionFactory()
        repository = PortfolioRepository(connection_factory=factory)

        repository.load_market_contexts(["sh.600000"])

        self.assertEqual(len(factory.executions), 6)

    def test_service_loads_all_position_contexts_in_one_batch(self):
        repository = StubPortfolioRepository()
        service = PortfolioService(repository=repository)

        result = service.list_positions()

        self.assertEqual(repository.loaded_codes, ["sh.600000", "sz.000001"])
        self.assertEqual([item["code"] for item in result["positions"]], ["sh.600000", "sz.000001"])
        self.assertEqual(result["summary"]["count"], 2)

    def test_service_has_no_direct_database_connection(self):
        service_source = Path("app/portfolio/service.py").read_text(encoding="utf-8")
        self.assertNotIn("mysql_conn", service_source)
        self.assertIn("PortfolioRepository", service_source)


if __name__ == "__main__":
    unittest.main()
