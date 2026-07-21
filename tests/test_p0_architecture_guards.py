from __future__ import annotations

import unittest
from inspect import signature
from unittest.mock import patch

from fastapi import HTTPException

from app.api.routes.backtest import BacktestRunRequest, normalize_run_row, run_backtest
from app.api.routes.selection import SelectionRunRequest, get_selection_results, run_selection
from app.error_learning.tracker import SelectionResultTracker
from app.shared.instrument_policy import (
    SUPPORTED_SELECTION_INSTRUMENT_TYPES,
    UnsupportedInstrumentError,
    require_supported_instrument,
)
from app.strategies.service import StrategyService


class EmptySelectionTracker(SelectionResultTracker):
    def _fetch_from_selection_result(self, **_kwargs):
        return []

    def _fetch_from_stock_snapshot(self, *_args, **_kwargs):
        raise AssertionError("市场快照不得再作为选股结果兜底")


class InstrumentPolicyTests(unittest.TestCase):
    def test_stock_is_supported_and_normalized(self):
        result = require_supported_instrument(
            " STOCK ",
            operation="selection",
            supported=SUPPORTED_SELECTION_INSTRUMENT_TYPES,
        )
        self.assertEqual(result, "stock")

    def test_unsupported_instrument_has_stable_error_contract(self):
        with self.assertRaises(UnsupportedInstrumentError) as raised:
            require_supported_instrument(
                "etf",
                operation="selection",
                supported=SUPPORTED_SELECTION_INSTRUMENT_TYPES,
            )

        self.assertEqual(raised.exception.as_detail()["code"], "unsupported_instrument")
        self.assertEqual(raised.exception.as_detail()["instrument_type"], "etf")


class SelectionGuardTests(unittest.TestCase):
    def test_selection_run_rejects_etf_before_task_creation(self):
        payload = SelectionRunRequest(
            strategy_id="test_strategy",
            instrument_type="etf",
        )

        with self.assertRaises(HTTPException) as raised:
            run_selection(payload)

        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(raised.exception.detail["code"], "unsupported_instrument")

    def test_selection_route_no_longer_accepts_background_tasks(self):
        self.assertNotIn("background_tasks", signature(run_selection).parameters)

    def test_selection_sync_execution_is_disabled_before_task_creation(self):
        payload = SelectionRunRequest(
            strategy_id="test_strategy",
            instrument_type="stock",
            async_run=False,
        )

        with self.assertRaises(HTTPException) as raised:
            run_selection(payload)

        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(raised.exception.detail["code"], "synchronous_selection_disabled")

    def test_selection_results_reject_index_before_database_query(self):
        with self.assertRaises(HTTPException) as raised:
            get_selection_results(
                run_id=None,
                strategy_id=None,
                limit=3,
                instrument_type="index",
            )

        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(raised.exception.detail["operation"], "selection_results")

    def test_strategy_service_rejects_etf_before_loading_strategy(self):
        service = StrategyService()
        service.get_default_strategy_id = lambda: self.fail("不应加载策略注册信息")

        with self.assertRaises(UnsupportedInstrumentError):
            service.run_strategy(instrument_type="etf")

    def test_empty_persisted_results_do_not_fall_back_to_market_snapshot(self):
        records = EmptySelectionTracker().build_latest_selection_snapshot(
            limit=3,
            instrument_type="stock",
        )
        self.assertEqual(records, [])


class BacktestGuardTests(unittest.TestCase):
    def test_synchronous_backtest_route_is_disabled_before_service_creation(self):
        from fastapi import HTTPException

        from app.api.routes.backtest import BacktestRunRequest, run_backtest

        payload = BacktestRunRequest(
            strategy_id="test_strategy",
            start_date="2026-04-24",
            end_date="2026-04-27",
            save=False,
        )
        with patch("app.api.routes.backtest.BacktestService") as service_cls, self.assertRaises(
            HTTPException
        ) as raised:
            run_backtest(payload)

        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(raised.exception.detail["code"], "synchronous_backtest_disabled")
        service_cls.assert_not_called()

    def test_backtest_rejects_etf_before_data_query(self):
        payload = BacktestRunRequest(
            strategy_id="test_strategy",
            start_date="2026-04-24",
            end_date="2026-04-27",
            instrument_type="etf",
        )

        with self.assertRaises(HTTPException) as raised:
            run_backtest(payload)

        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(raised.exception.detail["operation"], "backtest")

    def test_backtest_run_rows_always_expose_research_disclosure(self):
        normalized = normalize_run_row(
            {
                "run_id": "test-run",
                "strategy_id": "test_strategy",
                "strategy_version": "v2.1",
                "status": "success",
                "request_json": {"instrument_type": "stock"},
            }
        )

        self.assertTrue(normalized["research_only"])
        self.assertEqual(normalized["validation_status"], "validation_pending")
        self.assertEqual(normalized["methodology_version"], "legacy_pre_point_in_time_v1")
        self.assertIn("不可作为交易证据", normalized["risk_notice"])


if __name__ == "__main__":
    unittest.main()
