from __future__ import annotations

import unittest
from unittest.mock import patch

from app.backtest.service import BacktestRequest, BacktestService
from app.stock_selection.run_tasks import SelectionRunService
from app.strategies.service import StrategyService
from scripts.run_strategy_factor_ci_daily_update import runtime_strategy_ids


def ready_snapshot(*, factor_codes: int = 100, moneyflow_codes: int = 94) -> dict:
    return {
        "stock_count": 100,
        "reference_trade_date": "2026-07-15",
        "datasets": {
            "daily_kline": {"latest_at": "2026-07-15", "covered_codes": 100},
            "factor_input_daily": {"latest_at": "2026-07-14", "covered_codes": factor_codes},
            "stock_moneyflow_daily": {"latest_at": "2026-07-14", "covered_codes": moneyflow_codes},
            "stock_chip_daily": {"latest_at": "2026-07-14", "covered_codes": 100},
            "sector_opinion_daily": {"latest_at": "2026-07-15 15:30:00", "row_count": 10},
        },
    }


class StrategyCapabilityContractTests(unittest.TestCase):
    def test_registry_exposes_four_runtime_and_two_research_backtest_strategies(self):
        items = StrategyService(dataset_snapshot=ready_snapshot()).list_strategies()

        self.assertEqual({item["id"] for item in items if item["runtime_ready"]}, {
            "lowvol_reversal",
            "v13_three_factor",
            "v12_legacy",
            "a_share_sentiment",
        })
        self.assertEqual({item["id"] for item in items if item["backtest_ready"]}, {
            "lowvol_reversal",
            "v13_three_factor",
        })
        self.assertFalse(any(item["validated"] for item in items))

    def test_loadable_prototype_is_not_misreported_as_runtime_ready(self):
        item = StrategyService(dataset_snapshot=ready_snapshot()).get_strategy_capability("quality_lowvol")

        self.assertTrue(item["loadable"])
        self.assertTrue(item["data_ready"])
        self.assertEqual(item["runtime_status"], "prototype")
        self.assertFalse(item["runtime_ready"])
        self.assertTrue(any("prototype" in reason for reason in item["runtime_reasons"]))

    def test_required_dataset_gap_blocks_runtime(self):
        item = StrategyService(
            dataset_snapshot=ready_snapshot(factor_codes=80),
        ).get_strategy_capability("lowvol_reversal")

        self.assertFalse(item["data_ready"])
        self.assertFalse(item["runtime_ready"])
        self.assertTrue(any("factor_input_daily" in reason for reason in item["runtime_reasons"]))


class CapabilityPreflightTests(unittest.TestCase):
    def test_factor_ci_uses_dynamic_runtime_capability_set(self):
        items = [
            {"id": "ready", "runtime_ready": True, "executable": True},
            {"id": "prototype", "runtime_ready": False, "executable": True},
        ]
        with patch(
            "scripts.run_strategy_factor_ci_daily_update.StrategyService"
        ) as service_cls:
            service_cls.return_value.list_strategies.return_value = items
            result = runtime_strategy_ids(instrument_type="stock")

        self.assertEqual(result, ["ready"])
        service_cls.return_value.list_strategies.assert_called_once_with(instrument_type="stock")

    def test_selection_rejects_prototype_before_estimate_or_insert(self):
        class RejectingStrategyService:
            def get_default_strategy_id(self):
                return "quality_lowvol"

            def require_runtime_ready(self, *_args, **_kwargs):
                raise ValueError("策略 quality_lowvol 当前不可运行：实时状态为 prototype")

        service = object.__new__(SelectionRunService)
        service._estimate_seconds = lambda **_kwargs: self.fail("拒绝后不应估算任务时长")
        with patch("app.stock_selection.run_tasks.StrategyService", return_value=RejectingStrategyService()):
            with patch("app.stock_selection.run_tasks.mysql_conn", side_effect=AssertionError("拒绝后不应写数据库")):
                with self.assertRaisesRegex(ValueError, "prototype"):
                    service.submit({"strategy_id": "quality_lowvol", "instrument_type": "stock", "limit": 3})

    def test_backtest_uses_registry_capability_preflight(self):
        class RejectingStrategyService:
            def require_backtest_ready(self, *_args, **_kwargs):
                raise ValueError("策略 quality_lowvol 当前不可回测：回测状态为 disabled")

        request = BacktestRequest(
            strategy_id="quality_lowvol",
            start_date="2026-07-01",
            end_date="2026-07-02",
        )
        with patch("app.backtest.service.StrategyService", return_value=RejectingStrategyService()):
            with self.assertRaisesRegex(ValueError, "disabled"):
                BacktestService()._validate_request(request)


if __name__ == "__main__":
    unittest.main()
