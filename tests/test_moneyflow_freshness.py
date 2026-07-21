from __future__ import annotations

import inspect
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.api.routes import stocks, system
from app.dashboard.repository import DashboardRepository
from app.portfolio.repository import PortfolioRepository
from app.stock_selection.repository import SelectionRepository
from scripts import run_market_fund_flow_update, run_realtime_moneyflow_update


class MoneyflowFreshnessTests(unittest.TestCase):
    def test_realtime_moneyflow_payload_keeps_intraday_scope_and_yuan_units(self):
        payload = stocks._realtime_moneyflow_payload(
            {
                "trade_date": "2026-07-21",
                "quote_time": "2026-07-21 11:30:01",
                "net_amount": -50,
                "amount": 1000,
                "inflow_amount": 475,
                "outflow_amount": 525,
                "source": "akshare_ths_stock_fund_flow_individual",
                "source_unit": "元",
            }
        )

        self.assertEqual(payload["data_scope"], "intraday_realtime")
        self.assertEqual(payload["label"], "实时净流出")
        self.assertEqual(payload["net_flow_intensity_pct"], -5.0)
        self.assertEqual(payload["source_unit"], "元")

    def test_stock_detail_exposes_daily_and_fresh_realtime_moneyflow_separately(self):
        source = inspect.getsource(stocks.stock_detail)

        self.assertIn('"moneyflow": moneyflow', source)
        self.assertIn('"realtime_moneyflow": realtime_moneyflow', source)
        self.assertIn("trade_date = (SELECT MAX(trade_date) FROM stock_realtime_moneyflow_snapshot)", source)
        self.assertIn("INTERVAL 20 MINUTE", source)

    def test_all_realtime_moneyflow_consumers_filter_fresh_snapshots(self):
        consumers = [
            DashboardRepository.load_emotion_board_inputs,
            PortfolioRepository.load_market_contexts,
            SelectionRepository.load_candidate_rows,
        ]
        for consumer in consumers:
            with self.subTest(consumer=consumer.__qualname__):
                source = inspect.getsource(consumer)
                self.assertIn("stock_realtime_moneyflow_snapshot", source)
                self.assertIn("MAX(quote_time) FROM stock_realtime_moneyflow_snapshot", source)
                self.assertIn("INTERVAL 20 MINUTE", source)

    def test_dashboard_and_system_filter_stale_sector_snapshots(self):
        overview_source = inspect.getsource(DashboardRepository.load_market_overview_inputs)
        baseline_source = inspect.getsource(system._data_baseline_summary)

        self.assertIn("MAX(trade_date) FROM market_sector_fund_flow_snapshot", overview_source)
        self.assertGreaterEqual(overview_source.count("INTERVAL 20 MINUTE"), 3)
        self.assertIn('"label": "个股资金流（日频）"', baseline_source)
        self.assertIn('"label": "个股资金流（实时）"', baseline_source)
        self.assertIn('"label": "板块资金流（实时）"', baseline_source)
        self.assertGreaterEqual(baseline_source.count("INTERVAL 20 MINUTE"), 2)

    def test_snapshot_writers_remove_only_cross_trade_date_snapshot_residue(self):
        realtime_source = inspect.getsource(run_realtime_moneyflow_update.save_rows)
        sector_source = inspect.getsource(run_market_fund_flow_update.save_rows)

        self.assertIn("DELETE FROM stock_realtime_moneyflow_snapshot WHERE trade_date < %s", realtime_source)
        self.assertIn("DELETE FROM market_sector_fund_flow_snapshot WHERE trade_date < %s", sector_source)
        self.assertNotIn("DELETE FROM stock_realtime_moneyflow_intraday WHERE trade_date =", realtime_source)
        self.assertNotIn("DELETE FROM market_sector_fund_flow_intraday WHERE trade_date =", sector_source)

    def test_realtime_source_failure_is_not_logged_as_empty_success(self):
        broken_akshare = SimpleNamespace(
            stock_fund_flow_individual=lambda **_kwargs: (_ for _ in ()).throw(ConnectionError("source down"))
        )

        with patch.dict(sys.modules, {"akshare": broken_akshare}):
            with self.assertRaisesRegex(RuntimeError, "source unavailable"):
                run_realtime_moneyflow_update.fetch_rows(datetime(2026, 7, 21, 10, 0, 0))

    def test_stock_detail_frontend_renders_both_moneyflow_scopes(self):
        script = Path("app/api/web/js/stock-detail.js").read_text(encoding="utf-8")
        page = Path("app/api/web/pages/stock-detail.html").read_text(encoding="utf-8")

        self.assertIn("renderMoneyflowPanel(data.moneyflow, data.realtime_moneyflow)", script)
        self.assertIn("今日实时状态", script)
        self.assertIn("最近完整交易日", script)
        self.assertIn("资金流（日频 / 实时）", page)


if __name__ == "__main__":
    unittest.main()
