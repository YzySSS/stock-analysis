from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from fastapi import HTTPException

from app.api.routes.selection import (
    SelectionRunRequest,
    SelectionSaveItemRequest,
    _historical_strategy_summary,
    run_selection,
    save_selection_item,
)
from app.strategies.service import StrategyService


class SelectionSaveAsyncEnrichmentTests(unittest.TestCase):
    def test_frozen_strategy_results_expose_the_unified_selection_contract(self):
        item = StrategyService.normalize_selection_contract(
            {
                "code": "sh.600000",
                "score": 72,
                "trade_grade_state": "tradable",
                "trade_grade_reason": "stable deterministic gates passed",
                "factors": {"sector_heat": 80, "fund_flow": 70},
                "sentiment_context": {
                    "stock_news": [{"evidence_id": "news-1"}],
                },
            },
            validation_status="unvalidated",
        )

        for key in (
            "signal_grade",
            "validation_status",
            "score_breakdown",
            "gate_results",
            "evidence_ids",
            "ai_status",
        ):
            self.assertIn(key, item)
        self.assertEqual(item["signal_grade"], "tradable")
        self.assertEqual(item["evidence_ids"], ["news-1"])
        self.assertEqual(item["ai_status"], "not_requested")

    def test_strategy_save_only_persists_and_marks_enrichment_queued(self):
        selector = Mock()
        selector.strategy.config = {"score_threshold": 68}
        selector.save_single_result.return_value = "saved-run"

        service = StrategyService.__new__(StrategyService)
        service.get_strategy_meta = Mock(
            return_value={"id": "a_share_sentiment", "display_name": "sentiment", "version": "0.4.4"}
        )
        service.require_runtime_ready = Mock(return_value={"runtime_ready": True})

        with patch("app.strategies.service.StockSelector", return_value=selector), patch(
            "app.data_ingestion.intraday_bar_sync.get_or_fetch_intraday_bars"
        ) as fetch_bars:
            result = service.save_strategy_result(
                strategy_id="a_share_sentiment",
                run_id="source-run",
                item={"code": "sh.600000", "trade_date": "2026-07-21"},
            )

        fetch_bars.assert_not_called()
        selector.save_single_result.assert_called_once()
        self.assertEqual(result["run_id"], "saved-run")
        self.assertEqual(result["intraday_cache"]["status"], "queued")
        self.assertEqual(result["intraday_cache"]["trade_date"], "2026-07-21")

    def test_route_persists_enrichment_after_save(self):
        service = Mock()
        service.is_registered_strategy.return_value = True
        service.save_strategy_result.return_value = {
            "run_id": "saved-run",
            "code": "sh.600000",
            "strategy_id": "a_share_sentiment",
            "intraday_cache": {"status": "queued", "trade_date": "2026-07-21"},
        }
        payload = SelectionSaveItemRequest(
            run_id="source-run",
            strategy_id="a_share_sentiment",
            item={"code": "sh.600000", "trade_date": "2026-07-21"},
        )

        with patch("app.api.routes.selection.StrategyService", return_value=service), patch(
            "app.api.routes.selection._invalidate_tracking_summary_cache"
        ), patch("app.api.routes.selection.DurableTaskService") as durable_service:
            durable_service.return_value.enqueue_selection_enrichment.return_value = {
                "task_id": "task-enrich-1",
                "status": "queued",
            }
            result = save_selection_item(payload)

        self.assertEqual(result["intraday_cache"]["status"], "queued")
        self.assertEqual(result["intraday_cache"]["job_id"], "task-enrich-1")
        durable_service.return_value.enqueue_selection_enrichment.assert_called_once_with(
            "sh.600000", "2026-07-21"
        )

    def test_saved_result_is_returned_as_degraded_when_queue_insert_fails(self):
        service = Mock()
        service.is_registered_strategy.return_value = True
        service.save_strategy_result.return_value = {
            "run_id": "saved-run",
            "code": "sh.600000",
            "strategy_id": "a_share_sentiment",
            "intraday_cache": {"status": "queued", "trade_date": "2026-07-21"},
        }
        payload = SelectionSaveItemRequest(
            run_id="source-run",
            strategy_id="a_share_sentiment",
            item={"code": "sh.600000", "trade_date": "2026-07-21"},
        )

        with patch("app.api.routes.selection.StrategyService", return_value=service), patch(
            "app.api.routes.selection._invalidate_tracking_summary_cache"
        ), patch("app.api.routes.selection.DurableTaskService") as durable_service:
            durable_service.return_value.enqueue_selection_enrichment.side_effect = RuntimeError("db down")
            result = save_selection_item(payload)

        self.assertEqual(result["run_id"], "saved-run")
        self.assertEqual(result["intraday_cache"]["status"], "degraded")
        self.assertEqual(result["intraday_cache"]["error_code"], "durable_task_enqueue_failed")


class RetiredStrategyContractTests(unittest.TestCase):
    def test_unregistered_strategy_cannot_create_new_run(self):
        strategy_service = Mock()
        strategy_service.is_registered_strategy.return_value = False
        payload = SelectionRunRequest(strategy_id="v12_low_volatility")

        with patch("app.api.routes.selection.StrategyService", return_value=strategy_service), patch(
            "app.api.routes.selection.SelectionRunService"
        ) as run_service:
            with self.assertRaises(HTTPException) as raised:
                run_selection(payload)

        self.assertEqual(raised.exception.status_code, 410)
        self.assertEqual(raised.exception.detail["code"], "STRATEGY_RETIRED")
        self.assertEqual(raised.exception.detail["strategy_id"], "v12_low_volatility")
        run_service.assert_not_called()

    def test_retired_strategy_keeps_a_read_only_historical_description(self):
        detail = _historical_strategy_summary(
            "v12_low_volatility",
            [
                {
                    "strategy_display_name": "低波动 V12",
                    "strategy_version": "12.0.0",
                }
            ],
        )

        self.assertEqual(detail["id"], "v12_low_volatility")
        self.assertEqual(detail["display_name"], "低波动 V12")
        self.assertEqual(detail["status"], "retired")
        self.assertFalse(detail["executable"])
        self.assertFalse(detail["runtime_ready"])

    def test_registered_but_not_ready_strategy_save_maps_to_422(self):
        service = Mock()
        service.is_registered_strategy.return_value = True
        service.save_strategy_result.side_effect = ValueError("策略当前不可运行")
        payload = SelectionSaveItemRequest(
            run_id="source-run",
            strategy_id="temporarily_disabled_strategy",
            item={"code": "sh.600000"},
        )

        with patch("app.api.routes.selection.StrategyService", return_value=service):
            with self.assertRaises(HTTPException) as raised:
                save_selection_item(payload)

        self.assertEqual(raised.exception.status_code, 422)


if __name__ == "__main__":
    unittest.main()
