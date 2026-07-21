from __future__ import annotations

import unittest
from unittest.mock import patch

from app.backtest.service import BacktestService
from app.backtest.worker import run_worker


class BacktestWorkerResilienceTests(unittest.TestCase):
    def test_request_deserialization_failure_marks_run_failed(self):
        service = BacktestService()
        service.get_run = lambda _run_id: {
            "request_json": {
                "strategy_id": "test_strategy",
                "start_date": "2026-04-24",
                "end_date": "2026-04-27",
                "unknown_future_field": True,
            }
        }
        finished = {}
        service._finish_run = lambda *args: finished.update(
            {"run_id": args[0], "status": args[1], "error": args[-2], "error_code": args[-1]}
        )

        service.run_background("run-with-new-schema")

        self.assertEqual(finished["status"], "failed")
        self.assertEqual(finished["error_code"], "invalid_request")
        self.assertIn("unknown_future_field", finished["error"])

    def test_unhandled_run_failure_does_not_escape_worker_loop(self):
        class FakeService:
            @staticmethod
            def recover_stale_running_runs():
                return 0

            @staticmethod
            def claim_next_queued_run(worker_id=None):
                return "run"

            @staticmethod
            def run_background(_run_id):
                raise RuntimeError("unexpected")

        with patch("app.backtest.worker.BacktestService", return_value=FakeService()), patch(
            "app.backtest.worker.WorkerRuntimeHeartbeat"
        ), self.assertLogs(
            "backtest-worker", level="ERROR"
        ) as logs:
            run_worker(poll_seconds=0, once=True)

        self.assertTrue(any("unhandled failure" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()
