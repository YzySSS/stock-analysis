from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

from app.api.routes.portfolio import refresh_portfolio_advice
from app.portfolio.service import PortfolioService
from app.portfolio.worker import run_worker


class FakeJobStates:
    def __init__(self, cancel_results=None):
        self.cancel_results = list(cancel_results or [False])

    @staticmethod
    def owns_running_job(_run_id, _worker_id):
        return True

    def finish_cancelled_if_requested(self, _run_id, _worker_id):
        return self.cancel_results.pop(0) if self.cancel_results else False

    @staticmethod
    def heartbeat(_run_id, _worker_id):
        return True

    @staticmethod
    def is_cancel_requested(_run_id):
        return False


class PortfolioAdviceRunServiceTests(unittest.TestCase):
    def test_refresh_route_no_longer_accepts_background_tasks(self):
        parameters = inspect.signature(refresh_portfolio_advice).parameters
        self.assertNotIn("background_tasks", parameters)

    def test_idempotency_key_is_stable_for_equivalent_snapshot(self):
        first = PortfolioService._advice_idempotency_key(
            12,
            "prompt-v1",
            {"id": 12, "code": "sh.600000", "technical": {"ma5": 10, "ma20": 9}},
        )
        second = PortfolioService._advice_idempotency_key(
            12,
            "prompt-v1",
            {"technical": {"ma20": 9, "ma5": 10}, "code": "sh.600000", "id": 12},
        )
        self.assertEqual(first, second)

    def test_active_key_allows_only_one_active_task_per_position(self):
        self.assertEqual(PortfolioService._active_advice_key(12), PortfolioService._active_advice_key(12))
        self.assertNotEqual(PortfolioService._active_advice_key(12), PortfolioService._active_advice_key(13))

    def test_invalid_snapshot_fails_only_the_claimed_task(self):
        service = PortfolioService(job_states=FakeJobStates())
        service._get_advice_run_row = lambda _run_id: {
            "id": 7,
            "position_id": 12,
            "code": "sh.600000",
            "input_snapshot_json": {"id": 99, "code": "sh.600000"},
        }
        finished = {}
        service._finish_advice_failed = lambda *args: finished.update(
            {"run_id": args[0], "worker_id": args[1], "error_code": args[2], "message": args[3]}
        )

        with self.assertLogs("app.portfolio.service", level="ERROR"):
            service.run_claimed_advice(7, "worker-1")

        self.assertEqual(finished["run_id"], "7")
        self.assertEqual(finished["error_code"], "invalid_request")
        self.assertIn("position does not match", finished["message"])

    def test_cancel_requested_after_ai_boundary_skips_success(self):
        service = PortfolioService(
            job_states=FakeJobStates(cancel_results=[False, True]),
        )
        service._get_advice_run_row = lambda _run_id: {
            "id": 8,
            "position_id": 12,
            "code": "sh.600000",
            "prompt_version": "prompt-v1",
            "input_snapshot_json": {"id": 12, "code": "sh.600000", "local_trade_plan": {}},
        }
        service._mark_advice_execution_stage = lambda *_args: None
        service._generate_ai_review_from_snapshot = lambda _snapshot: (
            {"id": 12, "code": "sh.600000", "decision_level": "hold_watch"},
            "{}",
            "fake-model",
        )
        success = []
        service._finish_advice_success = lambda *_args: success.append(True)

        service.run_claimed_advice(8, "worker-1")

        self.assertEqual(success, [])


class PortfolioAdviceWorkerResilienceTests(unittest.TestCase):
    def test_unhandled_task_failure_does_not_escape_worker_loop(self):
        class FakeRecovery:
            total = 0
            requeued = 0
            failed = 0
            cancelled = 0

        class FakeService:
            @staticmethod
            def recover_stale_advice_runs():
                return FakeRecovery()

            @staticmethod
            def claim_next_advice_run(worker_id=None):
                return "7"

            @staticmethod
            def run_claimed_advice(_run_id, _worker_id):
                raise RuntimeError("unexpected")

        with patch("app.portfolio.worker.PortfolioService", return_value=FakeService()), patch(
            "app.portfolio.worker.WorkerRuntimeHeartbeat"
        ), self.assertLogs(
            "portfolio-advice-worker", level="ERROR"
        ) as logs:
            run_worker(poll_seconds=0, once=True)

        self.assertTrue(any("unhandled failure" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()
