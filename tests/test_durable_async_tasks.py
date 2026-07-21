from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from app.api.routes.stocks import refresh_stock_intraday_bars
from app.jobs.durable_tasks import (
    INTRADAY_REFRESH_JOB,
    TRACKING_DEEP_REVIEW_JOB,
    DurableTaskService,
    _run_intraday_isolated,
)
from app.jobs.durable_worker import run_worker
from app.jobs.mysql_state import StaleRecoveryResult
from app.orchestration.durable_task_schema import DURABLE_TASK_DDL
from app.tracking.deep_review import DeepReviewJobService


class FakeJobStates:
    def __init__(self) -> None:
        self.heartbeats: list[tuple[str, str]] = []

    def owns_running_job(self, task_id: str, worker_id: str) -> bool:
        return True

    def heartbeat(self, task_id: str, worker_id: str) -> bool:
        self.heartbeats.append((task_id, worker_id))
        return True

    def recover_stale(self, stale_seconds: int) -> StaleRecoveryResult:
        return StaleRecoveryResult(requeued=1)


class FakeTaskRepository:
    def __init__(self, task: dict) -> None:
        self.task = task
        self.succeeded: list[tuple] = []
        self.failed: list[tuple] = []
        self.reconciled = 0

    def get_claimed(self, task_id: str, worker_id: str):
        return dict(self.task)

    def finish_success(self, task_id: str, worker_id: str, result: dict) -> bool:
        self.succeeded.append((task_id, worker_id, result))
        return True

    def finish_failed(self, task_id: str, worker_id: str, error_code: str, error_message: str) -> bool:
        self.failed.append((task_id, worker_id, error_code, error_message))
        return True

    def reconcile_tracking_review_states(self) -> None:
        self.reconciled += 1


class NullTaskLogger:
    def start(self, *args, **kwargs) -> None:
        return None

    def finish(self, *args, **kwargs) -> None:
        return None


class DurableTaskSchemaTests(unittest.TestCase):
    def test_schema_satisfies_shared_mysql_job_lifecycle(self):
        normalized = " ".join(DURABLE_TASK_DDL.split())
        for column in (
            "task_id",
            "job_type",
            "payload_json",
            "active_idempotency_key",
            "worker_heartbeat_at",
            "attempt_count",
            "max_attempts",
            "cancel_requested",
            "phase",
            "progress_pct",
        ):
            self.assertIn(column, normalized)
        self.assertIn("UNIQUE KEY uniq_durable_task_active_idempotency", normalized)


class DurableTaskRouteTests(unittest.TestCase):
    def test_intraday_refresh_keeps_202_payload_and_adds_durable_job_id(self):
        with patch("app.api.routes.stocks.DurableTaskService") as service:
            service.return_value.enqueue_intraday_refresh.return_value = {
                "task_id": "task-refresh-1",
                "status": "queued",
            }
            result = refresh_stock_intraday_bars("600000", trade_date="2026-07-21")

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["job_id"], "task-refresh-1")
        self.assertEqual(result["code"], "sh.600000")
        service.return_value.enqueue_intraday_refresh.assert_called_once_with(
            "sh.600000", "2026-07-21"
        )


class DurableTaskServiceTests(unittest.TestCase):
    def _service(self, task: dict):
        repository = FakeTaskRepository(task)
        states = FakeJobStates()
        service = DurableTaskService(
            repository=repository,
            job_states=states,
            task_logger=NullTaskLogger(),
        )
        return service, repository, states

    def test_intraday_payload_is_rebuilt_from_persisted_row(self):
        service, repository, _states = self._service(
            {
                "job_type": INTRADAY_REFRESH_JOB,
                "related_entity_id": "sh.600000",
                "payload": {
                    "schema_version": 1,
                    "code": "sh.600000",
                    "trade_date": "2026-07-21",
                    "refresh": True,
                },
            }
        )
        output = {
            "code": "sh.600000",
            "trade_date": "2026-07-21",
            "source": "akshare",
            "source_status": "fetched",
            "count": 240,
            "saved_rows": 240,
        }
        with patch(
            "app.jobs.durable_tasks._run_intraday_isolated",
            return_value=output,
        ) as fetch:
            service.run_claimed("task-1", "worker-1")

        fetch.assert_called_once_with(
            code="sh.600000", trade_date="2026-07-21", refresh=True
        )
        self.assertEqual(repository.succeeded[0][2]["saved_rows"], 240)

    def test_invalid_payload_is_failed_and_does_not_escape_as_success(self):
        service, repository, _states = self._service(
            {
                "job_type": INTRADAY_REFRESH_JOB,
                "related_entity_id": "sh.600000",
                "payload": {"code": "", "refresh": True, "unexpected": 1},
            }
        )
        with self.assertRaises(ValueError):
            service.run_claimed("task-invalid", "worker-1")

        self.assertFalse(repository.succeeded)
        self.assertEqual(repository.failed[0][0], "task-invalid")

    def test_tracking_dispatch_uses_persisted_review_and_owner_fence(self):
        service, repository, states = self._service(
            {
                "job_type": TRACKING_DEEP_REVIEW_JOB,
                "related_entity_id": "review-1",
                "payload": {"schema_version": 1, "review_job_id": "review-1"},
            }
        )
        deep_review = Mock()
        with patch("app.tracking.deep_review.DeepReviewJobService", return_value=deep_review), patch(
            "app.tracking.deep_review.call_deepseek_review"
        ) as provider:
            service.run_claimed("task-review", "worker-1")

        kwargs = deep_review.execute_persisted_job.call_args.kwargs
        self.assertEqual(kwargs["review_job_id"], "review-1")
        self.assertIs(kwargs["review_callable"], provider)
        self.assertTrue(kwargs["raise_on_failure"])
        self.assertTrue(kwargs["ownership_check"]())
        self.assertEqual(repository.succeeded[0][2], {"review_job_id": "review-1"})
        self.assertTrue(states.owns_running_job("task-review", "worker-1"))

    def test_stale_recovery_reconciles_deep_review_projection(self):
        service, repository, _states = self._service({})
        result = service.recover_stale(123)
        self.assertEqual(result.requeued, 1)
        self.assertEqual(repository.reconciled, 1)

    def test_intraday_hard_timeout_terminates_spawn_child(self):
        class FakeEndpoint:
            def poll(self, timeout):
                return False

            def close(self):
                return None

        class FakeProcess:
            def __init__(self):
                self.alive = False
                self.terminated = False

            def start(self):
                self.alive = True

            def terminate(self):
                self.terminated = True
                self.alive = False

            def join(self, timeout=None):
                return None

            def is_alive(self):
                return self.alive

        process = FakeProcess()
        context = Mock()
        context.Pipe.return_value = (FakeEndpoint(), FakeEndpoint())
        context.Process.return_value = process
        with patch("app.jobs.durable_tasks.multiprocessing.get_context", return_value=context):
            with self.assertRaises(TimeoutError):
                _run_intraday_isolated(
                    code="sh.600000",
                    trade_date="2026-07-21",
                    refresh=True,
                    timeout_seconds=0.01,
                )

        self.assertTrue(process.terminated)
        context.Process.assert_called_once()

    def test_spawn_failure_closes_both_pipe_endpoints(self):
        class FakeEndpoint:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        receiver = FakeEndpoint()
        sender = FakeEndpoint()
        process = Mock()
        process.start.side_effect = OSError("spawn unavailable")
        context = Mock()
        context.Pipe.return_value = (receiver, sender)
        context.Process.return_value = process

        with patch("app.jobs.durable_tasks.multiprocessing.get_context", return_value=context):
            with self.assertRaises(OSError):
                _run_intraday_isolated(
                    code="sh.600000",
                    trade_date="2026-07-21",
                    refresh=True,
                )

        self.assertTrue(receiver.closed)
        self.assertTrue(sender.closed)


class DurableWorkerResilienceTests(unittest.TestCase):
    def test_one_failed_task_does_not_escape_worker_loop(self):
        class FakeService:
            def recover_stale(self):
                return StaleRecoveryResult()

            def claim_next(self, worker_id):
                return "task-1"

            def run_claimed(self, task_id, worker_id):
                raise RuntimeError("provider down")

        with patch("app.jobs.durable_worker.DurableTaskService", return_value=FakeService()), patch(
            "app.jobs.durable_worker.WorkerRuntimeHeartbeat"
        ), self.assertLogs("durable-task-worker", level="ERROR"):
            run_worker(poll_seconds=0, once=True)


class DeepReviewDurabilityTests(unittest.TestCase):
    def test_review_and_queue_spec_are_created_together_by_repository_contract(self):
        repository = Mock()
        repository.create.return_value = "task-review-atomic"
        service = DeepReviewJobService(repository=repository)

        result = service.create_job(
            selection_run_id="run-1",
            strategy_id="a_share_sentiment",
            strategy_version="0.4.4",
            model="deepseek-chat",
            request_payload={"prompt": "review prompt", "item_count": 1},
        )

        spec = repository.create.call_args.kwargs["durable_task_spec"]
        self.assertEqual(spec.job_type, TRACKING_DEEP_REVIEW_JOB)
        self.assertEqual(spec.payload["review_job_id"], result["review_job_id"])
        self.assertEqual(spec.related_entity_id, result["review_job_id"])
        self.assertEqual(spec.max_attempts, 2)
        self.assertEqual(result["durable_task_id"], "task-review-atomic")


if __name__ == "__main__":
    unittest.main()
