from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

from app.jobs.mysql_state import MySQLJobStateRepository, MySQLJobTable
from app.stock_selection.run_tasks import SelectionRunService
from app.stock_selection.worker import run_worker


class FakeCursor:
    def __init__(self, fetch_rows=None, rowcounts=None):
        self.fetch_rows = list(fetch_rows or [])
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
        return self.fetch_rows.pop(0) if self.fetch_rows else None


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def fake_mysql_conn(cursor):
    @contextmanager
    def _context(*_args, **_kwargs):
        yield FakeConnection(cursor)

    return _context


class MySQLJobStateTests(unittest.TestCase):
    def test_rejects_dynamic_unsafe_table_identifier(self):
        with self.assertRaises(ValueError):
            MySQLJobTable(table="selection_run; DROP TABLE selection_run")

    def test_claim_is_guarded_by_conditional_update(self):
        cursor = FakeCursor(fetch_rows=[{"run_id": "run-1"}], rowcounts=[0, 1])
        repository = MySQLJobStateRepository(MySQLJobTable(table="selection_run"))

        with patch("app.jobs.mysql_state.mysql_conn", fake_mysql_conn(cursor)):
            run_id = repository.claim_next("worker-1")

        self.assertEqual(run_id, "run-1")
        update_sql = cursor.executed[1][0]
        self.assertIn("status='queued'", update_sql)
        self.assertIn("attempt_count < max_attempts", update_sql)
        self.assertIn("attempt_count=attempt_count + 1", update_sql)

    def test_stale_recovery_splits_cancel_retry_and_exhaustion(self):
        cursor = FakeCursor(rowcounts=[1, 2, 3])
        repository = MySQLJobStateRepository(MySQLJobTable(table="selection_run"))

        with patch("app.jobs.mysql_state.mysql_conn", fake_mysql_conn(cursor)):
            result = repository.recover_stale(60)

        self.assertEqual(result.cancelled, 1)
        self.assertEqual(result.failed, 2)
        self.assertEqual(result.requeued, 3)
        self.assertEqual(result.total, 6)


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


class SelectionRunServiceTests(unittest.TestCase):
    def test_idempotency_key_is_stable_for_equivalent_payload(self):
        first = SelectionRunService._idempotency_key(
            {"strategy_id": "lowvol_reversal", "max_picks": 3, "save": False},
            date(2026, 7, 16),
        )
        second = SelectionRunService._idempotency_key(
            {"save": False, "max_picks": 3, "strategy_id": "lowvol_reversal"},
            date(2026, 7, 16),
        )
        self.assertEqual(first, second)

    def test_invalid_worker_payload_fails_only_the_claimed_task(self):
        service = SelectionRunService(job_states=FakeJobStates())
        service.get_run = lambda *_args, **_kwargs: {
            "request": {
                "strategy_id": "lowvol_reversal",
                "instrument_type": "stock",
                "limit": 3,
                "max_picks": 3,
                "save": False,
                "unknown_future_field": True,
            }
        }
        finished = {}
        service._finish_failed = lambda *args: finished.update(
            {"run_id": args[0], "worker_id": args[1], "error_code": args[2], "message": args[3]}
        )

        with self.assertLogs("app.stock_selection.run_tasks", level="ERROR"):
            service.run_claimed("run-invalid", "worker-1")

        self.assertEqual(finished["run_id"], "run-invalid")
        self.assertEqual(finished["error_code"], "invalid_request")
        self.assertIn("unknown_future_field", finished["message"])

    def test_cancel_requested_at_calculation_boundary_skips_success(self):
        service = SelectionRunService(job_states=FakeJobStates(cancel_results=[False, True]))
        service.get_run = lambda *_args, **_kwargs: {
            "request": {
                "strategy_id": "lowvol_reversal",
                "instrument_type": "stock",
                "market_board": None,
                "limit": 3,
                "max_picks": 3,
                "score_threshold": 60,
                "save": False,
            }
        }
        service._mark_execution_stage = lambda *_args: None
        success = []
        service._finish_success = lambda *_args: success.append(True)

        class FakeStrategyService:
            @staticmethod
            def run_strategy(**_kwargs):
                return {"count": 0, "results": []}

        with patch("app.stock_selection.run_tasks.StrategyService", return_value=FakeStrategyService()):
            service.run_claimed("run-cancel", "worker-1")

        self.assertEqual(success, [])


class SelectionWorkerResilienceTests(unittest.TestCase):
    def test_unhandled_task_failure_does_not_escape_worker_loop(self):
        class FakeRecovery:
            total = 0
            requeued = 0
            failed = 0
            cancelled = 0

        class FakeService:
            @staticmethod
            def recover_stale_running_runs():
                return FakeRecovery()

            @staticmethod
            def claim_next_queued_run(worker_id=None):
                return "run"

            @staticmethod
            def run_claimed(_run_id, _worker_id):
                raise RuntimeError("unexpected")

        with patch("app.stock_selection.worker.SelectionRunService", return_value=FakeService()), patch(
            "app.stock_selection.worker.WorkerRuntimeHeartbeat"
        ), self.assertLogs(
            "selection-worker", level="ERROR"
        ) as logs:
            run_worker(poll_seconds=0, once=True)

        self.assertTrue(any("unhandled failure" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()
