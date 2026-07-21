from __future__ import annotations

import asyncio
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.api.routes.selection import (
    _selection_run_event_stream,
    _selection_sse_is_ready,
    get_selection_run_events,
)


class FakeRequest:
    def __init__(self, disconnected: bool = False) -> None:
        self.disconnected = disconnected

    async def is_disconnected(self) -> bool:
        return self.disconnected


class FakeRedisCache:
    def __init__(self, values=None) -> None:
        self.values = list(values or [])
        self.get_thread_ids: list[int] = []

    def get(self, _key):
        self.get_thread_ids.append(threading.get_ident())
        return self.values.pop(0) if self.values else None

    @staticmethod
    def diagnostics():
        return {"backend": "redis", "status": "ready", "fallback_active": False}


async def collect_stream(iterator) -> str:
    chunks: list[str] = []
    async for chunk in iterator:
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8")
        chunks.append(chunk)
    return "".join(chunks)


class SelectionSseTests(unittest.TestCase):
    def test_sse_requires_a_ready_non_fallback_redis_backend(self):
        self.assertTrue(
            _selection_sse_is_ready(
                {"backend": "redis", "status": "ready", "fallback_active": False}
            )
        )
        self.assertFalse(_selection_sse_is_ready({"backend": "memory", "status": "ready"}))
        self.assertFalse(
            _selection_sse_is_ready(
                {"backend": "redis", "status": "fallback", "fallback_active": True}
            )
        )

    def test_endpoint_returns_structured_503_when_redis_is_unavailable(self):
        with patch(
            "app.api.routes.selection._selection_sse_cache_diagnostics",
            return_value={"backend": "memory", "status": "ready"},
        ):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(get_selection_run_events("run-1", FakeRequest()))

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.detail["code"], "SSE_UNAVAILABLE")
        self.assertEqual(raised.exception.detail["fallback"], "polling")

    def test_endpoint_offloads_lazy_redis_probe_from_event_loop(self):
        caller_thread_id = threading.get_ident()
        probe_thread_ids: list[int] = []

        def diagnostics_probe():
            probe_thread_ids.append(threading.get_ident())
            return {"backend": "memory", "status": "ready"}

        with patch(
            "app.api.routes.selection._selection_sse_cache_diagnostics",
            side_effect=diagnostics_probe,
        ):
            with self.assertRaises(HTTPException):
                asyncio.run(get_selection_run_events("run-1", FakeRequest()))

        self.assertEqual(len(probe_thread_ids), 1)
        self.assertNotEqual(probe_thread_ids[0], caller_thread_id)

    def test_endpoint_streams_terminal_status_when_redis_is_ready(self):
        service = Mock()
        service.get_run.return_value = {
            "run_id": "run-1",
            "status": "success",
            "progress_pct": 100,
            "result": {"items": []},
        }

        async def exercise() -> tuple[StreamingResponse, str]:
            response = await get_selection_run_events("run-1", FakeRequest())
            return response, await collect_stream(response.body_iterator)

        with patch(
            "app.api.routes.selection._selection_sse_cache_diagnostics",
            return_value={"backend": "redis", "status": "ready", "fallback_active": False},
        ), patch("app.api.routes.selection.SelectionRunService", return_value=service):
            response, body = asyncio.run(exercise())

        self.assertIsInstance(response, StreamingResponse)
        self.assertEqual(response.media_type, "text/event-stream")
        self.assertEqual(response.headers["cache-control"], "no-cache, no-transform")
        self.assertIn("event: status", body)
        self.assertIn('"status":"success"', body)
        service.get_run.assert_called_once_with("run-1", False)

    def test_stream_reads_redis_status_without_per_second_mysql_queries(self):
        service = Mock()
        cache = FakeRedisCache(
            [{"run_id": "run-2", "status": "success", "progress_pct": 100}]
        )

        async def exercise() -> str:
            return await collect_stream(
                _selection_run_event_stream(
                    request=FakeRequest(),
                    run_id="run-2",
                    service=service,
                    initial_status={"run_id": "run-2", "status": "queued", "progress_pct": 0},
                    cache_backend=cache,
                    timeout_seconds=1,
                    poll_seconds=0,
                    heartbeat_seconds=60,
                    db_reconcile_seconds=60,
                )
            )

        body = asyncio.run(exercise())

        self.assertEqual(body.count("event: status"), 2)
        self.assertIn('"status":"queued"', body)
        self.assertIn('"status":"success"', body)
        service.get_run.assert_not_called()
        self.assertTrue(cache.get_thread_ids)
        self.assertTrue(
            all(thread_id != threading.get_ident() for thread_id in cache.get_thread_ids)
        )

    def test_stream_reconciles_mysql_when_redis_has_no_status(self):
        service = Mock()
        service.get_run.return_value = {
            "run_id": "run-3",
            "status": "success",
            "progress_pct": 100,
        }

        body = asyncio.run(
            collect_stream(
                _selection_run_event_stream(
                    request=FakeRequest(),
                    run_id="run-3",
                    service=service,
                    initial_status={"run_id": "run-3", "status": "queued", "progress_pct": 0},
                    cache_backend=FakeRedisCache(),
                    timeout_seconds=1,
                    poll_seconds=0,
                    heartbeat_seconds=60,
                    db_reconcile_seconds=0,
                )
            )
        )

        self.assertIn('"status":"success"', body)
        service.get_run.assert_called_once_with("run-3", False)

    def test_frontend_attempts_event_source_then_falls_back_to_polling(self):
        script = (
            Path(__file__).resolve().parents[1] / "app" / "api" / "web" / "js" / "selection.js"
        ).read_text(encoding="utf-8")

        self.assertIn("new EventSource", script)
        self.assertIn("error.selectionSseFallback = true", script)
        self.assertIn("return waitForSelectionRunByPolling(runId, deadline)", script)


if __name__ == "__main__":
    unittest.main()
