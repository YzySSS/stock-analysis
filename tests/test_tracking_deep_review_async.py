from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from app.api.routes import tracking as tracking_route
from app.tracking.deep_review import DeepReviewJobService


class FakeDeepReviewRepository:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    def create(self, **kwargs) -> None:
        review_job_id = kwargs["review_job_id"]
        self.rows[review_job_id] = {
            "advice_id": review_job_id,
            "selection_run_id": kwargs["selection_run_id"],
            "strategy_id": kwargs["strategy_id"],
            "strategy_version": kwargs["strategy_version"],
            "model_version": kwargs["model"],
            "prompt_version": "tracking-deep-review-v1",
            "status": "queued",
            "response_json": {"request": kwargs["request_payload"]},
            "requested_at": "2026-07-21 12:00:00",
            "completed_at": None,
            "latency_ms": None,
            "error_code": None,
            "error_message": None,
        }

    def get(self, review_job_id: str):
        return self.rows.get(review_job_id)

    def mark_running(self, review_job_id: str) -> None:
        self.rows[review_job_id]["status"] = "running"

    def mark_success(self, *, review_job_id: str, request_payload: dict, analysis: str, latency_ms: int) -> None:
        row = self.rows[review_job_id]
        row.update(
            {
                "status": "success",
                "response_json": {"request": request_payload, "result": {"analysis": analysis}},
                "latency_ms": latency_ms,
                "completed_at": "2026-07-21 12:00:01",
            }
        )

    def mark_failed(
        self,
        *,
        review_job_id: str,
        request_payload: dict,
        error_code: str,
        error_message: str,
        latency_ms: int,
    ) -> None:
        row = self.rows[review_job_id]
        row.update(
            {
                "status": "failed",
                "response_json": json.dumps({"request": request_payload}, ensure_ascii=False),
                "error_code": error_code,
                "error_message": error_message,
                "latency_ms": latency_ms,
                "completed_at": "2026-07-21 12:00:01",
            }
        )


class RecordingDeepReviewService:
    def __init__(self) -> None:
        self.created: dict | None = None
        self.executed = False

    def create_job(self, **kwargs):
        self.created = kwargs
        return {
            "review_job_id": "review_test_job",
            "status": "queued",
            "model": kwargs["model"],
            "item_count": kwargs["request_payload"]["item_count"],
            "durable_task_id": "task-review-1",
        }

    def execute_job(self, **kwargs) -> None:
        self.executed = True

    def get_job(self, review_job_id: str):
        if review_job_id == "missing":
            return None
        return {"review_job_id": review_job_id, "status": "queued"}


def _tracking_payload_fixture() -> dict:
    item = {
        "code": "000001.SZ",
        "name": "平安银行",
        "strategy_id": "a_share_sentiment",
        "strategy_version": "0.4.4",
        "selection_date": "2026-07-21",
        "selection_datetime": "2026-07-21 10:00:00",
        "score": 72,
        "rank_no": 1,
        "selected_open_price": 10.0,
        "current_price": 10.5,
        "price_change_pct": 5.0,
        "include_in_stats": True,
        "factor_scores": {},
        "sentiment_context": {},
    }
    return {
        "items": [item],
        "summary": {"count": 1},
        "filtered_summary": {"count": 1},
    }


class TrackingDeepReviewRouteTests(unittest.TestCase):
    def test_post_only_persists_review_and_durable_queue_work(self):
        service = RecordingDeepReviewService()
        payload = tracking_route.TrackingDeepReviewRequest(
            strategy_id="a_share_sentiment",
            run_id="run-1",
            max_items=10,
        )

        with patch.object(tracking_route, "_DEEP_REVIEW_SERVICE", service), patch.object(
            tracking_route,
            "_tracking_payload",
            return_value=_tracking_payload_fixture(),
        ):
            result = tracking_route.run_tracking_deep_review(payload)

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["review_job_id"], "review_test_job")
        self.assertEqual(result["status_url"], "/api/tracking/deep-review/review_test_job")
        self.assertEqual(result["durable_task_id"], "task-review-1")
        self.assertFalse(service.executed)
        self.assertEqual(service.created["strategy_version"], "0.4.4")
        self.assertIn("prompt", service.created["request_payload"])

    def test_get_returns_job_or_404(self):
        service = RecordingDeepReviewService()
        with patch.object(tracking_route, "_DEEP_REVIEW_SERVICE", service):
            self.assertEqual(
                tracking_route.get_tracking_deep_review_job("review-1")["status"],
                "queued",
            )
            with self.assertRaises(HTTPException) as raised:
                tracking_route.get_tracking_deep_review_job("missing")
        self.assertEqual(raised.exception.status_code, 404)


class TrackingDeepReviewServiceTests(unittest.TestCase):
    def test_success_result_is_persisted_and_exposed(self):
        repository = FakeDeepReviewRepository()
        service = DeepReviewJobService(repository=repository)
        request_payload = {
            "prompt": "review prompt",
            "item_count": 2,
            "prompt_template": "template.md",
            "filters": {"strategy_id": "a_share_sentiment"},
        }
        created = service.create_job(
            selection_run_id="run-1",
            strategy_id="a_share_sentiment",
            strategy_version="0.4.4",
            model="deepseek-chat",
            request_payload=request_payload,
        )

        service.execute_job(
            review_job_id=created["review_job_id"],
            model="deepseek-chat",
            request_payload=request_payload,
            review_callable=lambda prompt, model: f"analysis for {prompt}",
        )

        job = service.get_job(created["review_job_id"])
        self.assertEqual(job["status"], "success")
        self.assertIn("analysis for review prompt", job["analysis"])
        self.assertIn("分析模型：deepseek-chat", job["analysis"])
        self.assertEqual(job["item_count"], 2)

    def test_external_failure_is_persisted_instead_of_raised(self):
        repository = FakeDeepReviewRepository()
        service = DeepReviewJobService(repository=repository)
        request_payload = {"prompt": "review prompt", "item_count": 1}
        created = service.create_job(
            selection_run_id=None,
            strategy_id="mixed",
            strategy_version="mixed",
            model="deepseek-chat",
            request_payload=request_payload,
        )

        def fail_review(prompt: str, model: str) -> str:
            raise RuntimeError("provider unavailable")

        service.execute_job(
            review_job_id=created["review_job_id"],
            model="deepseek-chat",
            request_payload=request_payload,
            review_callable=fail_review,
        )

        job = service.get_job(created["review_job_id"])
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["error"]["code"], "RuntimeError")
        self.assertIn("provider unavailable", job["error"]["message"])

    def test_lost_durable_ownership_discards_provider_result(self):
        repository = FakeDeepReviewRepository()
        service = DeepReviewJobService(repository=repository)
        request_payload = {"prompt": "review prompt", "item_count": 1}
        created = service.create_job(
            selection_run_id=None,
            strategy_id="mixed",
            strategy_version="mixed",
            model="deepseek-chat",
            request_payload=request_payload,
        )

        with self.assertRaises(RuntimeError):
            service.execute_job(
                review_job_id=created["review_job_id"],
                model="deepseek-chat",
                request_payload=request_payload,
                review_callable=lambda prompt, model: "stale worker result",
                ownership_check=lambda: False,
                raise_on_failure=True,
            )

        job = service.get_job(created["review_job_id"])
        self.assertEqual(job["status"], "running")
        self.assertIsNone(job["analysis"])
        self.assertIsNone(job["error"])


if __name__ == "__main__":
    unittest.main()
