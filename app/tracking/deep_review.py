from __future__ import annotations

import hashlib
import json
import os
import uuid
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from app.jobs.durable_tasks import (
    TRACKING_DEEP_REVIEW_JOB,
    DurableTaskRepository,
    DurableTaskSpec,
    build_task_spec,
)
from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]
ReviewCallable = Callable[[str, str], str]
PROMPT_VERSION = "tracking-deep-review-v1"


def call_deepseek_review(prompt: str, model: str, timeout_seconds: int = 90) -> str:
    """External provider adapter kept outside the HTTP route boundary."""

    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("未配置 DEEPSEEK_API_KEY 或 OPENAI_API_KEY")
    base_url = (
        os.getenv("DEEPSEEK_BASE_URL")
        or os.getenv("OPENAI_BASE_URL")
        or "https://api.deepseek.com/v1"
    )
    response = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"].strip()


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


class DeepReviewJobRepository:
    """MySQL persistence for asynchronous tracking review jobs."""

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self):
        return self._connection_factory(dict_cursor=True)

    def create(
        self,
        *,
        review_job_id: str,
        cache_key: str,
        input_hash: str,
        selection_run_id: str | None,
        strategy_id: str,
        strategy_version: str,
        model: str,
        request_payload: dict[str, Any],
        durable_task_spec: DurableTaskSpec | None = None,
    ) -> str | None:
        sql = """
        INSERT INTO ai_advice_snapshot (
            advice_id, cache_key, selection_run_id, strategy_id, strategy_version,
            code, advice_type, provider, model_version, prompt_version, input_hash,
            status, response_json, requested_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            '', 'tracking_deep_review', 'deepseek', %s, %s, %s,
            'queued', %s, %s
        )
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        review_job_id,
                        cache_key,
                        selection_run_id,
                        strategy_id,
                        strategy_version,
                        model,
                        PROMPT_VERSION,
                        input_hash,
                        json.dumps({"request": request_payload}, ensure_ascii=False, default=str),
                        _utcnow_naive(),
                    ),
                )
                if durable_task_spec is not None:
                    return DurableTaskRepository.insert_with_cursor(cursor, durable_task_spec)
        return None

    def get(self, review_job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT advice_id, selection_run_id, strategy_id, strategy_version,
                           model_version, prompt_version, status, response_json,
                           requested_at, completed_at, latency_ms, error_code, error_message
                    FROM ai_advice_snapshot
                    WHERE advice_id = %s AND advice_type = 'tracking_deep_review'
                    LIMIT 1
                    """,
                    (review_job_id,),
                )
                row = cursor.fetchone()
        if not row:
            return None
        normalized = dict(row)
        normalized["response_json"] = _json_object(normalized.get("response_json"))
        return normalized

    def mark_running(self, review_job_id: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE ai_advice_snapshot
                    SET status = 'running', error_code = NULL, error_message = NULL
                    WHERE advice_id = %s AND status = 'queued'
                    """,
                    (review_job_id,),
                )

    def mark_success(
        self,
        *,
        review_job_id: str,
        request_payload: dict[str, Any],
        analysis: str,
        latency_ms: int,
    ) -> None:
        response_payload = {
            "request": request_payload,
            "result": {"analysis": analysis},
        }
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE ai_advice_snapshot
                    SET status = 'success', summary = %s, response_json = %s,
                        completed_at = %s, latency_ms = %s,
                        error_code = NULL, error_message = NULL
                    WHERE advice_id = %s
                    """,
                    (
                        analysis,
                        json.dumps(response_payload, ensure_ascii=False, default=str),
                        _utcnow_naive(),
                        max(0, int(latency_ms)),
                        review_job_id,
                    ),
                )

    def mark_failed(
        self,
        *,
        review_job_id: str,
        request_payload: dict[str, Any],
        error_code: str,
        error_message: str,
        latency_ms: int,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE ai_advice_snapshot
                    SET status = 'failed', response_json = %s, completed_at = %s,
                        latency_ms = %s, error_code = %s, error_message = %s
                    WHERE advice_id = %s
                    """,
                    (
                        json.dumps({"request": request_payload}, ensure_ascii=False, default=str),
                        _utcnow_naive(),
                        max(0, int(latency_ms)),
                        error_code[:64],
                        error_message[:500],
                        review_job_id,
                    ),
                )


class DeepReviewJobService:
    """Creates review jobs and executes their external AI work after the response."""

    def __init__(self, repository: DeepReviewJobRepository | None = None) -> None:
        self.repository = repository or DeepReviewJobRepository()

    def create_job(
        self,
        *,
        selection_run_id: str | None,
        strategy_id: str,
        strategy_version: str,
        model: str,
        request_payload: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = str(request_payload.get("prompt") or "")
        input_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        review_job_id = f"review_{uuid.uuid4().hex}"
        # Re-running an identical review is allowed. The job id keeps the cache key
        # unique while input_hash still records deterministic input identity.
        cache_key = hashlib.sha256(f"{input_hash}:{review_job_id}".encode("utf-8")).hexdigest()
        durable_task_spec = build_task_spec(
            TRACKING_DEEP_REVIEW_JOB,
            {"schema_version": 1, "review_job_id": review_job_id},
            related_entity_id=review_job_id,
            idempotency_value=review_job_id,
            max_attempts=2,
        )
        persisted_task_id = self.repository.create(
            review_job_id=review_job_id,
            cache_key=cache_key,
            input_hash=input_hash,
            selection_run_id=selection_run_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            model=model,
            request_payload=request_payload,
            durable_task_spec=durable_task_spec,
        )
        return {
            "review_job_id": review_job_id,
            "status": "queued",
            "model": model,
            "item_count": request_payload.get("item_count"),
            "durable_task_id": persisted_task_id or durable_task_spec.task_id,
        }

    def execute_job(
        self,
        *,
        review_job_id: str,
        model: str,
        request_payload: dict[str, Any],
        review_callable: ReviewCallable,
        raise_on_failure: bool = False,
        ownership_check: Callable[[], bool] | None = None,
    ) -> None:
        started_at = datetime.now(timezone.utc)
        self.repository.mark_running(review_job_id)
        try:
            analysis = review_callable(str(request_payload.get("prompt") or ""), model)
            model_marker = f"分析模型：{model}"
            if model_marker not in analysis:
                analysis = f"{analysis.rstrip()}\n\n{model_marker}"
            if ownership_check is not None and not ownership_check():
                raise RuntimeError("durable task lost worker ownership before review persistence")
            latency_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
            self.repository.mark_success(
                review_job_id=review_job_id,
                request_payload=request_payload,
                analysis=analysis,
                latency_ms=latency_ms,
            )
        except Exception as exc:  # background work must persist failure instead of escaping
            latency_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
            if ownership_check is None or ownership_check():
                self.repository.mark_failed(
                    review_job_id=review_job_id,
                    request_payload=request_payload,
                    error_code=type(exc).__name__,
                    error_message=str(exc) or type(exc).__name__,
                    latency_ms=latency_ms,
                )
            if raise_on_failure:
                raise

    def execute_persisted_job(
        self,
        *,
        review_job_id: str,
        review_callable: ReviewCallable,
        raise_on_failure: bool = False,
        ownership_check: Callable[[], bool] | None = None,
    ) -> None:
        row = self.repository.get(review_job_id)
        if row is None:
            raise ValueError(f"deep review job not found: {review_job_id}")
        if str(row.get("status") or "") == "success":
            return
        response_payload = _json_object(row.get("response_json"))
        request_payload = _json_object(response_payload.get("request"))
        if not request_payload:
            raise ValueError(f"deep review request payload is missing: {review_job_id}")
        self.execute_job(
            review_job_id=review_job_id,
            model=str(row.get("model_version") or "deepseek-chat"),
            request_payload=request_payload,
            review_callable=review_callable,
            raise_on_failure=raise_on_failure,
            ownership_check=ownership_check,
        )

    def get_job(self, review_job_id: str) -> dict[str, Any] | None:
        row = self.repository.get(review_job_id)
        if row is None:
            return None
        response_payload = _json_object(row.get("response_json"))
        request_payload = _json_object(response_payload.get("request"))
        result_payload = _json_object(response_payload.get("result"))
        status = "queued" if row.get("status") == "pending" else row.get("status")
        return {
            "review_job_id": row.get("advice_id"),
            "status": status,
            "model": row.get("model_version"),
            "item_count": request_payload.get("item_count"),
            "prompt_template": request_payload.get("prompt_template"),
            "filters": request_payload.get("filters"),
            "analysis": result_payload.get("analysis") if status == "success" else None,
            "error": {
                "code": row.get("error_code"),
                "message": row.get("error_message"),
            }
            if status == "failed"
            else None,
            "requested_at": row.get("requested_at"),
            "completed_at": row.get("completed_at"),
            "latency_ms": row.get("latency_ms"),
        }
