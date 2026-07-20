from __future__ import annotations

import json
import os
from pathlib import Path
import time
from threading import Lock
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import requests

from app.error_learning.tracker import SelectionResultTracker
from app.tracking.repository import TRACKING_STATS_MAX_AGE_DAYS, TrackingRepository

router = APIRouter(tags=["tracking"])
_TRACKING_REPOSITORY = TrackingRepository()


_TRACKING_SUMMARY_CACHE_TTL_SECONDS = 60.0
_TRACKING_SUMMARY_CACHE: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_TRACKING_SUMMARY_CACHE_LOCK = Lock()


def _tracking_summary_cache_key(
    *,
    run_id: Optional[str],
    strategy_id: Optional[str],
    selection_date: Optional[str],
    instrument_type: str,
    latest_only: bool,
) -> tuple[Any, ...]:
    return (run_id, strategy_id, selection_date, instrument_type, latest_only)


def _get_cached_tracking_summary(key: tuple[Any, ...]) -> dict[str, Any] | None:
    now = time.monotonic()
    with _TRACKING_SUMMARY_CACHE_LOCK:
        cached = _TRACKING_SUMMARY_CACHE.get(key)
        if not cached or cached[0] <= now:
            _TRACKING_SUMMARY_CACHE.pop(key, None)
            return None
        return cached[1]


def _cache_tracking_summary(key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    with _TRACKING_SUMMARY_CACHE_LOCK:
        _TRACKING_SUMMARY_CACHE[key] = (time.monotonic() + _TRACKING_SUMMARY_CACHE_TTL_SECONDS, payload)


def _invalidate_tracking_summary_cache() -> None:
    with _TRACKING_SUMMARY_CACHE_LOCK:
        _TRACKING_SUMMARY_CACHE.clear()


class TrackingStatsToggleRequest(BaseModel):
    include_in_stats: bool


class TrackingDeepReviewRequest(BaseModel):
    strategy_id: Optional[str] = None
    selection_date: Optional[str] = None
    run_id: Optional[str] = None
    instrument_type: str = "stock"
    max_items: int = 80


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parents[3] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _build_tracking_summary(items: list[dict]) -> dict:
    pct_values = [item.get("price_change_pct") for item in items if item.get("price_change_pct") is not None]
    avg_return = round(sum(pct_values) / len(pct_values), 2) if pct_values else None
    benchmark_pct = 0.0
    excess_return_pct = round(avg_return - benchmark_pct, 2) if avg_return is not None else None
    win_count = len([value for value in pct_values if value > 0])
    flat_count = len([value for value in pct_values if value == 0])
    loss_count = len([value for value in pct_values if value < 0])
    win_rate = round((win_count / len(pct_values)) * 100, 2) if pct_values else None
    max_gain = max((item.get("max_gain_pct") for item in items if item.get("max_gain_pct") is not None), default=None)
    max_drawdown = min((item.get("max_drawdown_pct") for item in items if item.get("max_drawdown_pct") is not None), default=None)
    tracking_count = len([item for item in items if item.get("review_status") == "tracking"])
    best_item = max(
        (item for item in items if item.get("price_change_pct") is not None),
        key=lambda item: item.get("price_change_pct") or 0,
        default=None,
    )
    worst_item = min(
        (item for item in items if item.get("price_change_pct") is not None),
        key=lambda item: item.get("price_change_pct") or 0,
        default=None,
    )
    return {
        "count": len(items),
        "tracking_count": tracking_count,
        "avg_return_pct": avg_return,
        "benchmark_return_pct": benchmark_pct,
        "excess_return_pct": excess_return_pct,
        "win_rate_pct": win_rate,
        "win_count": win_count,
        "loss_count": loss_count,
        "flat_count": flat_count,
        "max_gain_pct": max_gain,
        "max_drawdown_pct": max_drawdown,
        "best_item": {
            "code": best_item.get("code"),
            "name": best_item.get("name"),
            "price_change_pct": best_item.get("price_change_pct"),
        } if best_item else None,
        "worst_item": {
            "code": worst_item.get("code"),
            "name": worst_item.get("name"),
            "price_change_pct": worst_item.get("price_change_pct"),
        } if worst_item else None,
    }


def _stats_items(items: list[dict]) -> list[dict]:
    return [
        item
        for item in items
        if item.get("include_in_stats", True) and not item.get("stats_window_expired", False)
    ]


def _list_tracking_runs(
    instrument_type: str,
    strategy_id: Optional[str] = None,
    selection_date: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _TRACKING_REPOSITORY.list_runs(
        instrument_type=instrument_type,
        strategy_id=strategy_id,
        selection_date=selection_date,
        limit=limit,
    )


def _count_tracking_items(
    instrument_type: str,
    strategy_id: Optional[str] = None,
    selection_date: Optional[str] = None,
    run_id: Optional[str] = None,
    latest_only: bool = False,
    include_in_stats_only: bool = False,
) -> int:
    return _TRACKING_REPOSITORY.count_items(
        instrument_type=instrument_type,
        strategy_id=strategy_id,
        selection_date=selection_date,
        run_id=run_id,
        latest_only=latest_only,
        include_in_stats_only=include_in_stats_only,
    )
def _build_strategy_summaries(items: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for item in items:
        key = str(item.get("strategy_id") or "")
        grouped.setdefault(key, []).append(item)

    summaries: list[dict] = []
    for key, group_items in grouped.items():
        strategy_summary = _build_tracking_summary(group_items)
        strategy_summary.update(
            {
                "strategy_id": key,
                "strategy_display_name": group_items[0].get("strategy_display_name") or key,
                "selection_dates": sorted({item.get("selection_date") for item in group_items if item.get("selection_date")}, reverse=True),
            }
        )
        summaries.append(strategy_summary)

    summaries.sort(key=lambda item: item.get("strategy_display_name") or item.get("strategy_id") or "")
    return summaries


def _compact_trade_plan(plan: dict[str, Any] | None) -> dict[str, Any] | None:
    if not plan:
        return None
    entry_zone = plan.get("entry_zone") or {}
    stop_loss = plan.get("stop_loss") or {}
    take_profit = plan.get("take_profit") or []
    first_take_profit = take_profit[0] if take_profit else {}
    return {
        "entry_price": plan.get("entry_price"),
        "entry_zone": {
            "low": entry_zone.get("low"),
            "high": entry_zone.get("high"),
        },
        "stop_loss": {"price": stop_loss.get("price")},
        "take_profit": [{"price": first_take_profit.get("price")}],
    }


def _compact_tracking_item(item: dict[str, Any]) -> dict[str, Any]:
    status = item.get("trade_plan_status") or {}
    return {
        "code": item.get("code"),
        "name": item.get("name"),
        "rank_no": item.get("rank_no"),
        "score": item.get("score"),
        "strategy_id": item.get("strategy_id"),
        "strategy_display_name": item.get("strategy_display_name"),
        "selection_date": item.get("selection_date"),
        "selection_datetime": item.get("selection_datetime"),
        "selected_open_price": item.get("selected_open_price"),
        "selected_close_price": item.get("selected_close_price"),
        "current_price": item.get("current_price"),
        "price_change_pct": item.get("price_change_pct"),
        "realtime_quote_time": item.get("realtime_quote_time"),
        "tracking_days": item.get("tracking_days"),
        "max_gain_pct": item.get("max_gain_pct"),
        "max_drawdown_pct": item.get("max_drawdown_pct"),
        "review_status": item.get("review_status"),
        "include_in_stats": item.get("include_in_stats", True),
        "stats_window_expired": item.get("stats_window_expired", False),
        "stats_age_days": item.get("stats_age_days"),
        "stats_exclusion_reason": item.get("stats_exclusion_reason"),
        "trade_plan": _compact_trade_plan(item.get("trade_plan")),
        "trade_plan_status": {
            "status": status.get("status"),
            "status_label": status.get("status_label"),
            "completed": status.get("completed"),
        }
        if status
        else None,
    }


def _tracking_payload(
    *,
    run_id: Optional[str] = None,
    strategy_id: Optional[str] = None,
    selection_date: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    instrument_type: str = "stock",
    latest_only: bool = False,
    compact: bool = False,
    include_runs: bool = True,
) -> dict:
    tracker = SelectionResultTracker(repository=_TRACKING_REPOSITORY)
    auto_excluded_count = tracker.enforce_stats_retention(
        instrument_type=instrument_type,
        max_age_days=TRACKING_STATS_MAX_AGE_DAYS,
    )
    if auto_excluded_count > 0:
        _invalidate_tracking_summary_cache()
    resolved_run_id = run_id
    total = _count_tracking_items(
        instrument_type=instrument_type,
        strategy_id=strategy_id,
        selection_date=selection_date,
        run_id=resolved_run_id,
        latest_only=latest_only,
    )
    cache_key = _tracking_summary_cache_key(
        run_id=resolved_run_id,
        strategy_id=strategy_id,
        selection_date=selection_date,
        instrument_type=instrument_type,
        latest_only=latest_only,
    )
    cached_summary = _get_cached_tracking_summary(cache_key)
    if cached_summary is not None and cached_summary.get("_total") != total:
        cached_summary = None

    page_records = tracker.build_latest_selection_snapshot(
        limit=limit,
        instrument_type=instrument_type,
        run_id=resolved_run_id,
        strategy_id=strategy_id,
        selection_date=selection_date,
        offset=offset,
        latest_only=latest_only,
    )
    items = tracker.to_dict_list(page_records)

    if cached_summary is None:
        summary_records = tracker.build_latest_selection_snapshot(
            limit=max(total, 1),
            instrument_type=instrument_type,
            run_id=resolved_run_id,
            strategy_id=strategy_id,
            selection_date=selection_date,
            offset=0,
            latest_only=latest_only,
            include_in_stats_only=True,
        )
        stats_items = tracker.to_dict_list(summary_records)
        cached_summary = {
            "_total": total,
            "filtered_summary": {
                **_build_tracking_summary(stats_items),
                "total_count": total,
                "excluded_count": total - len(stats_items),
            },
            "strategy_summaries": _build_strategy_summaries(stats_items),
        }
        _cache_tracking_summary(cache_key, cached_summary)

    page_stats_items = _stats_items(items)
    response_items = [_compact_tracking_item(item) for item in items] if compact else items
    payload = {
        "run_id": resolved_run_id,
        "strategy_id": strategy_id,
        "selection_date": selection_date,
        "summary": _build_tracking_summary(page_stats_items),
        "filtered_summary": cached_summary["filtered_summary"],
        "strategy_summaries": cached_summary["strategy_summaries"],
        "items": response_items,
        "stats_retention": {
            "max_age_days": TRACKING_STATS_MAX_AGE_DAYS,
            "basis": "selection_datetime",
            "auto_excluded_count": auto_excluded_count,
        },
        "pagination": {
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(items) < total,
        },
    }
    if include_runs:
        payload["available_runs"] = _list_tracking_runs(instrument_type=instrument_type, strategy_id=strategy_id)
    return payload


def _set_tracking_include_in_stats(
    *,
    code: str,
    selection_date: str,
    strategy_id: str,
    instrument_type: str,
    include_in_stats: bool,
) -> int:
    return _TRACKING_REPOSITORY.set_include_in_stats(
        code=code,
        selection_date=selection_date,
        strategy_id=strategy_id,
        instrument_type=instrument_type,
        include_in_stats=include_in_stats,
    )


def _compact_review_item(item: dict[str, Any]) -> dict[str, Any]:
    factor_scores = item.get("factor_scores") or {}
    sentiment_context = item.get("sentiment_context") or {}
    return {
        "code": item.get("code"),
        "name": item.get("name"),
        "strategy_id": item.get("strategy_id"),
        "strategy_display_name": item.get("strategy_display_name"),
        "selection_date": item.get("selection_date"),
        "selection_datetime": item.get("selection_datetime"),
        "score": item.get("score"),
        "rank_no": item.get("rank_no"),
        "selected_price": item.get("selected_open_price") or item.get("selected_close_price"),
        "current_price": item.get("current_price"),
        "price_change_pct": item.get("price_change_pct"),
        "max_gain_pct": item.get("max_gain_pct"),
        "max_drawdown_pct": item.get("max_drawdown_pct"),
        "tracking_days": item.get("tracking_days"),
        "review_status": item.get("review_status"),
        "include_in_stats": item.get("include_in_stats", True),
        "stats_window_expired": item.get("stats_window_expired", False),
        "stats_age_days": item.get("stats_age_days"),
        "stats_exclusion_reason": item.get("stats_exclusion_reason"),
        "realtime_pct_chg": item.get("realtime_pct_chg"),
        "realtime_quote_time": item.get("realtime_quote_time"),
        "trade_signal": {
            "state": sentiment_context.get("trade_signal_state") or factor_scores.get("trade_signal_state"),
            "label": sentiment_context.get("trade_signal_label") or factor_scores.get("trade_signal_label"),
            "reason": sentiment_context.get("trade_signal_reason") or factor_scores.get("trade_signal_reason"),
        },
        "sentiment": {
            "sector_name": sentiment_context.get("sector_name"),
            "sector_type": sentiment_context.get("sector_type"),
            "as_of": sentiment_context.get("as_of"),
            "match_reason": sentiment_context.get("opinion_match_reason"),
            "source_credibility": sentiment_context.get("source_credibility_level"),
            "deepseek": sentiment_context.get("deepseek"),
        },
        "price_preference": {
            "label": factor_scores.get("price_preference_label"),
            "delta": factor_scores.get("price_preference_delta_applied") or factor_scores.get("price_preference_delta"),
            "reason": factor_scores.get("price_preference_reason"),
        },
        "reasons": item.get("reason_summary") or [],
        "risks": item.get("risk_summary") or [],
    }


def _render_review_prompt(template: str, *, filters: dict[str, Any], summary: dict[str, Any], items: list[dict[str, Any]], model: str) -> str:
    replacements = {
        "{{filters}}": json.dumps(filters, ensure_ascii=False, indent=2, default=str),
        "{{summary}}": json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        "{{items}}": json.dumps(items, ensure_ascii=False, indent=2, default=str),
        "{{model}}": model,
    }
    prompt = template
    for key, value in replacements.items():
        prompt = prompt.replace(key, value)
    return prompt


def _call_deepseek_review(prompt: str, model: str, timeout_seconds: int = 90) -> str:
    _load_env_file()
    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise HTTPException(status_code=400, detail="未配置 DEEPSEEK_API_KEY 或 OPENAI_API_KEY，无法执行 AI 详细复盘")
    base_url = os.getenv("DEEPSEEK_BASE_URL") or os.getenv("OPENAI_BASE_URL") or "https://api.deepseek.com/v1"
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


@router.get("/tracking/deep-review/status")
def get_tracking_deep_review_status() -> dict:
    _load_env_file()
    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
    model = os.getenv("DEEPSEEK_REVIEW_MODEL") or os.getenv("DEEPSEEK_MODEL") or "deepseek-chat"
    return {
        "available": bool(api_key),
        "model": model if api_key else None,
        "message": None if api_key else "未配置 DEEPSEEK_API_KEY 或 OPENAI_API_KEY",
    }


@router.get("/tracking/latest")
def get_latest_tracking(
    limit: int = Query(default=10, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    instrument_type: str = Query(default="stock"),
    strategy_id: Optional[str] = Query(default=None),
    selection_date: Optional[str] = Query(default=None),
    compact: bool = Query(default=False),
    include_runs: bool = Query(default=True),
) -> dict:
    return _tracking_payload(
        strategy_id=strategy_id,
        selection_date=selection_date,
        limit=limit,
        offset=offset,
        instrument_type=instrument_type,
        latest_only=True,
        compact=compact,
        include_runs=include_runs,
    )


@router.get("/tracking")
def get_tracking_by_run(
    run_id: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    selection_date: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    instrument_type: str = Query(default="stock"),
    compact: bool = Query(default=False),
    include_runs: bool = Query(default=True),
) -> dict:
    return _tracking_payload(
        run_id=run_id,
        strategy_id=strategy_id,
        selection_date=selection_date,
        limit=limit,
        offset=offset,
        instrument_type=instrument_type,
        compact=compact,
        include_runs=include_runs,
    )


@router.delete("/tracking/item")
def delete_tracking_item(
    code: str = Query(...),
    selection_date: str = Query(...),
    strategy_id: str = Query(...),
    instrument_type: str = Query(default="stock"),
) -> dict:
    matched_count = _TRACKING_REPOSITORY.delete_item(
        code=code,
        selection_date=selection_date,
        strategy_id=strategy_id,
        instrument_type=instrument_type,
    )
    if matched_count <= 0:
        raise HTTPException(status_code=404, detail="未找到可删除的复盘记录")

    _invalidate_tracking_summary_cache()

    return {
        "code": code,
        "selection_date": selection_date,
        "strategy_id": strategy_id,
        "instrument_type": instrument_type,
        "deleted_count": matched_count,
    }


@router.patch("/tracking/item/stats")
def update_tracking_item_stats(
    payload: TrackingStatsToggleRequest,
    code: str = Query(...),
    selection_date: str = Query(...),
    strategy_id: str = Query(...),
    instrument_type: str = Query(default="stock"),
) -> dict:
    if payload.include_in_stats and _TRACKING_REPOSITORY.is_stats_window_expired(
        code=code,
        selection_date=selection_date,
        strategy_id=strategy_id,
        instrument_type=instrument_type,
        max_age_days=TRACKING_STATS_MAX_AGE_DAYS,
    ):
        raise HTTPException(
            status_code=409,
            detail=f"该记录已超过 {TRACKING_STATS_MAX_AGE_DAYS} 个自然日统计窗口，不能重新纳入统计",
        )
    matched_count = _set_tracking_include_in_stats(
        code=code,
        selection_date=selection_date,
        strategy_id=strategy_id,
        instrument_type=instrument_type,
        include_in_stats=payload.include_in_stats,
    )
    if matched_count <= 0:
        raise HTTPException(status_code=404, detail="未找到可更新的复盘记录")
    _invalidate_tracking_summary_cache()
    return {
        "code": code,
        "selection_date": selection_date,
        "strategy_id": strategy_id,
        "instrument_type": instrument_type,
        "include_in_stats": payload.include_in_stats,
        "updated_count": matched_count,
    }


@router.post("/tracking/deep-review")
def run_tracking_deep_review(payload: TrackingDeepReviewRequest) -> dict:
    max_items = max(1, min(int(payload.max_items or 80), 200))
    data = _tracking_payload(
        run_id=payload.run_id,
        strategy_id=payload.strategy_id,
        selection_date=payload.selection_date,
        limit=max_items,
        offset=0,
        instrument_type=payload.instrument_type or "stock",
        include_runs=False,
    )
    items = _stats_items(data.get("items") or [])
    if not items:
        raise HTTPException(status_code=400, detail="当前筛选条件下没有可复盘的股票")

    compact_items = [_compact_review_item(item) for item in items[:max_items]]
    summary = data.get("filtered_summary") or data.get("summary") or _build_tracking_summary(items)
    filters = {
        "strategy_id": payload.strategy_id or "全部策略",
        "selection_date": payload.selection_date or "全部日期",
        "run_id": payload.run_id,
        "instrument_type": payload.instrument_type,
        "item_count": len(compact_items),
    }
    _load_env_file()
    model = os.getenv("DEEPSEEK_REVIEW_MODEL") or os.getenv("DEEPSEEK_MODEL") or "deepseek-chat"
    template_path = Path(__file__).resolve().parents[2] / "prompts" / "tracking_deep_review_prompt.md"
    template = template_path.read_text(encoding="utf-8")
    prompt = _render_review_prompt(
        template,
        filters=filters,
        summary=summary,
        items=compact_items,
        model=model,
    )
    try:
        analysis = _call_deepseek_review(prompt, model=model)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"DeepSeek 复盘调用失败: {str(exc)[:300]}") from exc
    if f"分析模型：{model}" not in analysis:
        analysis = f"{analysis.rstrip()}\n\n分析模型：{model}"
    return {
        "analysis": analysis,
        "model": model,
        "item_count": len(compact_items),
        "prompt_template": str(template_path),
        "filters": filters,
    }


@router.get("/tracking/filters")
def get_tracking_filters(
    instrument_type: str = Query(default="stock"),
    strategy_id: Optional[str] = Query(default=None),
) -> dict:
    runs = _list_tracking_runs(instrument_type=instrument_type, strategy_id=strategy_id, limit=200)
    seen_dates = []
    seen = set()
    for item in runs:
        value = str(item.get("trade_date") or "")
        if value and value not in seen:
            seen.add(value)
            seen_dates.append(value)

    strategy_rows = _TRACKING_REPOSITORY.list_strategy_options(instrument_type)

    strategy_options = [
        {
            "strategy_id": str(row.get("strategy_id") or ""),
            "strategy_display_name": str(row.get("strategy_display_name") or row.get("strategy_id") or ""),
            "item_count": int(row.get("item_count") or 0),
        }
        for row in strategy_rows
        if row.get("strategy_id")
    ]
    return {
        "strategy_id": strategy_id,
        "selection_dates": seen_dates,
        "strategy_options": strategy_options,
        "available_runs": runs,
    }
