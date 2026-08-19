from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo


MODEL_ID = "broad_market_selloff_alert_v1"
MODEL_NAME = "全市场普跌预警 V1"
MODEL_VERSION = "1.0.0"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

MIN_SAMPLE_SIZE = 3_000
MIN_COVERAGE_RATIO = 0.95
MIN_FRESH_RATIO = 0.95


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return max(0.0, min(1.0, numerator / denominator))


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _percent(value: float | None, digits: int = 1) -> str:
    return "-" if value is None else f"{value * 100:.{digits}f}%"


def build_intraday_market_risk_alert(
    overview: Mapping[str, Any] | None,
    *,
    current_date: date | None = None,
) -> dict[str, Any]:
    """Build a deterministic, non-blocking broad-market selloff warning.

    This is an operational display overlay. It does not modify strategy
    scores, selection results, market-timing snapshots, or user permissions.
    """

    payload = dict(overview or {})
    observed_total = _integer(payload.get("total"))
    expected_total = _integer(payload.get("expected_total")) or observed_total
    fresh_count = (
        _integer(payload.get("fresh_count"))
        if payload.get("fresh_count") is not None
        else observed_total
    )
    coverage_ratio = _ratio(float(observed_total), float(expected_total))
    fresh_ratio = _ratio(float(fresh_count), float(observed_total))
    trade_date = _parse_date(payload.get("trade_date"))
    today = current_date or datetime.now(SHANGHAI_TZ).date()

    up_count = _integer(payload.get("up_count"))
    down_count = _integer(payload.get("down_count"))
    strong_down_count = _integer(payload.get("strong_down_count"))
    limit_up_count = _integer(
        payload.get("limit_up_like")
        if payload.get("limit_up_like") is not None
        else payload.get("limit_up_count")
    )
    limit_down_count = _integer(
        payload.get("limit_down_like")
        if payload.get("limit_down_like") is not None
        else payload.get("limit_down_count")
    )
    avg_pct_chg = _number(payload.get("avg_pct_chg"))
    amount_weighted_pct_chg = _number(payload.get("amount_weighted_pct_chg"))
    up_amount = max(0.0, _number(payload.get("up_amount")) or 0.0)
    down_amount = max(0.0, _number(payload.get("down_amount")) or 0.0)

    up_ratio = _ratio(float(up_count), float(observed_total))
    down_ratio = _ratio(float(down_count), float(observed_total))
    strong_down_ratio = _ratio(float(strong_down_count), float(observed_total))
    directional_amount = up_amount + down_amount
    down_amount_ratio = _ratio(down_amount, directional_amount)

    quality_status = "ready"
    quality_reasons: list[str] = []
    if trade_date is None or trade_date != today:
        quality_status = "stale_trade_date"
        quality_reasons.append("实时快照不是当前交易日")
    if observed_total < MIN_SAMPLE_SIZE:
        quality_status = "insufficient_coverage"
        quality_reasons.append(f"有效样本仅 {observed_total} 只")
    if coverage_ratio is None or coverage_ratio < MIN_COVERAGE_RATIO:
        quality_status = "insufficient_coverage"
        quality_reasons.append(
            f"股票池覆盖 {_percent(coverage_ratio)}，低于 {MIN_COVERAGE_RATIO * 100:.0f}%"
        )
    if fresh_ratio is None or fresh_ratio < MIN_FRESH_RATIO:
        quality_status = "insufficient_freshness"
        quality_reasons.append(
            f"新鲜快照 {_percent(fresh_ratio)}，低于 {MIN_FRESH_RATIO * 100:.0f}%"
        )
    if (
        down_ratio is None
        or avg_pct_chg is None
        or down_amount_ratio is None
    ):
        quality_status = "missing_inputs"
        quality_reasons.append("涨跌、等权收益或成交额方向输入不完整")

    base = {
        "model_id": MODEL_ID,
        "model_name": MODEL_NAME,
        "version": MODEL_VERSION,
        "active": False,
        "level": "none",
        "level_label": "无预警",
        "title": "暂无全市场普跌预警",
        "summary": "当前没有满足普跌预警的联合条件。",
        "observed_at": payload.get("latest_quote_time"),
        "trade_date": str(trade_date) if trade_date else None,
        "blocking": False,
        "selection_allowed": True,
        "operation_mode": "strong_warning_only",
        "operation_note": "仅强提醒，不拦截手动选股，不改写策略结果。",
        "action_label": "按原流程操作，继续关注市场宽度变化。",
        "evidence": [],
        "metrics": {
            "sample_size": observed_total,
            "up_count": up_count,
            "down_count": down_count,
            "up_ratio": round(up_ratio, 6) if up_ratio is not None else None,
            "down_ratio": round(down_ratio, 6) if down_ratio is not None else None,
            "avg_pct_chg": round(avg_pct_chg, 4) if avg_pct_chg is not None else None,
            "amount_weighted_pct_chg": (
                round(amount_weighted_pct_chg, 4)
                if amount_weighted_pct_chg is not None
                else None
            ),
            "down_amount_ratio": (
                round(down_amount_ratio, 6)
                if down_amount_ratio is not None
                else None
            ),
            "strong_down_count": strong_down_count,
            "strong_down_ratio": (
                round(strong_down_ratio, 6)
                if strong_down_ratio is not None
                else None
            ),
            "limit_up_count": limit_up_count,
            "limit_down_count": limit_down_count,
        },
        "data_quality": {
            "status": quality_status,
            "ready": quality_status == "ready",
            "observed_total": observed_total,
            "expected_total": expected_total,
            "coverage_ratio": (
                round(coverage_ratio, 6) if coverage_ratio is not None else None
            ),
            "fresh_count": fresh_count,
            "fresh_ratio": round(fresh_ratio, 6) if fresh_ratio is not None else None,
            "reasons": quality_reasons,
            "universe": "PIT_active_stocks_excluding_first_listing_day",
        },
    }
    if quality_status != "ready":
        base["title"] = "普跌预警数据暂不可用"
        base["summary"] = "；".join(quality_reasons) or "实时股票池证据不足。"
        return base

    assert down_ratio is not None
    assert avg_pct_chg is not None
    assert down_amount_ratio is not None

    red = (
        down_ratio >= 0.87
        and avg_pct_chg <= -2.50
        and down_amount_ratio >= 0.83
        and (
            (strong_down_ratio is not None and strong_down_ratio >= 0.15)
            or (
                amount_weighted_pct_chg is not None
                and amount_weighted_pct_chg <= -3.00
            )
        )
    )
    orange = (
        down_ratio >= 0.80
        and avg_pct_chg <= -1.00
        and down_amount_ratio >= 0.75
    )
    yellow_hits = sum(
        (
            down_ratio >= 0.70,
            avg_pct_chg <= -0.70,
            down_amount_ratio >= 0.65,
            strong_down_ratio is not None and strong_down_ratio >= 0.02,
            limit_down_count >= max(15, int(limit_up_count * 1.2)),
        )
    )
    yellow = down_ratio >= 0.70 and yellow_hits >= 3

    if red:
        level = "red"
        level_label = "红色预警"
        title = "全市场宽度踩踏"
        summary = "下跌范围、等权跌幅与下跌成交额同时达到极端区间。"
        action_label = "强提醒：结果仅作观察，暂停追涨；选股功能仍可正常使用。"
    elif orange:
        level = "orange"
        level_label = "橙色预警"
        title = "全市场普跌"
        summary = "多数股票与成交额同步走弱，风险正在跨行业扩散。"
        action_label = "强提醒：以观察和控制节奏为主；选股功能仍可正常使用。"
    elif yellow:
        level = "yellow"
        level_label = "黄色预警"
        title = "市场宽度快速转弱"
        summary = "下跌家数和弱势成交额开始扩散，需防范进一步恶化。"
        action_label = "提醒：降低追高冲动，继续观察后续完整快照。"
    else:
        return base

    evidence = [
        f"下跌 {down_count} / {observed_total} 只（{_percent(down_ratio)}）",
        f"全市场平均涨跌 {avg_pct_chg:+.2f}%",
        f"下跌股占方向成交额 {_percent(down_amount_ratio)}",
    ]
    if strong_down_ratio is not None:
        evidence.append(
            f"跌幅不低于 5%：{strong_down_count} 只（{_percent(strong_down_ratio)}）"
        )
    if amount_weighted_pct_chg is not None:
        evidence.append(f"成交额加权涨跌 {amount_weighted_pct_chg:+.2f}%")
    if limit_up_count or limit_down_count:
        evidence.append(f"涨停 / 跌停 {limit_up_count} / {limit_down_count}")

    base.update(
        {
            "active": True,
            "level": level,
            "level_label": level_label,
            "title": title,
            "summary": summary,
            "action_label": action_label,
            "evidence": evidence,
        }
    )
    return base
