from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional


PRICE_WEAK_TERMS = (
    "价格确认偏弱",
    "价格确认弱",
    "走势未确认",
    "盘中未翻红",
    "水下观察",
    "承接确认不足",
)

LOW_MATCH_TERMS = (
    "个股匹配度较低",
    "股票识别度较低",
    "识别度较低",
    "低辨识度",
    "弱匹配",
    "低匹配",
)


@dataclass(frozen=True)
class ReviewRuleConfig:
    repeat_loss_window_days: int = 30
    price_confirm_weak_threshold: float = 35.0
    price_confirm_watch_threshold: float = 45.0
    low_match_recognition_threshold: float = 58.0


def _to_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _text_blob(values: Iterable[Any]) -> str:
    parts = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            parts.extend(str(item) for item in value if item is not None)
        elif isinstance(value, dict):
            parts.extend(str(item) for item in value.values() if item is not None)
        else:
            parts.append(str(value))
    return " | ".join(parts)


def _has_any_term(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def evaluate_review_rules(
    *,
    factor_scores: Dict[str, Any] | None = None,
    sentiment_context: Dict[str, Any] | None = None,
    reason_summary: Iterable[Any] | None = None,
    risk_summary: Iterable[Any] | None = None,
    prior_selection_date: str | None = None,
    prior_return_pct: float | None = None,
    config: ReviewRuleConfig | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Evaluate post-review rules for A-share sentiment selections.

    These rules are intentionally small and data-driven. They are suitable for
    review/backtest scripts first; production callers can later reuse the same
    labels as score downgrades or hard filters.
    """

    config = config or ReviewRuleConfig()
    factor_scores = factor_scores or {}
    sentiment_context = sentiment_context or {}
    text = _text_blob(
        [
            reason_summary or [],
            risk_summary or [],
            factor_scores.get("trade_signal_reason"),
            factor_scores.get("trade_signal_label"),
            sentiment_context.get("trade_signal_reason"),
            sentiment_context.get("trade_signal_label"),
            sentiment_context.get("opinion_match_reason"),
            sentiment_context.get("stock_recognition_label"),
            sentiment_context.get("stock_recognition_reason"),
        ]
    )

    hits: Dict[str, Dict[str, Any]] = {}

    if prior_return_pct is not None and prior_return_pct < 0:
        hits["repeat_prev_loss"] = {
            "action": "cooldown_or_downgrade",
            "severity": "high",
            "reason": f"同一股票前次入选到本次入选前仍为亏损 {prior_return_pct:.2f}%",
            "prior_selection_date": prior_selection_date,
            "prior_return_pct": round(prior_return_pct, 4),
            "window_days": config.repeat_loss_window_days,
        }

    price_confirm = _to_float(factor_scores.get("price_confirm"))
    intraday_confirm = _to_float(factor_scores.get("intraday_confirm"))
    trade_state = (
        factor_scores.get("trade_signal_state")
        or sentiment_context.get("trade_signal_state")
        or ""
    )
    price_is_weak = False
    price_reasons = []
    if price_confirm is not None and price_confirm < config.price_confirm_weak_threshold:
        price_is_weak = True
        price_reasons.append(f"价格确认分 {price_confirm:.2f} < {config.price_confirm_weak_threshold:.2f}")
    if trade_state == "weak":
        price_is_weak = True
        price_reasons.append("交易状态为走势未确认")
    if (
        trade_state == "watch"
        and price_confirm is not None
        and price_confirm < config.price_confirm_watch_threshold
    ):
        price_is_weak = True
        price_reasons.append(
            f"观察状态且价格确认分 {price_confirm:.2f} < {config.price_confirm_watch_threshold:.2f}"
        )
    if _has_any_term(text, PRICE_WEAK_TERMS):
        price_is_weak = True
        price_reasons.append("说明文本命中价格/承接偏弱")
    if price_is_weak:
        hits["price_confirm_weak"] = {
            "action": "score_downgrade",
            "severity": "medium",
            "reason": "；".join(dict.fromkeys(price_reasons)) or "价格确认偏弱",
            "price_confirm": price_confirm,
            "intraday_confirm": intraday_confirm,
            "trade_signal_state": trade_state or None,
        }

    recognition_score = _to_float(
        sentiment_context.get("stock_recognition_score")
        or factor_scores.get("opinion_stock_recognition_score")
        or factor_scores.get("stock_recognition")
    )
    recognition_label = str(
        sentiment_context.get("stock_recognition_label")
        or factor_scores.get("opinion_stock_recognition_label")
        or ""
    )
    match_type = str(
        sentiment_context.get("opinion_match_type")
        or factor_scores.get("opinion_match_type")
        or ""
    )
    low_match = False
    low_match_reasons = []
    if recognition_score is not None and recognition_score < config.low_match_recognition_threshold:
        low_match = True
        low_match_reasons.append(
            f"个股辨识度 {recognition_score:.2f} < {config.low_match_recognition_threshold:.2f}"
        )
    if "低辨识度" in recognition_label:
        low_match = True
        low_match_reasons.append("板块内辨识度标签为低辨识度")
    if _has_any_term(text, LOW_MATCH_TERMS):
        low_match = True
        low_match_reasons.append("说明文本命中低匹配/低识别度")
    if low_match:
        hits["low_match"] = {
            "action": "exclude_or_strong_downgrade",
            "severity": "high",
            "reason": "；".join(dict.fromkeys(low_match_reasons)) or "个股匹配度/识别度偏低",
            "recognition_score": recognition_score,
            "recognition_label": recognition_label or None,
            "opinion_match_type": match_type or None,
        }

    return hits
