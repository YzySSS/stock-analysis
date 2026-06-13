from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from app.shared.db import mysql_conn


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return number


def _round_price(value: float) -> float:
    return round(float(value), 3)


def _moving_average(values: list[float], days: int) -> Optional[float]:
    if len(values) < days:
        return None
    return sum(values[-days:]) / days


def _atr(rows: list[dict[str, Any]], days: int = 14) -> Optional[float]:
    if len(rows) < 2:
        return None
    true_ranges: list[float] = []
    prev_close = _to_float(rows[0].get("close"))
    for row in rows[1:]:
        high = _to_float(row.get("high"))
        low = _to_float(row.get("low"))
        if high is None or low is None or prev_close is None:
            prev_close = _to_float(row.get("close")) or prev_close
            continue
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = _to_float(row.get("close")) or prev_close
    if not true_ranges:
        return None
    return sum(true_ranges[-days:]) / min(len(true_ranges), days)


def _fetch_technical_rows(code: str | None, trade_date: str | None = None) -> list[dict[str, Any]]:
    if not code:
        return []
    sql = """
        SELECT trade_date, open, high, low, close, amount
        FROM daily_kline
        WHERE code = %s
    """
    params: list[Any] = [code]
    if trade_date:
        sql += " AND trade_date <= %s"
        params.append(trade_date)
    sql += " ORDER BY trade_date DESC LIMIT 40"
    try:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
    except Exception:
        return []
    return list(reversed(rows or []))


def _technical_context(item: Dict[str, Any], raw_metrics: Dict[str, Any], entry_price: float) -> Dict[str, Any]:
    code = item.get("code") or raw_metrics.get("code")
    trade_date = raw_metrics.get("selected_price_trade_date") or raw_metrics.get("trade_date") or item.get("trade_date")
    rows = _fetch_technical_rows(str(code), str(trade_date) if trade_date else None) if code else []
    closes = [_to_float(row.get("close")) for row in rows]
    closes = [value for value in closes if value is not None]
    highs = [_to_float(row.get("high")) for row in rows]
    highs = [value for value in highs if value is not None]
    lows = [_to_float(row.get("low")) for row in rows]
    lows = [value for value in lows if value is not None]

    ma5 = _moving_average(closes, 5)
    ma10 = _moving_average(closes, 10)
    ma20 = _moving_average(closes, 20) or _to_float(raw_metrics.get("ma20"))
    high20 = max(highs[-20:]) if highs else _to_float(raw_metrics.get("realtime_high") or raw_metrics.get("high"))
    low20 = min(lows[-20:]) if lows else _to_float(raw_metrics.get("realtime_low") or raw_metrics.get("low"))
    atr14 = _atr(rows, 14)
    if atr14 is None:
        today_high = _to_float(raw_metrics.get("realtime_high") or raw_metrics.get("high"))
        today_low = _to_float(raw_metrics.get("realtime_low") or raw_metrics.get("low"))
        if today_high is not None and today_low is not None and today_high > today_low:
            atr14 = today_high - today_low
    atr14 = atr14 or max(entry_price * 0.03, 0.01)

    return {
        "ma5": _round_price(ma5) if ma5 is not None else None,
        "ma10": _round_price(ma10) if ma10 is not None else None,
        "ma20": _round_price(ma20) if ma20 is not None else None,
        "high20": _round_price(high20) if high20 is not None else None,
        "low20": _round_price(low20) if low20 is not None else None,
        "atr14": _round_price(atr14),
        "trade_date": str(rows[-1].get("trade_date")) if rows else trade_date,
        "source": "daily_kline" if rows else "raw_metrics_fallback",
    }


def _valid_below(values: list[Optional[float]], anchor: float) -> list[float]:
    return sorted({value for value in values if value is not None and 0 < value < anchor})


def _valid_above(values: list[Optional[float]], anchor: float) -> list[float]:
    return sorted({value for value in values if value is not None and value > anchor})


def _build_technical_levels(
    entry_price: float,
    *,
    strategy_id: str,
    technical: Dict[str, Any],
    raw_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    atr = _to_float(technical.get("atr14")) or max(entry_price * 0.03, 0.01)
    ma5 = _to_float(technical.get("ma5"))
    ma10 = _to_float(technical.get("ma10"))
    ma20 = _to_float(technical.get("ma20"))
    high20 = _to_float(technical.get("high20"))
    low20 = _to_float(technical.get("low20"))
    chip_low = _to_float(raw_metrics.get("chip_his_low"))
    chip_high = _to_float(raw_metrics.get("chip_his_high"))
    intraday_low = _to_float(raw_metrics.get("realtime_low") or raw_metrics.get("low"))
    intraday_high = _to_float(raw_metrics.get("realtime_high") or raw_metrics.get("high"))

    support_candidates = _valid_below([ma5, ma10, ma20, low20, chip_low, intraday_low], entry_price)
    resistance_candidates = _valid_above([ma5, ma10, ma20, high20, chip_high, intraday_high], entry_price)

    nearest_support = support_candidates[-1] if support_candidates else None
    first_resistance = resistance_candidates[0] if resistance_candidates else None
    second_resistance = resistance_candidates[1] if len(resistance_candidates) > 1 else None

    if nearest_support is not None:
        stop_loss = nearest_support - atr * 0.35
        stop_reason = f"止损放在最近技术支撑 {nearest_support:.3f} 下方，预留 ATR 缓冲"
    else:
        stop_loss = entry_price - atr * (1.25 if strategy_id == "a_share_sentiment" else 1.5)
        stop_reason = "支撑位不足，使用 ATR 波动止损兜底"

    min_stop_gap = atr * 0.75
    if entry_price - stop_loss < min_stop_gap:
        stop_loss = entry_price - min_stop_gap
        stop_reason += "；原止损过近，按 ATR 最小安全距离下移"
    stop_loss = max(stop_loss, 0.01)

    if first_resistance is not None:
        tp1 = first_resistance - atr * 0.15
        tp1_reason = f"第一止盈参考上方最近压力 {first_resistance:.3f}"
    else:
        tp1 = entry_price + atr * (1.15 if strategy_id == "a_share_sentiment" else 1.35)
        tp1_reason = "上方压力不足，使用 ATR 目标位"
    min_tp1_gap = atr * (0.85 if strategy_id == "a_share_sentiment" else 1.0)
    if tp1 - entry_price < min_tp1_gap:
        tp1 = entry_price + min_tp1_gap
        tp1_reason += "；压力位过近，按 ATR 最小目标位上移"

    if second_resistance is not None and second_resistance > tp1:
        tp2 = second_resistance - atr * 0.1
        tp2_reason = f"第二止盈参考下一压力 {second_resistance:.3f}"
    else:
        tp2 = max(tp1 + atr * 1.1, entry_price + atr * (2.0 if strategy_id == "a_share_sentiment" else 2.4))
        tp2_reason = "第二压力不足，使用 ATR 扩展目标"
    min_tp2_gap = atr * (0.9 if strategy_id == "a_share_sentiment" else 1.1)
    if tp2 - tp1 < min_tp2_gap:
        tp2 = tp1 + min_tp2_gap
        tp2_reason += "；与第一止盈过近，按 ATR 扩展第二目标"

    entry_anchor_values = [value for value in [ma5, ma10, nearest_support] if value is not None and value > 0]
    entry_low = min(entry_anchor_values) - atr * 0.15 if entry_anchor_values else entry_price - atr * 0.45
    entry_high = max(entry_price, min(entry_price + atr * 0.15, tp1 - atr * 0.25))
    if entry_low > entry_high:
        entry_low = entry_price - atr * 0.35
    if stop_loss >= entry_low:
        stop_loss = max(entry_low - atr * 0.25, 0.01)
        stop_reason += "；止损需低于建议买入区间，按 ATR 缓冲下移"

    return {
        "entry_low": max(_round_price(entry_low), 0.01),
        "entry_high": max(_round_price(entry_high), 0.01),
        "stop_loss": _round_price(stop_loss),
        "take_profit_1": _round_price(tp1),
        "take_profit_2": _round_price(tp2),
        "stop_reason": stop_reason,
        "take_profit_1_reason": tp1_reason,
        "take_profit_2_reason": tp2_reason,
        "support_candidates": [_round_price(value) for value in support_candidates[-4:]],
        "resistance_candidates": [_round_price(value) for value in resistance_candidates[:4]],
    }


def _price_from_item(item: Dict[str, Any], raw_metrics: Optional[Dict[str, Any]] = None) -> Optional[float]:
    raw_metrics = raw_metrics or {}
    candidates = [
        raw_metrics.get("selected_price"),
        item.get("selected_price"),
        item.get("realtime_price"),
        raw_metrics.get("realtime_price"),
        item.get("latest_price"),
        raw_metrics.get("latest_price"),
        item.get("close"),
        raw_metrics.get("close"),
    ]
    for value in candidates:
        price = _to_float(value)
        if price is not None:
            return price
    return None


def build_selection_trade_plan(
    item: Dict[str, Any],
    *,
    strategy_id: str,
    raw_metrics: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Build a lightweight trade plan for a saved selection result.

    V1 deliberately uses deterministic technical levels so the plan is stable
    after the selection is saved. The tracker can later judge whether the
    planned exit was touched without asking an LLM again.
    """
    raw_metrics = raw_metrics or {}
    entry_price = _price_from_item(item, raw_metrics)
    if entry_price is None:
        return None

    is_sentiment = strategy_id == "a_share_sentiment"
    technical = _technical_context(item, raw_metrics, entry_price)
    levels = _build_technical_levels(
        entry_price,
        strategy_id=strategy_id,
        technical=technical,
        raw_metrics=raw_metrics,
    )
    expire_trade_days = 5 if is_sentiment else 8

    trade_signal_state = raw_metrics.get("trade_signal_state") or item.get("trade_signal_state")
    trade_signal_label = raw_metrics.get("trade_signal_label") or item.get("trade_signal_label")
    trade_signal_reason = raw_metrics.get("trade_signal_reason") or item.get("trade_signal_reason")

    reasons = [
        "以入选价作为初始计划入场价，跟踪复盘从入选时间精确到秒开始计算",
        levels["stop_reason"],
        f"{levels['take_profit_1_reason']}，触发后视为本轮选股交易完成",
    ]
    if trade_signal_label:
        reasons.append(f"舆情交易状态：{trade_signal_label}{'，' + str(trade_signal_reason) if trade_signal_reason else ''}")

    return {
        "version": "selection_trade_plan_v2_technical",
        "strategy_id": strategy_id,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "entry_policy": "selection_price",
        "entry_price": _round_price(entry_price),
        "entry_zone": {
            "low": levels["entry_low"],
            "high": levels["entry_high"],
            "label": "建议买入区间",
        },
        "stop_loss": {
            "price": levels["stop_loss"],
            "pct_from_entry": round((levels["stop_loss"] - entry_price) / entry_price * 100, 2),
            "label": "计划止损",
            "reason": levels["stop_reason"],
        },
        "take_profit": [
            {
                "level": 1,
                "price": levels["take_profit_1"],
                "pct_from_entry": round((levels["take_profit_1"] - entry_price) / entry_price * 100, 2),
                "label": "第一止盈",
                "reason": levels["take_profit_1_reason"],
            },
            {
                "level": 2,
                "price": levels["take_profit_2"],
                "pct_from_entry": round((levels["take_profit_2"] - entry_price) / entry_price * 100, 2),
                "label": "第二止盈",
                "reason": levels["take_profit_2_reason"],
            },
        ],
        "technical": technical,
        "technical_levels": {
            "support_candidates": levels["support_candidates"],
            "resistance_candidates": levels["resistance_candidates"],
        },
        "expire_trade_days": expire_trade_days,
        "completion_rule": "入选后先触及第一止盈或计划止损，即视为本轮选股交易完成并冻结收益口径",
        "trade_signal_state": trade_signal_state,
        "trade_signal_label": trade_signal_label,
        "trade_signal_reason": trade_signal_reason,
        "reasons": reasons,
    }
