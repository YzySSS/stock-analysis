from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timedelta
from decimal import InvalidOperation
from pathlib import Path
from typing import Any

import requests

from app.orchestration.portfolio_schema import ensure_portfolio_schema
from app.shared.db import mysql_conn


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, InvalidOperation):
        return None
    return number if math.isfinite(number) else None


def _round(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def _round_price(value: float | None) -> float | None:
    return _round(value, 3)


def _pct(current: float | None, base: float | None) -> float | None:
    if current is None or base is None or base <= 0:
        return None
    return (current - base) / base * 100


def normalize_stock_code(raw_code: str) -> str:
    code = (raw_code or "").strip().lower()
    if not code:
        return code
    if "." in code:
        left, right = code.split(".", 1)
        if left in {"sh", "sz", "bj"}:
            return f"{left}.{right.zfill(6)}"
        if right in {"sh", "sz", "bj"}:
            return f"{right}.{left.zfill(6)}"
    digits = "".join(ch for ch in code if ch.isdigit())
    if len(digits) != 6:
        return code
    if digits.startswith(("5", "6", "9")):
        return f"sh.{digits}"
    if digits.startswith(("8", "4")):
        return f"bj.{digits}"
    return f"sz.{digits}"


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parents[2] / ".env"
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


def _extract_json_object(text: str) -> dict:
    text = (text or "").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    if fenced:
        return json.loads(fenced.group(1))
    match = re.search(r"\{.*\}", text, re.S)
    if match:
        return json.loads(match.group(0))
    raise ValueError("DeepSeek response did not contain JSON")


def _to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).replace("T", " ").split(".")[0]
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _strategy_label(strategy_id: str) -> str:
    labels = {
        "long_term": "长期",
        "short_term": "短期",
        "swing": "波段",
        # Legacy values kept for already-saved positions.
        "a_share_sentiment": "短期",
        "leader_tactics": "短期",
        "lowvol_reversal": "波段",
        "quality_lowvol": "波段",
    }
    return labels.get(strategy_id, strategy_id)


def _decision_level_from_plan(plan: dict[str, Any] | None) -> str:
    action = (plan or {}).get("action")
    if action == "stop_loss":
        return "critical_exit"
    if action == "reduce":
        return "reduce"
    if action in {"add_watch"}:
        return "add_allowed"
    if action in {"hold", "watch", "take_profit_1", "take_profit_2"}:
        return "hold_watch"
    if action == "wait":
        return "data_insufficient"
    return "no_action"


def _strategy_profile(strategy_id: str | None, max_loss_pct: float | None = None) -> dict[str, Any]:
    normalized = strategy_id or "short_term"
    if normalized in {"a_share_sentiment", "leader_tactics"}:
        normalized = "short_term"
    elif normalized in {"lowvol_reversal", "quality_lowvol"}:
        normalized = "swing"
    defaults = {
        "short_term": {"label": "短期", "max_days": 5, "max_loss_pct": 5.0, "chase_ma": "ma5", "chase_gap_pct": 6.0},
        "swing": {"label": "波段", "max_days": 28, "max_loss_pct": 10.0, "chase_ma": "ma20", "chase_gap_pct": 10.0},
        "long_term": {"label": "长期", "max_days": 180, "max_loss_pct": 18.0, "chase_ma": "ma20", "chase_gap_pct": 16.0},
    }
    profile = dict(defaults.get(normalized, defaults["short_term"]))
    profile["strategy_id"] = normalized
    if max_loss_pct is not None and max_loss_pct > 0:
        profile["max_loss_pct"] = max_loss_pct
    return profile


def _days_held(value: Any) -> int | None:
    if not value:
        return None
    buy_time = _parse_datetime(value)
    if not buy_time:
        return None
    return max((datetime.now() - buy_time).days, 0)


OUTCOME_HORIZONS = (1, 3, 5, 20)


def _advice_ttl_minutes(now: datetime | None = None) -> int:
    now = now or datetime.now()
    intraday_minutes = int(os.getenv("PORTFOLIO_ADVICE_INTRADAY_TTL_MINUTES", "15"))
    offhour_minutes = int(os.getenv("PORTFOLIO_ADVICE_OFFHOUR_TTL_MINUTES", "240"))
    if now.weekday() < 5 and now.replace(hour=9, minute=15, second=0, microsecond=0) <= now <= now.replace(hour=15, minute=15, second=0, microsecond=0):
        return intraday_minutes
    return offhour_minutes


def _moving_average(values: list[float], days: int) -> float | None:
    if len(values) < days:
        return None
    return sum(values[-days:]) / days


def _atr(rows: list[dict[str, Any]], days: int = 14) -> float | None:
    if len(rows) < 2:
        return None
    true_ranges: list[float] = []
    prev_close = _to_float(rows[0].get("close"))
    for row in rows[1:]:
        high = _to_float(row.get("high"))
        low = _to_float(row.get("low"))
        close = _to_float(row.get("close"))
        if high is None or low is None or prev_close is None:
            prev_close = close
            continue
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = close
    if len(true_ranges) < min(days, 5):
        return None
    sample = true_ranges[-days:]
    return sum(sample) / len(sample)


def _technical_summary(rows: list[dict[str, Any]], current_price: float | None) -> dict[str, Any]:
    closes = [_to_float(row.get("close")) for row in rows]
    closes = [value for value in closes if value is not None and value > 0]
    highs = [_to_float(row.get("high")) for row in rows]
    highs = [value for value in highs if value is not None and value > 0]
    lows = [_to_float(row.get("low")) for row in rows]
    lows = [value for value in lows if value is not None and value > 0]
    current = current_price or (closes[-1] if closes else None)
    ma5 = _moving_average(closes, 5)
    ma10 = _moving_average(closes, 10)
    ma20 = _moving_average(closes, 20)
    high20 = max(highs[-20:]) if highs else None
    low20 = min(lows[-20:]) if lows else None
    atr14 = _atr(rows, 14)

    position_20d = None
    if current is not None and high20 is not None and low20 is not None and high20 > low20:
        position_20d = (current - low20) / (high20 - low20) * 100

    trend_score = 50.0
    if current is not None:
        if ma5 is not None:
            trend_score += 12 if current >= ma5 else -12
        if ma10 is not None:
            trend_score += 10 if current >= ma10 else -10
        if ma20 is not None:
            trend_score += 10 if current >= ma20 else -10
        if len(closes) >= 6 and closes[-6] > 0:
            trend_score += max(min((current - closes[-6]) / closes[-6] * 100, 12), -12)
    trend_score = max(0, min(100, trend_score))
    if trend_score >= 72:
        trend_label = "偏强"
    elif trend_score >= 58:
        trend_label = "温和偏强"
    elif trend_score >= 44:
        trend_label = "震荡"
    else:
        trend_label = "偏弱"

    return {
        "ma5": _round_price(ma5),
        "ma10": _round_price(ma10),
        "ma20": _round_price(ma20),
        "atr14": _round_price(atr14),
        "high20": _round_price(high20),
        "low20": _round_price(low20),
        "position_20d_pct": _round(position_20d),
        "trend_score": _round(trend_score, 1),
        "trend_label": trend_label,
    }


def _fetch_etf_history(code: str, days: int = 90) -> list[dict[str, Any]]:
    digits = "".join(ch for ch in str(code or "") if ch.isdigit())
    if len(digits) != 6:
        return []
    try:
        import akshare as ak

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")
        df = ak.fund_etf_hist_em(
            symbol=digits,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
    except Exception:
        return []
    if df is None or getattr(df, "empty", True):
        return []

    rows: list[dict[str, Any]] = []
    for _, raw in df.tail(days).iterrows():
        rows.append(
            {
                "trade_date": str(raw.get("日期")) if raw.get("日期") is not None else None,
                "open": _to_float(raw.get("开盘")),
                "high": _to_float(raw.get("最高")),
                "low": _to_float(raw.get("最低")),
                "close": _to_float(raw.get("收盘")),
                "volume": _to_float(raw.get("成交量")),
                "amount": _to_float(raw.get("成交额")),
                "pct_chg": _to_float(raw.get("涨跌幅")),
                "change_amount": _to_float(raw.get("涨跌额")),
            }
        )
    return [row for row in rows if row.get("close") is not None]

def _build_trade_plan(position: dict[str, Any], market: dict[str, Any]) -> dict[str, Any]:
    strategy_id = position.get("strategy_id") or "short_term"
    cost = _to_float(position.get("cost_price"))
    current = market.get("current_price")
    technical = market.get("technical") or {}
    return_pct = market.get("return_pct")
    pct_chg = _to_float((market.get("quote") or {}).get("pct_chg"))
    sentiment = _to_float((market.get("sentiment") or {}).get("sentiment_score"))
    fund_net = _to_float((market.get("moneyflow") or {}).get("net_amount"))
    ma5 = _to_float(technical.get("ma5"))
    ma10 = _to_float(technical.get("ma10"))
    ma20 = _to_float(technical.get("ma20"))
    atr14 = _to_float(technical.get("atr14"))
    high20 = _to_float(technical.get("high20"))
    low20 = _to_float(technical.get("low20"))
    max_loss_pct = _to_float(position.get("max_loss_pct")) or 5.0
    is_short = strategy_id in {"short_term", "a_share_sentiment", "leader_tactics"}
    is_swing = strategy_id in {"swing", "lowvol_reversal", "quality_lowvol"}

    if current is None or cost is None or cost <= 0:
        return {
            "action": "wait",
            "action_label": "等待行情",
            "risk_level": "unknown",
            "entry_zone": None,
            "stop_loss": None,
            "take_profit": [],
            "reason": ["暂无有效实时价或成本价，无法生成交易计划。"],
            "invalid_conditions": [],
        }

    atr = atr14 or max(current * 0.035, 0.01)
    hard_stop = cost * (1 - max_loss_pct / 100)
    stop_multiplier = 1.2 if is_short else 1.6 if is_swing else 2.2
    technical_stop = current - atr * stop_multiplier
    stop_loss = min(current * 0.985, max(hard_stop, technical_stop))
    if ma20 is not None and current >= ma20:
        stop_loss = max(stop_loss, ma20 * 0.985)
    if low20 is not None and current >= low20:
        stop_loss = max(stop_loss, low20 * 0.99)
    stop_loss = min(stop_loss, current * 0.99)

    tp1_pct = 1.06 if is_short else 1.08 if is_swing else 1.12
    tp2_pct = 1.12 if is_short else 1.16 if is_swing else 1.25
    tp1_atr = 0.8 if is_short else 1.1 if is_swing else 1.6
    tp2_atr = 1.6 if is_short else 2.2 if is_swing else 3.2
    tp1 = max(cost * tp1_pct, current + atr * tp1_atr)
    tp2 = max(cost * tp2_pct, current + atr * tp2_atr)
    trailing_stop = max(stop_loss, current - atr * 1.1) if return_pct is not None and return_pct > 5 else None

    entry_low = None
    entry_high = None
    if is_short:
        if ma5 is not None and ma10 is not None and current >= ma10:
            entry_low = min(ma5, ma10) * 0.995
            entry_high = max(ma5, ma10) * 1.01
        else:
            entry_low = current - atr * 0.8
            entry_high = current - atr * 0.25
    elif is_swing:
        anchor = ma20 or low20 or current
        entry_low = min(anchor, current - atr * 0.8)
        entry_high = min(current, anchor + atr * 0.3)
    else:
        anchor = ma20 or ma10 or current
        entry_low = min(anchor * 0.98, current - atr * 1.0)
        entry_high = min(current, anchor * 1.01)

    support_candidates = [
        (stop_loss, "系统止损位"),
        (low20, "20日低点"),
        (ma20, "MA20"),
        (ma10, "MA10"),
        (ma5, "MA5"),
        (current - atr, "当前价-ATR"),
    ]
    resistance_candidates = [
        (high20, "20日高点"),
        (ma5 if ma5 and ma5 > current else None, "MA5"),
        (ma10 if ma10 and ma10 > current else None, "MA10"),
        (ma20 if ma20 and ma20 > current else None, "MA20"),
        (current + atr, "当前价+ATR"),
        (tp1, "止盈1"),
    ]
    support_levels = [
        {"price": _round_price(value), "reason": reason}
        for value, reason in sorted(
            ((value, reason) for value, reason in support_candidates if value is not None and value < current),
            key=lambda item: abs(current - item[0]),
        )[:3]
    ]
    resistance_levels = [
        {"price": _round_price(value), "reason": reason}
        for value, reason in sorted(
            ((value, reason) for value, reason in resistance_candidates if value is not None and value > current),
            key=lambda item: abs(item[0] - current),
        )[:3]
    ]

    reason: list[str] = []
    risk_flags: list[str] = []
    action = "hold"
    action_label = "继续持有"
    risk_level = "normal"

    if return_pct is not None and return_pct <= -max_loss_pct:
        action = "stop_loss"
        action_label = "触发止损"
        risk_level = "high"
        reason.append(f"持仓收益 {return_pct:.2f}%，已低于设定亏损阈值 {-max_loss_pct:.1f}%。")
    elif current <= stop_loss:
        action = "stop_loss"
        action_label = "跌破止损"
        risk_level = "high"
        reason.append("当前价已跌破系统止损位。")
    elif ma20 is not None and current < ma20 and (return_pct or 0) < 0:
        action = "reduce"
        action_label = "减仓观察"
        risk_level = "elevated"
        reason.append("当前价跌破 MA20 且持仓处于亏损，短线结构偏弱。")
    elif current >= tp2:
        action = "take_profit_2"
        action_label = "触发止盈2"
        risk_level = "normal"
        reason.append("当前价达到第二止盈位，建议大幅降仓或退出。")
    elif current >= tp1 or (return_pct is not None and return_pct >= 8):
        action = "take_profit_1"
        action_label = "触发止盈1"
        risk_level = "normal"
        reason.append("持仓收益已进入第一止盈区，建议分批锁定利润。")
    elif ma5 is not None and ma10 is not None and current >= ma5 >= ma10:
        action = "hold"
        action_label = "继续持有"
        reason.append("价格站在 MA5/MA10 上方，短线趋势仍可继续跟踪。")
    elif entry_low is not None and current <= entry_high and current >= entry_low and (return_pct or 0) <= 3:
        action = "add_watch"
        action_label = "接近加仓区"
        reason.append("当前价接近策略回踩区，可等待资金和分时确认后小幅加仓。")
    else:
        action = "watch"
        action_label = "观察中"
        reason.append("当前未触发明确止盈、止损或加仓条件。")

    if is_short:
        if sentiment is not None and sentiment < 45:
            risk_flags.append("舆情热度偏弱")
        if fund_net is not None and fund_net < 0:
            risk_flags.append("实时资金净流出")
        if pct_chg is not None and pct_chg < -3:
            risk_flags.append("盘中跌幅超过 3%，避免补仓")
        if sentiment is not None and sentiment >= 65:
            reason.append("舆情分仍在较高区间，买入逻辑暂未完全失效。")
        if fund_net is not None and fund_net > 0:
            reason.append("实时资金流为净流入，对短线持仓有支撑。")

    if risk_flags and action == "hold":
        action = "watch"
        action_label = "持有观察"
        risk_level = "elevated"

    invalid_conditions = [
        f"跌破止损位 {_round_price(stop_loss)}",
        "舆情热度连续转弱且资金净流出" if is_short else "跌破关键均线且无法收回",
    ]

    return {
        "strategy_id": strategy_id,
        "strategy_label": _strategy_label(strategy_id),
        "action": action,
        "action_label": action_label,
        "risk_level": risk_level,
        "entry_zone": {"low": _round_price(entry_low), "high": _round_price(entry_high)} if entry_low is not None and entry_high is not None else None,
        "stop_loss": {"price": _round_price(stop_loss), "reason": "成本止损、ATR 与结构位综合"},
        "support_levels": support_levels,
        "resistance_levels": resistance_levels,
        "take_profit": [
            {"level": 1, "price": _round_price(tp1), "suggestion": "减仓 30%-50%"},
            {"level": 2, "price": _round_price(tp2), "suggestion": "保留底仓或退出"},
        ],
        "trailing_stop": _round_price(trailing_stop),
        "risk_reward_ratio": _round((tp1 - current) / max(current - stop_loss, 0.01), 2) if tp1 > current else None,
        "reason": reason[:5],
        "risk_flags": risk_flags,
        "invalid_conditions": invalid_conditions,
    }


def _build_discipline_alerts(item: dict[str, Any], ai_review: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    plan = item.get("trade_plan") or {}
    market = item.get("market") or {}
    quote = market.get("quote") or {}
    technical = market.get("technical") or {}
    current = _to_float(quote.get("latest_price")) or _to_float(market.get("current_price"))
    stop_loss = _to_float((plan.get("stop_loss") or {}).get("price"))
    return_pct = _to_float(item.get("return_pct"))
    max_loss_pct = _to_float(item.get("max_loss_pct"))
    profile = _strategy_profile(item.get("strategy_id"), max_loss_pct)
    days_held = _days_held(item.get("buy_datetime"))
    action = plan.get("action")
    ai_status = (ai_review or {}).get("status")
    ai_level = (ai_review or {}).get("decision_level") if ai_status == "ok" else None
    alerts: list[dict[str, Any]] = []

    def add(level: str, alert_type: str, title: str, detail: str, source: str = "local_rule") -> None:
        alerts.append(
            {
                "level": level,
                "type": alert_type,
                "title": title,
                "detail": detail,
                "source": source,
            }
        )

    if current is None:
        add("warning", "data_insufficient", "行情数据不足", "缺少有效实时价，系统不能给出完整纪律判断。")
    if stop_loss is None:
        add("warning", "data_insufficient", "止损位不可用", "本地规则未能计算有效止损位，请先检查 K 线或成本价数据。")

    if current is not None and stop_loss is not None:
        if current <= stop_loss:
            add("critical", "stop_loss_breached", "已跌破纪律止损", f"当前价 {current:.3f} 已低于止损位 {stop_loss:.3f}，应优先处理风险。")
        elif current <= stop_loss * 1.015:
            gap_pct = (current - stop_loss) / stop_loss * 100
            add("warning", "near_stop_loss", "接近纪律止损", f"当前价距离止损位约 {gap_pct:.2f}%，盘中需重点盯防。")

    if action == "reduce":
        reasons = plan.get("reason") or []
        detail = str(reasons[0]) if reasons else "本地纪律规则建议降低仓位，先控制风险再等待结构修复。"
        add("warning", "reduce_suggested", "触发减仓纪律", detail)

    if return_pct is not None:
        threshold = float(profile["max_loss_pct"])
        if return_pct <= -threshold:
            add("critical", "loss_limit_breached", "亏损超过策略阈值", f"{profile['label']}策略默认亏损阈值为 {threshold:.1f}%，当前收益 {return_pct:.2f}%。")
        elif return_pct <= -threshold * 0.8:
            add("warning", "near_loss_limit", "接近亏损阈值", f"当前亏损已接近 {profile['label']}策略纪律线，避免情绪化补仓。")

    if days_held is not None and days_held > int(profile["max_days"]):
        add("warning", "holding_period_exceeded", "持仓周期超出策略定义", f"{profile['label']}策略参考周期 {profile['max_days']} 天，当前已持有 {days_held} 天。")

    chase_ma = _to_float(technical.get(profile["chase_ma"]))
    if current is not None and chase_ma is not None and chase_ma > 0:
        gap_pct = (current - chase_ma) / chase_ma * 100
        if gap_pct >= float(profile["chase_gap_pct"]):
            add("info", "chase_risk", "价格偏离均线较远", f"当前价高于 {profile['chase_ma'].upper()} 约 {gap_pct:.2f}%，不宜情绪化追高。")

    if ai_level == "critical_exit" and action not in {"stop_loss"}:
        add("critical", "ai_rule_conflict", "AI 与本地规则出现分歧", "AI 给出强制止损，但本地硬规则尚未触发，请优先查看详情核对输入快照。", "ai_review")
    elif ai_level == "reduce" and action in {"hold", "add_watch"}:
        add("warning", "ai_rule_conflict", "AI 建议偏谨慎", "AI 建议减仓，但本地规则仍偏持有或观察，建议复核风险项。", "ai_review")
    elif ai_level == "data_insufficient":
        add("warning", "ai_data_insufficient", "AI 判断数据不足", "当前 AI 建议认为输入数据不足，不应把结论当成可执行信号。", "ai_review")

    severity_rank = {"critical": 0, "warning": 1, "info": 2}
    return sorted(alerts, key=lambda alert: (severity_rank.get(alert["level"], 9), alert["type"]))[:6]


class PortfolioService:
    def __init__(self) -> None:
        ensure_portfolio_schema()

    def list_positions(self, include_inactive: bool = False) -> dict[str, Any]:
        where = "" if include_inactive else "WHERE p.is_active = 1"
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT p.*
                    FROM portfolio_position p
                    {where}
                    ORDER BY p.is_active DESC, p.updated_at DESC, p.id DESC
                    """
                )
                rows = cursor.fetchall()
        positions = [self._enrich_position(row) for row in rows]
        self._attach_cached_ai_reviews(positions)
        self._attach_advice_outcomes(positions)
        return {"summary": self._summary(positions), "positions": positions}

    def create_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        code = normalize_stock_code(str(payload.get("code") or ""))
        if not code:
            raise ValueError("股票代码不能为空")
        cost_price = _to_float(payload.get("cost_price"))
        quantity = int(payload.get("quantity") or 0)
        if cost_price is None or cost_price <= 0:
            raise ValueError("成本价必须大于 0")
        if quantity <= 0:
            raise ValueError("持仓数量必须大于 0")
        buy_datetime = payload.get("buy_datetime") or datetime.now().replace(microsecond=0).isoformat(sep=" ")
        strategy_id = str(payload.get("strategy_id") or "short_term")
        target_style = str(payload.get("target_style") or "short_swing")
        max_loss_pct = _to_float(payload.get("max_loss_pct"))
        note = str(payload.get("note") or "").strip() or None

        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                stock = self._stock_basic(cursor, code)
                if not stock:
                    raise ValueError(f"未找到股票代码: {code}")
                cursor.execute(
                    """
                    INSERT INTO portfolio_position
                        (code, name, strategy_id, cost_price, quantity, buy_datetime, target_style, max_loss_pct, note)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        code,
                        stock.get("name"),
                        strategy_id,
                        cost_price,
                        quantity,
                        buy_datetime,
                        target_style,
                        max_loss_pct,
                        note,
                    ),
                )
                position_id = cursor.lastrowid
        return self.get_position(int(position_id))

    def update_position(self, position_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        allowed = {"strategy_id", "cost_price", "quantity", "buy_datetime", "target_style", "max_loss_pct", "note", "is_active"}
        updates = {key: value for key, value in payload.items() if key in allowed}
        if not updates:
            return self.get_position(position_id)
        if "cost_price" in updates and (_to_float(updates["cost_price"]) is None or _to_float(updates["cost_price"]) <= 0):
            raise ValueError("成本价必须大于 0")
        if "quantity" in updates and int(updates["quantity"] or 0) <= 0:
            raise ValueError("持仓数量必须大于 0")

        sets = ", ".join(f"{key} = %s" for key in updates)
        values = list(updates.values())
        values.append(position_id)
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"UPDATE portfolio_position SET {sets} WHERE id = %s", values)
                if cursor.rowcount == 0:
                    raise LookupError(f"持仓不存在: {position_id}")
        self._invalidate_advice(position_id, "持仓信息已调整，需要重新生成建议")
        return self.get_position(position_id)

    def delete_position(self, position_id: int) -> dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE portfolio_position SET is_active = 0 WHERE id = %s", (position_id,))
                if cursor.rowcount == 0:
                    raise LookupError(f"持仓不存在: {position_id}")
        self._invalidate_advice(position_id, "持仓已删除")
        return {"id": position_id, "is_active": False}

    def get_position(self, position_id: int) -> dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM portfolio_position WHERE id = %s LIMIT 1", (position_id,))
                row = cursor.fetchone()
                if not row:
                    raise LookupError(f"持仓不存在: {position_id}")
        return self._enrich_position(row)

    def create_advice_refresh_run(self, position_id: int, force: bool = False) -> dict[str, Any]:
        position = self.get_position(position_id)
        cached = self._latest_valid_advice(position_id)
        if cached and not force:
            stale_reason = self._advice_stale_reason(position, cached)
            if stale_reason:
                self._expire_advice_run(int(cached["id"]), stale_reason)
            else:
                return self._advice_run_payload(cached, queued=False)
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO portfolio_advice_run
                        (position_id, code, status, decision_level, prompt_version, input_snapshot_json)
                    VALUES (%s, %s, 'queued', %s, %s, %s)
                    """,
                    (
                        position_id,
                        position.get("code"),
                        _decision_level_from_plan(position.get("trade_plan")),
                        self._prompt_version(),
                        _to_json(self._input_snapshot(position)),
                    ),
                )
                run_id = cursor.lastrowid
                cursor.execute("SELECT * FROM portfolio_advice_run WHERE id = %s", (run_id,))
                row = cursor.fetchone()
        return self._advice_run_payload(row, queued=True)

    def refresh_advice_run(self, run_id: int) -> dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM portfolio_advice_run WHERE id = %s LIMIT 1", (run_id,))
                run = cursor.fetchone()
                if not run:
                    raise LookupError(f"持仓建议任务不存在: {run_id}")
                cursor.execute(
                    """
                    UPDATE portfolio_advice_run
                    SET status='running', started_at=%s, error_message=NULL
                    WHERE id=%s
                    """,
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), run_id),
                )
        try:
            position = self.get_position(int(run["position_id"]))
            review, raw_response, model = self._generate_ai_review(position)
            expires_at = datetime.now() + timedelta(minutes=_advice_ttl_minutes())
            with mysql_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE portfolio_advice_run
                        SET status='succeeded',
                            decision_level=%s,
                            model_name=%s,
                            prompt_version=%s,
                            input_snapshot_json=%s,
                            raw_response=%s,
                            parsed_review_json=%s,
                            error_message=NULL,
                            expires_at=%s,
                            finished_at=%s
                        WHERE id=%s
                        """,
                        (
                            review.get("decision_level") or _decision_level_from_plan(position.get("trade_plan")),
                            model,
                            self._prompt_version(),
                            _to_json(self._input_snapshot(position)),
                            raw_response,
                            _to_json(review),
                            expires_at.strftime("%Y-%m-%d %H:%M:%S"),
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            run_id,
                        ),
                    )
                    cursor.execute("SELECT * FROM portfolio_advice_run WHERE id = %s", (run_id,))
                    row = cursor.fetchone()
            return self._advice_run_payload(row, queued=False)
        except Exception as exc:
            with mysql_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE portfolio_advice_run
                        SET status='failed', error_message=%s, finished_at=%s
                        WHERE id=%s
                        """,
                        (str(exc)[:500], datetime.now().strftime("%Y-%m-%d %H:%M:%S"), run_id),
                    )
                    cursor.execute("SELECT * FROM portfolio_advice_run WHERE id = %s", (run_id,))
                    row = cursor.fetchone()
            return self._advice_run_payload(row, queued=False)

    def evaluate_advice_outcomes(self, limit: int = 100, force: bool = False) -> dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM portfolio_advice_run
                    WHERE status = 'succeeded'
                      AND finished_at IS NOT NULL
                    ORDER BY finished_at DESC, id DESC
                    LIMIT %s
                    """,
                    (max(1, int(limit)),),
                )
                runs = cursor.fetchall()

        stats = {"runs": len(runs), "created_or_updated": 0, "skipped": 0, "errors": 0, "details": []}
        for run in runs:
            try:
                result = self._evaluate_single_advice_run(run, force=force)
                stats["created_or_updated"] += int(result.get("created_or_updated") or 0)
                stats["skipped"] += int(result.get("skipped") or 0)
                if result.get("created_or_updated") or result.get("error"):
                    stats["details"].append(result)
            except Exception as exc:
                stats["errors"] += 1
                stats["details"].append({"advice_run_id": int(run.get("id")), "error": str(exc)[:300]})
        return stats

    def _evaluate_single_advice_run(self, run: dict[str, Any], force: bool = False) -> dict[str, Any]:
        advice_run_id = int(run["id"])
        snapshot = _json_loads(run.get("input_snapshot_json")) or {}
        if not isinstance(snapshot, dict):
            return {"advice_run_id": advice_run_id, "created_or_updated": 0, "skipped": len(OUTCOME_HORIZONS), "error": "missing input snapshot"}

        base_price = _to_float(snapshot.get("realtime_price"))
        quote_time = snapshot.get("quote_time") or run.get("finished_at") or run.get("created_at")
        base_trade_date = self._base_trade_date(quote_time)
        if base_price is None or base_price <= 0 or not base_trade_date:
            return {"advice_run_id": advice_run_id, "created_or_updated": 0, "skipped": len(OUTCOME_HORIZONS), "error": "missing base price or trade date"}

        rows = self._future_kline_rows(str(run.get("code")), base_trade_date, max(OUTCOME_HORIZONS))
        if not rows:
            return {"advice_run_id": advice_run_id, "created_or_updated": 0, "skipped": len(OUTCOME_HORIZONS), "error": "no future kline"}

        existing = set()
        if not force:
            with mysql_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT horizon_days
                        FROM portfolio_advice_outcome
                        WHERE advice_run_id = %s
                        """,
                        (advice_run_id,),
                    )
                    existing = {int(row["horizon_days"]) for row in cursor.fetchall()}

        created_or_updated = 0
        skipped = 0
        for horizon in OUTCOME_HORIZONS:
            if horizon in existing:
                skipped += 1
                continue
            if len(rows) < horizon:
                skipped += 1
                continue
            outcome = self._build_outcome_payload(run, snapshot, rows[:horizon], horizon, base_price, base_trade_date)
            self._upsert_advice_outcome(outcome)
            created_or_updated += 1

        return {"advice_run_id": advice_run_id, "code": run.get("code"), "created_or_updated": created_or_updated, "skipped": skipped}

    def _base_trade_date(self, value: Any) -> str | None:
        parsed = _parse_datetime(value)
        if parsed:
            return parsed.date().isoformat()
        raw = str(value or "").strip()
        if len(raw) >= 10:
            return raw[:10]
        return None

    def _future_kline_rows(self, code: str, base_trade_date: str, limit: int) -> list[dict[str, Any]]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, open, high, low, close
                    FROM daily_kline
                    WHERE code = %s
                      AND trade_date > %s
                    ORDER BY trade_date ASC
                    LIMIT %s
                    """,
                    (code, base_trade_date, int(limit)),
                )
                return cursor.fetchall()

    def _build_outcome_payload(
        self,
        run: dict[str, Any],
        snapshot: dict[str, Any],
        rows: list[dict[str, Any]],
        horizon: int,
        base_price: float,
        base_trade_date: str,
    ) -> dict[str, Any]:
        latest = rows[-1]
        latest_price = _to_float(latest.get("close"))
        highs = [_to_float(row.get("high")) for row in rows]
        lows = [_to_float(row.get("low")) for row in rows]
        highs = [value for value in highs if value is not None]
        lows = [value for value in lows if value is not None]
        max_gain_pct = _pct(max(highs), base_price) if highs else None
        max_drawdown_pct = _pct(min(lows), base_price) if lows else None
        return_pct = _pct(latest_price, base_price)

        local_plan = snapshot.get("local_trade_plan") or {}
        stop_loss = _to_float((local_plan.get("stop_loss") or {}).get("price"))
        take_profit_prices = [_to_float(item.get("price")) for item in (local_plan.get("take_profit") or []) if isinstance(item, dict)]
        take_profit_prices = [value for value in take_profit_prices if value is not None]
        support_prices = [_to_float(item.get("price")) for item in (local_plan.get("support_levels") or []) if isinstance(item, dict)]
        support_prices = [value for value in support_prices if value is not None]
        resistance_prices = [_to_float(item.get("price")) for item in (local_plan.get("resistance_levels") or []) if isinstance(item, dict)]
        resistance_prices = [value for value in resistance_prices if value is not None]

        low_min = min(lows) if lows else None
        high_max = max(highs) if highs else None
        stop_loss_touched = bool(stop_loss is not None and low_min is not None and low_min <= stop_loss)
        take_profit_touched = bool(take_profit_prices and high_max is not None and high_max >= min(take_profit_prices))
        support_broken = bool(support_prices and low_min is not None and low_min <= min(support_prices))
        resistance_broken = bool(resistance_prices and high_max is not None and high_max >= min(resistance_prices))
        decision_level = run.get("decision_level") or (snapshot.get("ai_review") or {}).get("decision_level")
        outcome_label, quality_score = self._score_outcome(
            str(decision_level or "no_action"),
            return_pct,
            max_gain_pct,
            max_drawdown_pct,
            stop_loss_touched,
            take_profit_touched,
            support_broken,
            resistance_broken,
        )

        evidence = {
            "start_trade_date": base_trade_date,
            "end_trade_date": str(latest.get("trade_date")) if latest.get("trade_date") else None,
            "observed_trade_days": len(rows),
            "stop_loss": _round_price(stop_loss),
            "take_profit_prices": [_round_price(value) for value in take_profit_prices[:2]],
            "support_prices": [_round_price(value) for value in support_prices[:3]],
            "resistance_prices": [_round_price(value) for value in resistance_prices[:3]],
        }
        return {
            "advice_run_id": int(run["id"]),
            "position_id": int(run["position_id"]),
            "code": run.get("code"),
            "decision_level": decision_level,
            "base_price": _round_price(base_price),
            "base_trade_date": base_trade_date,
            "evaluate_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "horizon_days": horizon,
            "latest_price": _round_price(latest_price),
            "return_pct": _round(return_pct, 4),
            "max_gain_pct": _round(max_gain_pct, 4),
            "max_drawdown_pct": _round(max_drawdown_pct, 4),
            "stop_loss_touched": int(stop_loss_touched),
            "take_profit_touched": int(take_profit_touched),
            "support_broken": int(support_broken),
            "resistance_broken": int(resistance_broken),
            "outcome_label": outcome_label,
            "quality_score": _round(quality_score, 4),
            "evidence_json": _to_json(evidence),
        }

    def _score_outcome(
        self,
        decision_level: str,
        return_pct: float | None,
        max_gain_pct: float | None,
        max_drawdown_pct: float | None,
        stop_loss_touched: bool,
        take_profit_touched: bool,
        support_broken: bool,
        resistance_broken: bool,
    ) -> tuple[str, float | None]:
        if decision_level == "data_insufficient":
            return "data_insufficient", None
        if return_pct is None:
            return "data_insufficient", None

        drawdown = max_drawdown_pct or 0.0
        gain = max_gain_pct or return_pct
        risk_hit = stop_loss_touched or support_broken or drawdown <= -4.0
        upside_hit = take_profit_touched or resistance_broken or gain >= 4.0

        if decision_level == "critical_exit":
            if risk_hit or return_pct <= -1.0:
                return "hit", min(100.0, 75.0 + abs(min(return_pct, drawdown, 0.0)) * 3)
            if return_pct >= 3.0 or upside_hit:
                return "miss", max(0.0, 35.0 - return_pct * 4)
            return "neutral", 55.0

        if decision_level == "reduce":
            if risk_hit or return_pct <= 0:
                return "hit", min(95.0, 68.0 + abs(min(return_pct, drawdown, 0.0)) * 2.5)
            if return_pct >= 4.0 or upside_hit:
                return "miss", max(10.0, 45.0 - return_pct * 3)
            return "neutral", 55.0

        if decision_level == "add_allowed":
            if return_pct >= 2.0 or upside_hit:
                return "hit", min(100.0, 70.0 + max(return_pct, gain, 0.0) * 2)
            if risk_hit or return_pct <= -3.0:
                return "miss", max(0.0, 40.0 + return_pct * 5)
            return "neutral", 55.0

        if decision_level in {"hold_watch", "no_action"}:
            if risk_hit:
                return "miss", max(5.0, 45.0 + drawdown * 3)
            if return_pct >= 0 and drawdown > -4.0:
                return "hit", min(88.0, 62.0 + min(return_pct, 6.0) * 2)
            return "neutral", 55.0

        return "neutral", 50.0

    def _upsert_advice_outcome(self, outcome: dict[str, Any]) -> None:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO portfolio_advice_outcome
                        (advice_run_id, position_id, code, decision_level, base_price, base_trade_date,
                         evaluate_at, horizon_days, latest_price, return_pct, max_gain_pct, max_drawdown_pct,
                         stop_loss_touched, take_profit_touched, support_broken, resistance_broken,
                         outcome_label, quality_score, evidence_json)
                    VALUES
                        (%(advice_run_id)s, %(position_id)s, %(code)s, %(decision_level)s, %(base_price)s, %(base_trade_date)s,
                         %(evaluate_at)s, %(horizon_days)s, %(latest_price)s, %(return_pct)s, %(max_gain_pct)s, %(max_drawdown_pct)s,
                         %(stop_loss_touched)s, %(take_profit_touched)s, %(support_broken)s, %(resistance_broken)s,
                         %(outcome_label)s, %(quality_score)s, %(evidence_json)s)
                    ON DUPLICATE KEY UPDATE
                        decision_level = VALUES(decision_level),
                        base_price = VALUES(base_price),
                        base_trade_date = VALUES(base_trade_date),
                        evaluate_at = VALUES(evaluate_at),
                        latest_price = VALUES(latest_price),
                        return_pct = VALUES(return_pct),
                        max_gain_pct = VALUES(max_gain_pct),
                        max_drawdown_pct = VALUES(max_drawdown_pct),
                        stop_loss_touched = VALUES(stop_loss_touched),
                        take_profit_touched = VALUES(take_profit_touched),
                        support_broken = VALUES(support_broken),
                        resistance_broken = VALUES(resistance_broken),
                        outcome_label = VALUES(outcome_label),
                        quality_score = VALUES(quality_score),
                        evidence_json = VALUES(evidence_json)
                    """,
                    outcome,
                )

    def _stock_basic(self, cursor, code: str) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT code, name, industry, instrument_type, is_st, is_delisted
            FROM stock_basic
            WHERE code = %s
            LIMIT 1
            """,
            (code,),
        )
        return cursor.fetchone()

    def _enrich_position(self, row: dict[str, Any]) -> dict[str, Any]:
        code = row.get("code")
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                basic = self._stock_basic(cursor, code) or {}
                cursor.execute(
                    """
                    SELECT latest_price, pct_chg, change_amount, pre_close, high_price, low_price,
                           amount, trade_date, quote_time, updated_at, source
                    FROM stock_realtime_snapshot
                    WHERE code = %s
                    LIMIT 1
                    """,
                    (code,),
                )
                quote = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT trade_date, open, high, low, close, volume, amount
                    FROM daily_kline
                    WHERE code = %s
                    ORDER BY trade_date DESC
                    LIMIT 60
                    """,
                    (code,),
                )
                history = list(reversed(cursor.fetchall()))
                cursor.execute(
                    """
                    SELECT trade_date, sentiment_score, news_count, filtered_news_count, credibility_avg, quality_avg
                    FROM stock_sentiment_daily
                    WHERE code = %s
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    (code,),
                )
                sentiment = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT trade_date, quote_time, net_amount, amount, pct_chg, turnover_rate
                    FROM stock_realtime_moneyflow_snapshot
                    WHERE code = %s
                    LIMIT 1
                    """,
                    (code,),
                )
                moneyflow = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT trade_date, his_low, his_high, cost_5pct, cost_15pct, cost_50pct,
                           cost_85pct, cost_95pct, weight_avg, winner_rate
                    FROM stock_chip_daily
                    WHERE code = %s
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    (code,),
                )
                chip = cursor.fetchone() or {}

        if (
            basic.get("instrument_type") == "etf"
            and not history
            and os.getenv("PORTFOLIO_ENABLE_SYNC_ETF_HISTORY_FALLBACK") == "1"
        ):
            history = _fetch_etf_history(code)
            if history and not quote:
                latest = history[-1]
                quote = {
                    "latest_price": latest.get("close"),
                    "pct_chg": latest.get("pct_chg"),
                    "change_amount": latest.get("change_amount"),
                    "pre_close": history[-2].get("close") if len(history) >= 2 else None,
                    "open_price": latest.get("open"),
                    "high_price": latest.get("high"),
                    "low_price": latest.get("low"),
                    "amount": latest.get("amount"),
                    "trade_date": latest.get("trade_date"),
                    "quote_time": None,
                    "source": "akshare_fund_etf_hist_em",
                }

        cost = _to_float(row.get("cost_price"))
        quantity = int(row.get("quantity") or 0)
        latest_price = _to_float(quote.get("latest_price"))
        quote_from_daily_close = False
        if latest_price is None or latest_price <= 0:
            latest_daily = history[-1] if history else None
            latest_price = _to_float(latest_daily.get("close")) if latest_daily else None
            quote_from_daily_close = latest_price is not None and latest_price > 0
        cost_amount = cost * quantity if cost is not None else None
        market_value = latest_price * quantity if latest_price is not None else None
        pnl_amount = (market_value - cost_amount) if market_value is not None and cost_amount is not None else None
        return_pct = _pct(latest_price, cost)
        technical = _technical_summary(history, latest_price)
        latest_daily = history[-1] if history else {}
        previous_daily = history[-2] if len(history) >= 2 else {}
        daily_pre_close = _to_float(previous_daily.get("close"))
        daily_pct_chg = _pct(_to_float(latest_daily.get("close")), daily_pre_close)
        market = {
            "current_price": latest_price,
            "return_pct": return_pct,
            "quote": {
                "latest_price": _round_price(latest_price),
                "pct_chg": _to_float(quote.get("pct_chg")) if not quote_from_daily_close else _round(daily_pct_chg),
                "change_amount": _to_float(quote.get("change_amount")) if not quote_from_daily_close else _round(_to_float(latest_daily.get("close")) - daily_pre_close, 4) if daily_pre_close is not None else None,
                "pre_close": _to_float(quote.get("pre_close")) if not quote_from_daily_close else _round_price(daily_pre_close),
                "high_price": _to_float(quote.get("high_price")) if not quote_from_daily_close else _round_price(_to_float(latest_daily.get("high"))),
                "low_price": _to_float(quote.get("low_price")) if not quote_from_daily_close else _round_price(_to_float(latest_daily.get("low"))),
                "amount": _to_float(quote.get("amount")) if not quote_from_daily_close else _to_float(latest_daily.get("amount")),
                "trade_date": str(quote.get("trade_date")) if quote.get("trade_date") else str(latest_daily.get("trade_date")) if quote_from_daily_close and latest_daily.get("trade_date") else None,
                "quote_time": str(quote.get("quote_time")) if quote.get("quote_time") else str(latest_daily.get("trade_date")) if quote_from_daily_close and latest_daily.get("trade_date") else None,
                "source": quote.get("source") if quote else "daily_kline_close" if quote_from_daily_close else None,
            },
            "technical": technical,
            "sentiment": {
                "trade_date": str(sentiment.get("trade_date")) if sentiment.get("trade_date") else None,
                "sentiment_score": _to_float(sentiment.get("sentiment_score")),
                "news_count": int(sentiment.get("news_count") or 0),
                "filtered_news_count": int(sentiment.get("filtered_news_count") or 0),
                "credibility_avg": _to_float(sentiment.get("credibility_avg")),
                "quality_avg": _to_float(sentiment.get("quality_avg")),
            },
            "moneyflow": {
                "trade_date": str(moneyflow.get("trade_date")) if moneyflow.get("trade_date") else None,
                "quote_time": str(moneyflow.get("quote_time")) if moneyflow.get("quote_time") else None,
                "net_amount": _to_float(moneyflow.get("net_amount")),
                "amount": _to_float(moneyflow.get("amount")),
                "pct_chg": _to_float(moneyflow.get("pct_chg")),
                "turnover_rate": _to_float(moneyflow.get("turnover_rate")),
            },
            "chip": {
                "trade_date": str(chip.get("trade_date")) if chip.get("trade_date") else None,
                "his_low": _round_price(_to_float(chip.get("his_low"))),
                "his_high": _round_price(_to_float(chip.get("his_high"))),
                "cost_5pct": _round_price(_to_float(chip.get("cost_5pct"))),
                "cost_15pct": _round_price(_to_float(chip.get("cost_15pct"))),
                "cost_50pct": _round_price(_to_float(chip.get("cost_50pct"))),
                "cost_85pct": _round_price(_to_float(chip.get("cost_85pct"))),
                "cost_95pct": _round_price(_to_float(chip.get("cost_95pct"))),
                "weight_avg": _round_price(_to_float(chip.get("weight_avg"))),
                "winner_rate": _round(_to_float(chip.get("winner_rate")), 2),
            },
        }
        trade_plan = _build_trade_plan(row, market)

        item = {
            "id": int(row.get("id")),
            "code": code,
            "name": row.get("name") or basic.get("name"),
            "industry": basic.get("industry"),
            "strategy_id": row.get("strategy_id"),
            "strategy_label": _strategy_label(row.get("strategy_id") or ""),
            "cost_price": _round_price(cost),
            "quantity": quantity,
            "buy_datetime": str(row.get("buy_datetime")) if row.get("buy_datetime") else None,
            "target_style": row.get("target_style"),
            "max_loss_pct": _to_float(row.get("max_loss_pct")),
            "note": row.get("note"),
            "is_active": bool(row.get("is_active")),
            "created_at": str(row.get("created_at")) if row.get("created_at") else None,
            "updated_at": str(row.get("updated_at")) if row.get("updated_at") else None,
            "cost_amount": _round(cost_amount),
            "market_value": _round(market_value),
            "pnl_amount": _round(pnl_amount),
            "return_pct": _round(return_pct),
            "market": market,
            "trade_plan": trade_plan,
        }
        item["discipline_alerts"] = _build_discipline_alerts(item)
        return item

    def _prompt_version(self) -> str:
        return "portfolio_advice_v20260612_p1"

    def _input_snapshot(self, item: dict[str, Any]) -> dict[str, Any]:
        market = item.get("market") or {}
        return {
            "id": item.get("id"),
            "code": item.get("code"),
            "name": item.get("name"),
            "strategy": item.get("strategy_label") or item.get("strategy_id"),
            "cost_price": item.get("cost_price"),
            "quantity": item.get("quantity"),
            "buy_datetime": item.get("buy_datetime"),
            "return_pct": item.get("return_pct"),
            "realtime_price": (market.get("quote") or {}).get("latest_price"),
            "realtime_pct_chg": (market.get("quote") or {}).get("pct_chg"),
            "quote_time": (market.get("quote") or {}).get("quote_time") or (market.get("quote") or {}).get("trade_date"),
            "technical": market.get("technical"),
            "chip": market.get("chip"),
            "moneyflow": market.get("moneyflow"),
            "sentiment": market.get("sentiment"),
            "local_trade_plan": item.get("trade_plan"),
        }

    def _latest_valid_advice(self, position_id: int) -> dict[str, Any] | None:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM portfolio_advice_run
                    WHERE position_id = %s
                      AND status = 'succeeded'
                      AND expires_at IS NOT NULL
                      AND expires_at > NOW()
                    ORDER BY expires_at DESC, id DESC
                    LIMIT 1
                    """,
                    (position_id,),
                )
                return cursor.fetchone()

    def _advice_stale_reason(self, item: dict[str, Any], row: dict[str, Any]) -> str | None:
        if row.get("status") != "succeeded":
            return None
        expires_at = _parse_datetime(row.get("expires_at"))
        if expires_at and expires_at <= datetime.now():
            return "AI 建议已超过有效期，需要重新生成"

        snapshot = _json_loads(row.get("input_snapshot_json")) or {}
        if not isinstance(snapshot, dict):
            return None
        current_snapshot = self._input_snapshot(item)
        current_price = _to_float(current_snapshot.get("realtime_price"))
        previous_price = _to_float(snapshot.get("realtime_price"))
        if current_price is None:
            return None

        current_plan = current_snapshot.get("local_trade_plan") or {}
        previous_plan = snapshot.get("local_trade_plan") or {}
        current_stop = _to_float((current_plan.get("stop_loss") or {}).get("price"))
        if current_stop is not None and current_price <= current_stop and (previous_price is None or previous_price > current_stop):
            return f"实时价 {current_price:.3f} 已跌破止损位 {current_stop:.3f}"

        for level in previous_plan.get("resistance_levels") or []:
            resistance = _to_float((level or {}).get("price"))
            if resistance is not None and previous_price is not None and previous_price < resistance <= current_price:
                return f"实时价 {current_price:.3f} 已突破旧建议压力位 {resistance:.3f}"

        if previous_price is not None and previous_price > 0:
            price_change_pct = abs((current_price - previous_price) / previous_price * 100)
            threshold = float(os.getenv("PORTFOLIO_ADVICE_PRICE_CHANGE_REFRESH_PCT", "2.5"))
            if price_change_pct >= threshold:
                return f"实时价较旧建议快照变化 {price_change_pct:.2f}%"

        current_pct_chg = _to_float(current_snapshot.get("realtime_pct_chg"))
        previous_pct_chg = _to_float(snapshot.get("realtime_pct_chg"))
        if current_pct_chg is not None and previous_pct_chg is not None:
            pct_threshold = float(os.getenv("PORTFOLIO_ADVICE_INTRADAY_CHANGE_REFRESH_PCT", "3.0"))
            if abs(current_pct_chg - previous_pct_chg) >= pct_threshold:
                return f"盘中涨跌幅较旧建议快照变化 {abs(current_pct_chg - previous_pct_chg):.2f} 个百分点"

        current_moneyflow = current_snapshot.get("moneyflow") or {}
        previous_moneyflow = snapshot.get("moneyflow") or {}
        current_net = _to_float(current_moneyflow.get("net_amount"))
        previous_net = _to_float(previous_moneyflow.get("net_amount"))
        moneyflow_threshold = float(os.getenv("PORTFOLIO_ADVICE_MONEYFLOW_REFRESH_AMOUNT", "20000000"))
        if current_net is not None and previous_net is not None:
            if current_net * previous_net < 0 and abs(current_net - previous_net) >= moneyflow_threshold:
                return "实时资金流方向发生明显反转"
            if abs(current_net - previous_net) >= moneyflow_threshold * 2:
                return "实时资金流金额较旧建议快照明显变化"

        current_amount = _to_float(current_moneyflow.get("amount"))
        previous_amount = _to_float(previous_moneyflow.get("amount"))
        if current_amount is not None and previous_amount is not None and previous_amount > 0:
            if current_amount >= previous_amount * 1.5 and current_amount - previous_amount >= moneyflow_threshold:
                return "成交额较旧建议快照明显放大"

        return None

    def _expire_advice_run(self, run_id: int, reason: str) -> None:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE portfolio_advice_run
                    SET expires_at = LEAST(COALESCE(expires_at, NOW()), NOW()),
                        error_message = %s
                    WHERE id = %s
                    """,
                    (reason[:500], run_id),
                )

    def _attach_cached_ai_reviews(self, positions: list[dict[str, Any]]) -> None:
        if not positions:
            return
        ids = [int(item["id"]) for item in positions if item.get("id") is not None]
        if not ids:
            return
        placeholders = ", ".join(["%s"] * len(ids))
        latest_by_position: dict[int, dict[str, Any]] = {}
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT r.*
                    FROM portfolio_advice_run r
                    INNER JOIN (
                        SELECT position_id, MAX(id) AS id
                        FROM portfolio_advice_run
                        WHERE position_id IN ({placeholders})
                        GROUP BY position_id
                    ) latest ON latest.id = r.id
                    """,
                    ids,
                )
                for row in cursor.fetchall():
                    latest_by_position[int(row["position_id"])] = row

        for item in positions:
            row = latest_by_position.get(int(item.get("id")))
            if not row:
                item["ai_review"] = {
                    "status": "missing",
                    "message": "暂无 AI 持仓建议，可点击刷新分析。",
                    "decision_level": _decision_level_from_plan(item.get("trade_plan")),
                    "prompt_version": self._prompt_version(),
                }
                item["discipline_alerts"] = _build_discipline_alerts(item, item["ai_review"])
                continue
            stale_reason = self._advice_stale_reason(item, row)
            if stale_reason and row.get("status") == "succeeded":
                expires_dt = _parse_datetime(row.get("expires_at"))
                if not expires_dt or expires_dt > datetime.now():
                    self._expire_advice_run(int(row["id"]), stale_reason)
                    row["expires_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    row["error_message"] = stale_reason
            item["ai_review"] = self._review_from_run(row)
            item["discipline_alerts"] = _build_discipline_alerts(item, item["ai_review"])

    def _attach_advice_outcomes(self, positions: list[dict[str, Any]]) -> None:
        run_ids = [
            int((item.get("ai_review") or {}).get("run_id"))
            for item in positions
            if (item.get("ai_review") or {}).get("run_id") is not None
        ]
        if not run_ids:
            return
        placeholders = ", ".join(["%s"] * len(run_ids))
        outcomes_by_run: dict[int, list[dict[str, Any]]] = {}
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT *
                    FROM portfolio_advice_outcome
                    WHERE advice_run_id IN ({placeholders})
                    ORDER BY advice_run_id, horizon_days
                    """,
                    run_ids,
                )
                rows = cursor.fetchall()
        for row in rows:
            run_id = int(row["advice_run_id"])
            outcomes_by_run.setdefault(run_id, []).append(self._outcome_payload(row))

        for item in positions:
            ai_review = item.get("ai_review") or {}
            run_id = ai_review.get("run_id")
            if run_id is None:
                continue
            outcomes = outcomes_by_run.get(int(run_id), [])
            ai_review["outcomes"] = outcomes
            ai_review["outcome_summary"] = self._outcome_summary(outcomes)

    def _outcome_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        evidence = _json_loads(row.get("evidence_json")) or {}
        return {
            "id": int(row["id"]),
            "advice_run_id": int(row["advice_run_id"]),
            "horizon_days": int(row["horizon_days"]),
            "decision_level": row.get("decision_level"),
            "base_price": _round_price(_to_float(row.get("base_price"))),
            "base_trade_date": str(row.get("base_trade_date")) if row.get("base_trade_date") else None,
            "evaluate_at": str(row.get("evaluate_at")) if row.get("evaluate_at") else None,
            "latest_price": _round_price(_to_float(row.get("latest_price"))),
            "return_pct": _round(_to_float(row.get("return_pct")), 2),
            "max_gain_pct": _round(_to_float(row.get("max_gain_pct")), 2),
            "max_drawdown_pct": _round(_to_float(row.get("max_drawdown_pct")), 2),
            "stop_loss_touched": bool(row.get("stop_loss_touched")),
            "take_profit_touched": bool(row.get("take_profit_touched")),
            "support_broken": bool(row.get("support_broken")),
            "resistance_broken": bool(row.get("resistance_broken")),
            "outcome_label": row.get("outcome_label"),
            "quality_score": _round(_to_float(row.get("quality_score")), 1),
            "evidence": evidence,
        }

    def _outcome_summary(self, outcomes: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not outcomes:
            return None
        scored = [item for item in outcomes if item.get("quality_score") is not None]
        latest = outcomes[-1]
        labels = [item.get("outcome_label") for item in outcomes]
        return {
            "count": len(outcomes),
            "latest_horizon_days": latest.get("horizon_days"),
            "latest_label": latest.get("outcome_label"),
            "latest_return_pct": latest.get("return_pct"),
            "latest_quality_score": latest.get("quality_score"),
            "avg_quality_score": _round(sum(float(item["quality_score"]) for item in scored) / len(scored), 1) if scored else None,
            "hit_count": len([label for label in labels if label == "hit"]),
            "miss_count": len([label for label in labels if label == "miss"]),
        }

    def _review_from_run(self, row: dict[str, Any]) -> dict[str, Any]:
        status = row.get("status")
        parsed = _json_loads(row.get("parsed_review_json")) or {}
        expires_at = str(row.get("expires_at")) if row.get("expires_at") else None
        base = {
            "status": status,
            "model": row.get("model_name"),
            "prompt_version": row.get("prompt_version"),
            "decision_level": row.get("decision_level"),
            "expires_at": expires_at,
            "run_id": int(row.get("id")) if row.get("id") is not None else None,
            "created_at": str(row.get("created_at")) if row.get("created_at") else None,
            "updated_at": str(row.get("updated_at")) if row.get("updated_at") else None,
        }
        if status == "succeeded":
            expires_dt = _parse_datetime(row.get("expires_at"))
            if expires_dt and expires_dt <= datetime.now():
                return {
                    **base,
                    "status": "expired",
                    "source": "cache",
                    "summary": str(parsed.get("summary") or "")[:160],
                    "message": row.get("error_message") or "AI 持仓建议已过期，可点击刷新分析。",
                }
            return {
                **base,
                "status": "ok",
                "source": "cache",
                "summary": str(parsed.get("summary") or "")[:160],
                "analysis": [str(value)[:180] for value in (parsed.get("analysis") or [])[:5]],
                "risks": [str(value)[:180] for value in (parsed.get("risks") or [])[:5]],
                "operation_plan": str(parsed.get("operation_plan") or "")[:220],
                "confidence": _round(_to_float(parsed.get("confidence")), 2),
            }
        if status in {"queued", "running"}:
            return {**base, "message": "AI 持仓建议正在生成中。"}
        if status == "failed":
            return {**base, "message": f"AI 持仓建议生成失败：{row.get('error_message') or '-'}"}
        return {**base, "message": "暂无可用 AI 持仓建议。"}

    def _advice_run_payload(self, row: dict[str, Any] | None, queued: bool) -> dict[str, Any]:
        if not row:
            return {"status": "missing"}
        return {
            "id": int(row["id"]),
            "position_id": int(row["position_id"]),
            "code": row.get("code"),
            "status": row.get("status"),
            "queued": queued,
            "decision_level": row.get("decision_level"),
            "model": row.get("model_name"),
            "prompt_version": row.get("prompt_version"),
            "expires_at": str(row.get("expires_at")) if row.get("expires_at") else None,
            "error_message": row.get("error_message"),
            "created_at": str(row.get("created_at")) if row.get("created_at") else None,
            "updated_at": str(row.get("updated_at")) if row.get("updated_at") else None,
        }

    def _invalidate_advice(self, position_id: int, reason: str) -> None:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE portfolio_advice_run
                    SET expires_at = LEAST(COALESCE(expires_at, NOW()), NOW()),
                        error_message = COALESCE(error_message, %s)
                    WHERE position_id = %s
                      AND status = 'succeeded'
                      AND (expires_at IS NULL OR expires_at > NOW())
                    """,
                    (reason[:500], position_id),
                )

    def _generate_ai_review(self, item: dict[str, Any]) -> tuple[dict[str, Any], str, str]:
        _load_env_file()
        api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
        model = os.getenv("PORTFOLIO_DEEPSEEK_MODEL") or "deepseek-v4-pro"
        if not api_key:
            raise RuntimeError("未配置 DEEPSEEK_API_KEY 或 OPENAI_API_KEY")

        payload_items = [self._input_snapshot(item)]

        prompt = """
你是A股/ETF持仓管理助手。请严格基于我提供的持仓数据生成持仓建议，不要编造不存在的数据。
每条建议必须结合：实时价格、持仓成本、持仓策略、技术指标、筹码分布、资金流和本地系统给出的支撑/压力/止损/止盈。
支撑位、压力位、止损位必须优先使用 local_trade_plan、MA、ATR、20日高低点、筹码成本区这些实时指标，不要随意拍价格。
必须核对数值关系：如果 realtime_price 高于 stop_loss/support，就不能写“跌破止损/跌破支撑”；只能写“接近”“仍在上方”或“需观察是否跌破”。
长期策略偏重趋势和仓位耐心；波段策略偏重MA20/ATR/区间位置；短期策略偏重实时价格、资金流、舆情和MA5/MA10。
必须给出 decision_level，只能从以下值选择：
critical_exit / reduce / hold_watch / add_allowed / no_action / data_insufficient。

请输出严格JSON，不要Markdown：
{
  "items": [
    {
      "id": 1,
      "code": "必须原样复制输入股票代码",
      "summary": "一句话结论，不超过40字",
      "analysis": ["要点1", "要点2", "要点3"],
      "risks": ["风险1", "风险2"],
      "operation_plan": "具体操作建议，不超过60字",
      "decision_level": "hold_watch",
      "confidence": 0.0
    }
  ]
}

持仓数据：
""".strip() + "\n" + json.dumps(payload_items, ensure_ascii=False, default=str)

        response = requests.post(
            f"{(os.getenv('DEEPSEEK_BASE_URL') or os.getenv('OPENAI_BASE_URL') or 'https://api.deepseek.com/v1').rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
            },
            timeout=float(os.getenv("PORTFOLIO_DEEPSEEK_TIMEOUT", "90")),
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        parsed = _extract_json_object(content)
        review = None
        for candidate in parsed.get("items") or []:
            if candidate.get("code") and str(candidate.get("code")) == str(item.get("code")):
                review = candidate
                break
            if candidate.get("id") is not None and int(candidate.get("id")) == int(item.get("id")):
                review = candidate
                break
        if not review:
            raise RuntimeError("DeepSeek 未返回该持仓分析")
        if review.get("code") and str(review.get("code")) != str(item.get("code")):
            raise RuntimeError("DeepSeek 返回股票代码与请求不一致")
        level = review.get("decision_level") or _decision_level_from_plan(item.get("trade_plan"))
        allowed_levels = {"critical_exit", "reduce", "hold_watch", "add_allowed", "no_action", "data_insufficient"}
        if level not in allowed_levels:
            level = _decision_level_from_plan(item.get("trade_plan"))
        normalized = {
            "id": item.get("id"),
            "code": item.get("code"),
            "summary": str(review.get("summary") or "")[:160],
            "analysis": [str(value)[:180] for value in (review.get("analysis") or [])[:5]],
            "risks": [str(value)[:180] for value in (review.get("risks") or [])[:5]],
            "operation_plan": str(review.get("operation_plan") or "")[:220],
            "decision_level": level,
            "confidence": _round(_to_float(review.get("confidence")), 2),
        }
        return normalized, content, model

    def _summary(self, positions: list[dict[str, Any]]) -> dict[str, Any]:
        active = [item for item in positions if item.get("is_active")]
        cost_amount = sum(float(item.get("cost_amount") or 0) for item in active)
        market_value = sum(float(item.get("market_value") or 0) for item in active)
        pnl_amount = market_value - cost_amount if cost_amount or market_value else 0.0
        actions = [((item.get("trade_plan") or {}).get("action") or "watch") for item in active]
        alerts = [alert for item in active for alert in (item.get("discipline_alerts") or [])]
        risk_count = len([action for action in actions if action in {"stop_loss", "reduce"}])
        take_profit_count = len([action for action in actions if action in {"take_profit_1", "take_profit_2"}])
        add_watch_count = len([action for action in actions if action == "add_watch"])
        return {
            "count": len(active),
            "cost_amount": _round(cost_amount),
            "market_value": _round(market_value),
            "pnl_amount": _round(pnl_amount),
            "return_pct": _round(_pct(market_value, cost_amount)),
            "risk_count": risk_count,
            "take_profit_count": take_profit_count,
            "add_watch_count": add_watch_count,
            "critical_alert_count": len([alert for alert in alerts if alert.get("level") == "critical"]),
            "warning_alert_count": len([alert for alert in alerts if alert.get("level") == "warning"]),
            "data_alert_count": len([alert for alert in alerts if alert.get("type") in {"data_insufficient", "ai_data_insufficient"}]),
        }
