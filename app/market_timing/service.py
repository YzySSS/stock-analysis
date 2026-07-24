from __future__ import annotations

import json
from typing import Any

from app.market_timing.calibration import (
    ACTION_LABELS,
    MODEL_ID,
    MODEL_NAME,
    MODEL_VERSION,
    REALTIME_WEIGHTS,
    compose_timing_state,
)
from app.market_timing.scenario_forecast import MarketScenarioForecastRepository
from app.market_timing.v20 import MarketTimingV20Repository
from app.shared.db import mysql_conn


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _signal_label(signal: int) -> str:
    return {1: "偏多", 0: "中性", -1: "偏空"}.get(signal, "未知")


def _clamp(value: float, low: float = 0, high: float = 100) -> float:
    return max(low, min(high, value))


def _score_signal(value: float | None, bullish: float, bearish: float, *, reverse: bool = False) -> int:
    if value is None:
        return 0
    if reverse:
        if value <= bullish:
            return 1
        if value >= bearish:
            return -1
        return 0
    if value >= bullish:
        return 1
    if value <= bearish:
        return -1
    return 0


def _sector_net_amount_score(overview: dict[str, Any]) -> tuple[int, float | None, dict[str, float]]:
    strong = overview.get("strong_sectors") or []
    weak = overview.get("weak_sectors") or []

    positive = sum(max(_to_float(item.get("net_amount")) or 0, 0) for item in strong[:5])
    negative = sum(abs(min(_to_float(item.get("net_amount")) or 0, 0)) for item in weak[:5])
    total = positive + negative
    pressure = (positive - negative) / total if total else None

    signal = _score_signal(pressure, 0.18, -0.18)
    return signal, round(pressure, 4) if pressure is not None else None, {
        "positive_net_amount": round(positive, 4),
        "negative_net_amount": round(negative, 4),
    }


def _amount_pressure_score(pressure: float | None) -> tuple[int, float | None, str]:
    if pressure is None:
        return 0, None, "-"
    score = _clamp(50 + pressure * 90)
    return _score_signal(score, 60, 40), round(score, 1), f"{pressure * 100:.1f}%"


def _signal_score_from_signal(signal: int, neutral_score: float = 50) -> float:
    return {1: 75.0, 0: neutral_score, -1: 25.0}.get(signal, neutral_score)


def _limit_emotion_score(limit_up: float, limit_down: float) -> tuple[int, float, str]:
    if limit_up + limit_down <= 0:
        return 0, 50.0, "0/0"
    signal = 1 if limit_up >= limit_down * 1.8 else -1 if limit_down >= limit_up * 1.4 else 0
    score = _clamp(50 + ((limit_up - limit_down) / max(limit_up + limit_down, 1)) * 45)
    return signal, round(score, 1), f"{int(limit_up)}/{int(limit_down)}"


def _intraday_breadth_score(up_ratio: float | None, amount_pressure: float | None) -> tuple[int, float | None, str]:
    if up_ratio is None or amount_pressure is None:
        return 0, None, "-"
    breadth_seed = up_ratio * 0.62 + (amount_pressure + 1) / 2 * 0.38
    signal = _score_signal(breadth_seed, 0.58, 0.43)
    score = _clamp(breadth_seed * 100)
    return signal, round(score, 1), f"上涨 {up_ratio * 100:.1f}% / 额压 {amount_pressure * 100:.1f}%"


def _json_loads(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


def _attach_research_layers(signal: dict[str, Any]) -> dict[str, Any]:
    result = dict(signal)
    try:
        result["shadow_v20"] = MarketTimingV20Repository().latest()
    except Exception:
        result["shadow_v20"] = None
    try:
        result["scenario_forecast"] = MarketScenarioForecastRepository().latest()
    except Exception:
        result["scenario_forecast"] = None
    return result


def _latest_stored_timing_signal(index_code: str = "000300.SH") -> dict[str, Any] | None:
    try:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM market_timing_signal_daily
                    WHERE index_code = %s
                      AND model_id IN ('huatai_multidim_v19', 'huatai_multidim_v18')
                    ORDER BY trade_date DESC,
                             FIELD(model_id, 'huatai_multidim_v19', 'huatai_multidim_v18')
                    LIMIT 1
                    """,
                    (index_code,),
                )
                signal_row = cursor.fetchone()
                if not signal_row:
                    return None
                cursor.execute(
                    """
                    SELECT *
                    FROM market_timing_indicator_daily
                    WHERE trade_date = %s AND index_code = %s AND model_id = %s
                    ORDER BY FIELD(indicator_id,
                        'index_bollinger',
                        'multi_index_trend',
                        'index_pe_percentile',
                        'erp',
                        'margin_buy_ratio',
                        'option_pcr',
                        'qvix_volatility',
                        'iv_skew',
                        'futures_holding_net',
                        'up_down_amount_pressure'
                    ), indicator_id
                    """,
                    (signal_row.get("trade_date"), index_code, signal_row.get("model_id")),
                )
                indicator_rows = cursor.fetchall() or []
    except Exception:
        return None

    coverage = _json_loads(signal_row.get("coverage_json"), {})
    reasons = _json_loads(signal_row.get("reasons_json"), [])
    risk_notes = _json_loads(signal_row.get("risk_notes_json"), [])
    signals = []
    article_dimensions = {
        "index_bollinger": "技术/指数布林带",
        "multi_index_trend": "技术/多指数趋势",
        "index_pe_percentile": "估值/指数 PE 分位",
        "erp": "估值/ERP 风险溢价",
        "margin_buy_ratio": "资金/融资买入额",
        "option_pcr": "情绪/期权 PCR",
        "qvix_volatility": "情绪/QVIX 波动率代理",
        "iv_skew": "情绪/自算 IV 偏斜",
        "futures_holding_net": "情绪/股指期货会员持仓",
        "up_down_amount_pressure": "情绪/上涨下跌成交额差",
        "intraday_market_strength": "盘中/市场强度",
        "intraday_breadth": "盘中/涨跌扩散",
        "intraday_limit_emotion": "盘中/涨跌停情绪",
    }
    for row in indicator_rows:
        signals.append(
            {
                "dimension": row.get("dimension"),
                "label": row.get("indicator_name") or row.get("indicator_id"),
                "article_dimension": article_dimensions.get(row.get("indicator_id"), row.get("dimension")),
                "indicator_id": row.get("indicator_id"),
                "signal": int(row.get("signal_value") or 0),
                "signal_label": row.get("signal_label") or _signal_label(int(row.get("signal_value") or 0)),
                "score": _to_float(row.get("score")),
                "value": _to_float(row.get("value")),
                "value_label": row.get("value_label") or "-",
                "source_status": row.get("source_status") or "未知",
                "source": row.get("source"),
                "meta": _json_loads(row.get("metadata_json"), {}),
            }
        )

    position_upper = _to_float(signal_row.get("position_upper"))
    state = signal_row.get("state") or "cautious"
    state_label = signal_row.get("state_label") or "谨慎试探"
    action_label = {
        **ACTION_LABELS,
    }.get(state, "等待更多择时因子确认")

    coverage_items = [
        {"dimension": "技术", "factor": "指数布林带", "status": coverage.get("index_daily", "待数据"), "reason": "Tushare index_daily"},
        {"dimension": "技术", "factor": "多指数趋势确认", "status": coverage.get("multi_index_trend", "待数据"), "reason": "沪深300 + 中证1000 + 科创50"},
        {"dimension": "估值", "factor": "指数 PE 分位", "status": coverage.get("index_dailybasic", "待数据"), "reason": "Tushare index_dailybasic"},
        {"dimension": "估值", "factor": "ERP/风险溢价", "status": coverage.get("bond_yield_10y", coverage.get("yc_cb", "待数据")), "reason": "Tushare yc_cb 或 AkShare 中债 10 年收益率"},
        {"dimension": "资金", "factor": "融资买入额", "status": coverage.get("margin", "待数据"), "reason": "Tushare margin"},
        {"dimension": "情绪", "factor": "期权 PCR", "status": coverage.get("option_pcr", "待数据"), "reason": "Tushare opt_daily + opt_basic"},
        {"dimension": "情绪", "factor": "QVIX 波动率代理", "status": coverage.get("qvix", "待数据"), "reason": "AkShare QVIX"},
        {"dimension": "情绪", "factor": "IV 偏斜", "status": coverage.get("iv_skew", "待数据"), "reason": "CFFEX 指数期权 Black-Scholes 自算"},
        {"dimension": "情绪", "factor": "股指期货会员持仓", "status": coverage.get("fut_holding", "待数据"), "reason": "Tushare fut_holding"},
        {"dimension": "微观结构", "factor": "上涨/下跌股票成交额差", "status": coverage.get("local_amount_pressure", "待数据"), "reason": "本地 daily_kline"},
    ]

    return {
        "model_id": signal_row.get("model_id"),
        "model_name": signal_row.get("model_name") or MODEL_NAME,
        "version": signal_row.get("version") or MODEL_VERSION,
        "source": signal_row.get("source"),
        "as_of": str(signal_row.get("trade_date")) if signal_row.get("trade_date") else None,
        "trade_date": str(signal_row.get("trade_date")) if signal_row.get("trade_date") else None,
        "state": state,
        "state_label": state_label,
        "timing_score": _to_float(signal_row.get("timing_score")),
        "combined_signal": int(signal_row.get("combined_signal") or 0),
        "position_upper": position_upper,
        "position_upper_pct": round(position_upper * 100, 0) if position_upper is not None else None,
        "confidence": _to_float(signal_row.get("confidence")),
        "dimension_scores": coverage.get("dimension_scores") or {},
        "dimension_signals": coverage.get("dimension_signals") or {},
        "dimension_vote_sum": coverage.get("dimension_vote_sum"),
        "action_label": action_label,
        "signals": signals,
        "article_factor_coverage": coverage_items,
        "reasons": reasons,
        "risk_notes": risk_notes,
        "limitations": [
            "V1.9 已对结构性期货净持仓和 IV 偏斜做滚动基准校准，并按趋势、估值、资金、衍生品情绪和市场宽度五个维度合成",
            "多指数趋势使用沪深300、中证1000和科创50；衍生品因子历史仍在自然扩充",
            "该信号用于研究和仓位约束，不代表实盘买卖建议",
        ],
    }


def _refresh_stored_with_realtime_overview(stored_signal: dict[str, Any], overview: dict[str, Any] | None) -> dict[str, Any]:
    overview = overview or {}
    realtime_trade_date = str(overview.get("trade_date") or "") or None
    quote_time = str(overview.get("latest_quote_time") or "") or None
    pressure = _to_float(overview.get("amount_pressure"))
    total_amount = _to_float(overview.get("total_amount"))
    market_strength = _to_float(overview.get("market_strength"))
    up_ratio = _to_float(overview.get("up_ratio"))
    amount_weighted_pct_chg = _to_float(overview.get("amount_weighted_pct_chg"))
    limit_up = _to_float(overview.get("limit_up_like")) or _to_float(overview.get("limit_up_count")) or 0
    limit_down = _to_float(overview.get("limit_down_like")) or _to_float(overview.get("limit_down_count")) or 0
    if pressure is None or not realtime_trade_date or not quote_time or not total_amount:
        return stored_signal

    stored_trade_date = str(stored_signal.get("trade_date") or "") or None
    if stored_trade_date and realtime_trade_date < stored_trade_date:
        return stored_signal

    signal_value, score, value_label = _amount_pressure_score(pressure)
    signals = []
    seen_ids = set()
    for item in stored_signal.get("signals") or []:
        item = dict(item)
        if item.get("indicator_id") == "up_down_amount_pressure":
            meta = dict(item.get("meta") or {})
            meta.update(
                {
                    "up_amount": _to_float(overview.get("up_amount")) or 0,
                    "down_amount": _to_float(overview.get("down_amount")) or 0,
                    "total_amount": total_amount,
                    "total_count": overview.get("total"),
                    "quote_time": quote_time,
                    "source": "stock_realtime_snapshot",
                    "daily_model_trade_date": stored_trade_date,
                }
            )
            item.update(
                {
                    "signal": signal_value,
                    "signal_label": _signal_label(signal_value),
                    "score": score,
                    "value": round(pressure, 6),
                    "value_label": value_label,
                    "source_status": "盘中实时",
                    "source": "stock_realtime_snapshot",
                    "meta": meta,
                }
            )
        signals.append(item)
        if item.get("indicator_id"):
            seen_ids.add(item.get("indicator_id"))
    if "up_down_amount_pressure" not in seen_ids:
        signals.append(
            {
                "dimension": "sentiment",
                "label": "上涨/下跌成交额差",
                "article_dimension": "情绪/上涨下跌成交额差",
                "indicator_id": "up_down_amount_pressure",
                "signal": signal_value,
                "signal_label": _signal_label(signal_value),
                "score": score,
                "value": round(pressure, 6),
                "value_label": value_label,
                "source_status": "盘中实时",
                "source": "stock_realtime_snapshot",
                "meta": {
                    "up_amount": _to_float(overview.get("up_amount")) or 0,
                    "down_amount": _to_float(overview.get("down_amount")) or 0,
                    "total_amount": total_amount,
                    "total_count": overview.get("total"),
                    "quote_time": quote_time,
                    "daily_model_trade_date": stored_trade_date,
                },
            }
        )

    if market_strength is not None:
        signals.append(
            {
                "dimension": "intraday",
                "label": "盘中市场强度",
                "article_dimension": "盘中/市场强度",
                "indicator_id": "intraday_market_strength",
                "signal": _score_signal(market_strength, 62, 42),
                "signal_label": _signal_label(_score_signal(market_strength, 62, 42)),
                "score": round(_clamp(market_strength), 1),
                "value": round(market_strength, 4),
                "value_label": f"{market_strength:.1f}",
                "source_status": "盘中实时",
                "source": "stock_realtime_snapshot",
                "meta": {
                    "market_state": overview.get("market_state"),
                    "market_state_label": overview.get("market_state_label"),
                    "amount_weighted_pct_chg": amount_weighted_pct_chg,
                    "quote_time": quote_time,
                },
            },
        )
    breadth_signal, breadth_score, breadth_label = _intraday_breadth_score(up_ratio, pressure)
    if breadth_score is not None:
        signals.append(
            {
                "dimension": "intraday",
                "label": "盘中涨跌扩散",
                "article_dimension": "盘中/涨跌扩散",
                "indicator_id": "intraday_breadth",
                "signal": breadth_signal,
                "signal_label": _signal_label(breadth_signal),
                "score": breadth_score,
                "value": breadth_score,
                "value_label": breadth_label,
                "source_status": "盘中实时",
                "source": "stock_realtime_snapshot",
                "meta": {
                    "up_ratio": up_ratio,
                    "down_ratio": _to_float(overview.get("down_ratio")),
                    "amount_pressure": pressure,
                    "up_count": overview.get("up_count"),
                    "down_count": overview.get("down_count"),
                    "flat_count": overview.get("flat_count"),
                    "quote_time": quote_time,
                },
            },
        )
    limit_signal, limit_score, limit_label = _limit_emotion_score(limit_up, limit_down)
    if limit_up + limit_down > 0:
        signals.append(
            {
                "dimension": "intraday",
                "label": "盘中涨跌停情绪",
                "article_dimension": "盘中/涨跌停情绪",
                "indicator_id": "intraday_limit_emotion",
                "signal": limit_signal,
                "signal_label": _signal_label(limit_signal),
                "score": limit_score,
                "value": {"limit_up": int(limit_up), "limit_down": int(limit_down)},
                "value_label": limit_label,
                "source_status": "盘中实时",
                "source": "stock_realtime_snapshot",
                "meta": {
                    "limit_up_like": limit_up,
                    "limit_down_like": limit_down,
                    "quote_time": quote_time,
                },
            },
        )

    composition = compose_timing_state(signals, weights=REALTIME_WEIGHTS)
    state = composition["state"]
    state_label = composition["state_label"]
    position_upper = composition["position_upper"]
    action_label = composition["action_label"]
    reasons = list(stored_signal.get("reasons") or [])
    realtime_reason = f"盘中上涨/下跌成交额差 {value_label}，{_signal_label(signal_value)}"
    if market_strength is not None:
        realtime_reason = f"盘中市场强度 {market_strength:.1f}，成交额压力 {value_label}，{_signal_label(signal_value)}"
    reasons = [item for item in reasons if "上涨/下跌成交额差" not in item]
    reasons.insert(0, realtime_reason)
    risk_notes = list(stored_signal.get("risk_notes") or [])
    if realtime_trade_date != stored_trade_date:
        risk_notes.append(f"盘中成交额因子已更新至 {realtime_trade_date}，其余日频因子沿用 {stored_trade_date or '最近交易日'}")
    else:
        risk_notes.append(f"盘中实时层已更新至 {quote_time}，日频因子保留收盘确认口径")
    if amount_weighted_pct_chg is not None and amount_weighted_pct_chg <= -1.5:
        risk_notes.append(f"成交额加权跌幅 {amount_weighted_pct_chg:.2f}%，盘中建议按防守优先")

    refreshed = dict(stored_signal)
    refreshed.update(
        {
            "source": f"{stored_signal.get('source') or 'market_timing_v18_sources'} + stock_realtime_snapshot",
            "as_of": quote_time,
            "trade_date": realtime_trade_date,
            "state": state,
            "state_label": state_label,
            "timing_score": composition["timing_score"],
            "combined_signal": composition["combined_signal"],
            "position_upper": position_upper,
            "position_upper_pct": round(position_upper * 100, 0),
            "confidence": composition["confidence"],
            "action_label": action_label,
            "signals": signals,
            "dimensions": composition["dimensions"],
            "dimension_scores": composition["dimension_scores"],
            "dimension_signals": composition["dimension_signals"],
            "dimension_vote_sum": composition["dimension_vote_sum"],
            "reasons": reasons[:8],
            "risk_notes": risk_notes[:4],
        }
    )
    for item in refreshed.get("article_factor_coverage") or []:
        if item.get("factor") == "上涨/下跌股票成交额差":
            item["status"] = "盘中实时"
            item["reason"] = "盘中由 stock_realtime_snapshot 刷新，收盘后由 daily_kline 确认"
    refreshed.setdefault("article_factor_coverage", []).append(
        {"dimension": "盘中", "factor": "盘中市场强弱", "status": "盘中实时", "reason": "由 stock_realtime_snapshot 实时宽度、额压和涨跌停结构计算"}
    )
    return refreshed


def build_market_timing_signal(overview: dict[str, Any] | None) -> dict[str, Any]:
    """Build a lightweight market timing signal from existing homepage snapshots.

    V1 intentionally uses already-ingested data only. It is not a full replication
    of the Huatai four-dimensional timing paper yet; options, futures positions
    and ERP inputs should be added after their data sources are stable.
    """

    stored_signal = _latest_stored_timing_signal()
    if stored_signal:
        return _attach_research_layers(
            _refresh_stored_with_realtime_overview(stored_signal, overview)
        )

    overview = overview or {}
    market_strength = _to_float(overview.get("market_strength"))
    up_ratio = _to_float(overview.get("up_ratio"))
    amount_pressure = _to_float(overview.get("amount_pressure"))
    amount_weighted_pct_chg = _to_float(overview.get("amount_weighted_pct_chg"))
    limit_up = _to_float(overview.get("limit_up_like")) or 0
    limit_down = _to_float(overview.get("limit_down_like")) or 0

    trend_signal = _score_signal(market_strength, 62, 42)
    trend_score = market_strength if market_strength is not None else 50.0
    breadth_seed = None
    if up_ratio is not None and amount_pressure is not None:
        breadth_seed = up_ratio * 0.62 + (amount_pressure + 1) / 2 * 0.38
    breadth_signal = _score_signal(breadth_seed, 0.58, 0.43)
    breadth_score = (breadth_seed * 100) if breadth_seed is not None else 50.0
    capital_signal, capital_pressure, capital_meta = _sector_net_amount_score(overview)
    capital_score = ((capital_pressure + 1) * 50) if capital_pressure is not None else 50.0

    limit_pressure, limit_score, _ = _limit_emotion_score(limit_up, limit_down)

    vote_sum = trend_signal + breadth_signal + capital_signal + limit_pressure
    raw_score = 50 + trend_signal * 18 + breadth_signal * 15 + capital_signal * 12 + limit_pressure * 8
    if amount_weighted_pct_chg is not None:
        raw_score += max(min(amount_weighted_pct_chg, 3), -3) * 2.2
    score = round(_clamp(raw_score), 1)

    if score >= 75 and vote_sum >= 3:
        state = "strong_risk_on"
        state_label = "积极进攻"
        position_upper = 1.0
        action_label = ACTION_LABELS[state]
    elif score >= 60 and vote_sum >= 2:
        state = "risk_on"
        state_label = "正常开仓"
        position_upper = 0.8
        action_label = ACTION_LABELS[state]
    elif score <= 42 or vote_sum <= -2:
        state = "defensive"
        state_label = "防守观望"
        position_upper = 0.15
        action_label = ACTION_LABELS[state]
    else:
        state = "cautious"
        state_label = "谨慎试探"
        position_upper = 0.45
        action_label = ACTION_LABELS[state]

    reasons: list[str] = []
    if market_strength is not None:
        reasons.append(f"市场强度 {market_strength:.1f}，{_signal_label(trend_signal)}")
    if up_ratio is not None and amount_pressure is not None:
        reasons.append(f"上涨占比 {up_ratio * 100:.1f}%，成交额压力 {amount_pressure * 100:.1f}%")
    if capital_pressure is not None:
        reasons.append(f"板块资金压力 {capital_pressure * 100:.1f}%，{_signal_label(capital_signal)}")
    if limit_up + limit_down:
        reasons.append(f"涨停/跌停 {int(limit_up)} / {int(limit_down)}")

    risk_notes = []
    if overview.get("sector_source") != "akshare_realtime_fund_flow":
        risk_notes.append("板块资金使用本地行业成交额 fallback，资金维度置信度降低")
    if market_strength is None:
        risk_notes.append("市场强度缺失，择时信号按中性降级")

    return _attach_research_layers({
        "model_id": "market_timing_v1_realtime_proxy",
        "model_name": "市场择时 V1",
        "version": "v1",
        "source": "market_overview + market_sector_fund_flow_snapshot",
        "as_of": overview.get("latest_quote_time") or overview.get("trade_date"),
        "trade_date": overview.get("trade_date"),
        "state": state,
        "state_label": state_label,
        "timing_score": score,
        "combined_signal": 1 if state in {"strong_risk_on", "risk_on"} else -1 if state == "defensive" else 0,
        "position_upper": position_upper,
        "position_upper_pct": round(position_upper * 100, 0),
        "action_label": action_label,
        "signals": [
            {
                "dimension": "trend",
                "label": "市场强度",
                "article_dimension": "技术/宽基趋势代理",
                "signal": trend_signal,
                "signal_label": _signal_label(trend_signal),
                "score": round(_clamp(trend_score), 1),
                "value": market_strength,
                "value_label": f"{market_strength:.1f}" if market_strength is not None else "-",
                "source_status": "已接入",
            },
            {
                "dimension": "breadth",
                "label": "涨跌扩散",
                "article_dimension": "情绪/微观结构代理",
                "signal": breadth_signal,
                "signal_label": _signal_label(breadth_signal),
                "score": round(_clamp(breadth_score), 1),
                "value": round(breadth_seed * 100, 1) if breadth_seed is not None else None,
                "value_label": f"{breadth_score:.1f}" if breadth_seed is not None else "-",
                "source_status": "已接入",
            },
            {
                "dimension": "capital",
                "label": "板块资金",
                "article_dimension": "资金维度代理",
                "signal": capital_signal,
                "signal_label": _signal_label(capital_signal),
                "score": round(_clamp(capital_score), 1),
                "value": capital_pressure,
                "value_label": f"{capital_pressure * 100:.1f}%" if capital_pressure is not None else "-",
                "meta": capital_meta,
                "source_status": "已接入",
            },
            {
                "dimension": "limit_emotion",
                "label": "涨停情绪",
                "article_dimension": "情绪维度代理",
                "signal": limit_pressure,
                "signal_label": _signal_label(limit_pressure),
                "score": round(_clamp(limit_score), 1),
                "value": {"limit_up": int(limit_up), "limit_down": int(limit_down)},
                "value_label": f"{int(limit_up)}/{int(limit_down)}",
                "source_status": "已接入",
            },
        ],
        "article_factor_coverage": [
            {"dimension": "估值", "factor": "ERP/风险溢价", "status": "未接入", "reason": "需稳定指数盈利收益率与十年国债收益率数据"},
            {"dimension": "情绪", "factor": "期权 PCR / IV 偏斜", "status": "未接入", "reason": "需补期权行情与衍生指标"},
            {"dimension": "资金", "factor": "融资买入额", "status": "待接入", "reason": "可由 Tushare/AkShare 补日频数据"},
            {"dimension": "技术", "factor": "指数布林带", "status": "待接入", "reason": "需先确认宽基指数日线数据覆盖"},
            {"dimension": "情绪", "factor": "股指期货会员持仓", "status": "未接入", "reason": "需单独数据源和口径校验"},
            {"dimension": "微观结构", "factor": "上涨/下跌股票成交额差", "status": "已接入代理", "reason": "当前由实时涨跌扩散与成交额压力近似"},
        ],
        "reasons": reasons,
        "risk_notes": risk_notes,
        "limitations": [
            "V1 尚未接入 ERP、期权 PCR/IV、股指期货会员持仓等完整华泰四维指标",
            "该信号用于研究和仓位约束，不代表实盘买卖建议",
        ],
    })
