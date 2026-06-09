from __future__ import annotations

import json
from typing import Any

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


def _signal_score_from_signal(signal: int, neutral_score: float = 50) -> float:
    return {1: 75.0, 0: neutral_score, -1: 25.0}.get(signal, neutral_score)


def _json_loads(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


def _latest_stored_timing_signal(index_code: str = "000300.SH") -> dict[str, Any] | None:
    try:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM market_timing_signal_daily
                    WHERE index_code = %s AND model_id = 'huatai_multidim_v18'
                    ORDER BY trade_date DESC
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
                    WHERE trade_date = %s AND index_code = %s
                    ORDER BY FIELD(indicator_id,
                        'index_bollinger',
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
                    (signal_row.get("trade_date"), index_code),
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
        "index_pe_percentile": "估值/指数 PE 分位",
        "erp": "估值/ERP 风险溢价",
        "margin_buy_ratio": "资金/融资买入额",
        "option_pcr": "情绪/期权 PCR",
        "qvix_volatility": "情绪/QVIX 波动率代理",
        "iv_skew": "情绪/自算 IV 偏斜",
        "futures_holding_net": "情绪/股指期货会员持仓",
        "up_down_amount_pressure": "情绪/上涨下跌成交额差",
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
        "risk_on": "选股可正常执行，回测可按标准仓位观察",
        "defensive": "不建议新增重仓，选股结果以观察为主",
        "cautious": "可小仓验证，等待市场扩散或回踩确认",
    }.get(state, "等待更多择时因子确认")

    coverage_items = [
        {"dimension": "技术", "factor": "指数布林带", "status": coverage.get("index_daily", "待数据"), "reason": "Tushare index_daily"},
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
        "model_name": signal_row.get("model_name") or "华泰四维择时 V1.8",
        "version": signal_row.get("version") or "v1.8",
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
        "action_label": action_label,
        "signals": signals,
        "article_factor_coverage": coverage_items,
        "reasons": reasons,
        "risk_notes": risk_notes,
        "limitations": [
            "V1.8 已接入指数、估值、ERP、两融、期权 PCR、QVIX、自算 IV 偏斜、股指期货持仓和本地微观成交额因子",
            "IV 偏斜当前为 CFFEX 指数期权研究口径，后续可扩 ETF 期权和更严格 delta skew",
            "该信号用于研究和仓位约束，不代表实盘买卖建议",
        ],
    }


def build_market_timing_signal(overview: dict[str, Any] | None) -> dict[str, Any]:
    """Build a lightweight market timing signal from existing homepage snapshots.

    V1 intentionally uses already-ingested data only. It is not a full replication
    of the Huatai four-dimensional timing paper yet; options, futures positions
    and ERP inputs should be added after their data sources are stable.
    """

    stored_signal = _latest_stored_timing_signal()
    if stored_signal:
        return stored_signal

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

    limit_pressure = 0
    if limit_up + limit_down >= 8:
        limit_pressure = 1 if limit_up >= limit_down * 1.8 else -1 if limit_down >= limit_up * 1.4 else 0
    limit_score = _signal_score_from_signal(limit_pressure)
    if limit_up + limit_down:
        limit_score = _clamp(50 + ((limit_up - limit_down) / max(limit_up + limit_down, 1)) * 45)

    vote_sum = trend_signal + breadth_signal + capital_signal + limit_pressure
    raw_score = 50 + trend_signal * 18 + breadth_signal * 15 + capital_signal * 12 + limit_pressure * 8
    if amount_weighted_pct_chg is not None:
        raw_score += max(min(amount_weighted_pct_chg, 3), -3) * 2.2
    score = round(_clamp(raw_score), 1)

    if score >= 64 and vote_sum >= 2:
        state = "risk_on"
        state_label = "正常开仓"
        position_upper = 0.8
        action_label = "选股可正常执行，回测可按标准仓位观察"
    elif score <= 42 or vote_sum <= -2:
        state = "defensive"
        state_label = "防守观望"
        position_upper = 0.15
        action_label = "不建议新增重仓，选股结果以观察为主"
    else:
        state = "cautious"
        state_label = "谨慎试探"
        position_upper = 0.45
        action_label = "可小仓验证，等待市场扩散或回踩确认"

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

    return {
        "model_id": "market_timing_v1_realtime_proxy",
        "model_name": "市场择时 V1",
        "version": "v1",
        "source": "market_overview + market_sector_fund_flow_snapshot",
        "as_of": overview.get("latest_quote_time") or overview.get("trade_date"),
        "trade_date": overview.get("trade_date"),
        "state": state,
        "state_label": state_label,
        "timing_score": score,
        "combined_signal": 1 if state == "risk_on" else -1 if state == "defensive" else 0,
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
    }
