from __future__ import annotations

import json
from datetime import datetime, time
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.shared.db import mysql_conn

router = APIRouter(tags=["stocks"])


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if value == value else None


def _round(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None


def _rank_percentile(rank: int | None, total: int | None) -> float | None:
    if not rank or not total or total <= 1:
        return None
    return round((total - rank + 1) / total * 100, 2)


def _market_status(realtime: dict[str, Any] | None, basic: dict[str, Any]) -> dict[str, Any]:
    if basic.get("is_delisted"):
        return {"status": "delisted", "label": "已退市", "quote_time": None}
    if not realtime:
        return {"status": "no_quote", "label": "暂无行情", "quote_time": None}

    latest_price = _to_float(realtime.get("latest_price"))
    quote_time = realtime.get("quote_time")
    if latest_price is None or latest_price <= 0:
        return {"status": "suspended", "label": "停牌/无成交", "quote_time": str(quote_time) if quote_time else None}

    label = "交易中"
    status = "trading"
    if isinstance(quote_time, datetime):
        quote_clock = quote_time.time()
        if quote_clock >= time(15, 0):
            label = "已收盘"
            status = "closed"
        elif quote_clock < time(9, 25) or time(11, 35) <= quote_clock < time(12, 55):
            label = "非连续交易"
            status = "paused_session"

    return {"status": status, "label": label, "quote_time": str(quote_time) if quote_time else None}


def _moving_average(values: list[float], days: int) -> float | None:
    if len(values) < days:
        return None
    return sum(values[-days:]) / days


def _technical_summary(price_history: list[dict[str, Any]], realtime_price: float | None) -> dict[str, Any]:
    closes = [_to_float(row.get("close")) for row in price_history]
    closes = [value for value in closes if value is not None]
    if not closes:
        return {"trend_label": "数据不足", "trend_score": None, "ma": {}, "position_20d_pct": None}

    current = realtime_price or closes[-1]
    ma5 = _moving_average(closes, 5)
    ma10 = _moving_average(closes, 10)
    ma20 = _moving_average(closes, 20)
    highs = [_to_float(row.get("high")) for row in price_history]
    lows = [_to_float(row.get("low")) for row in price_history]
    highs = [value for value in highs if value is not None]
    lows = [value for value in lows if value is not None]
    high_20 = max(highs[-20:]) if highs else None
    low_20 = min(lows[-20:]) if lows else None

    score = 50.0
    if ma5 is not None:
        score += 12 if current >= ma5 else -12
    if ma10 is not None:
        score += 10 if current >= ma10 else -10
    if ma20 is not None:
        score += 8 if current >= ma20 else -8
    if len(closes) >= 6:
        five_day_return = (current - closes[-6]) / closes[-6] * 100 if closes[-6] else 0
        score += max(min(five_day_return, 12), -12)
    score = max(0, min(100, score))

    if score >= 70:
        label = "偏强"
    elif score >= 55:
        label = "温和偏强"
    elif score >= 45:
        label = "震荡"
    else:
        label = "偏弱"

    position = None
    if high_20 is not None and low_20 is not None and high_20 > low_20:
        position = (current - low_20) / (high_20 - low_20) * 100

    return {
        "trend_label": label,
        "trend_score": _round(score, 1),
        "ma": {"ma5": _round(ma5, 2), "ma10": _round(ma10, 2), "ma20": _round(ma20, 2)},
        "position_20d_pct": _round(position, 2),
        "high_20d": _round(high_20, 2),
        "low_20d": _round(low_20, 2),
    }


@router.get("/stocks/{code}/overview")
def stock_overview(code: str) -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT code, name, market, industry, instrument_type, is_st, is_delisted,
                       pe_tushare, pb_tushare, roe, revenue_yoy, profit_yoy, eps,
                       fundamental_period, valuation_updated_at, fundamental_updated_at
                FROM stock_basic
                WHERE code = %s
                LIMIT 1
                """,
                (code,),
            )
            basic = cursor.fetchone()
            if not basic:
                raise HTTPException(status_code=404, detail=f"Stock not found: {code}")

            cursor.execute(
                """
                SELECT latest_price, change_amount, pct_chg, pre_close, open_price, high_price, low_price,
                       volume, amount, trade_date, quote_time, updated_at
                FROM stock_realtime_snapshot
                WHERE code = %s
                LIMIT 1
                """,
                (code,),
            )
            realtime = cursor.fetchone()

            cursor.execute(
                """
                SELECT *
                FROM factor_input_daily
                WHERE code = %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (code,),
            )
            factor = cursor.fetchone() or {}

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
            history_desc = cursor.fetchall()
            history = list(reversed(history_desc))

            cursor.execute("SELECT COUNT(*) AS total FROM stock_realtime_snapshot WHERE code REGEXP '^(sh|sz|bj)\\.'")
            realtime_total = int((cursor.fetchone() or {}).get("total") or 0)
            rank_queries = {
                "amount_rank": "amount",
                "pct_chg_rank": "pct_chg",
            }
            ranks: dict[str, Any] = {"sample_size": realtime_total}
            for key, field in rank_queries.items():
                cursor.execute(
                    f"""
                    SELECT ranked.rank_no
                    FROM (
                        SELECT code, RANK() OVER (ORDER BY {field} DESC) AS rank_no
                        FROM stock_realtime_snapshot
                        WHERE code REGEXP '^(sh|sz|bj)\\.' AND {field} IS NOT NULL
                    ) ranked
                    WHERE ranked.code = %s
                    """,
                    (code,),
                )
                row = cursor.fetchone() or {}
                rank = int(row.get("rank_no") or 0) or None
                ranks[key] = rank
                ranks[key.replace("_rank", "_percentile")] = _rank_percentile(rank, realtime_total)

            latest_factor_date = factor.get("trade_date")
            factor_total = 0
            if latest_factor_date:
                cursor.execute("SELECT COUNT(*) AS total FROM factor_input_daily WHERE trade_date = %s", (latest_factor_date,))
                factor_total = int((cursor.fetchone() or {}).get("total") or 0)
                for key, field in {"total_mv_rank": "total_mv", "turnover_rank": "turnover_rate", "volume_ratio_rank": "volume_ratio"}.items():
                    cursor.execute(
                        f"""
                        SELECT ranked.rank_no
                        FROM (
                            SELECT code, RANK() OVER (ORDER BY {field} DESC) AS rank_no
                            FROM factor_input_daily
                            WHERE trade_date = %s AND {field} IS NOT NULL
                        ) ranked
                        WHERE ranked.code = %s
                        """,
                        (latest_factor_date, code),
                    )
                    row = cursor.fetchone() or {}
                    rank = int(row.get("rank_no") or 0) or None
                    ranks[key] = rank
                    ranks[key.replace("_rank", "_percentile")] = _rank_percentile(rank, factor_total)

            industry = basic.get("industry")
            industry_avg = {}
            if industry:
                cursor.execute(
                    """
                    SELECT AVG(pe_tushare) AS avg_pe, AVG(pb_tushare) AS avg_pb, AVG(roe) AS avg_roe, COUNT(*) AS peer_count
                    FROM stock_basic
                    WHERE instrument_type = 'stock' AND industry = %s
                    """,
                    (industry,),
                )
                industry_avg = cursor.fetchone() or {}

    latest_price = _to_float(realtime.get("latest_price")) if realtime else None
    amount = _to_float(realtime.get("amount")) if realtime else None
    pct_chg = _to_float(realtime.get("pct_chg")) if realtime else None
    volume_ratio = _to_float(factor.get("volume_ratio"))
    turnover = _to_float(factor.get("turnover_rate"))
    total_mv = _to_float(factor.get("total_mv"))
    amount_pressure_proxy = amount * pct_chg / 100 if amount is not None and pct_chg is not None else None
    capital_score = 50.0
    if ranks.get("amount_percentile") is not None:
        capital_score += (ranks["amount_percentile"] - 50) * 0.25
    if pct_chg is not None:
        capital_score += max(min(pct_chg * 4, 20), -20)
    if volume_ratio is not None:
        capital_score += max(min((volume_ratio - 1) * 10, 15), -10)
    capital_score = max(0, min(100, capital_score))

    technical = _technical_summary(history, latest_price)
    pe = _to_float(basic.get("pe_tushare"))
    pb = _to_float(basic.get("pb_tushare"))
    roe = _to_float(basic.get("roe"))
    avg_pe = _to_float(industry_avg.get("avg_pe")) if industry_avg else None
    avg_pb = _to_float(industry_avg.get("avg_pb")) if industry_avg else None
    avg_roe = _to_float(industry_avg.get("avg_roe")) if industry_avg else None

    return {
        "code": code,
        "name": basic.get("name"),
        "market_status": _market_status(realtime, basic),
        "quote": {
            "latest_price": latest_price,
            "change_amount": _to_float(realtime.get("change_amount")) if realtime else None,
            "pct_chg": pct_chg,
            "pre_close": _to_float(realtime.get("pre_close")) if realtime else None,
            "open_price": _to_float(realtime.get("open_price")) if realtime else None,
            "high_price": _to_float(realtime.get("high_price")) if realtime else None,
            "low_price": _to_float(realtime.get("low_price")) if realtime else None,
            "volume": _to_float(realtime.get("volume")) if realtime else None,
            "amount": amount,
            "quote_time": str(realtime.get("quote_time")) if realtime and realtime.get("quote_time") else None,
        },
        "rankings": ranks,
        "capital_flow": {
            "source": "realtime_proxy",
            "label": "资金代理强度",
            "score": _round(capital_score, 1),
            "amount_pressure_proxy": _round(amount_pressure_proxy, 2),
            "amount": _round(amount, 2),
            "volume_ratio": _round(volume_ratio, 2),
            "turnover_rate": _round(turnover, 2),
        },
        "technical_summary": technical,
        "fundamental_summary": {
            "pe": pe,
            "pb": pb,
            "roe": roe,
            "revenue_yoy": _to_float(basic.get("revenue_yoy")),
            "profit_yoy": _to_float(basic.get("profit_yoy")),
            "eps": _to_float(basic.get("eps")),
            "total_mv": _round(total_mv, 2),
            "industry": industry,
            "peer_count": int(industry_avg.get("peer_count") or 0) if industry_avg else 0,
            "industry_avg_pe": _round(avg_pe, 2),
            "industry_avg_pb": _round(avg_pb, 2),
            "industry_avg_roe": _round(avg_roe, 2),
            "valuation_position": {
                "pe_vs_industry_pct": _round((pe - avg_pe) / avg_pe * 100, 2) if pe is not None and avg_pe else None,
                "pb_vs_industry_pct": _round((pb - avg_pb) / avg_pb * 100, 2) if pb is not None and avg_pb else None,
            },
        },
        "factor_trade_date": str(latest_factor_date) if latest_factor_date else None,
        "factor_sample_size": factor_total,
    }


@router.get("/stocks/{code}")
def stock_detail(
    code: str,
    history_limit: int = Query(default=20, ge=5, le=120),
    news_limit: int = Query(default=12, ge=0, le=50),
    intraday_limit: int = Query(default=240, ge=0, le=400),
) -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    code,
                    name,
                    market,
                    industry,
                    instrument_type,
                    listing_date,
                    is_st,
                    is_delisted,
                    pe_tushare,
                    pb_tushare,
                    roe,
                    roa,
                    grossprofit_margin,
                    netprofit_margin,
                    revenue_yoy,
                    profit_yoy,
                    eps,
                    valuation_updated_at,
                    fundamental_period,
                    fundamental_updated_at,
                    updated_at
                FROM stock_basic
                WHERE code = %s
                LIMIT 1
                """,
                (code,),
            )
            basic = cursor.fetchone()
            if not basic:
                raise HTTPException(status_code=404, detail=f"Stock not found: {code}")

            cursor.execute(
                """
                SELECT
                    trade_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    amount,
                    updated_at
                FROM daily_kline
                WHERE code = %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (code,),
            )
            latest_kline = cursor.fetchone()

            cursor.execute(
                """
                SELECT
                    trade_date,
                    open,
                    high,
                    low,
                    close
                FROM daily_kline
                WHERE code = %s
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (code, history_limit),
            )
            price_history_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    run_id,
                    trade_date,
                    strategy_id,
                    score,
                    rank_no,
                    metadata_json,
                    created_at
                FROM selection_result
                WHERE code = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 10
                """,
                (code,),
            )
            selection_rows = cursor.fetchall()

            news_rows = []
            if news_limit > 0:
                cursor.execute(
                    """
                    SELECT
                        title,
                        summary,
                        source,
                        url,
                        published_at,
                        sentiment_score,
                        credibility_score,
                        credibility_level,
                        credibility_reason,
                        quality_score,
                        quality_level,
                        created_at
                    FROM stock_news
                    WHERE code = %s
                    ORDER BY COALESCE(published_at, created_at) DESC, id DESC
                    LIMIT %s
                    """,
                    (code, news_limit),
                )
                news_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    latest_price,
                    change_amount,
                    pct_chg,
                    bid_price,
                    ask_price,
                    pre_close,
                    open_price,
                    high_price,
                    low_price,
                    volume,
                    amount,
                    trade_date,
                    quote_time,
                    updated_at
                FROM stock_realtime_snapshot
                WHERE code = %s
                LIMIT 1
                """,
                (code,),
            )
            realtime_snapshot = cursor.fetchone()

            intraday_rows = []
            if intraday_limit > 0:
                cursor.execute(
                    """
                    SELECT quote_minute, latest_price, pct_chg, volume, amount
                    FROM stock_realtime_intraday
                    WHERE code = %s AND trade_date = CURDATE()
                    ORDER BY quote_minute DESC
                    LIMIT %s
                    """,
                    (code, intraday_limit),
                )
                intraday_rows = cursor.fetchall()

    selection_history = []
    latest_selection = None
    for row in selection_rows:
        metadata = row.get("metadata_json")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        metadata = metadata or {}

        item = {
            "run_id": row.get("run_id"),
            "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
            "strategy_id": row.get("strategy_id"),
            "strategy_display_name": metadata.get("strategy_display_name"),
            "strategy_version": metadata.get("strategy_version"),
            "score": _to_float(row.get("score")),
            "rank_no": row.get("rank_no"),
            "created_at": str(row.get("created_at")) if row.get("created_at") else None,
            "factor_scores": {
                **(metadata.get("raw_metrics") or {}),
                **(metadata.get("factors") or {}),
            },
        }
        selection_history.append(item)

    if selection_history:
        latest_selection = selection_history[0]

    latest_close = _to_float(latest_kline.get("close")) if latest_kline else None
    latest_open = _to_float(latest_kline.get("open")) if latest_kline else None
    intraday_change_pct = None
    if latest_open and latest_close:
        intraday_change_pct = round((latest_close - latest_open) / latest_open * 100, 2)

    price_history = [
        {
            "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
            "open": _to_float(row.get("open")),
            "high": _to_float(row.get("high")),
            "low": _to_float(row.get("low")),
            "close": _to_float(row.get("close")),
        }
        for row in reversed(price_history_rows)
    ]

    recent_news = [
        {
            "title": row.get("title"),
            "summary": row.get("summary"),
            "source": row.get("source"),
            "url": row.get("url"),
            "published_at": str(row.get("published_at")) if row.get("published_at") else None,
            "sentiment_score": _to_float(row.get("sentiment_score")),
            "credibility_score": _to_float(row.get("credibility_score")),
            "credibility_level": row.get("credibility_level"),
            "credibility_reason": row.get("credibility_reason"),
            "quality_score": _to_float(row.get("quality_score")),
            "quality_level": row.get("quality_level"),
            "created_at": str(row.get("created_at")) if row.get("created_at") else None,
        }
        for row in news_rows
    ]

    realtime = None
    if realtime_snapshot:
        realtime = {
            "latest_price": _to_float(realtime_snapshot.get("latest_price")),
            "change_amount": _to_float(realtime_snapshot.get("change_amount")),
            "pct_chg": _to_float(realtime_snapshot.get("pct_chg")),
            "bid_price": _to_float(realtime_snapshot.get("bid_price")),
            "ask_price": _to_float(realtime_snapshot.get("ask_price")),
            "pre_close": _to_float(realtime_snapshot.get("pre_close")),
            "open_price": _to_float(realtime_snapshot.get("open_price")),
            "high_price": _to_float(realtime_snapshot.get("high_price")),
            "low_price": _to_float(realtime_snapshot.get("low_price")),
            "volume": realtime_snapshot.get("volume"),
            "amount": _to_float(realtime_snapshot.get("amount")),
            "trade_date": str(realtime_snapshot.get("trade_date")) if realtime_snapshot.get("trade_date") else None,
            "quote_time": str(realtime_snapshot.get("quote_time")) if realtime_snapshot.get("quote_time") else None,
            "updated_at": str(realtime_snapshot.get("updated_at")) if realtime_snapshot.get("updated_at") else None,
        }

    intraday = [
        {
            "quote_minute": str(row.get("quote_minute")) if row.get("quote_minute") else None,
            "latest_price": _to_float(row.get("latest_price")),
            "pct_chg": _to_float(row.get("pct_chg")),
            "volume": row.get("volume"),
            "amount": _to_float(row.get("amount")),
        }
        for row in reversed(intraday_rows)
    ]

    return {
        "code": basic.get("code"),
        "name": basic.get("name"),
        "market": basic.get("market"),
        "industry": basic.get("industry"),
        "instrument_type": basic.get("instrument_type"),
        "listing_date": str(basic.get("listing_date")) if basic.get("listing_date") else None,
        "flags": {
            "is_st": bool(basic.get("is_st")),
            "is_delisted": bool(basic.get("is_delisted")),
        },
        "valuation": {
            "pe_tushare": _to_float(basic.get("pe_tushare")),
            "pb_tushare": _to_float(basic.get("pb_tushare")),
            "valuation_updated_at": str(basic.get("valuation_updated_at")) if basic.get("valuation_updated_at") else None,
        },
        "fundamentals": {
            "roe": _to_float(basic.get("roe")),
            "roa": _to_float(basic.get("roa")),
            "grossprofit_margin": _to_float(basic.get("grossprofit_margin")),
            "netprofit_margin": _to_float(basic.get("netprofit_margin")),
            "revenue_yoy": _to_float(basic.get("revenue_yoy")),
            "profit_yoy": _to_float(basic.get("profit_yoy")),
            "eps": _to_float(basic.get("eps")),
            "fundamental_period": basic.get("fundamental_period"),
            "fundamental_updated_at": str(basic.get("fundamental_updated_at")) if basic.get("fundamental_updated_at") else None,
        },
        "latest_kline": {
            "trade_date": str(latest_kline.get("trade_date")) if latest_kline and latest_kline.get("trade_date") else None,
            "open": latest_open,
            "high": _to_float(latest_kline.get("high")) if latest_kline else None,
            "low": _to_float(latest_kline.get("low")) if latest_kline else None,
            "close": latest_close,
            "volume": latest_kline.get("volume") if latest_kline else None,
            "amount": _to_float(latest_kline.get("amount")) if latest_kline else None,
            "intraday_change_pct": intraday_change_pct,
            "updated_at": str(latest_kline.get("updated_at")) if latest_kline and latest_kline.get("updated_at") else None,
        },
        "price_history": price_history,
        "realtime": realtime,
        "realtime_intraday": intraday,
        "latest_selection": latest_selection,
        "selection_history": selection_history,
        "recent_news": recent_news,
        "updated_at": str(basic.get("updated_at")) if basic.get("updated_at") else None,
    }
