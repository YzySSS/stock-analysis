from __future__ import annotations

import json
from datetime import datetime, time
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.data_ingestion.intraday_bar_sync import (
    SOURCE as INTRADAY_BAR_SOURCE,
    cached_bars,
    latest_trade_date_for_code,
    normalize_code,
)
from app.data_ingestion.market_opinion_repository import hydrate_sector_opinion_rows
from app.data_ingestion.realtime_lifecycle import fetch_rollup_bars
from app.jobs.durable_tasks import DurableTaskService
from app.shared.db import mysql_read_conn
from app.shared.sentiment_scoring import enrich_opinion_news_item, score_source

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


def _realtime_moneyflow_payload(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    net_amount = _to_float(row.get("net_amount"))
    amount = _to_float(row.get("amount"))
    if net_amount is None:
        label = "实时资金待确认"
    elif net_amount > 0:
        label = "实时净流入"
    elif net_amount < 0:
        label = "实时净流出"
    else:
        label = "实时资金持平"
    return {
        "data_scope": "intraday_realtime",
        "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
        "quote_time": str(row.get("quote_time")) if row.get("quote_time") else None,
        "latest_price": _to_float(row.get("latest_price")),
        "pct_chg": _to_float(row.get("pct_chg")),
        "turnover_rate": _to_float(row.get("turnover_rate")),
        "inflow_amount": _to_float(row.get("inflow_amount")),
        "outflow_amount": _to_float(row.get("outflow_amount")),
        "net_amount": net_amount,
        "amount": amount,
        "net_flow_intensity_pct": _round(net_amount / amount * 100, 2) if net_amount is not None and amount else None,
        "label": label,
        "source": row.get("source"),
        "source_unit": row.get("source_unit") or "元",
        "updated_at": str(row.get("updated_at")) if row.get("updated_at") else None,
    }


def _rank_percentile(rank: int | None, total: int | None) -> float | None:
    if not rank or not total or total <= 1:
        return None
    return round((total - rank + 1) / total * 100, 2)


def _pe_status(pe: float | None, eps: float | None) -> dict[str, Any]:
    if pe is not None and pe > 0:
        return {
            "pe_status": "valid",
            "pe_status_label": "PE 正常",
            "pe_valid": True,
            "pe_status_reason": "估值源返回正 PE",
        }
    if eps is not None and eps <= 0:
        return {
            "pe_status": "not_applicable_eps_nonpositive",
            "pe_status_label": "PE 不适用",
            "pe_valid": False,
            "pe_status_reason": "EPS 非正，PE 不具备可比意义",
        }
    if eps is None:
        return {
            "pe_status": "missing_eps",
            "pe_status_label": "PE 暂缺",
            "pe_valid": False,
            "pe_status_reason": "EPS 缺失，无法判断 PE 口径",
        }
    return {
        "pe_status": "missing_positive_eps",
        "pe_status_label": "PE 暂缺",
        "pe_valid": False,
        "pe_status_reason": "EPS 为正但估值源未返回有效正 PE",
    }


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
        elif time(9, 15) <= quote_clock < time(9, 30):
            label = "盘前竞价中"
            status = "pre_open_auction"
        elif quote_clock < time(9, 15) or time(11, 35) <= quote_clock < time(12, 55):
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
    with mysql_read_conn() as conn:
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
    eps = _to_float(basic.get("eps"))
    pe_status = _pe_status(pe, eps)
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
            "pe_status": pe_status["pe_status"],
            "pe_status_label": pe_status["pe_status_label"],
            "pe_valid": pe_status["pe_valid"],
            "pe_status_reason": pe_status["pe_status_reason"],
            "pb": pb,
            "roe": roe,
            "revenue_yoy": _to_float(basic.get("revenue_yoy")),
            "profit_yoy": _to_float(basic.get("profit_yoy")),
            "eps": eps,
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


@router.get("/stocks/{code}/intraday-bars")
def stock_intraday_bars(
    code: str,
    trade_date: str | None = Query(default=None),
    refresh: bool = Query(default=False),
) -> dict:
    """Return cached bars without external I/O or database writes.

    ``refresh`` remains in the signature for backwards compatibility, but a
    caller must use the POST refresh endpoint so a GET can never trigger an
    AkShare request or mutate MySQL.
    """

    if refresh:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "intraday_refresh_requires_async_post",
                "message": "分钟线刷新已改为异步命令，请调用 POST /api/stocks/{code}/intraday-bars/refresh。",
            },
        )
    final_code = normalize_code(code)
    final_trade_date = trade_date or latest_trade_date_for_code(final_code)
    items = cached_bars(final_code, final_trade_date) if final_trade_date else []
    return {
        "code": final_code,
        "trade_date": final_trade_date,
        "source": INTRADAY_BAR_SOURCE,
        "source_status": "cached" if items else "empty",
        "count": len(items),
        "saved_rows": 0,
        "stale": not bool(items),
        "refresh_endpoint": f"/api/stocks/{final_code}/intraday-bars/refresh",
        "items": items,
    }


@router.post("/stocks/{code}/intraday-bars/refresh", status_code=202)
def refresh_stock_intraday_bars(
    code: str,
    trade_date: str | None = Query(default=None),
) -> dict:
    final_code = normalize_code(code)
    queued = DurableTaskService().enqueue_intraday_refresh(final_code, trade_date)
    return {
        "status": "queued",
        "job_id": queued["task_id"],
        "code": final_code,
        "trade_date": trade_date,
        "message": "分钟线刷新已进入可恢复任务队列，当前 GET 接口继续返回已有缓存。",
    }


@router.get("/stocks/{code}/realtime-rollups")
def stock_realtime_rollups(
    code: str,
    interval: int = Query(default=5),
    trade_date: str | None = Query(default=None),
    limit: int = Query(default=400, ge=1, le=5000),
) -> dict:
    try:
        return fetch_rollup_bars(
            normalize_code(code),
            interval=interval,
            trade_date=trade_date,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/stocks/{code}")
def stock_detail(
    code: str,
    history_limit: int = Query(default=600, ge=5, le=1000),
    news_limit: int = Query(default=12, ge=0, le=50),
    intraday_limit: int = Query(default=240, ge=0, le=400),
) -> dict:
    with mysql_read_conn() as conn:
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
                      AND published_at IS NOT NULL
                      AND published_at >= DATE_SUB(NOW(), INTERVAL 14 DAY)
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

            cursor.execute(
                """
                SELECT
                    trade_date,
                    his_low,
                    his_high,
                    cost_5pct,
                    cost_15pct,
                    cost_50pct,
                    cost_85pct,
                    cost_95pct,
                    weight_avg,
                    winner_rate,
                    updated_at
                FROM stock_chip_daily
                WHERE code = %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (code,),
            )
            chip_row = cursor.fetchone()

            cursor.execute(
                """
                SELECT
                    trade_date,
                    buy_sm_amount,
                    sell_sm_amount,
                    buy_md_amount,
                    sell_md_amount,
                    buy_lg_amount,
                    sell_lg_amount,
                    buy_elg_amount,
                    sell_elg_amount,
                    net_mf_amount,
                    net_mf_vol,
                    source,
                    updated_at
                FROM stock_moneyflow_daily
                WHERE code = %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (code,),
            )
            moneyflow_row = cursor.fetchone()

            cursor.execute(
                """
                SELECT
                    trade_date,
                    quote_time,
                    latest_price,
                    pct_chg,
                    turnover_rate,
                    inflow_amount,
                    outflow_amount,
                    net_amount,
                    amount,
                    source,
                    source_unit,
                    updated_at
                FROM stock_realtime_moneyflow_snapshot
                WHERE code = %s
                  AND trade_date = (SELECT MAX(trade_date) FROM stock_realtime_moneyflow_snapshot)
                  AND quote_time >= DATE_SUB(
                      (SELECT MAX(quote_time) FROM stock_realtime_moneyflow_snapshot),
                      INTERVAL 20 MINUTE
                  )
                LIMIT 1
                """,
                (code,),
            )
            realtime_moneyflow_row = cursor.fetchone()

            intraday_rows = []
            if intraday_limit > 0:
                intraday_trade_date = realtime_snapshot.get("trade_date") if realtime_snapshot else None
                if not intraday_trade_date:
                    cursor.execute(
                        "SELECT MAX(trade_date) AS trade_date FROM stock_realtime_intraday WHERE code = %s",
                        (code,),
                    )
                    intraday_trade_date = (cursor.fetchone() or {}).get("trade_date")
                cursor.execute(
                    """
                    SELECT quote_minute, latest_price, pct_chg, volume, amount
                    FROM stock_realtime_intraday FORCE INDEX (idx_realtime_intraday_code_time)
                    WHERE code = %s AND trade_date = %s
                    ORDER BY quote_minute DESC
                    LIMIT %s
                    """,
                    (code, intraday_trade_date, intraday_limit),
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

        explain_raw_metrics = ((metadata.get("explain") or {}).get("raw_metrics") or {})
        saved_raw_metrics = metadata.get("raw_metrics") or {}
        item = {
            "run_id": row.get("run_id"),
            "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
            "strategy_id": row.get("strategy_id"),
            "strategy_display_name": metadata.get("strategy_display_name"),
            "strategy_version": metadata.get("strategy_version"),
            "score": _to_float(row.get("score")),
            "rank_no": row.get("rank_no"),
            "created_at": str(row.get("created_at")) if row.get("created_at") else None,
            "sentiment_source": (metadata.get("explain") or {}).get("sentiment_source"),
            "news_count": explain_raw_metrics.get("news_count"),
            "sentiment_context": metadata.get("sentiment_context"),
            "factor_scores": metadata.get("factors") or {},
            "raw_metrics": {
                **explain_raw_metrics,
                **saved_raw_metrics,
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
    pe = _to_float(basic.get("pe_tushare"))
    eps = _to_float(basic.get("eps"))
    pe_status = _pe_status(pe, eps)

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

    def _opinion_news_item(item: dict, reason: str) -> dict:
        enriched = enrich_opinion_news_item(item, reason)
        return {
            "title": item.get("title"),
            "summary": item.get("summary"),
            "source": item.get("source_name") or item.get("source_id"),
            "url": item.get("url"),
            "published_at": str(item.get("published_at")) if item.get("published_at") else None,
            "sentiment_score": enriched.get("sentiment_score"),
            "credibility_score": enriched.get("credibility_score"),
            "credibility_level": enriched.get("credibility_level"),
            "credibility_reason": enriched.get("credibility_reason"),
            "quality_score": enriched.get("quality_score"),
            "quality_level": enriched.get("quality_level"),
            "created_at": None,
        }

    recent_news = []
    for row in news_rows:
        source_rating = score_source(row.get("source"))
        sentiment_score = _to_float(row.get("sentiment_score"))
        credibility_score = _to_float(row.get("credibility_score"))
        recent_news.append({
            "title": row.get("title"),
            "summary": row.get("summary"),
            "source": row.get("source"),
            "url": row.get("url"),
            "published_at": str(row.get("published_at")) if row.get("published_at") else None,
            "sentiment_score": sentiment_score,
            "credibility_score": credibility_score if credibility_score is not None else source_rating.get("credibility_score"),
            "credibility_level": row.get("credibility_level") or source_rating.get("credibility_level"),
            "credibility_reason": row.get("credibility_reason") or source_rating.get("credibility_reason"),
            "quality_score": _to_float(row.get("quality_score")),
            "quality_level": row.get("quality_level"),
            "created_at": str(row.get("created_at")) if row.get("created_at") else None,
        })
    sector_opinion_news = []
    latest_sentiment_context = (latest_selection or {}).get("sentiment_context") or {}
    if not latest_sentiment_context:
        with mysql_read_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, payload_version, sector_type, sector_name, as_of_datetime, sector_score, top_stocks_json, top_news_json, source_json
                    FROM sector_opinion_daily
                    WHERE as_of_datetime = (SELECT MAX(as_of_datetime) FROM sector_opinion_daily)
                      AND sector_type = 'theme'
                    ORDER BY sector_score DESC
                    LIMIT 40
                    """
                )
                sector_rows = cursor.fetchall() or []
        hydrate_sector_opinion_rows(sector_rows)
        for sector_row in sector_rows:
            try:
                top_stocks = json.loads(sector_row.get("top_stocks_json") or "[]")
            except Exception:
                top_stocks = []
            if not any(str(stock.get("code") or "") == code for stock in top_stocks):
                continue
            try:
                top_news = json.loads(sector_row.get("top_news_json") or "[]")
            except Exception:
                top_news = []
            latest_sentiment_context = {
                "sector_name": sector_row.get("sector_name"),
                "sector_type": sector_row.get("sector_type"),
                "as_of": str(sector_row.get("as_of_datetime")) if sector_row.get("as_of_datetime") else None,
                "sector_score": _to_float(sector_row.get("sector_score")),
                "sector_top_news": top_news,
            }
            break
    if latest_sentiment_context:
        sector_opinion_news = [
            {
                **_opinion_news_item(item, "来自本次舆情选股的关联板块热度新闻"),
                "sector_name": latest_sentiment_context.get("sector_name"),
                "sector_type": latest_sentiment_context.get("sector_type"),
            }
            for item in (latest_sentiment_context.get("sector_top_news") or [])[:5]
            if item.get("title")
        ]
    if not recent_news and latest_selection:
        context = latest_sentiment_context
        opinion_news = context.get("stock_news") or context.get("top_news") or []
        recent_news = [
            _opinion_news_item(item, "来自本次舆情选股的个股命中新闻")
            for item in opinion_news[:5]
            if item.get("title")
        ]
    if sector_opinion_news:
        seen_titles = {str(item.get("title") or "") for item in recent_news}
        recent_news.extend(
            item
            for item in sector_opinion_news
            if str(item.get("title") or "") not in seen_titles
        )

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

    chip = None
    if chip_row:
        latest_reference_price = (realtime or {}).get("latest_price") or latest_close
        weight_avg = _to_float(chip_row.get("weight_avg"))
        cost_50pct = _to_float(chip_row.get("cost_50pct"))
        cost_15pct = _to_float(chip_row.get("cost_15pct"))
        cost_85pct = _to_float(chip_row.get("cost_85pct"))
        winner_rate = _to_float(chip_row.get("winner_rate"))
        price_vs_weight_avg_pct = (
            (latest_reference_price - weight_avg) / weight_avg * 100
            if latest_reference_price is not None and weight_avg
            else None
        )
        cost_band_width_pct = (
            (cost_85pct - cost_15pct) / cost_50pct * 100
            if cost_85pct is not None and cost_15pct is not None and cost_50pct
            else None
        )
        chip_label = "中性"
        if winner_rate is not None:
            if winner_rate >= 70:
                chip_label = "获利盘偏高"
            elif winner_rate <= 35:
                chip_label = "套牢盘偏多"
            elif price_vs_weight_avg_pct is not None and price_vs_weight_avg_pct > 5:
                chip_label = "价格高于平均成本"
            elif price_vs_weight_avg_pct is not None and price_vs_weight_avg_pct < -5:
                chip_label = "价格低于平均成本"
        chip = {
            "trade_date": str(chip_row.get("trade_date")) if chip_row.get("trade_date") else None,
            "his_low": _to_float(chip_row.get("his_low")),
            "his_high": _to_float(chip_row.get("his_high")),
            "cost_5pct": _to_float(chip_row.get("cost_5pct")),
            "cost_15pct": cost_15pct,
            "cost_50pct": cost_50pct,
            "cost_85pct": cost_85pct,
            "cost_95pct": _to_float(chip_row.get("cost_95pct")),
            "weight_avg": weight_avg,
            "winner_rate": winner_rate,
            "price_vs_weight_avg_pct": _round(price_vs_weight_avg_pct, 2),
            "cost_band_width_pct": _round(cost_band_width_pct, 2),
            "label": chip_label,
            "updated_at": str(chip_row.get("updated_at")) if chip_row.get("updated_at") else None,
        }

    moneyflow = None
    if moneyflow_row:
        net_mf_amount = _to_float(moneyflow_row.get("net_mf_amount"))
        buy_lg_amount = _to_float(moneyflow_row.get("buy_lg_amount"))
        sell_lg_amount = _to_float(moneyflow_row.get("sell_lg_amount"))
        buy_elg_amount = _to_float(moneyflow_row.get("buy_elg_amount"))
        sell_elg_amount = _to_float(moneyflow_row.get("sell_elg_amount"))
        buy_sm_amount = _to_float(moneyflow_row.get("buy_sm_amount"))
        sell_sm_amount = _to_float(moneyflow_row.get("sell_sm_amount"))
        buy_md_amount = _to_float(moneyflow_row.get("buy_md_amount"))
        sell_md_amount = _to_float(moneyflow_row.get("sell_md_amount"))
        large_net_amount = (
            (buy_lg_amount or 0) + (buy_elg_amount or 0) - (sell_lg_amount or 0) - (sell_elg_amount or 0)
            if any(v is not None for v in (buy_lg_amount, buy_elg_amount, sell_lg_amount, sell_elg_amount))
            else None
        )
        retail_net_amount = (
            (buy_sm_amount or 0) + (buy_md_amount or 0) - (sell_sm_amount or 0) - (sell_md_amount or 0)
            if any(v is not None for v in (buy_sm_amount, buy_md_amount, sell_sm_amount, sell_md_amount))
            else None
        )
        moneyflow_trade_date = str(moneyflow_row.get("trade_date")) if moneyflow_row.get("trade_date") else None
        latest_kline_trade_date = str(latest_kline.get("trade_date")) if latest_kline and latest_kline.get("trade_date") else None
        amount_base_yuan = (
            _to_float(latest_kline.get("amount"))
            if latest_kline and moneyflow_trade_date == latest_kline_trade_date
            else None
        )
        amount_base_wan = amount_base_yuan / 10000 if amount_base_yuan else None
        net_flow_intensity_pct = (net_mf_amount / amount_base_wan * 100 if net_mf_amount is not None and amount_base_wan else None)
        large_flow_ratio_pct = (large_net_amount / amount_base_wan * 100 if large_net_amount is not None and amount_base_wan else None)
        moneyflow_label = "资金分歧"
        if net_mf_amount is not None:
            if net_mf_amount > 0 and (large_net_amount or 0) > 0:
                moneyflow_label = "主力净流入"
            elif net_mf_amount < 0 and (large_net_amount or 0) < 0:
                moneyflow_label = "主力净流出"
            elif net_mf_amount > 0:
                moneyflow_label = "整体净流入"
            elif net_mf_amount < 0:
                moneyflow_label = "整体净流出"
        moneyflow = {
            "data_scope": "completed_trade_day",
            "trade_date": str(moneyflow_row.get("trade_date")) if moneyflow_row.get("trade_date") else None,
            "net_mf_amount": net_mf_amount,
            "net_mf_vol": _to_float(moneyflow_row.get("net_mf_vol")),
            "buy_lg_amount": buy_lg_amount,
            "sell_lg_amount": sell_lg_amount,
            "buy_elg_amount": buy_elg_amount,
            "sell_elg_amount": sell_elg_amount,
            "large_net_amount": _round(large_net_amount, 2),
            "retail_net_amount": _round(retail_net_amount, 2),
            "net_flow_intensity_pct": _round(net_flow_intensity_pct, 2),
            "large_flow_ratio_pct": _round(large_flow_ratio_pct, 2),
            "label": moneyflow_label,
            "source": moneyflow_row.get("source"),
            "source_unit": "万元",
            "updated_at": str(moneyflow_row.get("updated_at")) if moneyflow_row.get("updated_at") else None,
        }

    realtime_moneyflow = _realtime_moneyflow_payload(realtime_moneyflow_row)

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

    full_intraday_trade_date = (
        realtime.get("trade_date") if realtime else None
    ) or (str(latest_kline.get("trade_date")) if latest_kline and latest_kline.get("trade_date") else None)
    full_intraday_items = cached_bars(str(basic.get("code")), full_intraday_trade_date) if full_intraday_trade_date else []

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
            "pe_tushare": pe,
            "pe_status": pe_status["pe_status"],
            "pe_status_label": pe_status["pe_status_label"],
            "pe_valid": pe_status["pe_valid"],
            "pe_status_reason": pe_status["pe_status_reason"],
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
            "eps": eps,
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
        "intraday_bars": {
            "trade_date": full_intraday_trade_date,
            "source_status": "cached" if full_intraday_items else "empty",
            "count": len(full_intraday_items),
            "items": full_intraday_items,
        },
        "chip": chip,
        "moneyflow": moneyflow,
        "realtime_moneyflow": realtime_moneyflow,
        "latest_selection": latest_selection,
        "selection_history": selection_history,
        "recent_news": recent_news,
        "sector_opinion_news": sector_opinion_news,
        "updated_at": str(basic.get("updated_at")) if basic.get("updated_at") else None,
    }
