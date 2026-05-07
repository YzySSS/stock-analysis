from __future__ import annotations

import re
from typing import Annotated

from fastapi import APIRouter, Query

from app.error_learning.tracker import SelectionResultTracker
from app.shared.db import mysql_conn

router = APIRouter(tags=["dashboard"])


def _to_float(value) -> float | None:
    return float(value) if value is not None else None


def _to_int(value) -> int:
    return int(value or 0)


def _clean_industry_name(name: str | None) -> str:
    if not name:
        return "未分类"
    # stock_basic.industry 当前带证监会行业编码，如 C39计算机、通信和其他电子设备制造业。
    return re.sub(r"^[A-Z]\d{2}", "", str(name)).strip() or str(name)


def _market_state(strength: float | None, up_ratio: float | None) -> str:
    if strength is None or up_ratio is None:
        return "unknown"
    if strength >= 70 and up_ratio >= 0.6:
        return "strong"
    if strength >= 58 and up_ratio >= 0.52:
        return "warm"
    if strength <= 35 and up_ratio <= 0.4:
        return "weak"
    if strength <= 45:
        return "cold"
    return "neutral"


def _market_state_label(state: str) -> str:
    return {
        "strong": "强势",
        "warm": "偏强",
        "neutral": "震荡",
        "cold": "偏弱",
        "weak": "弱势",
        "unknown": "未知",
    }.get(state, state)


def _dashboard_data_stats() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='stock') AS total_stock_codes,
                    (SELECT COUNT(DISTINCT dk.code) FROM daily_kline dk INNER JOIN stock_basic sb ON dk.code = sb.code WHERE sb.instrument_type='stock') AS daily_kline_covered_codes,
                    (SELECT COUNT(*) FROM daily_kline) AS daily_kline_rows,
                    (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='stock' AND (roe IS NOT NULL OR roa IS NOT NULL OR grossprofit_margin IS NOT NULL OR revenue_yoy IS NOT NULL)) AS fundamental_filled_codes,
                    (SELECT MAX(trade_date) FROM daily_kline) AS daily_kline_latest_trade_date,
                    (SELECT MAX(fundamental_updated_at) FROM stock_basic) AS fundamental_latest_updated_at
                """
            )
            row = cursor.fetchone() or {}
            total_codes = int(row.get("total_stock_codes") or 0)
            kline_codes = int(row.get("daily_kline_covered_codes") or 0)
            fundamental_codes = int(row.get("fundamental_filled_codes") or 0)
            return {
                "total_stock_codes": total_codes,
                "daily_kline_covered_codes": kline_codes,
                "daily_kline_coverage_pct": round((kline_codes / total_codes) * 100, 2) if total_codes else None,
                "daily_kline_rows": int(row.get("daily_kline_rows") or 0),
                "fundamental_filled_codes": fundamental_codes,
                "fundamental_coverage_pct": round((fundamental_codes / total_codes) * 100, 2) if total_codes else None,
                "daily_kline_latest_trade_date": str(row.get("daily_kline_latest_trade_date")) if row.get("daily_kline_latest_trade_date") else None,
                "fundamental_latest_updated_at": str(row.get("fundamental_latest_updated_at")) if row.get("fundamental_latest_updated_at") else None,
            }


def _previous_market_strength(current_trade_date: str | None) -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            if current_trade_date:
                cursor.execute(
                    """
                    SELECT trade_date, market_strength
                    FROM market_context_daily
                    WHERE market_strength IS NOT NULL
                      AND trade_date < %s
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    (current_trade_date,),
                )
            else:
                cursor.execute(
                    """
                    SELECT trade_date, market_strength
                    FROM market_context_daily
                    WHERE market_strength IS NOT NULL
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """
                )
            row = cursor.fetchone() or {}
    return {
        "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
        "market_strength": round(float(row.get("market_strength")), 2) if row.get("market_strength") is not None else None,
    }


def _dashboard_market_overview() -> dict:
    """Compute today's market breadth/sector strength from realtime DB snapshots.

    AkShare has Eastmoney board spot functions, but on the current server they are not
    stable enough for request-time use. The homepage therefore reads only MySQL:
    stock_realtime_snapshot + stock_basic.industry.
    """

    st_name_sql = "(COALESCE(r.name, sb.name) LIKE '*ST%' OR COALESCE(r.name, sb.name) LIKE 'ST%' OR COALESCE(r.name, sb.name) LIKE '退市%')"
    limit_rate_sql = f"""
        CASE
            WHEN sb.code LIKE 'bj.%%' THEN 0.30
            WHEN sb.code LIKE 'sz.300%%' OR sb.code LIKE 'sz.301%%' OR sb.code LIKE 'sh.688%%' THEN 0.20
            WHEN {st_name_sql} THEN 0.05
            ELSE 0.10
        END
    """

    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(r.pct_chg > 0) AS up_count,
                    SUM(r.pct_chg < 0) AS down_count,
                    SUM(r.pct_chg = 0) AS flat_count,
                    SUM(CASE WHEN r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS limit_up_count,
                    SUM(CASE WHEN r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS limit_down_count,
                    SUM(CASE WHEN {st_name_sql} AND NOT (r.code LIKE 'bj.%%' OR r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS st_limit_up_count,
                    SUM(CASE WHEN {st_name_sql} AND NOT (r.code LIKE 'bj.%%' OR r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS st_limit_down_count,
                    SUM(CASE WHEN (r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board20_limit_up_count,
                    SUM(CASE WHEN (r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board20_limit_down_count,
                    SUM(CASE WHEN r.code LIKE 'bj.%%' AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board30_limit_up_count,
                    SUM(CASE WHEN r.code LIKE 'bj.%%' AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board30_limit_down_count,
                    SUM(CASE WHEN NOT {st_name_sql} AND NOT (r.code LIKE 'bj.%%') AND NOT (r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price >= ROUND(r.pre_close * (1 + ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board10_limit_up_count,
                    SUM(CASE WHEN NOT {st_name_sql} AND NOT (r.code LIKE 'bj.%%') AND NOT (r.code LIKE 'sz.300%%' OR r.code LIKE 'sz.301%%' OR r.code LIKE 'sh.688%%') AND r.pre_close > 0 AND r.latest_price > 0 AND r.latest_price <= ROUND(r.pre_close * (1 - ({limit_rate_sql})), 2) THEN 1 ELSE 0 END) AS board10_limit_down_count,
                    SUM(r.pct_chg >= 5) AS strong_up_count,
                    SUM(r.pct_chg <= -5) AS strong_down_count,
                    AVG(r.pct_chg) AS avg_pct_chg,
                    SUM(r.amount * r.pct_chg) / NULLIF(SUM(r.amount), 0) AS amount_weighted_pct_chg,
                    SUM(CASE WHEN r.pct_chg > 0 THEN r.amount ELSE 0 END) AS up_amount,
                    SUM(CASE WHEN r.pct_chg < 0 THEN r.amount ELSE 0 END) AS down_amount,
                    SUM(r.amount) AS total_amount,
                    MAX(r.quote_time) AS latest_quote_time,
                    MAX(r.trade_date) AS trade_date
                FROM stock_realtime_snapshot r
                LEFT JOIN stock_basic sb ON r.code = sb.code
                WHERE r.pct_chg IS NOT NULL
                  AND (r.code LIKE 'sh.%%' OR r.code LIKE 'sz.%%' OR r.code LIKE 'bj.%%')
                """
            )
            breadth = cursor.fetchone() or {}

            cursor.execute(
                """
                SELECT
                    sector_type,
                    sector_name,
                    pct_chg,
                    inflow_amount,
                    outflow_amount,
                    net_amount,
                    company_count,
                    leading_stock,
                    leading_stock_pct_chg,
                    leading_stock_price,
                    quote_time,
                    source_unit
                FROM market_sector_fund_flow_snapshot
                WHERE net_amount IS NOT NULL
                ORDER BY net_amount DESC, pct_chg DESC
                LIMIT 8
                """
            )
            fund_strong_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    sector_type,
                    sector_name,
                    pct_chg,
                    inflow_amount,
                    outflow_amount,
                    net_amount,
                    company_count,
                    leading_stock,
                    leading_stock_pct_chg,
                    leading_stock_price,
                    quote_time,
                    source_unit
                FROM market_sector_fund_flow_snapshot
                WHERE net_amount IS NOT NULL
                ORDER BY net_amount ASC, pct_chg ASC
                LIMIT 8
                """
            )
            fund_weak_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT MAX(quote_time) AS latest_fund_flow_time, COUNT(*) AS fund_flow_rows
                FROM market_sector_fund_flow_snapshot
                """
            )
            fund_flow_meta = cursor.fetchone() or {}

            cursor.execute(
                """
                SELECT
                    sb.industry,
                    COUNT(*) AS stock_count,
                    AVG(r.pct_chg) AS avg_pct_chg,
                    SUM(r.amount * r.pct_chg) / NULLIF(SUM(r.amount), 0) AS amount_weighted_pct_chg,
                    SUM(r.pct_chg > 0) AS up_count,
                    SUM(r.pct_chg < 0) AS down_count,
                    SUM(CASE WHEN r.pct_chg > 0 THEN r.amount ELSE 0 END) AS up_amount,
                    SUM(CASE WHEN r.pct_chg < 0 THEN r.amount ELSE 0 END) AS down_amount,
                    SUM(r.amount) AS amount
                FROM stock_realtime_snapshot r
                INNER JOIN stock_basic sb ON r.code = sb.code
                WHERE sb.instrument_type = 'stock'
                  AND r.pct_chg IS NOT NULL
                  AND sb.industry IS NOT NULL
                  AND sb.industry <> ''
                GROUP BY sb.industry
                HAVING stock_count >= 5
                ORDER BY amount_weighted_pct_chg DESC, avg_pct_chg DESC
                LIMIT 8
                """
            )
            strong_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    sb.industry,
                    COUNT(*) AS stock_count,
                    AVG(r.pct_chg) AS avg_pct_chg,
                    SUM(r.amount * r.pct_chg) / NULLIF(SUM(r.amount), 0) AS amount_weighted_pct_chg,
                    SUM(r.pct_chg > 0) AS up_count,
                    SUM(r.pct_chg < 0) AS down_count,
                    SUM(CASE WHEN r.pct_chg > 0 THEN r.amount ELSE 0 END) AS up_amount,
                    SUM(CASE WHEN r.pct_chg < 0 THEN r.amount ELSE 0 END) AS down_amount,
                    SUM(r.amount) AS amount
                FROM stock_realtime_snapshot r
                INNER JOIN stock_basic sb ON r.code = sb.code
                WHERE sb.instrument_type = 'stock'
                  AND r.pct_chg IS NOT NULL
                  AND sb.industry IS NOT NULL
                  AND sb.industry <> ''
                GROUP BY sb.industry
                HAVING stock_count >= 5
                ORDER BY amount_weighted_pct_chg ASC, avg_pct_chg ASC
                LIMIT 8
                """
            )
            weak_rows = cursor.fetchall()

    total = _to_int(breadth.get("total"))
    up_count = _to_int(breadth.get("up_count"))
    down_count = _to_int(breadth.get("down_count"))
    flat_count = _to_int(breadth.get("flat_count"))
    limit_up_count = _to_int(breadth.get("limit_up_count"))
    limit_down_count = _to_int(breadth.get("limit_down_count"))
    board30_limit_up_count = _to_int(breadth.get("board30_limit_up_count"))
    board30_limit_down_count = _to_int(breadth.get("board30_limit_down_count"))
    hs_limit_up_count = limit_up_count - board30_limit_up_count
    hs_limit_down_count = limit_down_count - board30_limit_down_count
    avg_pct_chg = _to_float(breadth.get("avg_pct_chg"))
    amount_weighted_pct_chg = _to_float(breadth.get("amount_weighted_pct_chg"))
    total_amount = _to_float(breadth.get("total_amount"))
    up_amount = _to_float(breadth.get("up_amount")) or 0
    down_amount = _to_float(breadth.get("down_amount")) or 0
    amount_pressure = round((up_amount - down_amount) / total_amount, 4) if total_amount else None
    up_ratio = round(up_count / total, 4) if total else None
    down_ratio = round(down_count / total, 4) if total else None

    if total and avg_pct_chg is not None and up_ratio is not None:
        weighted_pct = amount_weighted_pct_chg if amount_weighted_pct_chg is not None else avg_pct_chg
        raw_strength = 50 + weighted_pct * 7 + (up_ratio - 0.5) * 45 + ((amount_pressure or 0) * 18) + ((limit_up_count - limit_down_count) / total) * 80
        market_strength = round(max(0, min(100, raw_strength)), 2)
    else:
        market_strength = None

    state = _market_state(market_strength, up_ratio)
    previous_strength = _previous_market_strength(str(breadth.get("trade_date")) if breadth.get("trade_date") else None)
    strength_change = None
    if market_strength is not None and previous_strength.get("market_strength") is not None:
        strength_change = round(market_strength - float(previous_strength["market_strength"]), 2)

    def format_sector(row: dict) -> dict:
        stock_count = _to_int(row.get("stock_count"))
        sector_up_count = _to_int(row.get("up_count"))
        sector_down_count = _to_int(row.get("down_count"))
        return {
            "name": _clean_industry_name(row.get("industry")),
            "raw_name": row.get("industry"),
            "stock_count": stock_count,
            "avg_pct_chg": round(_to_float(row.get("avg_pct_chg")) or 0, 2),
            "amount_weighted_pct_chg": round(_to_float(row.get("amount_weighted_pct_chg")) or 0, 2),
            "up_count": sector_up_count,
            "down_count": sector_down_count,
            "up_ratio": round(sector_up_count / stock_count, 4) if stock_count else None,
            "amount": _to_float(row.get("amount")),
            "up_amount": _to_float(row.get("up_amount")),
            "down_amount": _to_float(row.get("down_amount")),
            "net_amount_proxy": (_to_float(row.get("up_amount")) or 0) - (_to_float(row.get("down_amount")) or 0),
        }

    def format_fund_sector(row: dict) -> dict:
        inflow = _to_float(row.get("inflow_amount")) or 0
        outflow = _to_float(row.get("outflow_amount")) or 0
        net_amount = _to_float(row.get("net_amount")) or 0
        company_count = _to_int(row.get("company_count"))
        return {
            "name": row.get("sector_name"),
            "raw_name": row.get("sector_name"),
            "sector_type": row.get("sector_type"),
            "sector_type_label": "概念" if row.get("sector_type") == "concept" else "行业",
            "stock_count": company_count,
            "avg_pct_chg": round(_to_float(row.get("pct_chg")) or 0, 2),
            "amount_weighted_pct_chg": round(_to_float(row.get("pct_chg")) or 0, 2),
            "up_count": None,
            "down_count": None,
            "up_ratio": None,
            "amount": inflow + outflow,
            "up_amount": inflow,
            "down_amount": outflow,
            "net_amount_proxy": net_amount,
            "net_amount": net_amount,
            "inflow_amount": inflow,
            "outflow_amount": outflow,
            "leading_stock": row.get("leading_stock"),
            "leading_stock_pct_chg": _to_float(row.get("leading_stock_pct_chg")),
            "source_unit": row.get("source_unit") or "亿元",
            "quote_time": str(row.get("quote_time")) if row.get("quote_time") else None,
        }

    has_fund_flow = bool(fund_strong_rows or fund_weak_rows)

    return {
        "source": "stock_realtime_snapshot + market_sector_fund_flow_snapshot",
        "board_api_status": "akshare_board_spot_unstable_remote_disconnected",
        "trade_date": str(breadth.get("trade_date")) if breadth.get("trade_date") else None,
        "latest_quote_time": str(breadth.get("latest_quote_time")) if breadth.get("latest_quote_time") else None,
        "market_strength": market_strength,
        "previous_market_strength": previous_strength.get("market_strength"),
        "previous_market_strength_trade_date": previous_strength.get("trade_date"),
        "market_strength_change": strength_change,
        "market_state": state,
        "market_state_label": _market_state_label(state),
        "total": total,
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "up_ratio": up_ratio,
        "down_ratio": down_ratio,
        "avg_pct_chg": round(avg_pct_chg, 2) if avg_pct_chg is not None else None,
        "amount_weighted_pct_chg": round(amount_weighted_pct_chg, 2) if amount_weighted_pct_chg is not None else None,
        "up_amount": up_amount,
        "down_amount": down_amount,
        "net_amount_proxy": up_amount - down_amount,
        "amount_pressure": amount_pressure,
        "limit_up_count": hs_limit_up_count,
        "limit_down_count": hs_limit_down_count,
        "limit_up_like": hs_limit_up_count,
        "limit_down_like": hs_limit_down_count,
        "all_limit_up_count": limit_up_count,
        "all_limit_down_count": limit_down_count,
        "limit_rule": "price_equals_exchange_limit_by_bucket: 北交所=30%, 创业板/科创板=20%, 主板ST/退市=5%, others=10%; latest_price must be > 0; based on realtime name + code prefix",
        "limit_breakdown": {
            "st": {"up": _to_int(breadth.get("st_limit_up_count")), "down": _to_int(breadth.get("st_limit_down_count"))},
            "board10": {"up": _to_int(breadth.get("board10_limit_up_count")), "down": _to_int(breadth.get("board10_limit_down_count"))},
            "board20": {"up": _to_int(breadth.get("board20_limit_up_count")), "down": _to_int(breadth.get("board20_limit_down_count"))},
            "board30": {"up": board30_limit_up_count, "down": board30_limit_down_count},
        },
        "strong_up_count": _to_int(breadth.get("strong_up_count")),
        "strong_down_count": _to_int(breadth.get("strong_down_count")),
        "total_amount": total_amount,
        "sector_source": "akshare_realtime_fund_flow" if has_fund_flow else "stock_realtime_snapshot_industry_fallback",
        "sector_source_label": "即时行业/概念资金流" if has_fund_flow else "本地行业成交额加权统计",
        "sector_fund_flow_time": str(fund_flow_meta.get("latest_fund_flow_time")) if fund_flow_meta.get("latest_fund_flow_time") else None,
        "sector_fund_flow_rows": _to_int(fund_flow_meta.get("fund_flow_rows")),
        "strong_sectors": [format_fund_sector(row) for row in fund_strong_rows] if has_fund_flow else [format_sector(row) for row in strong_rows],
        "weak_sectors": [format_fund_sector(row) for row in fund_weak_rows] if has_fund_flow else [format_sector(row) for row in weak_rows],
    }


@router.get("/dashboard/summary")
def dashboard_summary(limit: Annotated[int, Query(ge=1, le=20)] = 5) -> dict:
    tracker = SelectionResultTracker()

    preview_records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type="stock")
    preview_items = tracker.to_dict_list(preview_records)

    price_values = [item["price_change_pct"] for item in preview_items if item.get("price_change_pct") is not None]
    avg_price_change_pct = round(sum(price_values) / len(price_values), 2) if price_values else None

    latest_trade_date = preview_items[0].get("selection_date") if preview_items else None
    latest_selection_summary = None
    if preview_items:
        top_items = preview_items[:3]
        latest_selection_summary = {
            "run_id": preview_items[0].get("run_id"),
            "strategy_display_name": preview_items[0].get("strategy_display_name") or preview_items[0].get("strategy_id"),
            "selected_trade_date": preview_items[0].get("selection_date"),
            "pick_count": len(preview_items),
            "top_items": [
                {
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "score": item.get("score"),
                    "price_change_pct": item.get("price_change_pct"),
                }
                for item in top_items
            ],
        }

    return {
        "latest_trade_date": latest_trade_date,
        "market_overview": _dashboard_market_overview(),
        "latest_tracking_count": len(preview_items),
        "latest_tracking_avg_price_change_pct": avg_price_change_pct,
        "latest_tracking_preview": preview_items,
        "latest_selection_summary": latest_selection_summary,
    }
