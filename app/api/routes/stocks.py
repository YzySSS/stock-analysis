from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException

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


@router.get("/stocks/{code}")
def stock_detail(code: str) -> dict:
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
        "latest_selection": latest_selection,
        "selection_history": selection_history,
        "updated_at": str(basic.get("updated_at")) if basic.get("updated_at") else None,
    }
