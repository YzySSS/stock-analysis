from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException

from app.shared.db import mysql_read_conn

router = APIRouter(tags=["trade-strategies"])


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def normalize_trade_strategy(row: dict) -> dict:
    return {
        "strategy_id": row.get("strategy_id"),
        "display_name": row.get("display_name"),
        "version": row.get("version"),
        "status": row.get("status"),
        "is_builtin": bool(row.get("is_builtin")),
        "description": row.get("description"),
        "buy_rule": _json_value(row.get("buy_rule_json")),
        "sell_rule": _json_value(row.get("sell_rule_json")),
        "risk_rule": _json_value(row.get("risk_rule_json")),
        "cost_rule": _json_value(row.get("cost_rule_json")),
        "execution_rule": _json_value(row.get("execution_rule_json")),
        "created_at": str(row.get("created_at")) if row.get("created_at") else None,
        "updated_at": str(row.get("updated_at")) if row.get("updated_at") else None,
    }


@router.get("/trade-strategies")
def list_trade_strategies() -> dict:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM trade_strategy
                ORDER BY is_builtin DESC, id ASC
                """
            )
            rows = cursor.fetchall() or []
    return {"items": [normalize_trade_strategy(row) for row in rows]}


@router.get("/trade-strategies/{strategy_id}")
def get_trade_strategy(strategy_id: str) -> dict:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM trade_strategy
                WHERE strategy_id = %s
                ORDER BY version DESC
                LIMIT 1
                """,
                (strategy_id,),
            )
            row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="trade strategy not found")
    return normalize_trade_strategy(row)
