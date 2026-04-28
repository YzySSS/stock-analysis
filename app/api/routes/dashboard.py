from __future__ import annotations

from fastapi import APIRouter, Query

from app.error_learning.tracker import SelectionResultTracker
from app.shared.db import mysql_conn, ping_mysql
from app.strategies.service import StrategyService

router = APIRouter(tags=["dashboard"])


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


@router.get("/dashboard/summary")
def dashboard_summary(limit: int = Query(default=5, ge=1, le=20)) -> dict:
    strategy_service = StrategyService()
    tracker = SelectionResultTracker()

    strategies = strategy_service.list_strategies()
    preview_records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type="stock")
    preview_items = tracker.to_dict_list(preview_records)

    price_values = [item["price_change_pct"] for item in preview_items if item.get("price_change_pct") is not None]
    avg_price_change_pct = round(sum(price_values) / len(price_values), 2) if price_values else None

    mysql_info = ping_mysql()
    data_stats = _dashboard_data_stats()

    latest_trade_date = preview_items[0].get("selection_date") if preview_items else data_stats.get("daily_kline_latest_trade_date")
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
        "health": {
            "status": "ok",
            "database": mysql_info.get("db"),
            "version": mysql_info.get("version"),
        },
        "default_strategy": strategy_service.get_default_strategy_id(),
        "strategy_count": len(strategies),
        "latest_trade_date": latest_trade_date,
        "latest_tracking_count": len(preview_items),
        "latest_tracking_avg_price_change_pct": avg_price_change_pct,
        "latest_tracking_preview": preview_items,
        "data_stats": data_stats,
        "latest_selection_summary": latest_selection_summary,
    }
