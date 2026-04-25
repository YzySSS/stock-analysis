from __future__ import annotations

from fastapi import APIRouter, Query

from app.error_learning.tracker import SelectionResultTracker
from app.shared.db import ping_mysql
from app.strategies.service import StrategyService

router = APIRouter(tags=["dashboard"])


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

    latest_trade_date = preview_items[0].get("selection_date") if preview_items else None

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
    }
