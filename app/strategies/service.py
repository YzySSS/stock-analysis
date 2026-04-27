from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.selector import StockSelector


class StrategyService:
    def __init__(self, registry_path: Optional[str] = None):
        self.loader = StrategyLoader(registry_path=registry_path)

    def list_strategies(self) -> List[Dict[str, Any]]:
        return [
            {
                "id": item.get("id"),
                "display_name": item.get("display_name"),
                "version": item.get("version"),
                "status": item.get("status"),
                "description": item.get("description"),
                "tags": item.get("tags", []),
            }
            for item in self.loader.registry.get("strategies", [])
        ]

    def get_default_strategy_id(self) -> str:
        return self.loader.get_default_strategy_id()

    def get_strategy_meta(self, strategy_id: Optional[str] = None) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        return self.loader.get_strategy_meta(final_strategy_id)

    def get_strategy_detail(self, strategy_id: Optional[str] = None) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        meta = self.get_strategy_meta(final_strategy_id)
        config = self.loader.load_config(final_strategy_id)
        factor_configs = config.get("factors", {}) or {}

        factor_items = [
            {
                "key": key,
                "name": factor_meta.get("name") or key,
                "description": factor_meta.get("description") or "",
                "direction": factor_meta.get("direction") or "positive",
                "weight": factor_meta.get("weight", 0),
            }
            for key, factor_meta in factor_configs.items()
        ]

        return {
            "id": meta.get("id"),
            "display_name": meta.get("display_name"),
            "version": meta.get("version"),
            "status": meta.get("status"),
            "description": meta.get("description"),
            "tags": meta.get("tags", []),
            "score_threshold": config.get("selection", {}).get("score_threshold"),
            "max_picks": config.get("selection", {}).get("max_picks"),
            "factors": factor_items,
        }

    def run_strategy(
        self,
        strategy_id: Optional[str] = None,
        limit: int = 50,
        instrument_type: str = "stock",
        save: bool = True,
    ) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        selector = StockSelector(strategy_id=final_strategy_id)
        strategy_meta = self.get_strategy_meta(final_strategy_id)

        if save:
            result = selector.run_and_save(limit=limit, instrument_type=instrument_type)
        else:
            items = selector.run_from_mysql(limit=limit, instrument_type=instrument_type)
            result = {
                "run_id": None,
                "strategy_id": final_strategy_id,
                "count": len(items),
                "results": items,
            }

        result["strategy"] = {
            "id": strategy_meta.get("id"),
            "display_name": strategy_meta.get("display_name"),
            "version": strategy_meta.get("version"),
            "status": strategy_meta.get("status"),
        }
        return result
