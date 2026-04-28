from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.selector import StockSelector


class StrategyService:
    RUNTIME_READY_IDS = {"lowvol_reversal"}

    def __init__(self, registry_path: Optional[str] = None):
        self.loader = StrategyLoader(registry_path=registry_path)

    def _serialize_strategy_item(self, item: Dict[str, Any], default_strategy: str) -> Dict[str, Any]:
        strategy_id = item.get("id")
        executable = bool(item.get("executable", True))
        runtime_ready = strategy_id in self.RUNTIME_READY_IDS and executable
        mode = item.get("mode") or "current"
        status = item.get("status") or "unknown"

        if runtime_ready:
            availability = "runtime_ready"
            availability_label = "可执行"
            availability_note = "当前已接通 V1 执行链路，可直接运行并保存结果。"
        elif executable and status == "experimental":
            availability = "experimental"
            availability_label = "实验中"
            availability_note = "已注册但尚未接通现有执行协议，暂不在选股页开放运行。"
        else:
            availability = "display_only"
            availability_label = "仅展示"
            availability_note = "当前只保留配置与说明，用于页面展示或历史参考。"

        return {
            "id": strategy_id,
            "display_name": item.get("display_name"),
            "version": item.get("version"),
            "status": status,
            "mode": mode,
            "description": item.get("description"),
            "tags": item.get("tags", []),
            "executable": executable,
            "runtime_ready": runtime_ready,
            "availability": availability,
            "availability_label": availability_label,
            "availability_note": availability_note,
            "is_default": strategy_id == default_strategy,
        }

    def list_strategies(self) -> List[Dict[str, Any]]:
        default_strategy = self.get_default_strategy_id()
        return [
            self._serialize_strategy_item(item, default_strategy)
            for item in self.loader.registry.get("strategies", [])
        ]

    def get_default_strategy_id(self) -> str:
        return self.loader.get_default_strategy_id()

    def get_strategy_meta(self, strategy_id: Optional[str] = None) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        return self.loader.get_strategy_meta(final_strategy_id)

    def get_strategy_detail(self, strategy_id: Optional[str] = None, instrument_type: str = "stock", sample_limit: int = 200) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        meta = self.get_strategy_meta(final_strategy_id)
        serialized_meta = self._serialize_strategy_item(meta, self.get_default_strategy_id())
        config = self.loader.load_config(final_strategy_id)
        factor_configs = config.get("factors", {}) or {}
        executable = bool(meta.get("executable", True))
        runtime_ready = serialized_meta.get("runtime_ready", False)
        factor_stats = {}
        if runtime_ready:
            selector = StockSelector(strategy_id=final_strategy_id)
            factor_stats = selector.build_factor_analysis(instrument_type=instrument_type, limit=sample_limit)

        factor_items = []
        for key, factor_meta in factor_configs.items():
            stat = factor_stats.get(key, {})
            factor_items.append(
                {
                    "key": key,
                    "name": factor_meta.get("name") or key,
                    "category": factor_meta.get("category") or "general",
                    "description": factor_meta.get("description") or "",
                    "direction": factor_meta.get("direction") or "positive",
                    "weight": factor_meta.get("weight", 0),
                    "enabled": factor_meta.get("enabled", True),
                    "ci": stat.get("ci", factor_meta.get("ci_hint")),
                    "coverage": stat.get("coverage"),
                    "missing_rate": stat.get("missing_rate"),
                    "sample_size": stat.get("sample_size") if stat else None,
                    "is_placeholder": not bool(stat),
                }
            )

        return {
            "id": meta.get("id"),
            "display_name": meta.get("display_name"),
            "version": meta.get("version"),
            "status": meta.get("status"),
            "mode": meta.get("mode") or "current",
            "executable": executable,
            "runtime_ready": runtime_ready,
            "availability": serialized_meta.get("availability"),
            "availability_label": serialized_meta.get("availability_label"),
            "availability_note": serialized_meta.get("availability_note"),
            "description": meta.get("description"),
            "tags": meta.get("tags", []),
            "score_threshold": config.get("selection", {}).get("score_threshold"),
            "max_picks": config.get("selection", {}).get("max_picks"),
            "factor_sample_size": sample_limit if runtime_ready else None,
            "factors": factor_items,
        }

    def run_strategy(
        self,
        strategy_id: Optional[str] = None,
        limit: int = 50,
        instrument_type: str = "stock",
        save: bool = True,
        score_threshold: Optional[float] = None,
    ) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        strategy_meta = self.get_strategy_meta(final_strategy_id)
        serialized_meta = self._serialize_strategy_item(strategy_meta, self.get_default_strategy_id())
        if not serialized_meta.get("runtime_ready"):
            raise ValueError(f"策略 {final_strategy_id} 当前未接通 V1 执行链路，暂不可运行")

        overrides = {}
        if score_threshold is not None:
            overrides["score_threshold"] = float(score_threshold)

        selector = StockSelector(strategy_id=final_strategy_id, strategy_overrides=overrides)

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
            "runtime_ready": serialized_meta.get("runtime_ready"),
            "availability": serialized_meta.get("availability"),
            "availability_label": serialized_meta.get("availability_label"),
            "score_threshold": selector.strategy.config.get("score_threshold"),
        }
        return result
