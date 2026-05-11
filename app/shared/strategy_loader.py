"""策略注册表加载器。

用于从 app/strategies/registry/strategies.yaml 读取策略配置，
并动态加载默认策略或指定策略。
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class StrategyRegistryError(RuntimeError):
    """策略注册表异常。"""


class StrategyLoader:
    def __init__(self, registry_path: Optional[str] = None):
        root = Path(__file__).resolve().parents[2]
        self.registry_path = Path(registry_path) if registry_path else root / "app" / "strategies" / "registry" / "strategies.yaml"
        self.registry = self._load_registry()

    def _load_registry(self) -> Dict[str, Any]:
        if not self.registry_path.exists():
            raise StrategyRegistryError(f"策略注册表不存在: {self.registry_path}")

        with self.registry_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        if "strategies" not in data:
            raise StrategyRegistryError("策略注册表缺少 strategies 字段")

        return data

    def get_default_strategy_id(self) -> str:
        strategy_id = self.registry.get("default_strategy")
        if not strategy_id:
            raise StrategyRegistryError("策略注册表未配置 default_strategy")
        return strategy_id

    def get_strategy_meta(self, strategy_id: str) -> Dict[str, Any]:
        for item in self.registry.get("strategies", []):
            if item.get("id") == strategy_id:
                return item
        raise StrategyRegistryError(f"未找到策略: {strategy_id}")

    def load_config(self, strategy_id: str) -> Dict[str, Any]:
        meta = self.get_strategy_meta(strategy_id)
        config_path = meta.get("config_path")
        if not config_path:
            return {}

        root = Path(__file__).resolve().parents[2]
        full_path = root / config_path
        if not full_path.exists():
            raise StrategyRegistryError(f"策略配置文件不存在: {full_path}")

        with full_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def load_strategy(self, strategy_id: Optional[str] = None):
        strategy_id = strategy_id or self.get_default_strategy_id()
        meta = self.get_strategy_meta(strategy_id)

        entrypoint = meta.get("entrypoint")
        if not entrypoint:
            raise StrategyRegistryError(f"策略 {strategy_id} 缺少 entrypoint")

        module_path, class_name = entrypoint.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)

        config = self.load_config(strategy_id)
        strategy_config = {
            **config.get("selection", {}),
            "sentiment_prefetch": config.get("sentiment_prefetch", {}),
            "sentiment_rank": config.get("sentiment_rank", {}),
            "weights": {
                k: v.get("weight", 0)
                for k, v in config.get("factors", {}).items()
            },
            "hard_filters": config.get("hard_filters", {}),
            "market_weight_adjustments": config.get("market_weight_adjustments", {}),
        }
        return cls(config=strategy_config)
