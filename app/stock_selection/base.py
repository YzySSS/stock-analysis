from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseSelectionStrategy(ABC):
    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}

    @abstractmethod
    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @abstractmethod
    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def score(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def select(self, scored_stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def explain(self, stock: Dict[str, Any]) -> Dict[str, Any]:
        pass
