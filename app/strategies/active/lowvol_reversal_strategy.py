from __future__ import annotations

from typing import Any, Dict, List

from app.stock_selection.base import BaseSelectionStrategy


class LowVolReversalStrategy(BaseSelectionStrategy):
    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        return data_bundle

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates = data_bundle.get("candidates", [])
        results: List[Dict[str, Any]] = []
        for item in candidates:
            turnover = float(item.get("turnover_score", 0))
            lowvol = float(item.get("lowvol_score", 0))
            reversal = float(item.get("reversal_score", 0))
            results.append(
                {
                    **item,
                    "factors": {
                        "turnover": turnover,
                        "lowvol": lowvol,
                        "reversal": reversal,
                    },
                }
            )
        return results

    def score(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        weights = self.config.get("weights", {})
        scored: List[Dict[str, Any]] = []
        for item in stocks:
            factors = item.get("factors", {})
            total_score = (
                factors.get("turnover", 0) * weights.get("turnover", 0)
                + factors.get("lowvol", 0) * weights.get("lowvol", 0)
                + factors.get("reversal", 0) * weights.get("reversal", 0)
            )
            scored.append({**item, "score": round(total_score, 4)})
        return sorted(scored, key=lambda x: x.get("score", 0), reverse=True)

    def select(self, scored_stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        threshold = float(self.config.get("score_threshold", 0))
        max_picks = int(self.config.get("max_picks", 5))
        selected = [x for x in scored_stocks if float(x.get("score", 0)) >= threshold]
        return selected[:max_picks]

    def explain(self, stock: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "code": stock.get("code"),
            "score": stock.get("score"),
            "factors": stock.get("factors", {}),
            "strategy": "lowvol_reversal",
        }
