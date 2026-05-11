from __future__ import annotations

from collections import Counter
from math import isfinite
from typing import Any, Dict, List, Sequence

from app.stock_selection.base import BaseSelectionStrategy


class LowVolReversalStrategy(BaseSelectionStrategy):
    """Low-volatility reversal strategy, V2.1 implementation.

    V2 removed the old proxy-factor/code-order bug but exposed a real strategy
    problem: 20d deep reversal + high turnover caught falling knives. V2.1 keeps
    the market-derived cross-sectional scoring, but tightens A-share risk filters
    and changes the factor semantics to short-term reversal + low turnover + true
    low volatility.
    """

    DEFAULT_MIN_KLINE_COUNT_20 = 15
    DEFAULT_MIN_KLINE_COUNT_60 = 60
    DEFAULT_MIN_AVG_AMOUNT = 5_000_000
    DEFAULT_MIN_CLOSE = 2
    DEFAULT_MIN_LISTED_DAYS = 60
    DEFAULT_MAX_DAILY_DROP_PCT = -9.5
    DEFAULT_MAX_TURNOVER_RATE = 20

    def prepare_context(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
        return data_bundle

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if isfinite(number) else None

    @classmethod
    def _percentile_scores(cls, values: Sequence[float | None], higher_is_better: bool = True) -> List[float]:
        non_missing = sorted(v for v in values if v is not None and isfinite(v))
        if not values:
            return []
        if not non_missing:
            return [50.0 for _ in values]
        median = non_missing[len(non_missing) // 2]
        filled = [v if v is not None and isfinite(v) else median for v in values]
        if not higher_is_better:
            filled = [-v for v in filled]
        n = len(filled)
        if n == 1:
            return [50.0]
        indexed = sorted(enumerate(filled), key=lambda pair: pair[1])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and indexed[j + 1][1] == indexed[i][1]:
                j += 1
            average_rank = (i + j) / 2
            percentile = average_rank / (n - 1) * 100
            for k in range(i, j + 1):
                ranks[indexed[k][0]] = percentile
            i = j + 1
        return [round(score, 4) for score in ranks]

    def _is_valid_candidate(self, item: Dict[str, Any]) -> bool:
        min_kline_20 = int(self.config.get("min_kline_count_20", self.DEFAULT_MIN_KLINE_COUNT_20))
        min_kline_60 = int(self.config.get("min_kline_count_60", self.DEFAULT_MIN_KLINE_COUNT_60))
        min_avg_amount = float(self.config.get("min_avg_amount_20", self.DEFAULT_MIN_AVG_AMOUNT))
        min_close = float(self.config.get("min_close", self.DEFAULT_MIN_CLOSE))
        min_listed_days = int(self.config.get("min_listed_days", self.DEFAULT_MIN_LISTED_DAYS))
        max_daily_drop_pct = float(self.config.get("max_daily_drop_pct", self.DEFAULT_MAX_DAILY_DROP_PCT))
        max_turnover_rate = float(self.config.get("max_turnover_rate", self.DEFAULT_MAX_TURNOVER_RATE))

        if item.get("is_st"):
            return False
        if int(item.get("kline_count_20") or 0) < min_kline_20:
            return False
        if int(item.get("kline_count_60") or 0) < min_kline_60:
            return False
        listed_days = item.get("listed_days")
        if listed_days is not None and int(listed_days) < min_listed_days:
            return False
        close = self._to_float(item.get("close"))
        if close is None or close <= min_close:
            return False
        avg_amount = self._to_float(item.get("avg_amount_20"))
        if avg_amount is None or avg_amount < min_avg_amount:
            return False
        pct_chg_1d = self._to_float(item.get("pct_chg_1d"))
        if pct_chg_1d is not None and pct_chg_1d <= max_daily_drop_pct:
            return False
        turnover_rate = self._to_float(item.get("turnover_rate"))
        if turnover_rate is not None and turnover_rate >= max_turnover_rate:
            return False
        return True

    def compute_factors(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates = data_bundle.get("candidates", [])
        valid = [item for item in candidates if self._is_valid_candidate(item)]
        raw_rows: List[Dict[str, Any]] = []

        for item in valid:
            close = self._to_float(item.get("close"))
            close_5d = self._to_float(item.get("close_5d"))
            ma60 = self._to_float(item.get("ma60"))
            std_return_20 = self._to_float(item.get("std_return_20"))
            turnover_5d = self._to_float(item.get("turnover_rate_5d_avg")) or self._to_float(item.get("turnover_rate"))
            avg_amount_20 = self._to_float(item.get("avg_amount_20"))
            pct_chg_1d = self._to_float(item.get("pct_chg_1d"))

            return_5d = None
            if close is not None and close_5d and close_5d > 0:
                return_5d = (close - close_5d) / close_5d

            trend_ok = bool(close is not None and ma60 is not None and close > ma60)

            raw_rows.append(
                {
                    "item": item,
                    "raw_turnover": turnover_5d,
                    "raw_lowvol": std_return_20,
                    "raw_reversal": return_5d if trend_ok else None,
                    "trend_ok": trend_ok,
                    "raw_avg_amount_20": avg_amount_20,
                    "raw_pct_chg_1d": pct_chg_1d,
                    "raw_ma60": ma60,
                }
            )

        # V2.1 directions:
        # - turnover: lower/settled turnover is preferred for reversal candidates.
        # - lowvol: lower 20d return volatility is preferred.
        # - reversal: lower 5d return is preferred, but only above MA60; otherwise neutral.
        turnover_scores = self._percentile_scores([row["raw_turnover"] for row in raw_rows], higher_is_better=False)
        lowvol_scores = self._percentile_scores([row["raw_lowvol"] for row in raw_rows], higher_is_better=False)
        reversal_scores = self._percentile_scores([row["raw_reversal"] for row in raw_rows], higher_is_better=False)

        results: List[Dict[str, Any]] = []
        for index, row in enumerate(raw_rows):
            item = row["item"]
            reversal_score = reversal_scores[index] if row["trend_ok"] else 50.0
            results.append(
                {
                    **item,
                    "factors": {
                        "turnover": turnover_scores[index],
                        "lowvol": lowvol_scores[index],
                        "reversal": reversal_score,
                    },
                    "raw_lowvol_reversal_metrics": {
                        "turnover_rate_5d_avg": row["raw_turnover"],
                        "std_return_20": row["raw_lowvol"],
                        "return_5d": row["raw_reversal"],
                        "trend_ok": row["trend_ok"],
                        "ma60": row["raw_ma60"],
                        "avg_amount_20": row["raw_avg_amount_20"],
                        "pct_chg_1d": row["raw_pct_chg_1d"],
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
                float(factors.get("turnover") or 0) * weights.get("turnover", 0)
                + float(factors.get("lowvol") or 0) * weights.get("lowvol", 0)
                + float(factors.get("reversal") or 0) * weights.get("reversal", 0)
            )
            scored.append({**item, "score": round(total_score, 4)})
        return sorted(
            scored,
            key=lambda x: (
                -float(x.get("score") or 0),
                -float((x.get("factors") or {}).get("lowvol") or 0),
                -float((x.get("factors") or {}).get("reversal") or 0),
                -float((x.get("factors") or {}).get("turnover") or 0),
                -float(x.get("avg_amount_20") or 0),
                str(x.get("code") or ""),
            ),
        )

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
            "raw_metrics": stock.get("raw_lowvol_reversal_metrics", {}),
            "strategy": "lowvol_reversal",
            "version_note": "v2.1_short_reversal_low_turnover_low_volatility",
        }

    def score_diagnostics(self, scored_stocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        score_counts = Counter(round(float(item.get("score") or 0), 4) for item in scored_stocks)
        top_ties = score_counts.most_common(5)
        return {
            "candidate_count": len(scored_stocks),
            "qualified_count": sum(1 for item in scored_stocks if float(item.get("score") or 0) >= float(self.config.get("score_threshold", 0))),
            "top_ties": [{"score": score, "count": count} for score, count in top_ties],
        }
