from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.data_ingestion.intraday_bar_sync import get_or_fetch_intraday_bars
from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.selector import StockSelector
from app.stock_selection.sentiment_refresh import refresh_v12_candidate_sentiment


class StrategyService:
    RUNTIME_READY_IDS = {"lowvol_reversal", "v13_three_factor", "v12_legacy"}

    def __init__(self, registry_path: Optional[str] = None):
        self.loader = StrategyLoader(registry_path=registry_path)

    def _serialize_strategy_item(self, item: Dict[str, Any], default_strategy: str) -> Dict[str, Any]:
        strategy_id = item.get("id")
        executable = bool(item.get("executable", True))
        runtime_ready = strategy_id in self.RUNTIME_READY_IDS and executable
        mode = item.get("mode") or "current"
        status = item.get("status") or "unknown"

        if status in {"research", "paused"}:
            runtime_ready = False
            availability = "research"
            availability_label = "研究态"
            availability_note = "历史回测尚未验证出稳定正收益，当前暂停选股/实盘，仅保留回测与因子研究。"
        elif runtime_ready:
            availability = "runtime_ready"
            availability_label = "可执行"
            availability_note = "当前已接通选股中心执行链路，可直接运行并保存结果。"
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

    @staticmethod
    def _sentiment_score_0_100(value: Any) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return 0.0
        if number != number:
            return 0.0
        if -1 <= number <= 1:
            number = 50 + number * 50
        return round(max(0.0, min(number, 100.0)), 4)

    def _rank_v12_prefetch_by_sentiment(
        self,
        selector: StockSelector,
        preliminary_items: List[Dict[str, Any]],
        limit: int,
        instrument_type: str,
        candidate_limit: Optional[int],
    ) -> List[Dict[str, Any]]:
        rank_config = selector.strategy.config.get("sentiment_rank", {}) or {}
        v12_weight = float(rank_config.get("v12_weight", 0.8) or 0.8)
        sentiment_weight = float(rank_config.get("sentiment_weight", 0.2) or 0.2)
        weight_total = v12_weight + sentiment_weight
        if weight_total <= 0:
            v12_weight, sentiment_weight, weight_total = 0.8, 0.2, 1.0
        v12_weight = v12_weight / weight_total
        sentiment_weight = sentiment_weight / weight_total

        preliminary_codes = [item.get("code") for item in preliminary_items if item.get("code")]
        preliminary_code_set = set(preliminary_codes)
        preliminary_scores = {item.get("code"): item.get("score") for item in preliminary_items if item.get("code")}

        refreshed_bundle = selector.load_candidates_from_mysql(
            candidate_limit=candidate_limit,
            instrument_type=instrument_type,
        )
        refreshed_bundle["candidates"] = [
            item for item in refreshed_bundle.get("candidates", []) if item.get("code") in preliminary_code_set
        ]
        context = selector.strategy.prepare_context(refreshed_bundle)
        factor_rows = selector.strategy.compute_factors(context)

        ranked: List[Dict[str, Any]] = []
        for item in factor_rows:
            sentiment_score = self._sentiment_score_0_100(item.get("sentiment_score"))
            preliminary_score = float(preliminary_scores.get(item.get("code")) or 0)
            final_score = round(preliminary_score * v12_weight + sentiment_score * sentiment_weight, 4)
            factors = {**(item.get("factors") or {}), "sentiment": sentiment_score}
            enriched = {
                **item,
                "score": final_score,
                "factors": factors,
                "v12_preliminary_score": preliminary_score,
                "sentiment_rank_score": sentiment_score,
                "sentiment_rank_weights": {"v12": round(v12_weight, 4), "sentiment": round(sentiment_weight, 4)},
                "selection_phase": "v12_top40_weighted_sentiment_rank",
                "strategy_id": selector.strategy_id,
                "strategy_display_name": selector.strategy_meta.get("display_name"),
                "strategy_version": selector.strategy_meta.get("version"),
            }
            enriched["explain"] = {
                **selector._enhance_explain(enriched),
                "selection_phase": "V12 初筛 Top40 后，按 V12 初筛分 + Tavily 舆情分加权精排",
                "v12_preliminary_score": preliminary_score,
                "sentiment_score_0_100": sentiment_score,
                "weighted_score": final_score,
                "rank_weights": {"v12": round(v12_weight, 4), "sentiment": round(sentiment_weight, 4)},
            }
            ranked.append(enriched)

        ranked.sort(key=lambda row: (row.get("score") or 0, row.get("sentiment_rank_score") or 0), reverse=True)
        for index, item in enumerate(ranked[:limit], start=1):
            item["rank_no"] = index
        return ranked[:limit]

    def run_strategy(
        self,
        strategy_id: Optional[str] = None,
        limit: int = 50,
        instrument_type: str = "stock",
        save: bool = True,
        score_threshold: Optional[float] = None,
        run_id: Optional[str] = None,
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
        candidate_limit = None if instrument_type == "stock" else max(limit, 200)
        sentiment_prefetch_summary = None
        sentiment_prefetch_results = None

        if final_strategy_id == "v12_legacy":
            prefetch_config = selector.strategy.config.get("sentiment_prefetch", {}) or {}
            if prefetch_config.get("enabled", False):
                preliminary_limit = int(prefetch_config.get("candidate_limit") or 40)
                preliminary_bundle = selector.load_candidates_from_mysql(
                    candidate_limit=candidate_limit,
                    instrument_type=instrument_type,
                )
                preliminary_context = selector.strategy.prepare_context(preliminary_bundle)
                preliminary_factors = selector.strategy.compute_factors(preliminary_context)
                preliminary_items = selector.strategy.score(preliminary_factors)[:preliminary_limit]
                sentiment_prefetch_summary = refresh_v12_candidate_sentiment(
                    preliminary_items,
                    candidate_limit=preliminary_limit,
                    news_top_n=int(prefetch_config.get("news_top_n") or 10),
                    max_age_days=int(prefetch_config.get("max_age_days") or 7),
                    min_credibility=float(prefetch_config.get("min_credibility") or 0.35),
                    sleep_seconds=float(prefetch_config.get("sleep_seconds") or 0.1),
                    skip_existing=bool(prefetch_config.get("skip_existing", False)),
                )
                selector = StockSelector(strategy_id=final_strategy_id, strategy_overrides=overrides)
                sentiment_prefetch_results = self._rank_v12_prefetch_by_sentiment(
                    selector=selector,
                    preliminary_items=preliminary_items,
                    limit=limit,
                    instrument_type=instrument_type,
                    candidate_limit=candidate_limit,
                )

        if save:
            if sentiment_prefetch_results is not None:
                saved_run_id = selector.save_selection_results(sentiment_prefetch_results, run_id=run_id)
                result = {
                    "run_id": saved_run_id,
                    "strategy_id": final_strategy_id,
                    "strategy_display_name": strategy_meta.get("display_name"),
                    "strategy_version": strategy_meta.get("version"),
                    "score_threshold": selector.strategy.config.get("score_threshold"),
                    "count": len(sentiment_prefetch_results),
                    "results": sentiment_prefetch_results,
                }
            else:
                result = selector.run_and_save(
                    limit=limit,
                    instrument_type=instrument_type,
                    run_id=run_id,
                    candidate_limit=candidate_limit,
                )
        else:
            items = sentiment_prefetch_results if sentiment_prefetch_results is not None else selector.run_from_mysql(
                    limit=limit,
                    instrument_type=instrument_type,
                    candidate_limit=candidate_limit,
                )
            transient_run_id = run_id or selector.build_run_id(prefix="selection_preview")
            for item in items:
                item["run_id"] = transient_run_id
            result = {
                "run_id": transient_run_id,
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
        if selector.last_run_diagnostics:
            result["diagnostics"] = selector.last_run_diagnostics
        if sentiment_prefetch_summary is not None:
            result["sentiment_prefetch"] = sentiment_prefetch_summary
        return result

    def save_strategy_result(
        self,
        strategy_id: str,
        item: Dict[str, Any],
        run_id: Optional[str] = None,
        score_threshold: Optional[float] = None,
    ) -> Dict[str, Any]:
        strategy_meta = self.get_strategy_meta(strategy_id)
        serialized_meta = self._serialize_strategy_item(strategy_meta, self.get_default_strategy_id())
        if not serialized_meta.get("runtime_ready"):
            raise ValueError(f"策略 {strategy_id} 当前未接通 V1 执行链路，暂不可保存")

        overrides = {}
        if score_threshold is not None:
            overrides["score_threshold"] = float(score_threshold)

        selector = StockSelector(strategy_id=strategy_id, strategy_overrides=overrides)
        final_run_id = selector.save_single_result(item=item, run_id=run_id)

        intraday_cache = {"enabled": True, "status": "skipped", "reason": "missing code/trade_date"}
        code = item.get("code")
        trade_date = item.get("trade_date") or item.get("latest_trade_date")
        if code and trade_date:
            try:
                cached = get_or_fetch_intraday_bars(code=str(code), trade_date=str(trade_date), refresh=False)
                intraday_cache = {
                    "enabled": True,
                    "status": "ok" if cached.get("count", 0) >= 2 else "empty",
                    "source_status": cached.get("source_status"),
                    "trade_date": cached.get("trade_date"),
                    "count": cached.get("count", 0),
                }
            except Exception as exc:
                intraday_cache = {
                    "enabled": True,
                    "status": "failed",
                    "trade_date": str(trade_date),
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:200],
                }

        return {
            "run_id": final_run_id,
            "code": code,
            "strategy_id": strategy_id,
            "intraday_cache": intraday_cache,
            "strategy": {
                "id": strategy_meta.get("id"),
                "display_name": strategy_meta.get("display_name"),
                "version": strategy_meta.get("version"),
                "score_threshold": selector.strategy.config.get("score_threshold"),
            },
        }
