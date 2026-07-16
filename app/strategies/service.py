from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from app.data_ingestion.intraday_bar_sync import get_or_fetch_intraday_bars
from app.data_ingestion.market_opinion_repository import hydrate_sector_opinion_rows
from app.shared.db import mysql_conn
from app.shared.instrument_policy import SUPPORTED_SELECTION_INSTRUMENT_TYPES, require_supported_instrument
from app.shared.strategy_loader import StrategyLoader, StrategyRegistryError
from app.stock_selection.deepseek_sentiment_rerank import DeepSeekSentimentReranker
from app.stock_selection.selector import StockSelector
from app.data_ingestion.news_provider import NewsAggregator
from app.stock_selection.sentiment_refresh import refresh_v12_candidate_sentiment
from app.strategies.capability import StrategyCapabilityService


class StrategyService:
    def __init__(
        self,
        registry_path: Optional[str] = None,
        dataset_snapshot: Optional[Dict[str, Any]] = None,
    ):
        self.loader = StrategyLoader(registry_path=registry_path)
        self.capabilities = StrategyCapabilityService(
            loader=self.loader,
            dataset_snapshot=dataset_snapshot,
        )

    def _serialize_strategy_item(
        self,
        item: Dict[str, Any],
        default_strategy: str,
        instrument_type: str = "stock",
        dataset_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        strategy_id = item.get("id")
        executable = bool(item.get("executable", True))
        mode = item.get("mode") or "current"
        status = item.get("status") or "unknown"
        capability = self.capabilities.evaluate(
            item,
            instrument_type=instrument_type,
            dataset_snapshot=dataset_snapshot,
        )

        return {
            "id": strategy_id,
            "display_name": item.get("display_name"),
            "version": item.get("version"),
            "status": status,
            "mode": mode,
            "description": item.get("description"),
            "tags": item.get("tags", []),
            "executable": executable,
            **capability,
            "is_default": strategy_id == default_strategy,
        }

    def list_strategies(self, instrument_type: str = "stock") -> List[Dict[str, Any]]:
        default_strategy = self.get_default_strategy_id()
        dataset_snapshot = self.capabilities.get_dataset_snapshot()
        return [
            self._serialize_strategy_item(
                item,
                default_strategy,
                instrument_type=instrument_type,
                dataset_snapshot=dataset_snapshot,
            )
            for item in self.loader.registry.get("strategies", [])
        ]

    def get_strategy_capability(
        self,
        strategy_id: Optional[str] = None,
        instrument_type: str = "stock",
    ) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        try:
            meta = self.get_strategy_meta(final_strategy_id)
        except StrategyRegistryError as exc:
            raise ValueError(str(exc)) from exc
        return self._serialize_strategy_item(
            meta,
            self.get_default_strategy_id(),
            instrument_type=instrument_type,
        )

    def require_runtime_ready(
        self,
        strategy_id: Optional[str] = None,
        instrument_type: str = "stock",
    ) -> Dict[str, Any]:
        capability = self.get_strategy_capability(strategy_id, instrument_type=instrument_type)
        if not capability.get("runtime_ready"):
            reasons = capability.get("runtime_reasons") or ["未达到实时执行门槛"]
            raise ValueError(f"策略 {capability.get('id')} 当前不可运行：{reasons[0]}")
        return capability

    def require_backtest_ready(
        self,
        strategy_id: str,
        instrument_type: str = "stock",
    ) -> Dict[str, Any]:
        capability = self.get_strategy_capability(strategy_id, instrument_type=instrument_type)
        if not capability.get("backtest_ready"):
            reasons = capability.get("backtest_reasons") or ["未达到研究回测门槛"]
            raise ValueError(f"策略 {capability.get('id')} 当前不可回测：{reasons[0]}")
        return capability

    def get_default_strategy_id(self) -> str:
        return self.loader.get_default_strategy_id()

    def get_strategy_meta(self, strategy_id: Optional[str] = None) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        return self.loader.get_strategy_meta(final_strategy_id)

    def _load_daily_factor_stats(
        self,
        strategy_id: str,
        instrument_type: str = "stock",
        horizon_days: int = 1,
    ) -> Dict[str, Dict[str, Any]]:
        sql = """
        SELECT sf.*
        FROM strategy_factor_ci_daily sf
        INNER JOIN (
            SELECT MAX(trade_date) AS trade_date
            FROM strategy_factor_ci_daily
            WHERE strategy_id = %s
              AND instrument_type = %s
              AND horizon_days = %s
        ) latest ON sf.trade_date = latest.trade_date
        WHERE sf.strategy_id = %s
          AND sf.instrument_type = %s
          AND sf.horizon_days = %s
        """
        try:
            with mysql_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (strategy_id, instrument_type, horizon_days, strategy_id, instrument_type, horizon_days))
                    rows = cursor.fetchall() or []
        except Exception:
            return {}
        stats: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            stats[str(row.get("factor_key"))] = {
                "ci": row.get("ci"),
                "ic": row.get("ic"),
                "rank_ic": row.get("rank_ic"),
                "coverage": row.get("coverage"),
                "missing_rate": row.get("missing_rate"),
                "sample_size": row.get("sample_size"),
                "valid_sample_size": row.get("valid_sample_size"),
                "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
                "horizon_days": row.get("horizon_days"),
                "source": row.get("source"),
                "computed_at": str(row.get("computed_at")) if row.get("computed_at") else None,
            }
        return stats

    def get_strategy_detail(self, strategy_id: Optional[str] = None, instrument_type: str = "stock", sample_limit: int = 200) -> Dict[str, Any]:
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        meta = self.get_strategy_meta(final_strategy_id)
        serialized_meta = self._serialize_strategy_item(
            meta,
            self.get_default_strategy_id(),
            instrument_type=instrument_type,
        )
        config = self.loader.load_config(final_strategy_id)
        factor_configs = config.get("factors", {}) or {}
        executable = bool(meta.get("executable", True))
        runtime_ready = serialized_meta.get("runtime_ready", False)
        factor_stats = self._load_daily_factor_stats(final_strategy_id, instrument_type=instrument_type)
        if runtime_ready:
            factor_stats = factor_stats or {}

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
                    "ic": stat.get("ic"),
                    "rank_ic": stat.get("rank_ic"),
                    "coverage": stat.get("coverage"),
                    "missing_rate": stat.get("missing_rate"),
                    "sample_size": stat.get("sample_size") if stat else None,
                    "valid_sample_size": stat.get("valid_sample_size") if stat else None,
                    "ci_trade_date": stat.get("trade_date") if stat else None,
                    "ci_horizon_days": stat.get("horizon_days") if stat else None,
                    "ci_source": stat.get("source") if stat else "config_hint",
                    "ci_computed_at": stat.get("computed_at") if stat else None,
                    "is_placeholder": not bool(stat),
                }
            )
        factor_stat_values = [item for item in factor_items if not item.get("is_placeholder")]
        latest_ci_date = next((item.get("ci_trade_date") for item in factor_stat_values if item.get("ci_trade_date")), None)
        factor_sample_size = max(
            [int(item.get("sample_size") or 0) for item in factor_stat_values],
            default=None,
        )

        return {
            "id": meta.get("id"),
            "display_name": meta.get("display_name"),
            "version": meta.get("version"),
            "status": meta.get("status"),
            "mode": meta.get("mode") or "current",
            "executable": executable,
            "runtime_ready": runtime_ready,
            "loadable": serialized_meta.get("loadable"),
            "load_error": serialized_meta.get("load_error"),
            "instrument_compatible": serialized_meta.get("instrument_compatible"),
            "supported_instrument_types": serialized_meta.get("supported_instrument_types"),
            "required_datasets": serialized_meta.get("required_datasets"),
            "dataset_statuses": serialized_meta.get("dataset_statuses"),
            "data_ready": serialized_meta.get("data_ready"),
            "runtime_status": serialized_meta.get("runtime_status"),
            "runtime_reasons": serialized_meta.get("runtime_reasons"),
            "backtest_status": serialized_meta.get("backtest_status"),
            "backtest_ready": serialized_meta.get("backtest_ready"),
            "backtest_reasons": serialized_meta.get("backtest_reasons"),
            "backtest_note": serialized_meta.get("backtest_note"),
            "validation_status": serialized_meta.get("validation_status"),
            "validated": serialized_meta.get("validated"),
            "evidence_status": serialized_meta.get("evidence_status"),
            "evidence_note": serialized_meta.get("evidence_note"),
            "readiness_reasons": serialized_meta.get("readiness_reasons"),
            "reference_trade_date": serialized_meta.get("reference_trade_date"),
            "availability": serialized_meta.get("availability"),
            "availability_label": serialized_meta.get("availability_label"),
            "availability_note": serialized_meta.get("availability_note"),
            "description": meta.get("description"),
            "tags": meta.get("tags", []),
            "score_threshold": config.get("selection", {}).get("score_threshold"),
            "max_picks": config.get("selection", {}).get("max_picks"),
            "factor_sample_size": factor_sample_size,
            "factor_ci_date": latest_ci_date,
            "factor_ci_horizon_days": 1 if latest_ci_date else None,
            "factor_ci_source": "daily_full_sample" if latest_ci_date else "config_hint",
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
        market_board: Optional[str] = None,
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
            market_board=market_board,
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
                "selection_phase": "多因子初筛 Top40 后，按初筛分 + Tavily 舆情分加权精排",
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

    @staticmethod
    def _parse_json_list(value: Any) -> List[Dict[str, Any]]:
        if isinstance(value, list):
            return value
        if not value:
            return []
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []

    def _load_a_share_hot_sectors(self, selector: StockSelector, limit: int) -> List[Dict[str, Any]]:
        requested_as_of_dt = selector._requested_market_opinion_as_of()
        market_opinion_config = selector.strategy.config.get("market_opinion", {}) or {}
        allowed_sector_types = [
            str(value).strip()
            for value in (market_opinion_config.get("allowed_sector_types") or [])
            if str(value).strip()
        ]
        excluded_sector_names = {
            str(value).strip()
            for value in (market_opinion_config.get("excluded_sector_names") or [])
            if str(value).strip()
        }
        type_filter = ""
        type_params: list[str] = []
        if allowed_sector_types:
            placeholders = ",".join(["%s"] * len(allowed_sector_types))
            type_filter = f" AND sector_type IN ({placeholders})"
            type_params = allowed_sector_types
        if requested_as_of_dt:
            sql = f"""
            SELECT id, payload_version, trade_date, sector_type, sector_name, as_of_datetime, sector_score, weighted_impact_score,
                   news_count, source_count, stock_count, positive_news_count, negative_news_count,
                   top_stocks_json, top_news_json, source_json
            FROM sector_opinion_daily
            WHERE as_of_datetime = (
                SELECT MAX(as_of_datetime)
                FROM sector_opinion_daily
                WHERE as_of_datetime <= %s
            )
            {type_filter}
            ORDER BY sector_score DESC
            LIMIT %s
            """
            params = (requested_as_of_dt.strftime("%Y-%m-%d %H:%M:%S"), *type_params, limit)
        else:
            sql = f"""
            SELECT id, payload_version, trade_date, sector_type, sector_name, as_of_datetime, sector_score, weighted_impact_score,
                   news_count, source_count, stock_count, positive_news_count, negative_news_count,
                   top_stocks_json, top_news_json, source_json
            FROM sector_opinion_daily
            WHERE as_of_datetime = (SELECT MAX(as_of_datetime) FROM sector_opinion_daily)
            {type_filter}
            ORDER BY sector_score DESC
            LIMIT %s
            """
            params = (*type_params, limit)
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT sector_type, sector_name, net_amount, pct_chg, quote_time
                    FROM market_sector_fund_flow_snapshot
                    WHERE quote_time >= DATE_SUB((SELECT MAX(quote_time) FROM market_sector_fund_flow_snapshot), INTERVAL 20 MINUTE)
                    """
                )
                fund_rows = cursor.fetchall() or []
        hydrate_sector_opinion_rows(rows)

        sectors: List[Dict[str, Any]] = []
        for row in rows:
            sector_name = str(row.get("sector_name") or "").strip()
            if sector_name in excluded_sector_names:
                continue
            sectors.append({
                "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
                "sector_type": row.get("sector_type"),
                "sector_name": row.get("sector_name"),
                "as_of_datetime": str(row.get("as_of_datetime")) if row.get("as_of_datetime") else None,
                "sector_score": float(row.get("sector_score") or 0),
                "weighted_impact_score": float(row.get("weighted_impact_score") or 0),
                "news_count": int(row.get("news_count") or 0),
                "source_count": int(row.get("source_count") or 0),
                "stock_count": int(row.get("stock_count") or 0),
                "positive_news_count": int(row.get("positive_news_count") or 0),
                "negative_news_count": int(row.get("negative_news_count") or 0),
                "top_stocks": self._parse_json_list(row.get("top_stocks_json")),
                "top_news": self._parse_json_list(row.get("top_news_json")),
                "sources": self._parse_json_list(row.get("source_json")),
            })
        theme_tiers = StockSelector._build_theme_tiers(sectors, fund_rows)
        for sector in sectors:
            tier = theme_tiers.get(str(sector.get("sector_name") or ""))
            if not tier:
                continue
            sector.update(tier)
            sector["sector_rank_score"] = round(float(sector.get("sector_score") or 0) + float(tier.get("market_theme_score_delta") or 0) * 3, 4)
        sectors.sort(key=lambda row: row.get("sector_rank_score", row.get("sector_score") or 0), reverse=True)
        return sectors

    def _refresh_sector_tavily_news(self, sectors: List[Dict[str, Any]], days: int, sleep_seconds: float = 0.0) -> Dict[str, Any]:
        aggregator = NewsAggregator()
        summary = {
            "enabled": True,
            "available": aggregator.tavily.is_available(),
            "requested": len(sectors),
            "tavily_runs": 0,
            "total_news": 0,
            "errors": [],
        }
        if not aggregator.tavily.is_available():
            summary["errors"].append({"error": "TAVILY_API_KEY is not configured"})
            return summary

        import time

        for sector in sectors:
            name = sector.get("sector_name")
            if not name:
                continue
            query = f"{name} A股 板块 题材 最新 财经 新闻 政策 订单 景气"
            try:
                news = aggregator.tavily.search_query(query, days=days)
                sector["tavily_news"] = news[:6]
                summary["tavily_runs"] += 1
                summary["total_news"] += len(news)
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
            except Exception as exc:
                sector["tavily_news"] = []
                if len(summary["errors"]) < 5:
                    summary["errors"].append({"sector_name": name, "error": str(exc)[:300]})
        return summary

    def _rank_a_share_sentiment_progressive(
        self,
        selector: StockSelector,
        limit: int,
        instrument_type: str,
        candidate_limit: Optional[int],
        market_board: Optional[str] = None,
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        config = selector.strategy.config.get("progressive_rerank", {}) or {}
        sector_local_limit = int(config.get("sector_local_limit") or 10)
        sector_final_limit = int(config.get("sector_final_limit") or 3)
        stock_local_limit = int(config.get("stock_local_limit") or 30)
        sector_ai_weight = float(config.get("sector_ai_weight") or 0.45)
        stock_ai_weight = float(config.get("stock_ai_weight") or 0.35)
        min_confidence = float(config.get("min_confidence_for_boost") or 0.35)
        tavily_days = int(config.get("tavily_days") or 3)
        sector_sleep = float(config.get("sector_tavily_sleep_seconds") or 0)
        stock_sleep = float(config.get("stock_tavily_sleep_seconds") or 0.1)
        news_top_n = int(config.get("news_top_n") or 10)
        max_news_per_stock = int(config.get("max_news_per_stock") or 4)

        sectors = self._load_a_share_hot_sectors(selector, sector_local_limit)
        sector_tavily_summary = self._refresh_sector_tavily_news(sectors, days=tavily_days, sleep_seconds=sector_sleep)
        reranker = DeepSeekSentimentReranker(config=config)
        sector_ai_summary = reranker.rerank_sectors(sectors)
        ai_sector_by_name = {item.get("sector_name"): item for item in sector_ai_summary.get("items", []) if item.get("sector_name")}
        ranked_sectors: List[Dict[str, Any]] = []
        for sector in sectors:
            ai = ai_sector_by_name.get(sector.get("sector_name"))
            local_score = float(sector.get("sector_rank_score") or sector.get("sector_score") or 0)
            if ai:
                confidence = float(ai.get("confidence") or 0)
                effective_weight = sector_ai_weight if confidence >= min_confidence else sector_ai_weight * max(confidence, 0)
                final_score = round(local_score * (1 - effective_weight) + float(ai.get("ai_sector_score") or 50) * effective_weight, 4)
            else:
                final_score = local_score
            ranked_sectors.append({
                **sector,
                "final_sector_score": final_score,
                "deepseek": ai,
            })
        ranked_sectors.sort(key=lambda row: row.get("final_sector_score") or 0, reverse=True)
        selected_sectors = ranked_sectors[:sector_final_limit]
        selected_sector_keys = {(item.get("sector_type"), item.get("sector_name")) for item in selected_sectors}

        bundle = selector.load_candidates_from_mysql(candidate_limit=candidate_limit, instrument_type=instrument_type, market_board=market_board)
        context = selector.strategy.prepare_context(bundle)
        selector.last_run_diagnostics = {
            key: value
            for key, value in context.items()
            if key.endswith("_summary") or key.endswith("_diagnostics")
        }
        factor_rows = selector.strategy.compute_factors(context)
        scored = selector.strategy.score(factor_rows)
        scored = selector.apply_global_live_selection_rules(scored)
        sector_filtered = [
            item for item in scored
            if (item.get("opinion_sector_type"), item.get("opinion_sector_name")) in selected_sector_keys
        ]
        preliminary = sector_filtered[:stock_local_limit]

        stock_tavily_summary = refresh_v12_candidate_sentiment(
            preliminary,
            candidate_limit=stock_local_limit,
            news_top_n=news_top_n,
            max_age_days=tavily_days,
            min_credibility=float(config.get("min_credibility") or 0.35),
            sleep_seconds=stock_sleep,
            skip_existing=False,
        )
        stock_ai_summary = reranker.rerank(preliminary, max_news_per_stock=max_news_per_stock)
        ai_by_code = {item.get("code"): item for item in stock_ai_summary.get("items", []) if item.get("code")}

        ranked: List[Dict[str, Any]] = []
        for item in preliminary:
            code = item.get("code")
            ai = ai_by_code.get(code)
            base_score = float(item.get("score") or 0)
            if ai:
                ai_score = float(ai.get("ai_sentiment_score") or 50)
                confidence = float(ai.get("confidence") or 0)
                effective_weight = stock_ai_weight if confidence >= min_confidence else stock_ai_weight * max(confidence, 0)
                final_score = round(base_score * (1 - effective_weight) + ai_score * effective_weight, 4)
                factors = {**(item.get("factors") or {}), "deepseek_sentiment": ai_score}
            else:
                final_score = base_score
                factors = item.get("factors") or {}
            enriched = {
                **item,
                "score": final_score,
                "factors": factors,
                "base_score_before_deepseek": base_score,
                "deepseek_sentiment_score": ai.get("ai_sentiment_score") if ai else None,
                "deepseek_confidence": ai.get("confidence") if ai else None,
                "deepseek_label": ai.get("label") if ai else None,
                "deepseek_summary": ai.get("summary") if ai else None,
                "selection_phase": "a_share_sentiment_sector_then_stock_progressive_rerank" if ai else "a_share_sentiment_progressive_local_fallback",
                "strategy_id": selector.strategy_id,
                "strategy_display_name": selector.strategy_meta.get("display_name"),
                "strategy_version": selector.strategy_meta.get("version"),
            }
            if ai:
                enriched["strategy_notes"] = (enriched.get("strategy_notes") or []) + [
                    f"DeepSeek个股精排：{ai.get('summary') or ai.get('label') or '已分析'}"
                ]
                enriched["candidate_reasons"] = (enriched.get("candidate_reasons") or []) + list(ai.get("opportunities") or [])[:2]
                enriched["candidate_risks"] = (enriched.get("candidate_risks") or []) + list(ai.get("risks") or [])[:2]
            enriched["explain"] = {
                **selector._enhance_explain(enriched),
                "selection_phase": enriched.get("selection_phase"),
                "base_score_before_deepseek": base_score,
                "deepseek": ai or None,
                "progressive_rerank": {
                    "selected_sectors": [
                        {
                            "sector_type": sector.get("sector_type"),
                            "sector_name": sector.get("sector_name"),
                            "final_sector_score": sector.get("final_sector_score"),
                            "market_theme_label": sector.get("market_theme_label"),
                            "market_theme_trend_score": sector.get("market_theme_trend_score"),
                            "deepseek": sector.get("deepseek"),
                        }
                        for sector in selected_sectors
                    ],
                    "stock_local_limit": stock_local_limit,
                },
            }
            sentiment_context = selector._build_sentiment_context(enriched, enriched["explain"])
            if sentiment_context:
                enriched["sentiment_context"] = sentiment_context
            ranked.append(enriched)

        ranked = selector.apply_global_live_selection_rules(ranked)
        threshold = float(selector.strategy.config.get("score_threshold", 60) or 60)
        selected = [item for item in ranked if float(item.get("score") or 0) >= threshold][:limit]
        for index, item in enumerate(selected, start=1):
            item["rank_no"] = index

        summary = {
            "enabled": True,
            "sector_local_limit": sector_local_limit,
            "sector_final_limit": sector_final_limit,
            "stock_local_limit": stock_local_limit,
            "final_limit": limit,
            "score_threshold": threshold,
            "local_sector_count": len(sectors),
            "selected_sector_count": len(selected_sectors),
            "sector_tavily": sector_tavily_summary,
            "sector_deepseek": sector_ai_summary,
            "stock_tavily": stock_tavily_summary,
            "stock_deepseek": stock_ai_summary,
            "selected_sectors": [
                {
                    "sector_type": sector.get("sector_type"),
                    "sector_name": sector.get("sector_name"),
                    "final_sector_score": sector.get("final_sector_score"),
                    "local_sector_score": sector.get("sector_score"),
                    "sector_rank_score": sector.get("sector_rank_score"),
                    "market_theme_label": sector.get("market_theme_label"),
                    "market_theme_trend_score": sector.get("market_theme_trend_score"),
                    "deepseek": sector.get("deepseek"),
                }
                for sector in selected_sectors
            ],
            "candidate_count_in_selected_sectors": len(sector_filtered),
            "preliminary_stock_count": len(preliminary),
            "qualified_count": len([item for item in ranked if float(item.get("score") or 0) >= threshold]),
        }
        return selected, summary

    def _rank_a_share_sentiment_with_deepseek(
        self,
        selector: StockSelector,
        limit: int,
        instrument_type: str,
        candidate_limit: Optional[int],
        market_board: Optional[str] = None,
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        rerank_config = selector.strategy.config.get("deepseek_rerank", {}) or {}
        top_n = int(rerank_config.get("top_n") or 20)
        ai_weight = float(rerank_config.get("ai_weight") or 0.30)
        min_confidence = float(rerank_config.get("min_confidence_for_boost") or 0.35)
        max_news_per_stock = int(rerank_config.get("max_news_per_stock") or 3)

        bundle = selector.load_candidates_from_mysql(candidate_limit=candidate_limit, instrument_type=instrument_type, market_board=market_board)
        context = selector.strategy.prepare_context(bundle)
        selector.last_run_diagnostics = {
            key: value
            for key, value in context.items()
            if key.endswith("_summary") or key.endswith("_diagnostics")
        }
        factor_rows = selector.strategy.compute_factors(context)
        scored = selector.strategy.score(factor_rows)
        scored = selector.apply_global_live_selection_rules(scored)
        preliminary = scored[:top_n]
        reranker = DeepSeekSentimentReranker(config=rerank_config)
        summary = reranker.rerank(preliminary, max_news_per_stock=max_news_per_stock)
        summary.update({
            "top_n": top_n,
            "ai_weight": ai_weight,
            "min_confidence_for_boost": min_confidence,
            "requested": len(preliminary),
        })
        ai_by_code = {item.get("code"): item for item in summary.get("items", []) if item.get("code")}

        ranked: List[Dict[str, Any]] = []
        for item in preliminary:
            code = item.get("code")
            ai = ai_by_code.get(code)
            base_score = float(item.get("score") or 0)
            if ai:
                ai_score = float(ai.get("ai_sentiment_score") or 50)
                confidence = float(ai.get("confidence") or 0)
                effective_weight = ai_weight if confidence >= min_confidence else ai_weight * max(confidence, 0)
                final_score = round(base_score * (1 - effective_weight) + ai_score * effective_weight, 4)
                factors = {**(item.get("factors") or {}), "deepseek_sentiment": ai_score}
                enriched = {
                    **item,
                    "score": final_score,
                    "factors": factors,
                    "deepseek_sentiment_score": ai_score,
                    "deepseek_confidence": confidence,
                    "deepseek_label": ai.get("label"),
                    "deepseek_summary": ai.get("summary"),
                    "selection_phase": "a_share_sentiment_topn_deepseek_rerank",
                    "strategy_id": selector.strategy_id,
                    "strategy_display_name": selector.strategy_meta.get("display_name"),
                    "strategy_version": selector.strategy_meta.get("version"),
                }
                enriched["strategy_notes"] = (enriched.get("strategy_notes") or []) + [
                    f"DeepSeek舆情精排：{ai.get('summary') or ai.get('label') or '已分析'}"
                ]
                enriched["candidate_reasons"] = (enriched.get("candidate_reasons") or []) + list(ai.get("opportunities") or [])[:2]
                enriched["candidate_risks"] = (enriched.get("candidate_risks") or []) + list(ai.get("risks") or [])[:2]
            else:
                enriched = {
                    **item,
                    "selection_phase": "a_share_sentiment_local_cache_rank",
                    "strategy_id": selector.strategy_id,
                    "strategy_display_name": selector.strategy_meta.get("display_name"),
                    "strategy_version": selector.strategy_meta.get("version"),
                }
            enriched["explain"] = {
                **selector._enhance_explain(enriched),
                "selection_phase": enriched.get("selection_phase"),
                "base_score_before_deepseek": base_score,
                "deepseek": ai or None,
            }
            sentiment_context = selector._build_sentiment_context(enriched, enriched["explain"])
            if sentiment_context:
                enriched["sentiment_context"] = sentiment_context
            ranked.append(enriched)

        ranked = selector.apply_global_live_selection_rules(ranked)
        selected = selector.strategy.select(ranked)
        for index, item in enumerate(selected[:limit], start=1):
            item["rank_no"] = index
        return selected[:limit], summary

    def run_strategy(
        self,
        strategy_id: Optional[str] = None,
        limit: int = 50,
        instrument_type: str = "stock",
        market_board: Optional[str] = None,
        save: bool = True,
        score_threshold: Optional[float] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        instrument_type = require_supported_instrument(
            instrument_type,
            operation="selection",
            supported=SUPPORTED_SELECTION_INSTRUMENT_TYPES,
        )
        final_strategy_id = strategy_id or self.get_default_strategy_id()
        strategy_meta = self.get_strategy_meta(final_strategy_id)
        serialized_meta = self.require_runtime_ready(
            final_strategy_id,
            instrument_type=instrument_type,
        )

        overrides = {}
        if score_threshold is not None:
            overrides["score_threshold"] = float(score_threshold)
        if limit is not None:
            overrides["max_picks"] = int(limit)

        selector = StockSelector(strategy_id=final_strategy_id, strategy_overrides=overrides)
        candidate_limit = None if instrument_type == "stock" else max(limit, 200)
        sentiment_prefetch_summary = None
        sentiment_prefetch_results = None
        progressive_rerank_summary = None
        progressive_rerank_results = None
        deepseek_rerank_summary = None
        deepseek_rerank_results = None

        if final_strategy_id == "v12_legacy":
            prefetch_config = selector.strategy.config.get("sentiment_prefetch", {}) or {}
            if prefetch_config.get("enabled", False):
                preliminary_limit = int(prefetch_config.get("candidate_limit") or 40)
                preliminary_bundle = selector.load_candidates_from_mysql(
                    candidate_limit=candidate_limit,
                    instrument_type=instrument_type,
                    market_board=market_board,
                )
                preliminary_context = selector.strategy.prepare_context(preliminary_bundle)
                preliminary_factors = selector.strategy.compute_factors(preliminary_context)
                preliminary_items = selector.apply_global_live_selection_rules(selector.strategy.score(preliminary_factors))[:preliminary_limit]
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
                    market_board=market_board,
                )

        if final_strategy_id == "a_share_sentiment":
            progressive_config = selector.strategy.config.get("progressive_rerank", {}) or {}
            rerank_config = selector.strategy.config.get("deepseek_rerank", {}) or {}
            if progressive_config.get("enabled", False):
                progressive_rerank_results, progressive_rerank_summary = self._rank_a_share_sentiment_progressive(
                    selector=selector,
                    limit=limit,
                    instrument_type=instrument_type,
                    candidate_limit=candidate_limit,
                    market_board=market_board,
                )
            elif rerank_config.get("enabled", False):
                deepseek_rerank_results, deepseek_rerank_summary = self._rank_a_share_sentiment_with_deepseek(
                    selector=selector,
                    limit=limit,
                    instrument_type=instrument_type,
                    candidate_limit=candidate_limit,
                    market_board=market_board,
                )

        if save:
            if sentiment_prefetch_results is not None or progressive_rerank_results is not None or deepseek_rerank_results is not None:
                selected_items = (
                    sentiment_prefetch_results
                    if sentiment_prefetch_results is not None
                    else progressive_rerank_results
                    if progressive_rerank_results is not None
                    else deepseek_rerank_results
                    or []
                )
                saved_run_id = selector.save_selection_results(selected_items, run_id=run_id)
                result = {
                    "run_id": saved_run_id,
                    "strategy_id": final_strategy_id,
                    "strategy_display_name": strategy_meta.get("display_name"),
                    "strategy_version": strategy_meta.get("version"),
                    "score_threshold": selector.strategy.config.get("score_threshold"),
                    "count": len(selected_items),
                    "results": selected_items,
                }
            else:
                result = selector.run_and_save(
                    limit=limit,
                    instrument_type=instrument_type,
                    run_id=run_id,
                    candidate_limit=candidate_limit,
                    market_board=market_board,
                )
        else:
            items = sentiment_prefetch_results if sentiment_prefetch_results is not None else progressive_rerank_results if progressive_rerank_results is not None else deepseek_rerank_results if deepseek_rerank_results is not None else selector.run_from_mysql(
                    limit=limit,
                    instrument_type=instrument_type,
                    candidate_limit=candidate_limit,
                    market_board=market_board,
                )
            transient_run_id = run_id or selector.build_run_id(prefix="selection_preview")
            for item in items:
                item["run_id"] = transient_run_id
            result = {
                "run_id": transient_run_id,
                "strategy_id": final_strategy_id,
                "score_threshold": selector.strategy.config.get("score_threshold"),
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
        if progressive_rerank_summary is not None:
            result["progressive_rerank"] = progressive_rerank_summary
        if deepseek_rerank_summary is not None:
            result["deepseek_rerank"] = deepseek_rerank_summary
        return result

    def save_strategy_result(
        self,
        strategy_id: str,
        item: Dict[str, Any],
        run_id: Optional[str] = None,
        score_threshold: Optional[float] = None,
    ) -> Dict[str, Any]:
        strategy_meta = self.get_strategy_meta(strategy_id)
        self.require_runtime_ready(strategy_id, instrument_type="stock")

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
