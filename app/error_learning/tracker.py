from __future__ import annotations

import json
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

from app.error_learning.models import SelectionTrackingRecord
from app.shared.sentiment_scoring import enrich_opinion_news_item
from app.stock_selection.trade_plan import build_selection_trade_plan
from app.tracking.repository import TRACKING_STATS_MAX_AGE_DAYS, TrackingRepository


class SelectionResultTracker:
    def __init__(self, repository: TrackingRepository | None = None) -> None:
        self.repository = repository or TrackingRepository()

    @staticmethod
    def _build_sentiment_context(metadata: Dict[str, Any], factor_scores: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        existing = metadata.get("sentiment_context")
        if isinstance(existing, dict) and existing.get("sector_name"):
            return existing
        sector_name = factor_scores.get("opinion_sector_name")
        if not sector_name and factor_scores.get("sentiment_mode") != "market_opinion_v2":
            return None
        stock_news = factor_scores.get("opinion_stock_news") or []
        top_news = factor_scores.get("opinion_top_news") or []
        sector_top_news = factor_scores.get("opinion_sector_top_news") or []
        return {
            "sector_name": sector_name,
            "sector_type": factor_scores.get("opinion_sector_type"),
            "as_of": factor_scores.get("opinion_as_of_datetime"),
            "trade_date": factor_scores.get("opinion_trade_date"),
            "opinion_match_reason": factor_scores.get("opinion_match_reason"),
            "stock_news": [enrich_opinion_news_item(item, "来自本次舆情选股的个股命中新闻") for item in stock_news],
            "top_news": [enrich_opinion_news_item(item, "来自本次舆情选股的热点新闻") for item in top_news],
            "sector_top_news": [enrich_opinion_news_item(item, "来自本次舆情选股的关联板块热度新闻") for item in sector_top_news],
            "sources": factor_scores.get("opinion_sources") or [],
            "news_count": factor_scores.get("opinion_news_count"),
            "source_count": factor_scores.get("opinion_source_count"),
            "positive": factor_scores.get("opinion_positive_news_count"),
            "negative": factor_scores.get("opinion_negative_news_count"),
            "sector_score": factor_scores.get("opinion_sector_score"),
            "weighted_impact_score": factor_scores.get("opinion_weighted_impact_score"),
            "sentiment_mode": factor_scores.get("sentiment_mode"),
            "source_credibility_level": factor_scores.get("source_credibility_level"),
            "source_credibility_score": factor_scores.get("source_credibility_score"),
            "source_credibility_reason": factor_scores.get("source_credibility_reason"),
            "trade_signal_state": factor_scores.get("trade_signal_state"),
            "trade_signal_label": factor_scores.get("trade_signal_label"),
            "trade_signal_reason": factor_scores.get("trade_signal_reason"),
            "trade_grade_state": factor_scores.get("trade_grade_state"),
            "trade_grade_label": factor_scores.get("trade_grade_label"),
            "trade_grade_reason": factor_scores.get("trade_grade_reason"),
            "theme_trade_slot_state": factor_scores.get("theme_trade_slot_state"),
            "daily_trend_state": factor_scores.get("daily_trend_state"),
            "daily_trend_label": factor_scores.get("daily_trend_label"),
            "chip_structure_state": factor_scores.get("chip_structure_state"),
            "chip_structure_label": factor_scores.get("chip_structure_label"),
            "market_context_score": factor_scores.get("market_context"),
            "market_context_label": factor_scores.get("market_context_label"),
            "market_context_reason": factor_scores.get("market_context_reason"),
            "market_index_trend_score": factor_scores.get("market_index_trend_score"),
            "market_index_day_score": factor_scores.get("market_index_day_score"),
            "market_index_pct_chg": factor_scores.get("market_index_pct_chg"),
            "market_breadth_score": factor_scores.get("market_breadth_score"),
            "market_volume_score": factor_scores.get("market_volume_score"),
            "market_index_count": factor_scores.get("market_index_count"),
            "market_index_codes": factor_scores.get("market_index_codes"),
            "csi300_pct_chg": factor_scores.get("csi300_pct_chg"),
            "csi500_pct_chg": factor_scores.get("csi500_pct_chg"),
            "csi1000_pct_chg": factor_scores.get("csi1000_pct_chg"),
        }

    def build_latest_selection_snapshot(
        self,
        limit: int = 20,
        instrument_type: str = "stock",
        run_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        selection_date: Optional[str] = None,
        offset: int = 0,
        latest_only: bool = True,
        include_in_stats_only: bool = False,
    ) -> List[SelectionTrackingRecord]:
        rows = self._fetch_from_selection_result(
            limit=limit,
            instrument_type=instrument_type,
            run_id=run_id,
            strategy_id=strategy_id,
            selection_date=selection_date,
            offset=offset,
            latest_only=latest_only,
            include_in_stats_only=include_in_stats_only,
        )
        if rows:
            return [self._build_record_from_selection_result(row) for row in rows]
        return []

    def enforce_stats_retention(
        self,
        *,
        instrument_type: str = "stock",
        max_age_days: int = TRACKING_STATS_MAX_AGE_DAYS,
        as_of_datetime: datetime | None = None,
    ) -> int:
        return self.repository.exclude_expired_from_stats(
            instrument_type=instrument_type,
            max_age_days=max_age_days,
            as_of_datetime=as_of_datetime,
        )

    def _fetch_from_selection_result(
        self,
        limit: int,
        instrument_type: str,
        run_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        selection_date: Optional[str] = None,
        offset: int = 0,
        latest_only: bool = True,
        include_in_stats_only: bool = False,
    ) -> List[Dict[str, Any]]:
        return self.repository.list_selection_result_rows(
            limit=limit,
            instrument_type=instrument_type,
            run_id=run_id,
            strategy_id=strategy_id,
            selection_date=selection_date,
            offset=offset,
            latest_only=latest_only,
            include_in_stats_only=include_in_stats_only,
        )
    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        return value if value == value else None

    @staticmethod
    def _to_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        try:
            return datetime.fromisoformat(str(value)).date()
        except ValueError:
            return None

    @staticmethod
    def _to_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, time.min)
        raw = str(value).strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None

    @staticmethod
    def _calc_tracking_days(start: date | None, end: date | None, trade_day_count: Any = None) -> int | None:
        if trade_day_count is not None:
            try:
                return max(int(trade_day_count), 0)
            except (TypeError, ValueError):
                pass
        if not start or not end:
            return None
        return max((end - start).days, 0)

    @staticmethod
    def _stats_window_state(
        selection_datetime: datetime | None,
        *,
        as_of_datetime: datetime | None = None,
        max_age_days: int = TRACKING_STATS_MAX_AGE_DAYS,
    ) -> tuple[bool, float | None, str | None]:
        if selection_datetime is None:
            return False, None, None
        current = (as_of_datetime or datetime.now()).replace(tzinfo=None)
        elapsed_seconds = max((current - selection_datetime.replace(tzinfo=None)).total_seconds(), 0.0)
        age_days = round(elapsed_seconds / 86400.0, 2)
        expired = elapsed_seconds > max(1, int(max_age_days)) * 86400
        reason = f"已超过入选后 {max(1, int(max_age_days))} 个自然日统计窗口" if expired else None
        return expired, age_days, reason

    @staticmethod
    def _combine_period_extreme(period_value: float | None, realtime_value: float | None, prefer_max: bool) -> float | None:
        values = [value for value in [period_value, realtime_value] if value is not None]
        if not values:
            return None
        return max(values) if prefer_max else min(values)

    @staticmethod
    def _extract_take_profit_levels(trade_plan: Dict[str, Any]) -> list[Dict[str, Any]]:
        levels = trade_plan.get("take_profit") or []
        if isinstance(levels, dict):
            levels = [levels]
        normalized = []
        for item in levels:
            if not isinstance(item, dict):
                continue
            price = SelectionResultTracker._to_float(item.get("price"))
            if price is None:
                continue
            normalized.append({**item, "price": price})
        normalized.sort(key=lambda item: item.get("price") or 0)
        return normalized

    def _evaluate_trade_plan(
        self,
        trade_plan: Dict[str, Any] | None,
        *,
        entry_price: float | None,
        period_max_high: float | None,
        period_min_low: float | None,
        current_price: float | None,
        exit_tradable: bool = True,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(trade_plan, dict) or not trade_plan:
            return None
        plan_entry_price = self._to_float(trade_plan.get("entry_price")) or entry_price
        if plan_entry_price is None:
            return None

        stop_loss = trade_plan.get("stop_loss") or {}
        stop_price = self._to_float(stop_loss.get("price")) if isinstance(stop_loss, dict) else None
        take_profit_levels = self._extract_take_profit_levels(trade_plan)
        first_take_profit = take_profit_levels[0] if take_profit_levels else None
        highest_take_profit = take_profit_levels[-1] if take_profit_levels else None
        high_watermark = self._combine_period_extreme(period_max_high, current_price, prefer_max=True)
        low_watermark = self._combine_period_extreme(period_min_low, current_price, prefer_max=False)

        hit_stop = stop_price is not None and low_watermark is not None and low_watermark <= stop_price
        hit_take_profit = first_take_profit is not None and high_watermark is not None and high_watermark >= first_take_profit["price"]
        hit_second_take_profit = highest_take_profit is not None and high_watermark is not None and high_watermark >= highest_take_profit["price"]

        status = {
            "status": "tracking",
            "status_label": "计划跟踪中" if exit_tradable else "待下一交易日触发",
            "entry_status": "entered",
            "entry_price": round(plan_entry_price, 3),
            "exit_tradable": exit_tradable,
            "exit_rule": "入选当天不触发止盈止损，最早从下一个交易日开始。",
            "completed": False,
            "completion_price": None,
            "completion_return_pct": None,
            "completion_reason": None,
            "high_watermark": round(high_watermark, 3) if high_watermark is not None else None,
            "low_watermark": round(low_watermark, 3) if low_watermark is not None else None,
        }
        if not exit_tradable:
            return status

        completion_price = None
        completion_reason = None
        if hit_stop and hit_take_profit:
            completion_price = stop_price
            completion_reason = "same_period_stop_and_take_profit"
            status["status"] = "completed_ambiguous"
            status["status_label"] = "同周期双触发，按止损冻结"
        elif hit_stop:
            completion_price = stop_price
            completion_reason = "stop_loss"
            status["status"] = "completed_stop_loss"
            status["status_label"] = "已触发止损"
        elif hit_second_take_profit:
            completion_price = highest_take_profit["price"]
            completion_reason = "take_profit_2"
            status["status"] = "completed_take_profit_2"
            status["status_label"] = "已触发第二止盈"
        elif hit_take_profit:
            completion_price = first_take_profit["price"]
            completion_reason = "take_profit_1"
            status["status"] = "completed_take_profit_1"
            status["status_label"] = "已触发第一止盈"

        if completion_price is not None:
            status["completed"] = True
            status["completion_price"] = round(completion_price, 3)
            status["completion_reason"] = completion_reason
            status["completion_return_pct"] = round((completion_price - plan_entry_price) / plan_entry_price * 100, 2)

        return status

    def _build_record_from_selection_result(self, row: Dict[str, Any]) -> SelectionTrackingRecord:
        selected_open_price = self._to_float(row.get("selected_open_price"))
        selected_close_price = self._to_float(row.get("selected_close_price"))
        current_price = self._to_float(row.get("current_price"))
        daily_current_price = self._to_float(row.get("daily_current_price"))
        realtime_price = self._to_float(row.get("realtime_price"))
        realtime_pct_chg = self._to_float(row.get("realtime_pct_chg"))
        realtime_quote_time = str(row["realtime_quote_time"]) if row.get("realtime_quote_time") else None
        latest_trade_date = str(row["latest_trade_date"]) if row.get("latest_trade_date") else None
        realtime_trade_date = str(row["realtime_trade_date"]) if row.get("realtime_trade_date") else None
        metric_trade_date = str(row["metric_trade_date"]) if row.get("metric_trade_date") else latest_trade_date
        selection_datetime = self._to_datetime(row.get("selection_datetime"))
        stats_window_expired, stats_age_days, stats_exclusion_reason = self._stats_window_state(selection_datetime)
        realtime_quote_dt = self._to_datetime(row.get("realtime_quote_time"))
        selection_dt = selection_datetime.date() if selection_datetime else self._to_date(row.get("selection_date"))
        latest_dt = self._to_date(row.get("latest_trade_date"))
        realtime_dt = self._to_date(row.get("realtime_trade_date"))
        tracking_end_dt = max([dt for dt in [latest_dt, realtime_dt] if dt], default=None)
        trade_day_count = row.get("trade_day_count")
        if realtime_dt and selection_dt and realtime_dt > selection_dt and (not latest_dt or realtime_dt > latest_dt):
            try:
                trade_day_count = max(int(trade_day_count or 0) + 1, 1)
            except (TypeError, ValueError):
                trade_day_count = 1
        metadata = row.get("metadata_json")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        metadata = metadata or {}
        explain = metadata.get("explain", {}) or {}
        raw_metrics = {
            **(metadata.get("raw_metrics", {}) or {}),
            **(explain.get("raw_metrics", {}) or {}),
        }
        selected_price = self._to_float(
            raw_metrics.get("selected_price")
            or metadata.get("selected_price")
        )
        if selected_price is not None:
            selected_open_price = selected_price
            selected_close_price = self._to_float(raw_metrics.get("close")) or selected_close_price
        elif selected_open_price is None:
            selected_open_price = self._to_float(raw_metrics.get("open"))
        if selected_close_price is None:
            selected_close_price = self._to_float(raw_metrics.get("close"))
        tracking_days = self._calc_tracking_days(selection_dt, tracking_end_dt, trade_day_count)
        review_status = "tracking" if tracking_end_dt and selection_dt and tracking_end_dt >= selection_dt else "pending"
        base_price = selected_open_price or selected_close_price
        current_price_for_return = current_price
        if selection_datetime:
            has_realtime_after_selection = realtime_quote_dt is not None and realtime_quote_dt >= selection_datetime
            has_daily_after_selection = latest_dt is not None and latest_dt > selection_datetime.date()
            if has_realtime_after_selection and realtime_price is not None:
                current_price_for_return = realtime_price
            elif has_daily_after_selection and daily_current_price is not None:
                current_price_for_return = daily_current_price
            else:
                current_price_for_return = None
        price_change_pct = None
        if base_price and current_price_for_return:
            price_change_pct = round((current_price_for_return - base_price) / base_price * 100, 2)
        period_base_price = selected_open_price or selected_close_price
        max_gain_pct = None
        max_drawdown_pct = None
        realtime_price_after_selection = realtime_price if (
            realtime_dt is not None and selection_dt is not None and realtime_dt > selection_dt
        ) else None
        period_max_high = self._combine_period_extreme(self._to_float(row.get("period_max_high")), realtime_price_after_selection, prefer_max=True)
        period_min_low = self._combine_period_extreme(self._to_float(row.get("period_min_low")), realtime_price_after_selection, prefer_max=False)
        if period_base_price and period_max_high:
            max_gain_pct = max(round((period_max_high - period_base_price) / period_base_price * 100, 2), 0.0)
        if period_base_price and period_min_low:
            max_drawdown_pct = min(round((period_min_low - period_base_price) / period_base_price * 100, 2), 0.0)
        trade_plan = metadata.get("trade_plan") if isinstance(metadata.get("trade_plan"), dict) else None
        if trade_plan is None or str(trade_plan.get("version") or "") == "selection_trade_plan_v1":
            trade_plan = build_selection_trade_plan(
                {
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "strategy_id": row.get("strategy_id"),
                    "selected_price": period_base_price,
                    "realtime_price": realtime_price,
                    "close": selected_close_price,
                },
                strategy_id=row.get("strategy_id") or "",
                raw_metrics=raw_metrics,
            )
        trade_plan_status = self._evaluate_trade_plan(
            trade_plan,
            entry_price=period_base_price,
            period_max_high=period_max_high,
            period_min_low=period_min_low,
            current_price=current_price_for_return,
            exit_tradable=bool(tracking_days is not None and tracking_days >= 1),
        )
        if trade_plan_status and trade_plan_status.get("completed"):
            frozen_price = self._to_float(trade_plan_status.get("completion_price"))
            frozen_return = self._to_float(trade_plan_status.get("completion_return_pct"))
            if frozen_price is not None:
                current_price = frozen_price
                current_price_for_return = frozen_price
            if frozen_return is not None:
                price_change_pct = frozen_return
                if trade_plan_status.get("completion_reason") == "stop_loss":
                    max_drawdown_pct = min(max_drawdown_pct if max_drawdown_pct is not None else frozen_return, frozen_return)
                elif str(trade_plan_status.get("completion_reason") or "").startswith("take_profit"):
                    max_gain_pct = max(max_gain_pct if max_gain_pct is not None else frozen_return, frozen_return)
            review_status = "completed"
        summary = explain.get("summary", {}) or {}
        factor_scores = {
            **raw_metrics,
            **metadata.get("factors", {}),
            **summary,
        }

        factor_scores = {
            **factor_scores,
            "trade_date": metric_trade_date or factor_scores.get("trade_date"),
        }
        fundamental_keys = ["pe_tushare", "pb_tushare", "roe", "roa", "grossprofit_margin", "netprofit_margin", "revenue_yoy", "profit_yoy", "eps"]
        missing_fundamentals = [key for key in fundamental_keys if factor_scores.get(key) is None]
        factor_scores["fundamental_missing_fields"] = missing_fundamentals
        factor_scores["fundamental_completeness"] = round((len(fundamental_keys) - len(missing_fundamentals)) / len(fundamental_keys), 4)

        return SelectionTrackingRecord(
            run_id=row.get("run_id"),
            latest_run_id=row.get("latest_run_id") or row.get("run_id"),
            rank_no=row.get("rank_no"),
            code=row["code"],
            name=row["name"],
            selection_date=str(row["selection_date"]) if row.get("selection_date") else "",
            selection_datetime=str(row["selection_datetime"]) if row.get("selection_datetime") else None,
            strategy_id=row.get("strategy_id") or "",
            strategy_display_name=metadata.get("strategy_display_name"),
            strategy_version=row.get("strategy_version") or metadata.get("strategy_version"),
            industry=row.get("industry"),
            score=self._to_float(row.get("score")),
            include_in_stats=bool(row.get("include_in_stats", 1)),
            stats_window_expired=stats_window_expired,
            stats_age_days=stats_age_days,
            stats_exclusion_reason=stats_exclusion_reason,
            factor_scores=factor_scores,
            selected_open_price=selected_open_price,
            selected_close_price=selected_close_price,
            current_price=current_price,
            latest_trade_date=latest_trade_date,
            price_change_pct=price_change_pct,
            reason_summary=explain.get("reasons") or [],
            risk_summary=explain.get("risks") or [],
            sentiment_context=self._build_sentiment_context(metadata, factor_scores),
            tracking_days=tracking_days,
            review_status=review_status,
            max_gain_pct=max_gain_pct,
            max_drawdown_pct=max_drawdown_pct,
            daily_current_price=daily_current_price,
            realtime_price=realtime_price,
            realtime_pct_chg=realtime_pct_chg,
            realtime_quote_time=realtime_quote_time,
            realtime_price_change_pct=price_change_pct if realtime_price is not None else None,
            trade_plan=trade_plan,
            trade_plan_status=trade_plan_status,
        )

    def _build_record_from_snapshot(self, row: Dict[str, Any]) -> SelectionTrackingRecord:
        selected_open_price = self._to_float(row.get("selected_open_price"))
        selected_close_price = self._to_float(row.get("selected_close_price"))
        current_price = self._to_float(row.get("current_price"))
        daily_current_price = self._to_float(row.get("daily_current_price"))
        realtime_price = self._to_float(row.get("realtime_price"))
        realtime_pct_chg = self._to_float(row.get("realtime_pct_chg"))
        realtime_quote_time = str(row["realtime_quote_time"]) if row.get("realtime_quote_time") else None
        latest_trade_date = str(row["latest_trade_date"]) if row.get("latest_trade_date") else None
        selection_dt = self._to_date(row.get("selection_date"))
        latest_dt = self._to_date(row.get("latest_trade_date"))
        base_price = selected_open_price or selected_close_price
        price_change_pct = None
        if base_price and current_price:
            price_change_pct = round((current_price - base_price) / base_price * 100, 2)
        tracking_days = self._calc_tracking_days(selection_dt, latest_dt)
        review_status = "tracking" if latest_dt and selection_dt and latest_dt >= selection_dt else "pending"
        max_gain_pct = price_change_pct if price_change_pct is not None and price_change_pct > 0 else 0.0 if price_change_pct is not None else None
        max_drawdown_pct = price_change_pct if price_change_pct is not None and price_change_pct < 0 else 0.0 if price_change_pct is not None else None

        factor_scores = {
            "pe_tushare": self._to_float(row.get("pe_tushare")),
            "pb_tushare": self._to_float(row.get("pb_tushare")),
            "roe": self._to_float(row.get("roe")),
            "roa": self._to_float(row.get("roa")),
            "grossprofit_margin": self._to_float(row.get("grossprofit_margin")),
            "netprofit_margin": self._to_float(row.get("netprofit_margin")),
            "revenue_yoy": self._to_float(row.get("revenue_yoy")),
            "profit_yoy": self._to_float(row.get("profit_yoy")),
            "eps": self._to_float(row.get("eps")),
        }
        fundamental_keys = ["pe_tushare", "pb_tushare", "roe", "roa", "grossprofit_margin", "netprofit_margin", "revenue_yoy", "profit_yoy", "eps"]
        missing_fundamentals = [key for key in fundamental_keys if factor_scores.get(key) is None]
        factor_scores["fundamental_missing_fields"] = missing_fundamentals
        factor_scores["fundamental_completeness"] = round((len(fundamental_keys) - len(missing_fundamentals)) / len(fundamental_keys), 4)

        return SelectionTrackingRecord(
            latest_run_id=None,
            code=row["code"],
            name=row["name"],
            selection_date=str(row["selection_date"]) if row.get("selection_date") else "",
            selection_datetime=None,
            strategy_id="historical_import",
            strategy_display_name="历史导入记录",
            strategy_version="legacy",
            industry=row.get("industry"),
            score=None,
            factor_scores=factor_scores,
            selected_open_price=selected_open_price,
            selected_close_price=selected_close_price,
            current_price=current_price,
            latest_trade_date=latest_trade_date,
            price_change_pct=price_change_pct,
            reason_summary=[],
            risk_summary=[],
            sentiment_context=None,
            tracking_days=tracking_days,
            review_status=review_status,
            max_gain_pct=max_gain_pct,
            max_drawdown_pct=max_drawdown_pct,
            daily_current_price=daily_current_price,
            realtime_price=realtime_price,
            realtime_pct_chg=realtime_pct_chg,
            realtime_quote_time=realtime_quote_time,
            realtime_price_change_pct=price_change_pct if realtime_price is not None else None,
        )

    def to_dict_list(self, records: List[SelectionTrackingRecord]) -> List[Dict[str, Any]]:
        return [
            {
                "run_id": item.run_id,
                "latest_run_id": item.latest_run_id,
                "persisted_key": "::".join([item.selection_date or "", item.strategy_id or "", item.code or ""]),
                "rank_no": item.rank_no,
                "code": item.code,
                "name": item.name,
                "selection_date": item.selection_date,
                "selection_datetime": item.selection_datetime,
                "strategy_id": item.strategy_id,
                "score": item.score,
                "include_in_stats": item.include_in_stats,
                "stats_window_expired": item.stats_window_expired,
                "stats_age_days": item.stats_age_days,
                "stats_exclusion_reason": item.stats_exclusion_reason,
                "strategy_display_name": item.strategy_display_name,
                "strategy_version": item.strategy_version,
                "industry": item.industry,
                "industry_display": item.industry or "暂无行业",
                "factor_scores": item.factor_scores,
                "selected_open_price": item.selected_open_price,
                "selected_close_price": item.selected_close_price,
                "current_price": item.current_price,
                "daily_current_price": item.daily_current_price,
                "realtime_price": item.realtime_price,
                "realtime_pct_chg": item.realtime_pct_chg,
                "realtime_quote_time": item.realtime_quote_time,
                "realtime_price_change_pct": item.realtime_price_change_pct,
                "latest_trade_date": item.latest_trade_date,
                "price_change_pct": item.price_change_pct,
                "reason_summary": item.reason_summary or [],
                "risk_summary": item.risk_summary or [],
                "sentiment_context": item.sentiment_context,
                "tracking_days": item.tracking_days,
                "review_status": item.review_status,
                "max_gain_pct": item.max_gain_pct,
                "max_drawdown_pct": item.max_drawdown_pct,
                "trade_plan": item.trade_plan,
                "trade_plan_status": item.trade_plan_status,
            }
            for item in records
        ]
