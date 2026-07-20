from __future__ import annotations

import json
import math
from datetime import datetime, time
from typing import Any, Dict, List, Optional

from app.data_ingestion.market_opinion_repository import hydrate_sector_opinion_rows
from app.shared.sentiment_scoring import enrich_opinion_news_item
from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.repository import SelectionRepository
from app.stock_selection.trade_plan import build_selection_trade_plan


THEME_INDUSTRY_HINTS: Dict[str, set[str]] = {
    "AI算力": {"软件服务", "通信设备", "IT设备", "半导体", "元器件", "互联网"},
    "机器人": {"专用机械", "机械基件", "电器仪表", "元器件", "软件服务", "运输设备", "汽车配件"},
    "半导体": {"半导体", "元器件", "IT设备", "互联网"},
    "绿电": {"新型电力", "水力发电", "电气设备"},
    "锂电池": {"电气设备", "化工原料", "小金属", "汽车配件"},
}

THEME_FUND_FLOW_ALIASES: Dict[str, List[str]] = {
    "AI算力": ["AI算力", "算力", "数据中心", "液冷服务器", "CPO", "东数西算"],
    "机器人": ["机器人", "人形机器人", "机器人概念", "减速器"],
    "半导体": ["半导体", "芯片", "第三代半导体", "先进封装"],
    "低空经济": ["低空经济", "飞行汽车", "无人机", "eVTOL"],
    "军工航天": ["军工", "航天", "商业航天", "卫星导航"],
    "锂电池": ["锂电池", "固态电池", "动力电池"],
}

THEME_TIER_LABELS = {
    "mainline": "主线命中",
    "strong_side": "强支线",
    "side": "支线",
    "broad_related": "泛相关",
    "unknown": "未分层",
}


class StockSelector:
    MARKET_BOARD_LABELS = {
        "all": "全市场",
        "main": "主板",
        "star": "科创板",
        "chinext": "创业板",
        "bse": "北交所",
    }

    def __init__(
        self,
        strategy_id: Optional[str] = None,
        strategy_overrides: Optional[Dict[str, Any]] = None,
        repository: SelectionRepository | None = None,
    ):
        self.loader = StrategyLoader()
        self.repository = repository or SelectionRepository()
        self.strategy_id = strategy_id or self.loader.get_default_strategy_id()
        self.strategy_meta = self.loader.get_strategy_meta(self.strategy_id)
        self.strategy = self.loader.load_strategy(self.strategy_id)
        self.strategy_overrides = strategy_overrides or {}
        self.last_run_diagnostics: Dict[str, Any] = {}
        self._apply_strategy_overrides()

    def _apply_strategy_overrides(self) -> None:
        if not self.strategy_overrides:
            return
        for key, value in self.strategy_overrides.items():
            if value is None:
                continue
            self.strategy.config[key] = value

    @staticmethod
    def build_run_id(prefix: str = "selection") -> str:
        return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def _requested_market_opinion_as_of(self) -> Optional[datetime]:
        generic_as_of = self.strategy.config.get("as_of_datetime")
        if generic_as_of:
            try:
                return datetime.fromisoformat(str(generic_as_of).replace("T", " ").replace("Z", "+00:00"))
            except ValueError:
                return None
        if self.strategy_id != "a_share_sentiment":
            return None
        market_opinion_config = self.strategy.config.get("market_opinion", {}) or {}
        requested_as_of = market_opinion_config.get("as_of_datetime") or self.strategy.config.get("market_opinion_as_of")
        if not requested_as_of:
            return None
        try:
            return datetime.fromisoformat(str(requested_as_of).replace("T", " ").replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _round_score(value: float) -> float:
        return round(max(0.0, min(value, 100.0)), 2)

    @classmethod
    def normalize_market_board(cls, value: Optional[str]) -> str:
        board = str(value or "all").strip().lower()
        aliases = {
            "": "all",
            "stock": "all",
            "all_stock": "all",
            "main_board": "main",
            "sse_main": "main",
            "szse_main": "main",
            "kechuang": "star",
            "sci_tech": "star",
            "science_technology": "star",
            "gem": "chinext",
            "cyb": "chinext",
            "beijing": "bse",
            "bj": "bse",
        }
        board = aliases.get(board, board)
        if board not in cls.MARKET_BOARD_LABELS:
            return "all"
        return board

    @classmethod
    def market_board_filter_sql(cls, value: Optional[str]) -> tuple[str, str, str]:
        board = cls.normalize_market_board(value)
        label = cls.MARKET_BOARD_LABELS[board]
        return SelectionRepository.market_board_filter_sql(board), board, label

    @staticmethod
    def _normalize_0_100(value: float, low: float, high: float) -> float:
        if high <= low:
            return 50.0
        return StockSelector._round_score((value - low) / (high - low) * 100)

    @staticmethod
    def _positive_ratio_score(positive: int, negative: int) -> float:
        total = positive + negative
        if total <= 0:
            return 50.0
        return StockSelector._round_score(50 + (positive - negative) / (total + 1) * 50)

    @staticmethod
    def _fund_flow_score(net_amount: Any, pct_chg: Any) -> float:
        try:
            net = float(net_amount or 0)
        except (TypeError, ValueError):
            net = 0.0
        try:
            pct = float(pct_chg or 0)
        except (TypeError, ValueError):
            pct = 0.0
        # AkShare sector fund flow unit is 亿元. Keep this as a soft validation
        # signal: it can lift a live theme, but it should not hard-kill one.
        return StockSelector._round_score(50 + max(-80.0, min(net, 80.0)) * 0.5 + max(-5.0, min(pct, 5.0)) * 6)

    @staticmethod
    def _theme_aliases(theme_name: str) -> List[str]:
        aliases = THEME_FUND_FLOW_ALIASES.get(theme_name, [])
        return [theme_name, *[alias for alias in aliases if alias != theme_name]]

    @staticmethod
    def _match_theme_fund_flow(theme_name: str, fund_rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        aliases = [alias for alias in StockSelector._theme_aliases(theme_name) if alias]
        if not aliases:
            return None
        best: Optional[Dict[str, Any]] = None
        best_score = -1.0
        for row in fund_rows:
            sector_name = str(row.get("sector_name") or "")
            if not sector_name:
                continue
            if not any(alias in sector_name or sector_name in alias for alias in aliases):
                continue
            fund_score = StockSelector._fund_flow_score(row.get("net_amount"), row.get("pct_chg"))
            if fund_score > best_score:
                best_score = fund_score
                best = {**row, "fund_flow_score": fund_score}
        return best

    @staticmethod
    def _stock_recognition_context(stock: Dict[str, Any], rank: int, stock_count: int, match_type: str | None) -> Dict[str, Any]:
        """Score whether a stock is a recognizable front-row name inside a hot theme."""
        stock_score = float(stock.get("score") or 0)
        pct_chg = float(stock.get("pct_chg") or 0)
        amount = float(stock.get("amount") or 0)
        news_count = int(stock.get("news_count") or 0)
        rank_score = StockSelector._round_score(max(35.0, 100.0 - max(rank - 1, 0) * 4.0))
        price_strength = StockSelector._round_score(50.0 + max(min(pct_chg, 10.0), -10.0) * 4.2)
        amount_attention = (
            StockSelector._round_score(45.0 + min(math.log10(amount / 100_000_000 + 1) * 18.0, 45.0))
            if amount > 0
            else 50.0
        )
        direct_bonus = 10.0 if match_type == "direct_news_match" else -6.0 if match_type == "sector_candidate" else 0.0
        news_bonus = min(news_count * 4.0, 12.0)
        limitup_bonus = 8.0 if pct_chg >= 9.0 else 0.0
        recognition = StockSelector._round_score(
            rank_score * 0.30
            + stock_score * 0.26
            + price_strength * 0.22
            + amount_attention * 0.12
            + direct_bonus
            + news_bonus
            + limitup_bonus
        )
        if recognition >= 82:
            label = "板块前排"
        elif recognition >= 70:
            label = "板块活跃"
        elif recognition >= 58:
            label = "板块候选"
        else:
            label = "低辨识度"
        reason = (
            f"板块内第{rank}/{stock_count}，"
            f"涨跌幅{pct_chg:.1f}%，"
            f"成交额{amount / 100_000_000:.1f}亿，"
            f"{'个股新闻直接命中' if match_type == 'direct_news_match' else '板块候选池映射'}"
        )
        return {
            "opinion_stock_rank": rank,
            "opinion_stock_pool_size": stock_count,
            "opinion_stock_recognition_score": recognition,
            "opinion_stock_recognition_label": label,
            "opinion_stock_recognition_reason": reason,
            "opinion_stock_pct_chg": pct_chg,
            "opinion_stock_amount": amount,
        }

    @staticmethod
    def _theme_trend_score(sector: Dict[str, Any], fund_match: Optional[Dict[str, Any]]) -> float:
        sector_score = float(sector.get("sector_score") or 0)
        weighted_impact = float(sector.get("weighted_impact_score") or 0)
        source_count = int(sector.get("source_count") or 0)
        news_count = int(sector.get("news_count") or 0)
        positive = int(sector.get("positive_news_count") or 0)
        negative = int(sector.get("negative_news_count") or 0)
        source_score = min(source_count, 10) * 10
        news_breadth = StockSelector._round_score(min(math.log1p(max(news_count, 0)) / math.log1p(700), 1.0) * 100)
        positive_score = StockSelector._positive_ratio_score(positive, negative)
        fund_score = float((fund_match or {}).get("fund_flow_score") or 50.0)
        return StockSelector._round_score(
            sector_score * 0.25
            + weighted_impact * 0.25
            + source_score * 0.05
            + news_breadth * 0.05
            + positive_score * 0.10
            + fund_score * 0.30
        )

    @staticmethod
    def _build_theme_tiers(sectors: List[Dict[str, Any]], fund_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        theme_scores: List[Dict[str, Any]] = []
        for sector in sectors:
            if str(sector.get("sector_type") or "") != "theme":
                continue
            theme_name = str(sector.get("sector_name") or "").strip()
            if not theme_name:
                continue
            fund_match = StockSelector._match_theme_fund_flow(theme_name, fund_rows)
            trend_score = StockSelector._theme_trend_score(sector, fund_match)
            theme_scores.append({
                "theme_name": theme_name,
                "trend_score": trend_score,
                "fund_flow": fund_match,
                "sector_score": float(sector.get("sector_score") or 0),
                "weighted_impact_score": float(sector.get("weighted_impact_score") or 0),
            })
        theme_scores.sort(key=lambda row: row.get("trend_score") or 0, reverse=True)
        if not theme_scores:
            return {}
        top_score = float(theme_scores[0].get("trend_score") or 0)
        tiers: Dict[str, Dict[str, Any]] = {}
        for index, row in enumerate(theme_scores, start=1):
            score = float(row.get("trend_score") or 0)
            if index == 1 and score >= 70:
                tier = "mainline"
            elif top_score - score <= 8 and score >= 67:
                tier = "mainline"
            elif score >= 60:
                tier = "strong_side"
            elif score >= 50:
                tier = "side"
            else:
                tier = "broad_related"
            delta = {"mainline": 6.0, "strong_side": 2.0, "side": 0.0, "broad_related": -4.0}.get(tier, 0.0)
            fund = row.get("fund_flow") or {}
            reason_parts = [
                f"主题趋势分{score:.1f}",
                f"本地舆情{row.get('sector_score'):.1f}",
                f"事件影响{row.get('weighted_impact_score'):.1f}",
            ]
            if fund:
                reason_parts.append(f"实时板块资金{fund.get('sector_name')}净额{fund.get('net_amount')}亿")
            else:
                reason_parts.append("暂无匹配实时板块资金")
            tiers[row["theme_name"]] = {
                "market_theme_tier": tier,
                "market_theme_label": THEME_TIER_LABELS.get(tier, "未分层"),
                "market_theme_trend_score": round(score, 2),
                "market_theme_rank": index,
                "market_theme_score_delta": delta,
                "market_theme_reason": "，".join(reason_parts),
                "market_theme_fund_flow": fund,
            }
        return tiers

    @staticmethod
    def _limit_rate(code: str | None, name: str | None = None, is_st: bool = False) -> float:
        code = str(code or "")
        name = str(name or "")
        if code.startswith("bj."):
            return 0.30
        if code.startswith(("sz.300", "sz.301", "sh.688")):
            return 0.20
        if is_st or "ST" in name.upper() or "退" in name:
            return 0.05
        return 0.10

    @classmethod
    def _limit_up_state(
        cls,
        code: str | None,
        name: str | None,
        is_st: bool,
        price: float | None,
        pre_close: float | None,
    ) -> Dict[str, Any]:
        if price is None or pre_close is None or price <= 0 or pre_close <= 0:
            return {"is_limit_up": False, "limit_rate": None, "limit_up_price": None}
        rate = cls._limit_rate(code, name, is_st)
        limit_up_price = round(pre_close * (1 + rate), 2)
        return {
            "is_limit_up": price >= limit_up_price - 0.01,
            "limit_rate": rate,
            "limit_up_price": limit_up_price,
        }

    @staticmethod
    def _price_preference(reference_price: float | None) -> Dict[str, Any]:
        if reference_price is None or reference_price <= 0:
            return {"score_delta": 0.0, "label": "价格未知", "reason": "缺少有效价格"}
        if reference_price <= 20:
            return {"score_delta": 3.0, "label": "低价友好", "reason": "股价较低，散户交易门槛友好"}
        if reference_price <= 60:
            return {"score_delta": 1.5, "label": "价格适中", "reason": "股价处于较易交易区间"}
        if reference_price <= 120:
            return {"score_delta": 0.0, "label": "价格中性", "reason": "股价不做额外调整"}
        if reference_price <= 200:
            return {"score_delta": -4.0, "label": "高价降权", "reason": "股价偏高，除非超强热点否则降低权重"}
        return {"score_delta": -8.0, "label": "高价显著降权", "reason": "股价过高，普通舆情候选降低权重"}

    @staticmethod
    def _selection_clock_mode(now: Optional[datetime] = None) -> str:
        now = now or datetime.now()
        if now.weekday() >= 5:
            return "postclose"
        current = now.time()
        if time(9, 25) <= current <= time(15, 5):
            return "intraday"
        if current < time(9, 25):
            return "preopen"
        return "postclose"

    @staticmethod
    def _pe_status(pe: Optional[float], eps: Optional[float]) -> Dict[str, Any]:
        if pe is not None and pe > 0:
            return {
                "pe_status": "valid",
                "pe_status_label": "PE 正常",
                "pe_valid": True,
                "pe_status_reason": "估值源返回正 PE",
            }
        if eps is not None and eps <= 0:
            return {
                "pe_status": "not_applicable_eps_nonpositive",
                "pe_status_label": "PE 不适用",
                "pe_valid": False,
                "pe_status_reason": "EPS 非正，PE 不具备可比意义",
            }
        if eps is None:
            return {
                "pe_status": "missing_eps",
                "pe_status_label": "PE 暂缺",
                "pe_valid": False,
                "pe_status_reason": "EPS 缺失，无法判断 PE 口径",
            }
        return {
            "pe_status": "missing_positive_eps",
            "pe_status_label": "PE 暂缺",
            "pe_valid": False,
            "pe_status_reason": "EPS 为正但估值源未返回有效正 PE",
        }

    def _build_candidate(self, row: Dict[str, Any]) -> Dict[str, Any]:
        pe = float(row["pe_tushare"]) if row.get("pe_tushare") is not None else None
        pb = float(row["pb_tushare"]) if row.get("pb_tushare") is not None else None
        roe = float(row["roe"]) if row.get("roe") is not None else None
        roa = float(row["roa"]) if row.get("roa") is not None else None
        grossprofit_margin = float(row["grossprofit_margin"]) if row.get("grossprofit_margin") is not None else None
        netprofit_margin = float(row["netprofit_margin"]) if row.get("netprofit_margin") is not None else None
        revenue_yoy = float(row["revenue_yoy"]) if row.get("revenue_yoy") is not None else None
        profit_yoy = float(row["profit_yoy"]) if row.get("profit_yoy") is not None else None
        eps = float(row["eps"]) if row.get("eps") is not None else None
        close = float(row["close"]) if row.get("close") is not None else None
        amount = float(row["amount"]) if row.get("amount") is not None else None
        open_price = float(row["open"]) if row.get("open") is not None else None
        high_price = float(row["high"]) if row.get("high") is not None else None
        low_price = float(row["low"]) if row.get("low") is not None else None
        ma5 = float(row["ma5"]) if row.get("ma5") is not None else None
        ma10 = float(row["ma10"]) if row.get("ma10") is not None else None
        ma20 = float(row["ma20"]) if row.get("ma20") is not None else None
        ma30 = float(row["ma30"]) if row.get("ma30") is not None else None
        ma60 = float(row["ma60"]) if row.get("ma60") is not None else None
        close_5d = float(row["close_5d"]) if row.get("close_5d") is not None else None
        close_20d = float(row["close_20d"]) if row.get("close_20d") is not None else None
        prev_close_1d = float(row["prev_close_1d"]) if row.get("prev_close_1d") is not None else None
        max_close_20 = float(row["max_close_20"]) if row.get("max_close_20") is not None else None
        min_close_20 = float(row["min_close_20"]) if row.get("min_close_20") is not None else None
        avg_amount_5 = float(row["avg_amount_5"]) if row.get("avg_amount_5") is not None else None
        avg_amount_20 = float(row["avg_amount_20"]) if row.get("avg_amount_20") is not None else None
        kline_count_20 = int(row.get("kline_count_20") or 0)
        kline_count_60 = int(row.get("kline_count_60") or 0)
        std_return_20 = float(row["std_return_20"]) if row.get("std_return_20") is not None else None
        pct_chg_1d = float(row["pct_chg_1d"]) if row.get("pct_chg_1d") is not None else None
        realtime_price = float(row["realtime_price"]) if row.get("realtime_price") is not None else None
        realtime_pct_chg = float(row["realtime_pct_chg"]) if row.get("realtime_pct_chg") is not None else None
        realtime_pre_close = float(row["realtime_pre_close"]) if row.get("realtime_pre_close") is not None else None
        realtime_open = float(row["realtime_open"]) if row.get("realtime_open") is not None else None
        realtime_high = float(row["realtime_high"]) if row.get("realtime_high") is not None else None
        realtime_low = float(row["realtime_low"]) if row.get("realtime_low") is not None else None
        realtime_amount = float(row["realtime_amount"]) if row.get("realtime_amount") is not None else None
        intraday_high_drawdown_pct = (
            (realtime_price - realtime_high) / realtime_high * 100
            if realtime_price is not None and realtime_high and realtime_high > 0
            else None
        )
        intraday_open_drawdown_pct = (
            (realtime_price - realtime_open) / realtime_open * 100
            if realtime_price is not None and realtime_open and realtime_open > 0
            else None
        )
        realtime_amount_ratio = (
            realtime_amount / amount
            if realtime_amount is not None and amount and amount > 0
            else None
        )
        reference_price = realtime_price if realtime_price is not None else close
        reference_pre_close = realtime_pre_close if realtime_pre_close is not None else prev_close_1d
        limit_up_state = self._limit_up_state(
            code=row.get("code"),
            name=row.get("name"),
            is_st=bool(row.get("is_st")),
            price=reference_price,
            pre_close=reference_pre_close,
        )
        price_preference = self._price_preference(reference_price)
        turnover_rate = float(row["turnover_rate"]) if row.get("turnover_rate") is not None else None
        turnover_rate_5d_avg = float(row["turnover_rate_5d_avg"]) if row.get("turnover_rate_5d_avg") is not None else None
        listed_days = int(row.get("listed_days") or 0) if row.get("listed_days") is not None else None
        volume_ratio = float(row["volume_ratio"]) if row.get("volume_ratio") is not None else None
        total_mv = float(row["total_mv"]) if row.get("total_mv") is not None else None
        completeness_score = float(row["completeness_score"]) if row.get("completeness_score") is not None else None
        net_mf_amount = float(row["net_mf_amount"]) if row.get("net_mf_amount") is not None else None
        net_mf_vol = float(row["net_mf_vol"]) if row.get("net_mf_vol") is not None else None
        buy_lg_amount = float(row["buy_lg_amount"]) if row.get("buy_lg_amount") is not None else None
        sell_lg_amount = float(row["sell_lg_amount"]) if row.get("sell_lg_amount") is not None else None
        buy_elg_amount = float(row["buy_elg_amount"]) if row.get("buy_elg_amount") is not None else None
        sell_elg_amount = float(row["sell_elg_amount"]) if row.get("sell_elg_amount") is not None else None
        realtime_mf_inflow = float(row["realtime_mf_inflow"]) if row.get("realtime_mf_inflow") is not None else None
        realtime_mf_outflow = float(row["realtime_mf_outflow"]) if row.get("realtime_mf_outflow") is not None else None
        realtime_mf_net = float(row["realtime_mf_net"]) if row.get("realtime_mf_net") is not None else None
        realtime_mf_amount = float(row["realtime_mf_amount"]) if row.get("realtime_mf_amount") is not None else None
        realtime_mf_turnover_rate = float(row["realtime_mf_turnover_rate"]) if row.get("realtime_mf_turnover_rate") is not None else None
        popularity_rank = int(row.get("popularity_rank") or 0) if row.get("popularity_rank") is not None else None
        popularity_source_score = float(row["popularity_source_score"]) if row.get("popularity_source_score") is not None else None
        popularity_score = float(row["popularity_score"]) if row.get("popularity_score") is not None else None
        chip_his_low = float(row["chip_his_low"]) if row.get("chip_his_low") is not None else None
        chip_his_high = float(row["chip_his_high"]) if row.get("chip_his_high") is not None else None
        chip_cost_5pct = float(row["chip_cost_5pct"]) if row.get("chip_cost_5pct") is not None else None
        chip_cost_15pct = float(row["chip_cost_15pct"]) if row.get("chip_cost_15pct") is not None else None
        chip_cost_50pct = float(row["chip_cost_50pct"]) if row.get("chip_cost_50pct") is not None else None
        chip_cost_85pct = float(row["chip_cost_85pct"]) if row.get("chip_cost_85pct") is not None else None
        chip_cost_95pct = float(row["chip_cost_95pct"]) if row.get("chip_cost_95pct") is not None else None
        chip_weight_avg = float(row["chip_weight_avg"]) if row.get("chip_weight_avg") is not None else None
        chip_winner_rate = float(row["chip_winner_rate"]) if row.get("chip_winner_rate") is not None else None
        has_trade_data = row.get("trade_date") is not None

        value_score = 0.20
        quality_score = 0.20
        stability_score = 0.20
        data_quality_score = 0.0
        reasons: List[str] = []
        risks: List[str] = []
        missing_fields: List[str] = []
        pe_status = self._pe_status(pe, eps)

        if has_trade_data:
            stability_score += 0.20
            data_quality_score += 0.35
            reasons.append("存在最新日线数据")
        else:
            risks.append("缺少最新行情数据")
            missing_fields.append("trade_date")

        if close is not None and close > 0:
            data_quality_score += 0.15
            if 5 <= close <= 60:
                stability_score += 0.10
                reasons.append("股价处于较易交易区间")
            elif close > 120:
                risks.append("股价偏高，交易门槛较高")
            else:
                stability_score += 0.04
            if price_preference.get("score_delta", 0) > 0:
                reasons.append(price_preference["reason"])
            elif price_preference.get("score_delta", 0) < 0:
                risks.append(price_preference["reason"])
        else:
            missing_fields.append("close")
            risks.append("缺少收盘价数据")

        if pe is not None:
            data_quality_score += 0.15
            if 0 < pe <= 20:
                value_score += 0.30
                reasons.append("PE 处于较优估值区间")
            elif 20 < pe <= 35:
                value_score += 0.18
                reasons.append("PE 估值中等偏合理")
            elif 35 < pe <= 80:
                value_score += 0.08
            elif pe > 80:
                risks.append("PE 偏高，估值压力较大")
            else:
                risks.append("PE 为负或异常，盈利质量需谨慎")
        else:
            if pe_status["pe_status"] == "not_applicable_eps_nonpositive":
                risks.append("PE 不适用：EPS 非正，估值因子按中性处理")
            elif pe_status["pe_status"] == "missing_eps":
                missing_fields.append("eps")
                risks.append("PE 暂缺：EPS 缺失，估值因子按中性处理")
            else:
                risks.append("PE 暂缺：估值源未返回有效正 PE，估值因子按中性处理")

        if pb is not None:
            data_quality_score += 0.15
            if 0 < pb <= 2:
                value_score += 0.20
                reasons.append("PB 较低，具备一定安全边际")
            elif 2 < pb <= 4:
                value_score += 0.12
            elif pb > 6:
                risks.append("PB 偏高，资产定价不便宜")
        else:
            missing_fields.append("pb_tushare")
            risks.append("缺少 PB 数据")

        if roe is not None:
            data_quality_score += 0.20
            if roe >= 15:
                quality_score += 0.35
                reasons.append("ROE 较高，盈利质量较强")
            elif roe >= 10:
                quality_score += 0.22
                reasons.append("ROE 良好")
            elif roe >= 5:
                quality_score += 0.10
            elif roe < 0:
                risks.append("ROE 为负，基本面偏弱")
            else:
                risks.append("ROE 偏低，盈利能力一般")
        else:
            missing_fields.append("roe")
            risks.append("缺少 ROE 数据")

        completeness = 1 - (len(set(missing_fields)) / 4 if missing_fields else 0)
        reversal_score = self._round_score((0.35 + quality_score * 0.25 + value_score * 0.15) * 100)
        turnover_score = self._round_score((0.30 + stability_score * 0.30 + data_quality_score * 0.20) * 100)
        lowvol_score = self._round_score((0.30 + value_score * 0.35 + stability_score * 0.20) * 100)

        fundamental_context = {
            "roe": roe,
            "roa": roa,
            "grossprofit_margin": grossprofit_margin,
            "netprofit_margin": netprofit_margin,
            "revenue_yoy": revenue_yoy,
            "profit_yoy": profit_yoy,
            "eps": eps,
        }

        if roa is None:
            missing_fields.append("roa")
        if grossprofit_margin is None:
            missing_fields.append("grossprofit_margin")
        if netprofit_margin is None:
            missing_fields.append("netprofit_margin")
        if revenue_yoy is None:
            missing_fields.append("revenue_yoy")
        if profit_yoy is None:
            missing_fields.append("profit_yoy")
        if eps is None:
            missing_fields.append("eps")

        if roa is not None and roa >= 6:
            reasons.append("ROA 表现较稳")
        if grossprofit_margin is not None and grossprofit_margin >= 20:
            reasons.append("毛利率表现较好")
        if revenue_yoy is not None and revenue_yoy >= 10:
            reasons.append("营收同比保持增长")
        if profit_yoy is not None and profit_yoy < 0:
            risks.append("利润同比下滑")

        return {
            "code": row["code"],
            "name": row["name"],
            "industry": row.get("industry"),
            "instrument_type": row.get("instrument_type"),
            "is_st": bool(row.get("is_st")),
            "trade_date": str(row["trade_date"]) if row.get("trade_date") else None,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close,
            "amount": amount,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma30": ma30,
            "ma60": ma60,
            "close_5d": close_5d,
            "close_20d": close_20d,
            "prev_close_1d": prev_close_1d,
            "max_close_20": max_close_20,
            "min_close_20": min_close_20,
            "avg_amount_5": avg_amount_5,
            "avg_amount_20": avg_amount_20,
            "kline_count_20": kline_count_20,
            "kline_count_60": kline_count_60,
            "std_return_20": std_return_20,
            "pct_chg_1d": pct_chg_1d,
            "realtime_price": realtime_price,
            "realtime_pct_chg": realtime_pct_chg,
            "realtime_pre_close": realtime_pre_close,
            "realtime_open": realtime_open,
            "realtime_high": realtime_high,
            "realtime_low": realtime_low,
            "realtime_amount": realtime_amount,
            "intraday_high_drawdown_pct": intraday_high_drawdown_pct,
            "intraday_open_drawdown_pct": intraday_open_drawdown_pct,
            "realtime_amount_ratio": realtime_amount_ratio,
            "realtime_quote_time": str(row["realtime_quote_time"]) if row.get("realtime_quote_time") else None,
            "realtime_trade_date": str(row["realtime_trade_date"]) if row.get("realtime_trade_date") else None,
            "turnover_rate": turnover_rate,
            "turnover_rate_5d_avg": turnover_rate_5d_avg,
            "listed_days": listed_days,
            "volume_ratio": volume_ratio,
            "total_mv": total_mv,
            "completeness_score": completeness_score,
            "net_mf_amount": net_mf_amount,
            "net_mf_vol": net_mf_vol,
            "buy_lg_amount": buy_lg_amount,
            "sell_lg_amount": sell_lg_amount,
            "buy_elg_amount": buy_elg_amount,
            "sell_elg_amount": sell_elg_amount,
            "realtime_mf_inflow": realtime_mf_inflow,
            "realtime_mf_outflow": realtime_mf_outflow,
            "realtime_mf_net": realtime_mf_net,
            "realtime_mf_amount": realtime_mf_amount,
            "realtime_mf_turnover_rate": realtime_mf_turnover_rate,
            "realtime_mf_quote_time": str(row["realtime_mf_quote_time"]) if row.get("realtime_mf_quote_time") else None,
            "realtime_mf_trade_date": str(row["realtime_mf_trade_date"]) if row.get("realtime_mf_trade_date") else None,
            "popularity_source": row.get("popularity_source"),
            "popularity_rank": popularity_rank,
            "popularity_source_score": popularity_source_score,
            "popularity_score": popularity_score,
            "popularity_quote_time": str(row["popularity_quote_time"]) if row.get("popularity_quote_time") else None,
            "is_limit_up": limit_up_state.get("is_limit_up"),
            "limit_up_price": limit_up_state.get("limit_up_price"),
            "limit_rate": limit_up_state.get("limit_rate"),
            "price_preference_delta": price_preference.get("score_delta"),
            "price_preference_label": price_preference.get("label"),
            "price_preference_reason": price_preference.get("reason"),
            "chip_his_low": chip_his_low,
            "chip_his_high": chip_his_high,
            "chip_cost_5pct": chip_cost_5pct,
            "chip_cost_15pct": chip_cost_15pct,
            "chip_cost_50pct": chip_cost_50pct,
            "chip_cost_85pct": chip_cost_85pct,
            "chip_cost_95pct": chip_cost_95pct,
            "chip_weight_avg": chip_weight_avg,
            "chip_winner_rate": chip_winner_rate,
            "pe_tushare": pe,
            "pe_status": pe_status["pe_status"],
            "pe_status_label": pe_status["pe_status_label"],
            "pe_valid": pe_status["pe_valid"],
            "pe_status_reason": pe_status["pe_status_reason"],
            "pb_tushare": pb,
            "roe": roe,
            "roa": roa,
            "grossprofit_margin": grossprofit_margin,
            "netprofit_margin": netprofit_margin,
            "revenue_yoy": revenue_yoy,
            "profit_yoy": profit_yoy,
            "eps": eps,
            "sentiment_score": float(row["sentiment_score"]) if row.get("sentiment_score") is not None else None,
            "news_count": int(row.get("news_count") or 0),
            "market_strength": float(row["market_strength"]) if row.get("market_strength") is not None else None,
            "market_state": row.get("market_state"),
            "market_index_trend_score": float(row["market_index_trend_score"]) if row.get("market_index_trend_score") is not None else None,
            "market_index_day_score": float(row["market_index_day_score"]) if row.get("market_index_day_score") is not None else None,
            "market_index_pct_chg": float(row["market_index_pct_chg"]) if row.get("market_index_pct_chg") is not None else None,
            "market_breadth_score": float(row["market_breadth_score"]) if row.get("market_breadth_score") is not None else None,
            "market_volume_score": float(row["market_volume_score"]) if row.get("market_volume_score") is not None else None,
            "market_index_count": int(row.get("market_index_count") or 0),
            "market_index_codes": row.get("market_index_codes"),
            "csi300_pct_chg": float(row["csi300_pct_chg"]) if row.get("csi300_pct_chg") is not None else None,
            "csi500_pct_chg": float(row["csi500_pct_chg"]) if row.get("csi500_pct_chg") is not None else None,
            "csi1000_pct_chg": float(row["csi1000_pct_chg"]) if row.get("csi1000_pct_chg") is not None else None,
            "fundamental_context": fundamental_context,
            "value_score": self._round_score(value_score),
            "quality_score": self._round_score(quality_score),
            "stability_score": self._round_score(stability_score),
            "data_quality_score": self._round_score(data_quality_score),
            "completeness_score": self._round_score(completeness),
            "turnover_score": turnover_score,
            "lowvol_score": lowvol_score,
            "reversal_score": reversal_score,
            "candidate_reasons": reasons,
            "candidate_risks": risks,
            "missing_fields": sorted(set(missing_fields)),
        }

    def _attach_market_opinion_context(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Attach NewsNow/RSS-driven sector opinion context for A股舆情选股 V2.

        `sector_opinion_daily.top_stocks_json` is the P0 bridge from hot
        themes/sectors to stock candidates. Keep this enrichment separate from
        the generic candidate SQL so other strategies are unaffected.
        """
        diagnostics = {"enabled": self.strategy_id == "a_share_sentiment", "matched_candidates": 0}
        if self.strategy_id != "a_share_sentiment" or not candidates:
            return diagnostics

        market_opinion_config = self.strategy.config.get("market_opinion", {}) or {}
        allowed_sector_types = {
            str(value).strip()
            for value in (market_opinion_config.get("allowed_sector_types") or [])
            if str(value).strip()
        }
        excluded_sector_names = {
            str(value).strip()
            for value in (market_opinion_config.get("excluded_sector_names") or [])
            if str(value).strip()
        }
        excluded_industries = {
            str(value).strip()
            for value in (market_opinion_config.get("excluded_industries") or [])
            if str(value).strip()
        }
        max_age_minutes = int(market_opinion_config.get("max_age_minutes") or 60)
        allow_same_trade_date_stale = bool(market_opinion_config.get("allow_same_trade_date_stale", True))
        requested_as_of = market_opinion_config.get("as_of_datetime") or self.strategy.config.get("market_opinion_as_of")
        requested_as_of_dt = self._requested_market_opinion_as_of()
        if requested_as_of:
            if not requested_as_of_dt:
                diagnostics["reason"] = "invalid_market_opinion_as_of"
                diagnostics["requested_as_of"] = str(requested_as_of)
                return diagnostics
        latest_candidate_trade_date = max(
            (str(item.get("trade_date")) for item in candidates if item.get("trade_date")),
            default=None,
        )
        sectors, fund_rows = self.repository.load_market_opinion_rows(
            requested_as_of=(
                requested_as_of_dt.strftime("%Y-%m-%d %H:%M:%S")
                if requested_as_of_dt
                else None
            ),
            latest_candidate_trade_date=latest_candidate_trade_date,
        )
        hydrate_sector_opinion_rows(sectors)

        diagnostics.update({
            "sector_count": len(sectors),
            "max_age_minutes": max_age_minutes,
            "allow_same_trade_date_stale": allow_same_trade_date_stale,
            "latest_candidate_trade_date": latest_candidate_trade_date,
            "requested_as_of": requested_as_of_dt.strftime("%Y-%m-%d %H:%M:%S") if requested_as_of_dt else None,
        })
        if not sectors:
            diagnostics["reason"] = "no_fresh_or_trade_date_aligned_sector_opinion_daily_rows"
            return diagnostics

        latest_as_of = sectors[0].get("as_of_datetime")
        if isinstance(latest_as_of, str):
            latest_as_of_dt = datetime.fromisoformat(latest_as_of.replace("Z", "+00:00"))
        else:
            latest_as_of_dt = latest_as_of
        if not latest_as_of_dt:
            diagnostics["reason"] = "sector_opinion_missing_as_of_datetime"
            return diagnostics
        now_dt = datetime.now(latest_as_of_dt.tzinfo) if latest_as_of_dt.tzinfo else datetime.now()
        age_minutes = (now_dt - latest_as_of_dt).total_seconds() / 60
        sector_trade_date = str(sectors[0].get("trade_date")) if sectors and sectors[0].get("trade_date") else None
        same_trade_date = bool(latest_candidate_trade_date and sector_trade_date == latest_candidate_trade_date)
        stale_accepted = (not requested_as_of_dt) and allow_same_trade_date_stale and same_trade_date
        diagnostics.update({
            "latest_as_of": str(latest_as_of),
            "age_minutes": round(age_minutes, 2),
            "sector_trade_date": sector_trade_date,
            "same_trade_date_stale_accepted": bool(age_minutes > max_age_minutes and stale_accepted),
        })
        if not requested_as_of_dt and age_minutes > max_age_minutes and not stale_accepted:
            diagnostics["reason"] = "sector_opinion_stale"
            return diagnostics

        eligible_sectors: List[Dict[str, Any]] = []
        for sector in sectors:
            sector_type = str(sector.get("sector_type") or "").strip()
            sector_name = str(sector.get("sector_name") or "").strip()
            if allowed_sector_types and sector_type not in allowed_sector_types:
                continue
            if sector_name in excluded_sector_names:
                continue
            eligible_sectors.append(sector)

        by_code: Dict[str, Dict[str, Any]] = {}
        theme_tiers = self._build_theme_tiers(eligible_sectors, fund_rows)
        for sector in eligible_sectors:
            try:
                stocks = json.loads(sector.get("top_stocks_json") or "[]")
            except Exception:
                stocks = []
            try:
                top_news = json.loads(sector.get("top_news_json") or "[]")
            except Exception:
                top_news = []
            try:
                sources = json.loads(sector.get("source_json") or "[]")
            except Exception:
                sources = []

            sector_score = float(sector.get("sector_score") or 0)
            weighted_impact = float(sector.get("weighted_impact_score") or 0)
            sector_type = str(sector.get("sector_type") or "").strip()
            sector_name = str(sector.get("sector_name") or "").strip()
            stock_count = len(stocks or [])
            for rank, stock in enumerate(stocks or [], start=1):
                code = stock.get("code")
                if not code:
                    continue
                stock_industry = str(stock.get("industry") or "").strip()
                if stock_industry in excluded_industries:
                    continue
                allowed_industries = THEME_INDUSTRY_HINTS.get(str(sector_name or ""))
                if sector_type == "theme" and allowed_industries and stock_industry not in allowed_industries:
                    continue
                stock_score = float(stock.get("score") or 0)
                combined = sector_score * 0.72 + stock_score * 0.28
                existing = by_code.get(code)
                if existing and existing.get("opinion_combined_score", 0) >= combined:
                    continue
                match_reason = stock.get("match_reason") or "板块热度候选股"
                match_type = stock.get("match_type")
                if not match_type:
                    if "板块候选池" in match_reason:
                        match_type = "sector_candidate"
                    elif "命中" in match_reason:
                        match_type = "direct_news_match"
                    else:
                        match_type = "legacy_snapshot"
                stock_news = stock.get("matched_news") or []
                recognition_context = self._stock_recognition_context(stock, rank, stock_count, match_type)
                theme_tier = theme_tiers.get(sector_name) if sector_type == "theme" else None
                market_theme_score_delta = float((theme_tier or {}).get("market_theme_score_delta") or 0)
                if match_type == "sector_candidate" and theme_tier:
                    market_theme_score_delta -= 2.0
                if match_type == "sector_candidate" and recognition_context["opinion_stock_recognition_score"] < 58:
                    market_theme_score_delta -= 3.0
                by_code[code] = {
                    "opinion_sector_type": sector.get("sector_type"),
                    "opinion_sector_name": sector.get("sector_name"),
                    "opinion_as_of_datetime": str(sector.get("as_of_datetime")) if sector.get("as_of_datetime") else None,
                    "opinion_trade_date": str(sector.get("trade_date")) if sector.get("trade_date") else None,
                    "opinion_sector_score": round(sector_score, 4),
                    "opinion_weighted_impact_score": round(weighted_impact, 4),
                    "opinion_news_count": int(sector.get("news_count") or 0),
                    "opinion_source_count": int(sector.get("source_count") or 0),
                    "opinion_stock_count": int(sector.get("stock_count") or 0),
                    "opinion_positive_news_count": int(sector.get("positive_news_count") or 0),
                    "opinion_negative_news_count": int(sector.get("negative_news_count") or 0),
                    "opinion_stock_score": round(stock_score, 4),
                    "opinion_combined_score": round(combined, 4),
                    "opinion_match_type": match_type,
                    "opinion_match_reason": match_reason,
                    "opinion_stock_news": stock_news[:3],
                    "opinion_top_news": (stock_news or top_news)[:3],
                    "opinion_sector_top_news": top_news[:3],
                    "opinion_sources": sources[:8],
                    **recognition_context,
                    **(theme_tier or {}),
                    "market_theme_score_delta": market_theme_score_delta if theme_tier else None,
                    "market_theme_match_adjustment": -2.0 if match_type == "sector_candidate" and theme_tier else 0.0,
                }

        matched = 0
        for item in candidates:
            context = by_code.get(item.get("code"))
            if not context:
                continue
            matched += 1
            item.update(context)
            sector_name = context.get("opinion_sector_name")
            if sector_name:
                item.setdefault("candidate_reasons", []).append(f"舆情热度映射到热点板块/主题：{sector_name}")
            if context.get("opinion_stock_recognition_label"):
                item.setdefault("candidate_reasons", []).append(
                    f"{context['opinion_stock_recognition_label']}：{context.get('opinion_stock_recognition_reason')}"
                )
        diagnostics.update(
            {
                "latest_as_of": str(sectors[0].get("as_of_datetime")) if sectors and sectors[0].get("as_of_datetime") else None,
                "matched_candidates": matched,
                "mapped_stock_count": len(by_code),
                "top_sectors": [
                    {
                        "sector_type": row.get("sector_type"),
                        "sector_name": row.get("sector_name"),
                        "sector_score": float(row.get("sector_score") or 0),
                        "news_count": int(row.get("news_count") or 0),
                    }
                    for row in sectors[:5]
                ],
                "theme_tiers": [
                    {
                        "theme_name": name,
                        "tier": item.get("market_theme_tier"),
                        "label": item.get("market_theme_label"),
                        "trend_score": item.get("market_theme_trend_score"),
                        "rank": item.get("market_theme_rank"),
                        "score_delta": item.get("market_theme_score_delta"),
                        "reason": item.get("market_theme_reason"),
                    }
                    for name, item in sorted(theme_tiers.items(), key=lambda pair: pair[1].get("market_theme_rank") or 999)[:10]
                ],
            }
        )
        return diagnostics

    def load_candidates_from_mysql(
        self,
        candidate_limit: Optional[int] = None,
        instrument_type: str = "stock",
        market_board: Optional[str] = None,
    ) -> Dict[str, Any]:
        requested_as_of_dt = self._requested_market_opinion_as_of()
        clock_mode = self._selection_clock_mode(requested_as_of_dt)
        use_realtime = requested_as_of_dt is None and clock_mode == "intraday"
        use_current_popularity = requested_as_of_dt is None
        candidate_as_of_diagnostics: Dict[str, Any] = {}
        daily_kline_operator: str | None = None
        cutoff_date: str | None = None
        if requested_as_of_dt:
            daily_kline_operator = "<=" if requested_as_of_dt.time() >= time(15, 5) else "<"
            cutoff_date = requested_as_of_dt.strftime("%Y-%m-%d")
            candidate_as_of_diagnostics = {
                "requested_as_of": requested_as_of_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "daily_kline_date_operator": daily_kline_operator,
                "daily_kline_cutoff_date": cutoff_date,
            }
        _, normalized_market_board, market_board_label = self.market_board_filter_sql(market_board)
        rows = self.repository.load_candidate_rows(
            daily_kline_operator=daily_kline_operator,
            cutoff_date=cutoff_date,
            use_realtime=use_realtime,
            use_current_popularity=use_current_popularity,
            instrument_type=instrument_type,
            market_board=normalized_market_board,
            candidate_limit=candidate_limit,
        )

        candidates = [self._build_candidate(row) for row in rows]
        opinion_diagnostics = self._attach_market_opinion_context(candidates)
        bundle = {
            "candidates": candidates,
            "selection_clock_diagnostics": {
                "clock_mode": clock_mode,
                "use_realtime": use_realtime,
                "realtime_policy": "intraday_only",
            },
            "market_board_filter_summary": {
                "market_board": normalized_market_board,
                "market_board_label": market_board_label,
            },
        }
        if candidate_as_of_diagnostics:
            candidate_as_of_diagnostics["latest_candidate_trade_date"] = max(
                (str(item.get("trade_date")) for item in candidates if item.get("trade_date")),
                default=None,
            )
            bundle["candidate_as_of_diagnostics"] = candidate_as_of_diagnostics
        if opinion_diagnostics.get("enabled"):
            bundle["market_opinion_diagnostics"] = opinion_diagnostics
        return bundle

    @staticmethod
    def _build_raw_metrics(item: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        raw_metrics = {
            "open": item.get("open"),
            "close": item.get("close"),
            "industry": item.get("industry"),
            "ma5": item.get("ma5"),
            "ma10": item.get("ma10"),
            "ma20": item.get("ma20"),
            "ma30": item.get("ma30"),
            "ma60": item.get("ma60"),
            "close_5d": item.get("close_5d"),
            "close_20d": item.get("close_20d"),
            "prev_close_1d": item.get("prev_close_1d"),
            "max_close_20": item.get("max_close_20"),
            "min_close_20": item.get("min_close_20"),
            "avg_amount_5": item.get("avg_amount_5"),
            "avg_amount_20": item.get("avg_amount_20"),
            "kline_count_20": item.get("kline_count_20"),
            "kline_count_60": item.get("kline_count_60"),
            "std_return_20": item.get("std_return_20"),
            "pct_chg_1d": item.get("pct_chg_1d"),
            "realtime_price": item.get("realtime_price"),
            "realtime_pct_chg": item.get("realtime_pct_chg"),
            "realtime_pre_close": item.get("realtime_pre_close"),
            "realtime_open": item.get("realtime_open"),
            "realtime_high": item.get("realtime_high"),
            "realtime_low": item.get("realtime_low"),
            "realtime_amount": item.get("realtime_amount"),
            "intraday_high_drawdown_pct": item.get("intraday_high_drawdown_pct"),
            "intraday_open_drawdown_pct": item.get("intraday_open_drawdown_pct"),
            "realtime_amount_ratio": item.get("realtime_amount_ratio"),
            "realtime_quote_time": item.get("realtime_quote_time"),
            "realtime_trade_date": item.get("realtime_trade_date"),
            "amount": item.get("amount"),
            "turnover_rate": item.get("turnover_rate"),
            "turnover_rate_5d_avg": item.get("turnover_rate_5d_avg"),
            "listed_days": item.get("listed_days"),
            "volume_ratio": item.get("volume_ratio"),
            "total_mv": item.get("total_mv"),
            "completeness_score": item.get("completeness_score"),
            "net_mf_amount": item.get("net_mf_amount"),
            "net_mf_vol": item.get("net_mf_vol"),
            "buy_lg_amount": item.get("buy_lg_amount"),
            "sell_lg_amount": item.get("sell_lg_amount"),
            "buy_elg_amount": item.get("buy_elg_amount"),
            "sell_elg_amount": item.get("sell_elg_amount"),
            "realtime_mf_inflow": item.get("realtime_mf_inflow"),
            "realtime_mf_outflow": item.get("realtime_mf_outflow"),
            "realtime_mf_net": item.get("realtime_mf_net"),
            "realtime_mf_amount": item.get("realtime_mf_amount"),
            "realtime_mf_turnover_rate": item.get("realtime_mf_turnover_rate"),
            "realtime_mf_quote_time": item.get("realtime_mf_quote_time"),
            "realtime_mf_trade_date": item.get("realtime_mf_trade_date"),
            "popularity_source": item.get("popularity_source"),
            "popularity_rank": item.get("popularity_rank"),
            "popularity_source_score": item.get("popularity_source_score"),
            "popularity_score": item.get("popularity_score"),
            "popularity_quote_time": item.get("popularity_quote_time"),
            "is_limit_up": item.get("is_limit_up"),
            "limit_up_price": item.get("limit_up_price"),
            "limit_rate": item.get("limit_rate"),
            "price_preference_delta": item.get("price_preference_delta"),
            "price_preference_label": item.get("price_preference_label"),
            "price_preference_reason": item.get("price_preference_reason"),
            "price_preference_delta_applied": item.get("price_preference_delta_applied"),
            "base_score_before_price_preference": item.get("base_score_before_price_preference"),
            "market_theme_tier": item.get("market_theme_tier"),
            "market_theme_label": item.get("market_theme_label"),
            "market_theme_trend_score": item.get("market_theme_trend_score"),
            "market_theme_rank": item.get("market_theme_rank"),
            "market_theme_score_delta": item.get("market_theme_score_delta"),
            "market_theme_match_adjustment": item.get("market_theme_match_adjustment"),
            "market_theme_reason": item.get("market_theme_reason"),
            "market_theme_fund_flow": item.get("market_theme_fund_flow"),
            "chip_his_low": item.get("chip_his_low"),
            "chip_his_high": item.get("chip_his_high"),
            "chip_cost_5pct": item.get("chip_cost_5pct"),
            "chip_cost_15pct": item.get("chip_cost_15pct"),
            "chip_cost_50pct": item.get("chip_cost_50pct"),
            "chip_cost_85pct": item.get("chip_cost_85pct"),
            "chip_cost_95pct": item.get("chip_cost_95pct"),
            "chip_weight_avg": item.get("chip_weight_avg"),
            "chip_winner_rate": item.get("chip_winner_rate"),
            "pe_tushare": item.get("pe_tushare"),
            "pe_status": item.get("pe_status"),
            "pe_status_label": item.get("pe_status_label"),
            "pe_valid": item.get("pe_valid"),
            "pe_status_reason": item.get("pe_status_reason"),
            "pb_tushare": item.get("pb_tushare"),
            "roe": item.get("roe"),
            "roa": item.get("roa"),
            "grossprofit_margin": item.get("grossprofit_margin"),
            "netprofit_margin": item.get("netprofit_margin"),
            "revenue_yoy": item.get("revenue_yoy"),
            "profit_yoy": item.get("profit_yoy"),
            "eps": item.get("eps"),
            "sentiment_score": item.get("sentiment_score"),
            "news_count": item.get("news_count"),
            "market_strength": item.get("market_strength"),
            "market_state": item.get("market_state"),
            "market_index_trend_score": item.get("market_index_trend_score"),
            "market_index_day_score": item.get("market_index_day_score"),
            "market_index_pct_chg": item.get("market_index_pct_chg"),
            "market_breadth_score": item.get("market_breadth_score"),
            "market_volume_score": item.get("market_volume_score"),
            "market_index_count": item.get("market_index_count"),
            "market_index_codes": item.get("market_index_codes"),
            "csi300_pct_chg": item.get("csi300_pct_chg"),
            "csi500_pct_chg": item.get("csi500_pct_chg"),
            "csi1000_pct_chg": item.get("csi1000_pct_chg"),
            "trade_grade_state": item.get("trade_grade_state"),
            "trade_grade_label": item.get("trade_grade_label"),
            "trade_grade_reason": item.get("trade_grade_reason"),
            "theme_trade_slot_state": item.get("theme_trade_slot_state"),
            "trade_date": item.get("trade_date"),
        }
        if extra:
            raw_metrics.update(extra)
        return raw_metrics

    def _enhance_explain(self, item: Dict[str, Any]) -> Dict[str, Any]:
        base_explain = self.strategy.explain(item)
        strategy_raw_metrics = base_explain.get("raw_metrics") or {}
        if not strategy_raw_metrics and item.get("raw_lowvol_reversal_metrics"):
            strategy_raw_metrics = item.get("raw_lowvol_reversal_metrics") or {}
        return {
            **base_explain,
            "summary": {
                "value_score": item.get("value_score"),
                "quality_score": item.get("quality_score"),
                "stability_score": item.get("stability_score"),
                "data_quality_score": item.get("data_quality_score"),
                "completeness_score": item.get("completeness_score"),
            },
            "reasons": item.get("candidate_reasons", []),
            "risks": item.get("candidate_risks", []),
            "missing_fields": item.get("missing_fields", []),
            "raw_metrics": self._build_raw_metrics(item, strategy_raw_metrics),
            "fundamental_context": item.get("fundamental_context", {}),
        }

    @staticmethod
    def _build_sentiment_context(item: Dict[str, Any], explain: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        explain = explain or {}
        raw_metrics = {
            **(explain.get("raw_metrics") or {}),
            **(item.get("strategy_raw_metrics") or {}),
        }
        factor_scores = item.get("factors") or explain.get("factors") or {}
        sector_name = item.get("opinion_sector_name") or raw_metrics.get("opinion_sector_name")
        if not sector_name and raw_metrics.get("sentiment_mode") != "market_opinion_v2":
            return None
        stock_news = item.get("opinion_stock_news") or raw_metrics.get("opinion_stock_news") or []
        top_news = item.get("opinion_top_news") or raw_metrics.get("opinion_top_news") or []
        sector_top_news = item.get("opinion_sector_top_news") or raw_metrics.get("opinion_sector_top_news") or []
        enriched_stock_news = [enrich_opinion_news_item(news, "来自本次舆情选股的个股命中新闻") for news in stock_news]
        enriched_top_news = [enrich_opinion_news_item(news, "来自本次舆情选股的热点新闻") for news in top_news]
        enriched_sector_news = [enrich_opinion_news_item(news, "来自本次舆情选股的关联板块热度新闻") for news in sector_top_news]
        return {
            "sector_name": sector_name,
            "sector_type": item.get("opinion_sector_type") or raw_metrics.get("opinion_sector_type"),
            "as_of": item.get("opinion_as_of_datetime") or raw_metrics.get("opinion_as_of_datetime"),
            "trade_date": item.get("opinion_trade_date") or raw_metrics.get("opinion_trade_date"),
            "opinion_match_type": item.get("opinion_match_type") or raw_metrics.get("opinion_match_type"),
            "opinion_match_reason": item.get("opinion_match_reason") or raw_metrics.get("opinion_match_reason"),
            "stock_news": enriched_stock_news,
            "top_news": enriched_top_news,
            "sector_top_news": enriched_sector_news,
            "sources": item.get("opinion_sources") or raw_metrics.get("opinion_sources") or [],
            "news_count": item.get("opinion_news_count") or raw_metrics.get("opinion_news_count"),
            "source_count": item.get("opinion_source_count") or raw_metrics.get("opinion_source_count"),
            "positive": item.get("opinion_positive_news_count") or raw_metrics.get("opinion_positive_news_count"),
            "negative": item.get("opinion_negative_news_count") or raw_metrics.get("opinion_negative_news_count"),
            "sector_score": item.get("opinion_sector_score") or raw_metrics.get("opinion_sector_score"),
            "weighted_impact_score": item.get("opinion_weighted_impact_score") or raw_metrics.get("opinion_weighted_impact_score"),
            "stock_rank": item.get("opinion_stock_rank") or raw_metrics.get("opinion_stock_rank"),
            "stock_pool_size": item.get("opinion_stock_pool_size") or raw_metrics.get("opinion_stock_pool_size"),
            "stock_recognition_score": item.get("opinion_stock_recognition_score") or raw_metrics.get("opinion_stock_recognition_score"),
            "stock_recognition_label": item.get("opinion_stock_recognition_label") or raw_metrics.get("opinion_stock_recognition_label"),
            "stock_recognition_reason": item.get("opinion_stock_recognition_reason") or raw_metrics.get("opinion_stock_recognition_reason"),
            "sentiment_mode": raw_metrics.get("sentiment_mode"),
            "source_credibility_level": raw_metrics.get("source_credibility_level"),
            "source_credibility_score": raw_metrics.get("source_credibility_score"),
            "source_credibility_reason": raw_metrics.get("source_credibility_reason"),
            "trade_signal_state": raw_metrics.get("trade_signal_state"),
            "trade_signal_label": raw_metrics.get("trade_signal_label"),
            "trade_signal_reason": raw_metrics.get("trade_signal_reason"),
            "trade_grade_state": item.get("trade_grade_state") or raw_metrics.get("trade_grade_state"),
            "trade_grade_label": item.get("trade_grade_label") or raw_metrics.get("trade_grade_label"),
            "trade_grade_reason": item.get("trade_grade_reason") or raw_metrics.get("trade_grade_reason"),
            "theme_trade_slot_state": item.get("theme_trade_slot_state") or raw_metrics.get("theme_trade_slot_state"),
            "daily_trend_state": raw_metrics.get("daily_trend_state"),
            "daily_trend_label": raw_metrics.get("daily_trend_label"),
            "daily_trend_score": factor_scores.get("daily_trend"),
            "chip_structure_state": raw_metrics.get("chip_structure_state"),
            "chip_structure_label": raw_metrics.get("chip_structure_label"),
            "chip_structure_score": factor_scores.get("chip_structure"),
            "market_context_score": factor_scores.get("market_context"),
            "market_context_label": raw_metrics.get("market_context_label"),
            "market_context_reason": raw_metrics.get("market_context_reason"),
            "market_index_trend_score": raw_metrics.get("market_index_trend_score"),
            "market_index_day_score": raw_metrics.get("market_index_day_score"),
            "market_index_pct_chg": raw_metrics.get("market_index_pct_chg"),
            "market_breadth_score": raw_metrics.get("market_breadth_score"),
            "market_volume_score": raw_metrics.get("market_volume_score"),
            "market_index_count": raw_metrics.get("market_index_count"),
            "market_index_codes": raw_metrics.get("market_index_codes"),
            "csi300_pct_chg": raw_metrics.get("csi300_pct_chg"),
            "csi500_pct_chg": raw_metrics.get("csi500_pct_chg"),
            "csi1000_pct_chg": raw_metrics.get("csi1000_pct_chg"),
            "market_theme_tier": raw_metrics.get("market_theme_tier"),
            "market_theme_label": raw_metrics.get("market_theme_label"),
            "market_theme_trend_score": raw_metrics.get("market_theme_trend_score"),
            "market_theme_rank": raw_metrics.get("market_theme_rank"),
            "market_theme_score_delta": raw_metrics.get("market_theme_score_delta"),
            "market_theme_reason": raw_metrics.get("market_theme_reason"),
            "market_theme_fund_flow": raw_metrics.get("market_theme_fund_flow"),
            "deepseek": {
                "score": item.get("deepseek_sentiment_score"),
                "confidence": item.get("deepseek_confidence"),
                "label": item.get("deepseek_label"),
                "summary": item.get("deepseek_summary"),
            } if item.get("deepseek_sentiment_score") is not None or item.get("deepseek_summary") else None,
        }

    @staticmethod
    def _append_unique(values: List[str], value: str) -> List[str]:
        if value and value not in values:
            values.append(value)
        return values

    def finalize_sentiment_results(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Attach risk-controlled trade grades without removing ranked candidates.

        Only the highest-ranked eligible stock in a theme can be marked
        tradable.  Other stocks remain in the result pool as observation-grade
        candidates, preserving the soft-scoring semantics of the strategy.
        """
        if self.strategy_id != "a_share_sentiment":
            return items

        grade_config = self.strategy.config.get("trade_grading", {}) or {}
        max_tradable_per_theme = max(int(grade_config.get("max_tradable_per_theme") or 1), 1)
        required_daily_states = set(grade_config.get("required_daily_states") or ["confirmed", "breakout"])
        accepted_chip_states = set(grade_config.get("accepted_chip_states") or ["supportive", "neutral"])
        require_risk_compliance = bool(grade_config.get("require_risk_control_compliance", True))
        claimed_themes: Dict[str, int] = {}
        finalized: List[Dict[str, Any]] = []
        for index, original in enumerate(items, start=1):
            item = {**original, "rank_no": original.get("rank_no") or index}
            existing_explain = item.get("explain") if isinstance(item.get("explain"), dict) else {}
            strategy_raw_metrics = {
                **(existing_explain.get("raw_metrics") or {}),
                **(item.get("strategy_raw_metrics") or {}),
            }
            trade_plan = item.get("trade_plan") or build_selection_trade_plan(
                item,
                strategy_id="a_share_sentiment",
                raw_metrics=self._build_raw_metrics(item, strategy_raw_metrics),
            )
            if trade_plan:
                item["trade_plan"] = trade_plan

            trade_signal_state = strategy_raw_metrics.get("trade_signal_state")
            daily_trend_state = strategy_raw_metrics.get("daily_trend_state")
            chip_structure_state = strategy_raw_metrics.get("chip_structure_state")
            risk_control = trade_plan.get("risk_control") if isinstance(trade_plan, dict) else {}
            risk_compliant = bool(risk_control and risk_control.get("compliant"))

            eligible = True
            grade_reason = ""
            if trade_signal_state != "tradable":
                eligible = False
                grade_reason = strategy_raw_metrics.get("trade_signal_reason") or "盘中价格与资金尚未形成可交易确认"
            elif daily_trend_state not in required_daily_states:
                eligible = False
                grade_reason = "日线趋势尚未达到确认或突破状态"
            elif chip_structure_state not in accepted_chip_states:
                eligible = False
                grade_reason = (
                    "筹码数据不足，暂不升级为可交易级"
                    if chip_structure_state == "unavailable"
                    else "筹码结构尚未形成有效承接"
                )
            elif require_risk_compliance and not risk_compliant:
                eligible = False
                grade_reason = "交易计划未同时满足约5%止损和第一止盈盈亏比不低于1.2"

            theme_name = str(
                item.get("opinion_sector_name")
                or strategy_raw_metrics.get("opinion_sector_name")
                or ""
            ).strip()
            theme_key = theme_name.casefold() if theme_name else f"__code__:{item.get('code') or index}"
            theme_trade_slot_state = "not_eligible"
            if eligible and claimed_themes.get(theme_key, 0) >= max_tradable_per_theme:
                eligible = False
                theme_trade_slot_state = "duplicate_theme"
                grade_reason = f"同一主题“{theme_name}”已保留排名更高的一只可交易级标的"
            elif eligible:
                claimed_themes[theme_key] = claimed_themes.get(theme_key, 0) + 1
                theme_trade_slot_state = "primary"
                grade_reason = "舆情、盘中确认、日线、筹码和交易计划均满足要求，且为本主题最高排名的合格标的"

            grade_state = "tradable" if eligible else "watch"
            grade_label = "可交易级" if eligible else "观察级"
            item.update(
                {
                    "trade_grade_state": grade_state,
                    "trade_grade_label": grade_label,
                    "trade_grade_reason": grade_reason,
                    "theme_trade_slot_state": theme_trade_slot_state,
                }
            )
            if isinstance(trade_plan, dict):
                trade_plan.update(
                    {
                        "trade_signal_state": trade_signal_state,
                        "trade_signal_label": strategy_raw_metrics.get("trade_signal_label"),
                        "trade_signal_reason": strategy_raw_metrics.get("trade_signal_reason"),
                        "trade_grade_state": grade_state,
                        "trade_grade_label": grade_label,
                        "trade_grade_reason": grade_reason,
                    }
                )
            strategy_raw_metrics.update(
                {
                    "trade_grade_state": grade_state,
                    "trade_grade_label": grade_label,
                    "trade_grade_reason": grade_reason,
                    "theme_trade_slot_state": theme_trade_slot_state,
                    "trade_grade_inputs": {
                        "trade_signal_state": trade_signal_state,
                        "daily_trend_state": daily_trend_state,
                        "chip_structure_state": chip_structure_state,
                        "risk_control_compliant": risk_compliant,
                    },
                }
            )
            item["strategy_raw_metrics"] = strategy_raw_metrics

            reasons = list(item.get("candidate_reasons") or [])
            risks = list(item.get("candidate_risks") or [])
            if eligible:
                self._append_unique(reasons, f"可交易级：{grade_reason}")
            else:
                self._append_unique(risks, f"观察级：{grade_reason}")
            item["candidate_reasons"] = reasons
            item["candidate_risks"] = risks

            refreshed_explain = self._enhance_explain(item)
            item["explain"] = {**existing_explain, **refreshed_explain}
            sentiment_context = self._build_sentiment_context(item, item["explain"])
            if sentiment_context:
                item["sentiment_context"] = sentiment_context
            finalized.append(item)
        return finalized

    def _selection_price_snapshot(self, item: Dict[str, Any], clock_mode: Optional[str] = None) -> Dict[str, Any]:
        clock_mode = clock_mode or self._selection_clock_mode()
        explain = item.get("explain") or {}
        raw_metrics = {
            **(explain.get("raw_metrics") or {}),
            **(item.get("strategy_raw_metrics") or {}),
            **(item.get("factor_scores") or {}),
        }
        preserved_price = item.get("selected_price") or raw_metrics.get("selected_price")
        preserved_price_type = item.get("selected_price_type") or raw_metrics.get("selected_price_type")
        preserved_price_source = item.get("selected_price_source") or raw_metrics.get("selected_price_source")
        if preserved_price is not None and preserved_price_type == "realtime":
            return {
                "selected_price": preserved_price,
                "selected_price_type": "realtime",
                "selected_price_source": preserved_price_source or "stock_realtime_snapshot",
                "selected_price_trade_date": item.get("selected_price_trade_date") or raw_metrics.get("selected_price_trade_date") or item.get("realtime_trade_date") or raw_metrics.get("realtime_trade_date") or datetime.now().strftime("%Y-%m-%d"),
                "selected_price_quote_time": item.get("selected_price_quote_time") or raw_metrics.get("selected_price_quote_time") or item.get("realtime_quote_time") or raw_metrics.get("realtime_quote_time"),
            }
        realtime_price = item.get("realtime_price") or raw_metrics.get("realtime_price")
        realtime_trade_date = item.get("realtime_trade_date") or raw_metrics.get("realtime_trade_date")
        realtime_quote_time = item.get("realtime_quote_time") or raw_metrics.get("realtime_quote_time")
        if clock_mode == "intraday" and realtime_price is not None:
            return {
                "selected_price": realtime_price,
                "selected_price_type": "realtime",
                "selected_price_source": "stock_realtime_snapshot",
                "selected_price_trade_date": realtime_trade_date or datetime.now().strftime("%Y-%m-%d"),
                "selected_price_quote_time": realtime_quote_time,
            }
        return {
            "selected_price": item.get("close"),
            "selected_price_type": "daily_close",
            "selected_price_source": "daily_kline",
            "selected_price_trade_date": item.get("trade_date"),
            "selected_price_quote_time": None,
        }

    @staticmethod
    def _factor_coverage(candidates: List[Dict[str, Any]], field: str) -> Dict[str, Any]:
        total = len(candidates)
        present = len([item for item in candidates if item.get(field) is not None])
        coverage = round((present / total) * 100, 2) if total else None
        missing_rate = round(100 - coverage, 2) if coverage is not None else None
        return {
            "sample_size": total,
            "coverage": coverage,
            "missing_rate": missing_rate,
        }

    def build_factor_analysis(self, instrument_type: str = "stock", limit: int = 200) -> Dict[str, Dict[str, Any]]:
        data_bundle = self.load_candidates_from_mysql(candidate_limit=limit, instrument_type=instrument_type)
        context = self.strategy.prepare_context(data_bundle)
        factor_rows = self.strategy.compute_factors(context)
        factor_keys = sorted({key for item in factor_rows for key in (item.get("factors") or {}).keys()})
        stats: Dict[str, Dict[str, Any]] = {}
        for key in factor_keys:
            total = len(factor_rows)
            values = [float((item.get("factors") or {}).get(key)) for item in factor_rows if (item.get("factors") or {}).get(key) is not None]
            present = len(values)
            coverage = round((present / total) * 100, 2) if total else None
            avg = round(sum(values) / len(values), 4) if values else None
            stats[key] = {
                "sample_size": total,
                "coverage": coverage,
                "missing_rate": round(100 - coverage, 2) if coverage is not None else None,
                "ci": round((avg or 0) / 100, 4) if avg is not None else None,
            }
        return stats

    def apply_global_live_selection_rules(self, scored: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        adjusted: List[Dict[str, Any]] = []
        removed_limit_up = 0
        high_price_adjusted = 0
        low_price_boosted = 0
        allow_limit_up = bool(self.strategy.config.get("allow_limit_up", False))
        for item in scored:
            if item.get("is_limit_up") and not allow_limit_up:
                removed_limit_up += 1
                continue
            already_adjusted = item.get("price_preference_delta_applied") is not None
            delta = 0.0 if already_adjusted else float(item.get("price_preference_delta") or 0)
            ai_score = item.get("deepseek_sentiment_score")
            ai_confidence = item.get("deepseek_confidence")
            if delta < 0 and ai_score is not None and ai_confidence is not None:
                try:
                    if float(ai_score) >= 85 and float(ai_confidence) >= 0.65:
                        delta = delta * 0.35
                except (TypeError, ValueError):
                    pass
            old_score = float(item.get("score") or 0)
            new_score = self._round_score(old_score + delta)
            if delta < 0:
                high_price_adjusted += 1
            elif delta > 0:
                low_price_boosted += 1
            risks = list(item.get("candidate_risks") or [])
            reasons = list(item.get("candidate_reasons") or [])
            reason_text = item.get("price_preference_reason")
            if delta < 0 and reason_text:
                risks.append(reason_text)
            elif delta > 0 and reason_text:
                reasons.append(reason_text)
            adjusted.append({
                **item,
                "score": new_score,
                "base_score_before_price_preference": old_score,
                "price_preference_delta_applied": round(delta, 4),
                "candidate_reasons": reasons,
                "candidate_risks": risks,
            })
        self.last_run_diagnostics["global_live_selection_rules"] = {
            "removed_limit_up": removed_limit_up,
            "high_price_adjusted": high_price_adjusted,
            "low_price_boosted": low_price_boosted,
            "allow_limit_up": allow_limit_up,
            "rule": "limit_up_excluded_and_price_preference_adjusted",
        }
        return sorted(adjusted, key=lambda row: row.get("score", 0), reverse=True)

    def run(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        context = self.strategy.prepare_context(data_bundle)
        self.last_run_diagnostics = {
            key: value
            for key, value in context.items()
            if key.endswith("_summary") or key.endswith("_diagnostics")
        }
        factor_rows = self.strategy.compute_factors(context)
        scored = self.strategy.score(factor_rows)
        scored = self.apply_global_live_selection_rules(scored)
        if hasattr(self.strategy, "score_diagnostics"):
            self.last_run_diagnostics["score_diagnostics"] = self.strategy.score_diagnostics(scored)
        selected = self.strategy.select(scored)
        results = []
        for index, item in enumerate(selected, start=1):
            explain = self._enhance_explain(item)
            enriched = {
                **item,
                "rank_no": index,
                "explain": explain,
                "strategy_id": self.strategy_id,
                "strategy_display_name": self.strategy_meta.get("display_name"),
                "strategy_version": self.strategy_meta.get("version"),
                "run_diagnostics": self.last_run_diagnostics,
            }
            sentiment_context = self._build_sentiment_context(enriched, explain)
            if sentiment_context:
                enriched["sentiment_context"] = sentiment_context
            trade_plan = build_selection_trade_plan(
                enriched,
                strategy_id=enriched.get("strategy_id") or self.strategy_id,
                raw_metrics=self._build_raw_metrics(enriched),
            )
            if trade_plan:
                enriched["trade_plan"] = trade_plan
            results.append(enriched)
        return self.finalize_sentiment_results(results) if self.strategy_id == "a_share_sentiment" else results

    def run_from_mysql(
        self,
        limit: int = 50,
        instrument_type: str = "stock",
        candidate_limit: Optional[int] = None,
        market_board: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        data_bundle = self.load_candidates_from_mysql(candidate_limit=candidate_limit, instrument_type=instrument_type, market_board=market_board)
        return self.run(data_bundle)[:limit]

    def save_selection_results(
        self,
        results: List[Dict[str, Any]],
        run_id: Optional[str] = None,
        trade_date: Optional[str] = None,
    ) -> str:
        if not results:
            return run_id or self.build_run_id()

        final_run_id = run_id or self.build_run_id()
        clock_mode = self._selection_clock_mode()
        price_snapshots = [self._selection_price_snapshot(item, clock_mode=clock_mode) for item in results]
        result_trade_dates = [
            snapshot.get("selected_price_trade_date") or item.get("trade_date")
            for item, snapshot in zip(results, price_snapshots)
            if snapshot.get("selected_price_trade_date") or item.get("trade_date")
        ]
        final_trade_date = trade_date or (result_trade_dates[0] if result_trade_dates else datetime.now().strftime("%Y-%m-%d"))

        payload = []
        for index, (item, price_snapshot) in enumerate(zip(results, price_snapshots), start=1):
            raw_metrics = self._build_raw_metrics(item)
            raw_metrics.update(price_snapshot)
            trade_plan = item.get("trade_plan") or build_selection_trade_plan(
                item,
                strategy_id=item.get("strategy_id") or self.strategy_id,
                raw_metrics=raw_metrics,
            )
            metadata = {
                "name": item.get("name"),
                "instrument_type": item.get("instrument_type"),
                "strategy_display_name": item.get("strategy_display_name") or self.strategy_meta.get("display_name"),
                "strategy_version": item.get("strategy_version") or self.strategy_meta.get("version"),
                "saved_from_run_id": item.get("run_id"),
                "selection_clock_mode": clock_mode,
                **price_snapshot,
                "factors": item.get("factors", {}),
                "explain": item.get("explain", {}),
                "sentiment_context": item.get("sentiment_context"),
                "raw_metrics": raw_metrics,
                "trade_plan": trade_plan,
            }
            payload.append(
                (
                    final_run_id,
                    final_trade_date,
                    item.get("strategy_id") or self.strategy_id,
                    item.get("code"),
                    item.get("score"),
                    item.get("rank_no") or index,
                    json.dumps(metadata, ensure_ascii=False),
                )
            )

        self.repository.save_result_rows(payload=payload, run_id=final_run_id)

        return final_run_id

    def save_single_result(self, item: Dict[str, Any], run_id: Optional[str] = None) -> str:
        return self.save_selection_results([item], run_id=run_id)

    def run_and_save(
        self,
        limit: int = 50,
        instrument_type: str = "stock",
        run_id: Optional[str] = None,
        candidate_limit: Optional[int] = None,
        market_board: Optional[str] = None,
    ) -> Dict[str, Any]:
        results = self.run_from_mysql(limit=limit, instrument_type=instrument_type, candidate_limit=candidate_limit, market_board=market_board)
        saved_run_id = self.save_selection_results(results, run_id=run_id)
        return {
            "run_id": saved_run_id,
            "strategy_id": self.strategy_id,
            "strategy_display_name": self.strategy_meta.get("display_name"),
            "strategy_version": self.strategy_meta.get("version"),
            "score_threshold": self.strategy.config.get("score_threshold"),
            "count": len(results),
            "diagnostics": self.last_run_diagnostics,
            "results": results,
        }


if __name__ == "__main__":
    raise SystemExit(
        "Direct synchronous selection is disabled; use "
        "`python -m app.stock_selection.run_selection` to submit a worker task."
    )
