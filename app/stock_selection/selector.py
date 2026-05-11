from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.shared.db import mysql_conn
from app.shared.strategy_loader import StrategyLoader
from app.orchestration.market_sentiment_schema import ensure_market_sentiment_schema


class StockSelector:
    def __init__(self, strategy_id: Optional[str] = None, strategy_overrides: Optional[Dict[str, Any]] = None):
        self.loader = StrategyLoader()
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

    @staticmethod
    def _round_score(value: float) -> float:
        return round(max(0.0, min(value, 100.0)), 2)

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
        ma20 = float(row["ma20"]) if row.get("ma20") is not None else None
        ma60 = float(row["ma60"]) if row.get("ma60") is not None else None
        close_5d = float(row["close_5d"]) if row.get("close_5d") is not None else None
        close_20d = float(row["close_20d"]) if row.get("close_20d") is not None else None
        prev_close_1d = float(row["prev_close_1d"]) if row.get("prev_close_1d") is not None else None
        max_close_20 = float(row["max_close_20"]) if row.get("max_close_20") is not None else None
        min_close_20 = float(row["min_close_20"]) if row.get("min_close_20") is not None else None
        avg_amount_20 = float(row["avg_amount_20"]) if row.get("avg_amount_20") is not None else None
        kline_count_20 = int(row.get("kline_count_20") or 0)
        kline_count_60 = int(row.get("kline_count_60") or 0)
        std_return_20 = float(row["std_return_20"]) if row.get("std_return_20") is not None else None
        pct_chg_1d = float(row["pct_chg_1d"]) if row.get("pct_chg_1d") is not None else None
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
            missing_fields.append("pe_tushare")
            risks.append("缺少 PE 数据")

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
            "instrument_type": row.get("instrument_type"),
            "is_st": bool(row.get("is_st")),
            "trade_date": str(row["trade_date"]) if row.get("trade_date") else None,
            "close": close,
            "amount": amount,
            "ma20": ma20,
            "ma60": ma60,
            "close_5d": close_5d,
            "close_20d": close_20d,
            "prev_close_1d": prev_close_1d,
            "max_close_20": max_close_20,
            "min_close_20": min_close_20,
            "avg_amount_20": avg_amount_20,
            "kline_count_20": kline_count_20,
            "kline_count_60": kline_count_60,
            "std_return_20": std_return_20,
            "pct_chg_1d": pct_chg_1d,
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

    def load_candidates_from_mysql(
        self,
        candidate_limit: Optional[int] = None,
        instrument_type: str = "stock",
    ) -> Dict[str, Any]:
        ensure_market_sentiment_schema()
        sql = """
        SELECT
            sb.code,
            sb.name,
            sb.instrument_type,
            sb.is_st,
            sb.pe_tushare,
            sb.pb_tushare,
            sb.roe,
            sb.roa,
            sb.grossprofit_margin,
            sb.netprofit_margin,
            sb.revenue_yoy,
            sb.profit_yoy,
            sb.eps,
            dk.close,
            dk.amount,
            dk.trade_date,
            COALESCE(lf.ma20, ma.ma20) AS ma20,
            lf.ma60,
            lf.close_5d,
            COALESCE(lf.close_20d, ma.close_20d) AS close_20d,
            lf.prev_close_1d,
            COALESCE(lf.max_close_20, ma.max_close_20) AS max_close_20,
            COALESCE(lf.min_close_20, ma.min_close_20) AS min_close_20,
            COALESCE(lf.avg_amount_20, ma.avg_amount_20) AS avg_amount_20,
            COALESCE(lf.kline_count_20, ma.kline_count_20) AS kline_count_20,
            lf.kline_count_60,
            lf.std_return_20,
            lf.pct_chg_1d,
            fid.turnover_rate,
            lf.turnover_rate_5d_avg,
            DATEDIFF(dk.trade_date, sb.listing_date) AS listed_days,
            fid.volume_ratio,
            fid.total_mv,
            fid.completeness_score,
            mf.net_mf_amount,
            mf.net_mf_vol,
            mf.buy_lg_amount,
            mf.sell_lg_amount,
            mf.buy_elg_amount,
            mf.sell_elg_amount,
            chip.his_low AS chip_his_low,
            chip.his_high AS chip_his_high,
            chip.cost_5pct AS chip_cost_5pct,
            chip.cost_15pct AS chip_cost_15pct,
            chip.cost_50pct AS chip_cost_50pct,
            chip.cost_85pct AS chip_cost_85pct,
            chip.cost_95pct AS chip_cost_95pct,
            chip.weight_avg AS chip_weight_avg,
            chip.winner_rate AS chip_winner_rate,
            ssd.sentiment_score,
            ssd.news_count,
            mcd.market_strength,
            mcd.market_state
        FROM stock_basic sb
        LEFT JOIN (
            SELECT d1.code, d1.trade_date, d1.close, d1.amount
            FROM daily_kline d1
            INNER JOIN (
                SELECT code, MAX(trade_date) AS max_date
                FROM daily_kline
                GROUP BY code
            ) d2 ON d1.code = d2.code AND d1.trade_date = d2.max_date
        ) dk ON sb.code = dk.code
        LEFT JOIN (
            SELECT
                code,
                AVG(close) AS ma20,
                MAX(CASE WHEN rn = 20 THEN close END) AS close_20d,
                MAX(close) AS max_close_20,
                MIN(close) AS min_close_20,
                AVG(amount) AS avg_amount_20,
                COUNT(*) AS kline_count_20
            FROM (
                SELECT
                    code,
                    trade_date,
                    close,
                    amount,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
                FROM daily_kline
                WHERE trade_date >= (
                    SELECT MIN(trade_date)
                    FROM (
                        SELECT DISTINCT trade_date
                        FROM daily_kline
                        ORDER BY trade_date DESC
                        LIMIT 30
                    ) recent_trade_dates
                )
            ) ranked
            WHERE rn <= 20
            GROUP BY code
        ) ma ON sb.code = ma.code
        LEFT JOIN lowvol_reversal_feature_daily lf ON lf.code = sb.code AND lf.trade_date = dk.trade_date
        LEFT JOIN factor_input_daily fid ON fid.code = sb.code AND fid.trade_date = dk.trade_date
        LEFT JOIN stock_moneyflow_daily mf ON mf.code = sb.code AND mf.trade_date = dk.trade_date
        LEFT JOIN stock_chip_daily chip ON chip.code = sb.code AND chip.trade_date = dk.trade_date
        LEFT JOIN stock_sentiment_daily ssd ON sb.code = ssd.code AND dk.trade_date = ssd.trade_date
        LEFT JOIN market_context_daily mcd ON dk.trade_date = mcd.trade_date AND mcd.index_code = '000300.SH'
        WHERE sb.is_delisted = 0
          AND sb.instrument_type = %s
        ORDER BY (dk.trade_date IS NULL), dk.trade_date DESC, sb.code
        """
        params = [instrument_type]
        if candidate_limit:
            sql += " LIMIT %s"
            params.append(int(candidate_limit))
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()

        candidates = [self._build_candidate(row) for row in rows]
        return {"candidates": candidates}

    def _enhance_explain(self, item: Dict[str, Any]) -> Dict[str, Any]:
        base_explain = self.strategy.explain(item)
        strategy_raw_metrics = base_explain.get("raw_metrics") or {}
        if not strategy_raw_metrics and item.get("raw_lowvol_reversal_metrics"):
            strategy_raw_metrics = item.get("raw_lowvol_reversal_metrics") or {}
        common_raw_metrics = {
            "close": item.get("close"),
            "ma20": item.get("ma20"),
            "ma60": item.get("ma60"),
            "close_5d": item.get("close_5d"),
            "close_20d": item.get("close_20d"),
            "prev_close_1d": item.get("prev_close_1d"),
            "max_close_20": item.get("max_close_20"),
            "min_close_20": item.get("min_close_20"),
            "avg_amount_20": item.get("avg_amount_20"),
            "kline_count_20": item.get("kline_count_20"),
            "kline_count_60": item.get("kline_count_60"),
            "std_return_20": item.get("std_return_20"),
            "pct_chg_1d": item.get("pct_chg_1d"),
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
            "trade_date": item.get("trade_date"),
        }
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
            "raw_metrics": {
                **common_raw_metrics,
                **strategy_raw_metrics,
            },
            "fundamental_context": item.get("fundamental_context", {}),
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

    def run(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        context = self.strategy.prepare_context(data_bundle)
        self.last_run_diagnostics = {
            key: value
            for key, value in context.items()
            if key.endswith("_summary") or key.endswith("_diagnostics")
        }
        factor_rows = self.strategy.compute_factors(context)
        scored = self.strategy.score(factor_rows)
        if hasattr(self.strategy, "score_diagnostics"):
            self.last_run_diagnostics["score_diagnostics"] = self.strategy.score_diagnostics(scored)
        selected = self.strategy.select(scored)
        return [
            {
                **item,
                "rank_no": index,
                "explain": self._enhance_explain(item),
                "strategy_id": self.strategy_id,
                "strategy_display_name": self.strategy_meta.get("display_name"),
                "strategy_version": self.strategy_meta.get("version"),
                "run_diagnostics": self.last_run_diagnostics,
            }
            for index, item in enumerate(selected, start=1)
        ]

    def run_from_mysql(
        self,
        limit: int = 50,
        instrument_type: str = "stock",
        candidate_limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        data_bundle = self.load_candidates_from_mysql(candidate_limit=candidate_limit, instrument_type=instrument_type)
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
        result_trade_dates = [item.get("trade_date") for item in results if item.get("trade_date")]
        final_trade_date = trade_date or (result_trade_dates[0] if result_trade_dates else datetime.now().strftime("%Y-%m-%d"))
        sql = """
        INSERT INTO selection_result (
            run_id, trade_date, strategy_id, code, score, rank_no, metadata_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            run_id = VALUES(run_id),
            score = VALUES(score),
            rank_no = VALUES(rank_no),
            metadata_json = VALUES(metadata_json)
        """

        payload = []
        for index, item in enumerate(results, start=1):
            metadata = {
                "name": item.get("name"),
                "instrument_type": item.get("instrument_type"),
                "strategy_display_name": item.get("strategy_display_name") or self.strategy_meta.get("display_name"),
                "strategy_version": item.get("strategy_version") or self.strategy_meta.get("version"),
                "saved_from_run_id": item.get("run_id"),
                "factors": item.get("factors", {}),
                "explain": item.get("explain", {}),
                "raw_metrics": {
                    "open": item.get("open"),
                    "close": item.get("close"),
                    "ma20": item.get("ma20"),
                    "ma60": item.get("ma60"),
                    "close_5d": item.get("close_5d"),
                    "close_20d": item.get("close_20d"),
                    "prev_close_1d": item.get("prev_close_1d"),
                    "max_close_20": item.get("max_close_20"),
                    "min_close_20": item.get("min_close_20"),
                    "avg_amount_20": item.get("avg_amount_20"),
                    "kline_count_20": item.get("kline_count_20"),
                    "kline_count_60": item.get("kline_count_60"),
                    "std_return_20": item.get("std_return_20"),
                    "pct_chg_1d": item.get("pct_chg_1d"),
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
                    "pb_tushare": item.get("pb_tushare"),
                    "roe": item.get("roe"),
                    "roa": item.get("roa"),
                    "grossprofit_margin": item.get("grossprofit_margin"),
                    "netprofit_margin": item.get("netprofit_margin"),
                    "revenue_yoy": item.get("revenue_yoy"),
                    "profit_yoy": item.get("profit_yoy"),
                    "trade_date": item.get("trade_date"),
                },
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

        dedupe_sql = """
        DELETE newer
        FROM selection_result newer
        INNER JOIN selection_result older
          ON newer.trade_date = older.trade_date
         AND newer.strategy_id = older.strategy_id
         AND newer.code = older.code
         AND newer.id > older.id
        """

        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, payload)
                cursor.execute(dedupe_sql)

        return final_run_id

    def save_single_result(self, item: Dict[str, Any], run_id: Optional[str] = None) -> str:
        return self.save_selection_results([item], run_id=run_id, trade_date=item.get("trade_date"))

    def run_and_save(
        self,
        limit: int = 50,
        instrument_type: str = "stock",
        run_id: Optional[str] = None,
        candidate_limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        results = self.run_from_mysql(limit=limit, instrument_type=instrument_type, candidate_limit=candidate_limit)
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
    selector = StockSelector()
    results = selector.run_from_mysql(limit=20, instrument_type="stock")
    for item in results:
        print(item)
