from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.shared.db import mysql_conn
from app.shared.strategy_loader import StrategyLoader


class StockSelector:
    def __init__(self, strategy_id: Optional[str] = None, strategy_overrides: Optional[Dict[str, Any]] = None):
        self.loader = StrategyLoader()
        self.strategy_id = strategy_id or self.loader.get_default_strategy_id()
        self.strategy_meta = self.loader.get_strategy_meta(self.strategy_id)
        self.strategy = self.loader.load_strategy(self.strategy_id)
        self.strategy_overrides = strategy_overrides or {}
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
        close = float(row["close"]) if row.get("close") is not None else None
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
            "trade_date": str(row["trade_date"]) if row.get("trade_date") else None,
            "close": close,
            "pe_tushare": pe,
            "pb_tushare": pb,
            "roe": roe,
            "roa": roa,
            "grossprofit_margin": grossprofit_margin,
            "netprofit_margin": netprofit_margin,
            "revenue_yoy": revenue_yoy,
            "profit_yoy": profit_yoy,
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

    def load_candidates_from_mysql(self, limit: int = 50, instrument_type: str = "stock") -> Dict[str, Any]:
        sql = """
        SELECT
            sb.code,
            sb.name,
            sb.instrument_type,
            sb.pe_tushare,
            sb.pb_tushare,
            sb.roe,
            sb.roa,
            sb.grossprofit_margin,
            sb.netprofit_margin,
            sb.revenue_yoy,
            sb.profit_yoy,
            dk.close,
            dk.trade_date
        FROM stock_basic sb
        LEFT JOIN (
            SELECT d1.code, d1.trade_date, d1.close
            FROM daily_kline d1
            INNER JOIN (
                SELECT code, MAX(trade_date) AS max_date
                FROM daily_kline
                GROUP BY code
            ) d2 ON d1.code = d2.code AND d1.trade_date = d2.max_date
        ) dk ON sb.code = dk.code
        WHERE sb.is_delisted = 0
          AND sb.instrument_type = %s
        ORDER BY (dk.trade_date IS NULL), dk.trade_date DESC, sb.code
        LIMIT %s
        """
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (instrument_type, limit))
                rows = cursor.fetchall()

        candidates = [self._build_candidate(row) for row in rows]
        return {"candidates": candidates}

    def _enhance_explain(self, item: Dict[str, Any]) -> Dict[str, Any]:
        base_explain = self.strategy.explain(item)
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
                "close": item.get("close"),
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
        data_bundle = self.load_candidates_from_mysql(limit=limit, instrument_type=instrument_type)
        candidates = data_bundle.get("candidates", [])
        stats = {
            "turnover": self._factor_coverage(candidates, "turnover_score"),
            "lowvol": self._factor_coverage(candidates, "lowvol_score"),
            "reversal": self._factor_coverage(candidates, "reversal_score"),
        }
        ci_mapping = {
            "turnover": "turnover_score",
            "lowvol": "lowvol_score",
            "reversal": "reversal_score",
        }
        for key, field in ci_mapping.items():
            values = [float(item.get(field)) for item in candidates if item.get(field) is not None]
            avg = round(sum(values) / len(values), 4) if values else None
            if avg is not None:
                stats[key]["ci"] = round(avg - 0.5, 4)
            else:
                stats[key]["ci"] = None
        return stats

    def run(self, data_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        context = self.strategy.prepare_context(data_bundle)
        factor_rows = self.strategy.compute_factors(context)
        scored = self.strategy.score(factor_rows)
        selected = self.strategy.select(scored)
        return [
            {
                **item,
                "explain": self._enhance_explain(item),
                "strategy_id": self.strategy_id,
                "strategy_display_name": self.strategy_meta.get("display_name"),
                "strategy_version": self.strategy_meta.get("version"),
            }
            for item in selected
        ]

    def run_from_mysql(self, limit: int = 50, instrument_type: str = "stock") -> List[Dict[str, Any]]:
        data_bundle = self.load_candidates_from_mysql(limit=limit, instrument_type=instrument_type)
        return self.run(data_bundle)

    def save_selection_results(
        self,
        results: List[Dict[str, Any]],
        run_id: Optional[str] = None,
        trade_date: Optional[str] = None,
    ) -> str:
        if not results:
            return run_id or self.build_run_id()

        final_run_id = run_id or self.build_run_id()
        final_trade_date = trade_date or results[0].get("trade_date") or datetime.now().strftime("%Y-%m-%d")
        sql = """
        INSERT INTO selection_result (
            run_id, trade_date, strategy_id, code, score, rank_no, metadata_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            trade_date = VALUES(trade_date),
            strategy_id = VALUES(strategy_id),
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
                "factors": item.get("factors", {}),
                "explain": item.get("explain", {}),
                "raw_metrics": {
                    "close": item.get("close"),
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
                    index,
                    json.dumps(metadata, ensure_ascii=False),
                )
            )

        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, payload)

        return final_run_id

    def run_and_save(self, limit: int = 50, instrument_type: str = "stock", run_id: Optional[str] = None) -> Dict[str, Any]:
        results = self.run_from_mysql(limit=limit, instrument_type=instrument_type)
        saved_run_id = self.save_selection_results(results, run_id=run_id)
        return {
            "run_id": saved_run_id,
            "strategy_id": self.strategy_id,
            "strategy_display_name": self.strategy_meta.get("display_name"),
            "strategy_version": self.strategy_meta.get("version"),
            "score_threshold": self.strategy.config.get("score_threshold"),
            "count": len(results),
            "results": results,
        }


if __name__ == "__main__":
    selector = StockSelector()
    results = selector.run_from_mysql(limit=20, instrument_type="stock")
    for item in results:
        print(item)
