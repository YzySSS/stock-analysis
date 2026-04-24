from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.shared.db import mysql_conn
from app.shared.strategy_loader import StrategyLoader


class StockSelector:
    def __init__(self, strategy_id: Optional[str] = None):
        self.loader = StrategyLoader()
        self.strategy_id = strategy_id or self.loader.get_default_strategy_id()
        self.strategy = self.loader.load_strategy(self.strategy_id)

    @staticmethod
    def _round_score(value: float) -> float:
        return round(max(0.0, min(value, 1.0)), 4)

    def _build_candidate(self, row: Dict[str, Any]) -> Dict[str, Any]:
        pe = float(row["pe_tushare"]) if row.get("pe_tushare") is not None else None
        pb = float(row["pb_tushare"]) if row.get("pb_tushare") is not None else None
        roe = float(row["roe"]) if row.get("roe") is not None else None
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
        reversal_score = self._round_score(0.35 + quality_score * 0.25 + value_score * 0.15)
        turnover_score = self._round_score(0.30 + stability_score * 0.30 + data_quality_score * 0.20)
        lowvol_score = self._round_score(0.30 + value_score * 0.35 + stability_score * 0.20)

        return {
            "code": row["code"],
            "name": row["name"],
            "instrument_type": row.get("instrument_type"),
            "trade_date": str(row["trade_date"]) if row.get("trade_date") else None,
            "close": close,
            "pe_tushare": pe,
            "pb_tushare": pb,
            "roe": roe,
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
                "trade_date": item.get("trade_date"),
            },
        }

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
            }
            for item in selected
        ]

    def run_from_mysql(self, limit: int = 50, instrument_type: str = "stock") -> List[Dict[str, Any]]:
        data_bundle = self.load_candidates_from_mysql(limit=limit, instrument_type=instrument_type)
        return self.run(data_bundle)


if __name__ == "__main__":
    selector = StockSelector()
    results = selector.run_from_mysql(limit=20, instrument_type="stock")
    for item in results:
        print(item)
