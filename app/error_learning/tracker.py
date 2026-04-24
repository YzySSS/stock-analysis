from __future__ import annotations

from typing import Any, Dict, List

from app.error_learning.models import SelectionTrackingRecord
from app.shared.db import mysql_conn


class SelectionResultTracker:
    def build_latest_selection_snapshot(self, limit: int = 20, instrument_type: str = "stock") -> List[SelectionTrackingRecord]:
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
            dk.trade_date AS selection_date,
            dk.open AS selected_open_price,
            dk.close AS current_price
        FROM stock_basic sb
        LEFT JOIN (
            SELECT d1.code, d1.trade_date, d1.open, d1.close
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

        return [self._build_record(row) for row in rows]

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        return value if value == value else None

    def _build_record(self, row: Dict[str, Any]) -> SelectionTrackingRecord:
        selected_open_price = self._to_float(row.get("selected_open_price"))
        current_price = self._to_float(row.get("current_price"))
        price_change_pct = None
        if selected_open_price and current_price:
            price_change_pct = round((current_price - selected_open_price) / selected_open_price * 100, 2)

        factor_scores = {
            "pe_tushare": self._to_float(row.get("pe_tushare")),
            "pb_tushare": self._to_float(row.get("pb_tushare")),
            "roe": self._to_float(row.get("roe")),
            "roa": self._to_float(row.get("roa")),
            "grossprofit_margin": self._to_float(row.get("grossprofit_margin")),
            "netprofit_margin": self._to_float(row.get("netprofit_margin")),
            "revenue_yoy": self._to_float(row.get("revenue_yoy")),
            "profit_yoy": self._to_float(row.get("profit_yoy")),
        }

        return SelectionTrackingRecord(
            code=row["code"],
            name=row["name"],
            selection_date=str(row["selection_date"]) if row.get("selection_date") else "",
            strategy_id="lowvol_reversal",
            score=None,
            factor_scores=factor_scores,
            selected_open_price=selected_open_price,
            current_price=current_price,
            price_change_pct=price_change_pct,
        )

    def to_dict_list(self, records: List[SelectionTrackingRecord]) -> List[Dict[str, Any]]:
        return [
            {
                "code": item.code,
                "name": item.name,
                "selection_date": item.selection_date,
                "strategy_id": item.strategy_id,
                "score": item.score,
                "factor_scores": item.factor_scores,
                "selected_open_price": item.selected_open_price,
                "current_price": item.current_price,
                "price_change_pct": item.price_change_pct,
            }
            for item in records
        ]
