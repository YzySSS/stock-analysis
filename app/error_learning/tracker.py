from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from app.error_learning.models import SelectionTrackingRecord
from app.shared.db import mysql_conn


class SelectionResultTracker:
    def build_latest_selection_snapshot(
        self,
        limit: int = 20,
        instrument_type: str = "stock",
        run_id: Optional[str] = None,
    ) -> List[SelectionTrackingRecord]:
        rows = self._fetch_from_selection_result(limit=limit, instrument_type=instrument_type, run_id=run_id)
        if rows:
            return [self._build_record_from_selection_result(row) for row in rows]
        rows = self._fetch_from_stock_snapshot(limit=limit, instrument_type=instrument_type)
        return [self._build_record_from_snapshot(row) for row in rows]

    def _fetch_from_selection_result(
        self,
        limit: int,
        instrument_type: str,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            sr.run_id,
            sr.rank_no,
            sr.trade_date AS selection_date,
            sr.strategy_id,
            sr.code,
            sr.score,
            sr.metadata_json,
            sb.name,
            sb.instrument_type,
            selected_dk.open AS selected_open_price,
            selected_dk.close AS selected_close_price,
            latest_dk.trade_date AS latest_trade_date,
            latest_dk.close AS current_price
        FROM selection_result sr
        INNER JOIN stock_basic sb ON sr.code = sb.code
        LEFT JOIN daily_kline selected_dk ON sr.code = selected_dk.code AND sr.trade_date = selected_dk.trade_date
        LEFT JOIN (
            SELECT d1.code, d1.trade_date, d1.close
            FROM daily_kline d1
            INNER JOIN (
                SELECT code, MAX(trade_date) AS max_date
                FROM daily_kline
                GROUP BY code
            ) d2 ON d1.code = d2.code AND d1.trade_date = d2.max_date
        ) latest_dk ON sr.code = latest_dk.code
        WHERE sb.instrument_type = %s
        """
        params: List[Any] = [instrument_type]
        if run_id:
            sql += " AND sr.run_id = %s"
            params.append(run_id)
        else:
            sql += " AND sr.run_id = (SELECT run_id FROM selection_result ORDER BY created_at DESC LIMIT 1)"
        sql += " ORDER BY sr.rank_no ASC, sr.id ASC LIMIT %s"
        params.append(limit)

        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()

    def _fetch_from_stock_snapshot(self, limit: int, instrument_type: str) -> List[Dict[str, Any]]:
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
            dk.close AS selected_close_price,
            dk.trade_date AS latest_trade_date,
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
                return cursor.fetchall()

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        return value if value == value else None

    def _build_record_from_selection_result(self, row: Dict[str, Any]) -> SelectionTrackingRecord:
        selected_open_price = self._to_float(row.get("selected_open_price"))
        selected_close_price = self._to_float(row.get("selected_close_price"))
        current_price = self._to_float(row.get("current_price"))
        latest_trade_date = str(row["latest_trade_date"]) if row.get("latest_trade_date") else None
        base_price = selected_close_price or selected_open_price
        price_change_pct = None
        if base_price and current_price:
            price_change_pct = round((current_price - base_price) / base_price * 100, 2)

        metadata = row.get("metadata_json")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        metadata = metadata or {}
        explain = metadata.get("explain", {}) or {}
        raw_metrics = metadata.get("raw_metrics", {})
        factor_scores = {
            **raw_metrics,
            **metadata.get("factors", {}),
        }

        return SelectionTrackingRecord(
            run_id=row.get("run_id"),
            rank_no=row.get("rank_no"),
            code=row["code"],
            name=row["name"],
            selection_date=str(row["selection_date"]) if row.get("selection_date") else "",
            strategy_id=row.get("strategy_id") or "",
            strategy_display_name=metadata.get("strategy_display_name"),
            strategy_version=metadata.get("strategy_version"),
            score=self._to_float(row.get("score")),
            factor_scores=factor_scores,
            selected_open_price=selected_open_price,
            selected_close_price=selected_close_price,
            current_price=current_price,
            latest_trade_date=latest_trade_date,
            price_change_pct=price_change_pct,
            reason_summary=explain.get("reasons") or [],
            risk_summary=explain.get("risks") or [],
        )

    def _build_record_from_snapshot(self, row: Dict[str, Any]) -> SelectionTrackingRecord:
        selected_open_price = self._to_float(row.get("selected_open_price"))
        selected_close_price = self._to_float(row.get("selected_close_price"))
        current_price = self._to_float(row.get("current_price"))
        latest_trade_date = str(row["latest_trade_date"]) if row.get("latest_trade_date") else None
        base_price = selected_close_price or selected_open_price
        price_change_pct = None
        if base_price and current_price:
            price_change_pct = round((current_price - base_price) / base_price * 100, 2)

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
            strategy_display_name="低波动反转策略",
            strategy_version="v1",
            score=None,
            factor_scores=factor_scores,
            selected_open_price=selected_open_price,
            selected_close_price=selected_close_price,
            current_price=current_price,
            latest_trade_date=latest_trade_date,
            price_change_pct=price_change_pct,
            reason_summary=[],
            risk_summary=[],
        )

    def to_dict_list(self, records: List[SelectionTrackingRecord]) -> List[Dict[str, Any]]:
        return [
            {
                "run_id": item.run_id,
                "rank_no": item.rank_no,
                "code": item.code,
                "name": item.name,
                "selection_date": item.selection_date,
                "strategy_id": item.strategy_id,
                "score": item.score,
                "strategy_display_name": item.strategy_display_name,
                "strategy_version": item.strategy_version,
                "factor_scores": item.factor_scores,
                "selected_open_price": item.selected_open_price,
                "selected_close_price": item.selected_close_price,
                "current_price": item.current_price,
                "latest_trade_date": item.latest_trade_date,
                "price_change_pct": item.price_change_pct,
                "reason_summary": item.reason_summary or [],
                "risk_summary": item.risk_summary or [],
            }
            for item in records
        ]
