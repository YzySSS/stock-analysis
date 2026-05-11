from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from app.error_learning.models import SelectionTrackingRecord
from app.shared.db import mysql_conn


class SelectionResultTracker:
    def build_latest_selection_snapshot(
        self,
        limit: int = 20,
        instrument_type: str = "stock",
        run_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        selection_date: Optional[str] = None,
        offset: int = 0,
        latest_only: bool = True,
    ) -> List[SelectionTrackingRecord]:
        rows = self._fetch_from_selection_result(
            limit=limit,
            instrument_type=instrument_type,
            run_id=run_id,
            strategy_id=strategy_id,
            selection_date=selection_date,
            offset=offset,
            latest_only=latest_only,
        )
        if rows:
            return [self._build_record_from_selection_result(row) for row in rows]
        rows = self._fetch_from_stock_snapshot(limit=limit, instrument_type=instrument_type)
        return [self._build_record_from_snapshot(row) for row in rows]

    def _fetch_from_selection_result(
        self,
        limit: int,
        instrument_type: str,
        run_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        selection_date: Optional[str] = None,
        offset: int = 0,
        latest_only: bool = True,
    ) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            sr.run_id,
            sr.run_id AS latest_run_id,
            sr.rank_no,
            sr.trade_date AS selection_date,
            sr.strategy_id,
            sr.code,
            sr.score,
            COALESCE(sr.include_in_stats, 1) AS include_in_stats,
            sr.metadata_json,
            sb.name,
            sb.industry,
            sb.instrument_type,
            selected_dk.open AS selected_open_price,
            selected_dk.close AS selected_close_price,
            period_dk.max_high AS period_max_high,
            period_dk.min_low AS period_min_low,
            period_dk.trade_day_count AS trade_day_count,
            COALESCE(metadata_selected_dk.trade_date, latest_dk.trade_date) AS metric_trade_date,
            latest_dk.trade_date AS latest_trade_date,
            latest_dk.close AS daily_current_price,
            realtime.latest_price AS realtime_price,
            realtime.pct_chg AS realtime_pct_chg,
            realtime.quote_time AS realtime_quote_time,
            realtime.trade_date AS realtime_trade_date,
            realtime.high_price AS realtime_high_price,
            realtime.low_price AS realtime_low_price,
            COALESCE(realtime.latest_price, latest_dk.close) AS current_price
        FROM selection_result sr
        INNER JOIN stock_basic sb ON sr.code = sb.code
        LEFT JOIN daily_kline selected_dk ON sr.code = selected_dk.code AND sr.trade_date = selected_dk.trade_date
        LEFT JOIN daily_kline metadata_selected_dk
          ON sr.code = metadata_selected_dk.code
         AND metadata_selected_dk.trade_date = CASE
              WHEN JSON_UNQUOTE(JSON_EXTRACT(sr.metadata_json, '$.raw_metrics.trade_date')) IN ('', 'null') THEN NULL
              ELSE STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(sr.metadata_json, '$.raw_metrics.trade_date')), '%%Y-%%m-%%d')
             END
        LEFT JOIN (
            SELECT d1.code, d1.trade_date, d1.close
            FROM daily_kline d1
            INNER JOIN (
                SELECT code, MAX(trade_date) AS max_date
                FROM daily_kline
                GROUP BY code
            ) d2 ON d1.code = d2.code AND d1.trade_date = d2.max_date
        ) latest_dk ON sr.code = latest_dk.code
        LEFT JOIN stock_realtime_snapshot realtime ON sr.code = realtime.code
        LEFT JOIN (
            SELECT
                sr_inner.id AS selection_result_id,
                MAX(dk.high) AS max_high,
                MIN(dk.low) AS min_low,
                GREATEST(COUNT(DISTINCT dk.trade_date) - 1, 0) AS trade_day_count
            FROM selection_result sr_inner
            INNER JOIN daily_kline dk
              ON dk.code = sr_inner.code
             AND dk.trade_date BETWEEN sr_inner.trade_date AND (SELECT MAX(trade_date) FROM daily_kline)
            GROUP BY sr_inner.id
        ) period_dk ON sr.id = period_dk.selection_result_id
        WHERE sb.instrument_type = %s
        """
        params: List[Any] = [instrument_type]
        if run_id:
            sql += " AND sr.run_id = %s"
            params.append(run_id)
            sql += " ORDER BY sr.rank_no ASC, sr.id ASC LIMIT %s"
            params.append(limit)
        elif selection_date:
            sql += " AND sr.trade_date = %s"
            params.append(selection_date)
            if strategy_id:
                sql += " AND sr.strategy_id = %s"
                params.append(strategy_id)
            sql += " ORDER BY sr.trade_date DESC, sr.rank_no ASC, sr.id DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
        elif not latest_only:
            if strategy_id:
                sql += " AND sr.strategy_id = %s"
                params.append(strategy_id)
            sql += " ORDER BY sr.trade_date DESC, sr.rank_no ASC, sr.id DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
        else:
            latest_trade_date_sql = "SELECT MAX(sr2.trade_date) FROM selection_result sr2 INNER JOIN stock_basic sb2 ON sr2.code = sb2.code WHERE sb2.instrument_type = %s"
            latest_params: List[Any] = [instrument_type]
            if strategy_id:
                latest_trade_date_sql += " AND sr2.strategy_id = %s"
                latest_params.append(strategy_id)
            sql = """
            SELECT
                sr.run_id,
                sr.run_id AS latest_run_id,
                sr.rank_no,
                sr.trade_date AS selection_date,
                sr.strategy_id,
                sr.code,
                sr.score,
                COALESCE(sr.include_in_stats, 1) AS include_in_stats,
                sr.metadata_json,
                sb.name,
                sb.industry,
                sb.instrument_type,
                selected_dk.open AS selected_open_price,
                selected_dk.close AS selected_close_price,
                period_dk.max_high AS period_max_high,
                period_dk.min_low AS period_min_low,
                period_dk.trade_day_count AS trade_day_count,
                COALESCE(metadata_selected_dk.trade_date, latest_dk.trade_date) AS metric_trade_date,
                latest_dk.trade_date AS latest_trade_date,
                latest_dk.close AS daily_current_price,
                realtime.latest_price AS realtime_price,
                realtime.pct_chg AS realtime_pct_chg,
                realtime.quote_time AS realtime_quote_time,
                realtime.trade_date AS realtime_trade_date,
                realtime.high_price AS realtime_high_price,
                realtime.low_price AS realtime_low_price,
                COALESCE(realtime.latest_price, latest_dk.close) AS current_price
            FROM selection_result sr
            INNER JOIN (
                SELECT code, trade_date, strategy_id, MAX(id) AS max_id
                FROM selection_result
                WHERE trade_date = (""" + latest_trade_date_sql + """)
            """
            params = []
            params.extend(latest_params)
            if strategy_id:
                sql += " AND strategy_id = %s"
                params.append(strategy_id)
            sql += " GROUP BY code, trade_date, strategy_id ) latest_sr ON sr.id = latest_sr.max_id "
            sql += " INNER JOIN stock_basic sb ON sr.code = sb.code "
            sql += " LEFT JOIN daily_kline selected_dk ON sr.code = selected_dk.code AND sr.trade_date = selected_dk.trade_date "
            sql += " LEFT JOIN daily_kline metadata_selected_dk ON sr.code = metadata_selected_dk.code AND metadata_selected_dk.trade_date = CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(sr.metadata_json, '$.raw_metrics.trade_date')) IN ('', 'null') THEN NULL ELSE STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(sr.metadata_json, '$.raw_metrics.trade_date')), '%%Y-%%m-%%d') END "
            sql += " LEFT JOIN ( SELECT d1.code, d1.trade_date, d1.close FROM daily_kline d1 INNER JOIN ( SELECT code, MAX(trade_date) AS max_date FROM daily_kline GROUP BY code ) d2 ON d1.code = d2.code AND d1.trade_date = d2.max_date ) latest_dk ON sr.code = latest_dk.code "
            sql += " LEFT JOIN stock_realtime_snapshot realtime ON sr.code = realtime.code "
            sql += " LEFT JOIN ( SELECT sr_inner.id AS selection_result_id, MAX(dk.high) AS max_high, MIN(dk.low) AS min_low, GREATEST(COUNT(DISTINCT dk.trade_date) - 1, 0) AS trade_day_count FROM selection_result sr_inner INNER JOIN daily_kline dk ON dk.code = sr_inner.code AND dk.trade_date BETWEEN sr_inner.trade_date AND (SELECT MAX(trade_date) FROM daily_kline) GROUP BY sr_inner.id ) period_dk ON sr.id = period_dk.selection_result_id "
            sql += " WHERE sb.instrument_type = %s "
            params.append(instrument_type)
            sql += " ORDER BY sr.rank_no ASC, sr.id DESC LIMIT %s"
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
            sb.industry,
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
            dk.close AS daily_current_price,
            realtime.latest_price AS realtime_price,
            realtime.pct_chg AS realtime_pct_chg,
            realtime.quote_time AS realtime_quote_time,
            realtime.trade_date AS realtime_trade_date,
            realtime.high_price AS realtime_high_price,
            realtime.low_price AS realtime_low_price,
            COALESCE(realtime.latest_price, dk.close) AS current_price
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
        LEFT JOIN stock_realtime_snapshot realtime ON sb.code = realtime.code
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
    def _combine_period_extreme(period_value: float | None, realtime_value: float | None, prefer_max: bool) -> float | None:
        values = [value for value in [period_value, realtime_value] if value is not None]
        if not values:
            return None
        return max(values) if prefer_max else min(values)

    def _build_record_from_selection_result(self, row: Dict[str, Any]) -> SelectionTrackingRecord:
        selected_open_price = self._to_float(row.get("selected_open_price"))
        selected_close_price = self._to_float(row.get("selected_close_price"))
        current_price = self._to_float(row.get("current_price"))
        daily_current_price = self._to_float(row.get("daily_current_price"))
        realtime_price = self._to_float(row.get("realtime_price"))
        realtime_pct_chg = self._to_float(row.get("realtime_pct_chg"))
        realtime_quote_time = str(row["realtime_quote_time"]) if row.get("realtime_quote_time") else None
        realtime_high_price = self._to_float(row.get("realtime_high_price"))
        realtime_low_price = self._to_float(row.get("realtime_low_price"))
        latest_trade_date = str(row["latest_trade_date"]) if row.get("latest_trade_date") else None
        realtime_trade_date = str(row["realtime_trade_date"]) if row.get("realtime_trade_date") else None
        metric_trade_date = str(row["metric_trade_date"]) if row.get("metric_trade_date") else latest_trade_date
        selection_dt = self._to_date(row.get("selection_date"))
        latest_dt = self._to_date(row.get("latest_trade_date"))
        realtime_dt = self._to_date(row.get("realtime_trade_date"))
        tracking_end_dt = max([dt for dt in [latest_dt, realtime_dt] if dt], default=None)
        trade_day_count = row.get("trade_day_count")
        if realtime_dt and selection_dt and realtime_dt > selection_dt and (not latest_dt or realtime_dt > latest_dt):
            try:
                trade_day_count = max(int(trade_day_count or 0) + 1, 1)
            except (TypeError, ValueError):
                trade_day_count = 1
        base_price = selected_open_price or selected_close_price
        price_change_pct = None
        if base_price and current_price:
            price_change_pct = round((current_price - base_price) / base_price * 100, 2)
        tracking_days = self._calc_tracking_days(selection_dt, tracking_end_dt, trade_day_count)
        review_status = "tracking" if tracking_end_dt and selection_dt and tracking_end_dt >= selection_dt else "pending"
        period_max_high = self._combine_period_extreme(self._to_float(row.get("period_max_high")), realtime_high_price, prefer_max=True)
        period_min_low = self._combine_period_extreme(self._to_float(row.get("period_min_low")), realtime_low_price, prefer_max=False)
        max_gain_pct = None
        max_drawdown_pct = None
        period_base_price = selected_open_price or selected_close_price
        if period_base_price and period_max_high:
            max_gain_pct = round((period_max_high - period_base_price) / period_base_price * 100, 2)
        if period_base_price and period_min_low:
            max_drawdown_pct = round((period_min_low - period_base_price) / period_base_price * 100, 2)

        metadata = row.get("metadata_json")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        metadata = metadata or {}
        explain = metadata.get("explain", {}) or {}
        raw_metrics = metadata.get("raw_metrics", {})
        if selected_open_price is None:
            selected_open_price = self._to_float(raw_metrics.get("open"))
        if selected_close_price is None:
            selected_close_price = self._to_float(raw_metrics.get("close"))
        base_price = selected_open_price or selected_close_price
        if base_price and current_price:
            price_change_pct = round((current_price - base_price) / base_price * 100, 2)
        period_base_price = selected_open_price or selected_close_price
        if period_base_price and period_max_high:
            max_gain_pct = round((period_max_high - period_base_price) / period_base_price * 100, 2)
        if period_base_price and period_min_low:
            max_drawdown_pct = round((period_min_low - period_base_price) / period_base_price * 100, 2)
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
            strategy_id=row.get("strategy_id") or "",
            strategy_display_name=metadata.get("strategy_display_name"),
            strategy_version=metadata.get("strategy_version"),
            industry=row.get("industry"),
            score=self._to_float(row.get("score")),
            include_in_stats=bool(row.get("include_in_stats", 1)),
            factor_scores=factor_scores,
            selected_open_price=selected_open_price,
            selected_close_price=selected_close_price,
            current_price=current_price,
            latest_trade_date=latest_trade_date,
            price_change_pct=price_change_pct,
            reason_summary=explain.get("reasons") or [],
            risk_summary=explain.get("risks") or [],
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
            strategy_id="lowvol_reversal",
            strategy_display_name="低波动反转策略",
            strategy_version="v1",
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
                "strategy_id": item.strategy_id,
                "score": item.score,
                "include_in_stats": item.include_in_stats,
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
                "tracking_days": item.tracking_days,
                "review_status": item.review_status,
                "max_gain_pct": item.max_gain_pct,
                "max_drawdown_pct": item.max_drawdown_pct,
            }
            for item in records
        ]
