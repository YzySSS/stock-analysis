from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import tushare as ts

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


@dataclass
class FactorInputDailyRecord:
    code: str
    trade_date: str
    pe_tushare: float | None = None
    pb_tushare: float | None = None
    turnover_rate: float | None = None
    turnover_rate_f: float | None = None
    volume_ratio: float | None = None
    total_mv: float | None = None
    circ_mv: float | None = None
    roe: float | None = None
    roa: float | None = None
    grossprofit_margin: float | None = None
    netprofit_margin: float | None = None
    revenue_yoy: float | None = None
    profit_yoy: float | None = None
    fundamental_period: str | None = None
    fundamental_publish_date: str | None = None
    valuation_source: str = "tushare_daily_basic"
    fundamental_source: str = "stock_basic_snapshot"
    valuation_updated_at: str | None = None
    fundamental_updated_at: str | None = None
    completeness_score: float | None = None
    source: str = "tushare_daily_basic"


class FactorInputHistorySync:
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        self.pro = ts.pro_api(self.token)
        self.task_logger = TaskRunLogger()

    @staticmethod
    def _daterange(start_date: str, end_date: str) -> List[str]:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        dates: List[str] = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates

    def fetch_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        sql = """
        SELECT DISTINCT trade_date
        FROM daily_kline
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (start_date, end_date))
                return [str(row["trade_date"]) for row in cursor.fetchall()]

    def fetch_stock_codes(self, limit: int | None = None, offset: int = 0) -> List[str]:
        sql = """
        SELECT code
        FROM stock_basic
        WHERE instrument_type = 'stock' AND is_delisted = 0 AND name NOT LIKE '%%指数%%'
        ORDER BY code
        """
        params: list[object] = []
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend([int(limit), int(offset)])
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return [row["code"] for row in cursor.fetchall()]

    def fetch_daily_basic_map(self, trade_date: str) -> Dict[str, Dict[str, float | None]]:
        ts_trade_date = trade_date.replace("-", "")
        df = self.pro.daily_basic(
            trade_date=ts_trade_date,
            fields="ts_code,pe,pb,turnover_rate,turnover_rate_f,volume_ratio,total_mv,circ_mv",
        )
        result: Dict[str, Dict[str, float | None]] = {}
        for _, row in df.iterrows():
            code = str(row["ts_code"]).split(".")[0]
            def val(field: str) -> float | None:
                value = row.get(field)
                return float(value) if value == value else None

            result[code] = {
                "pe_tushare": val("pe"),
                "pb_tushare": val("pb"),
                "turnover_rate": val("turnover_rate"),
                "turnover_rate_f": val("turnover_rate_f"),
                "volume_ratio": val("volume_ratio"),
                "total_mv": val("total_mv"),
                "circ_mv": val("circ_mv"),
            }
        return result

    def fetch_stock_basic_snapshot(self, codes: Sequence[str] | None = None) -> Dict[str, Dict[str, object]]:
        sql = """
        SELECT code, roe, roa, grossprofit_margin, netprofit_margin, revenue_yoy, profit_yoy, fundamental_period, fundamental_updated_at
        FROM stock_basic
        WHERE instrument_type = 'stock'
        """
        params: list[object] = []
        if codes:
            placeholders = ",".join(["%s"] * len(codes))
            sql += f" AND code IN ({placeholders})"
            params.extend(codes)
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        return {
            row["code"]: {
                "roe": float(row["roe"]) if row.get("roe") is not None else None,
                "roa": float(row["roa"]) if row.get("roa") is not None else None,
                "grossprofit_margin": float(row["grossprofit_margin"]) if row.get("grossprofit_margin") is not None else None,
                "netprofit_margin": float(row["netprofit_margin"]) if row.get("netprofit_margin") is not None else None,
                "revenue_yoy": float(row["revenue_yoy"]) if row.get("revenue_yoy") is not None else None,
                "profit_yoy": float(row["profit_yoy"]) if row.get("profit_yoy") is not None else None,
                "fundamental_period": row.get("fundamental_period"),
                "fundamental_updated_at": str(row.get("fundamental_updated_at")) if row.get("fundamental_updated_at") else None,
            }
            for row in rows
        }

    @staticmethod
    def _upsert_sql(*, preserve_existing_fundamentals: bool = False) -> str:
        sql = """
        INSERT INTO factor_input_daily (
            code, trade_date, pe_tushare, pb_tushare, turnover_rate, turnover_rate_f,
            volume_ratio, total_mv, circ_mv, roe, roa,
            grossprofit_margin, netprofit_margin, revenue_yoy, profit_yoy,
            fundamental_period, fundamental_publish_date, valuation_source, fundamental_source,
            valuation_updated_at, fundamental_updated_at, completeness_score, source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            pe_tushare = VALUES(pe_tushare),
            pb_tushare = VALUES(pb_tushare),
            turnover_rate = VALUES(turnover_rate),
            turnover_rate_f = VALUES(turnover_rate_f),
            volume_ratio = VALUES(volume_ratio),
            total_mv = VALUES(total_mv),
            circ_mv = VALUES(circ_mv),
            valuation_source = VALUES(valuation_source),
            valuation_updated_at = VALUES(valuation_updated_at),
        """
        if preserve_existing_fundamentals:
            return sql + """
            completeness_score = GREATEST(
                COALESCE(factor_input_daily.completeness_score, 0),
                COALESCE(VALUES(completeness_score), 0)
            ),
            source = VALUES(source)
            """
        return sql + """
            fundamental_source = VALUES(fundamental_source),
            fundamental_updated_at = VALUES(fundamental_updated_at),
            roe = VALUES(roe),
            roa = VALUES(roa),
            grossprofit_margin = VALUES(grossprofit_margin),
            netprofit_margin = VALUES(netprofit_margin),
            revenue_yoy = VALUES(revenue_yoy),
            profit_yoy = VALUES(profit_yoy),
            fundamental_period = VALUES(fundamental_period),
            fundamental_publish_date = VALUES(fundamental_publish_date),
            completeness_score = VALUES(completeness_score),
            source = VALUES(source)
        """

    def save_records(
        self,
        records: Iterable[FactorInputDailyRecord],
        *,
        preserve_existing_fundamentals: bool = False,
    ) -> int:
        rows = list(records)
        if not rows:
            return 0
        sql = self._upsert_sql(
            preserve_existing_fundamentals=preserve_existing_fundamentals
        )
        data = [
            (
                r.code, r.trade_date, r.pe_tushare, r.pb_tushare, r.turnover_rate, r.turnover_rate_f,
                r.volume_ratio, r.total_mv, r.circ_mv, r.roe, r.roa,
                r.grossprofit_margin, r.netprofit_margin, r.revenue_yoy, r.profit_yoy,
                r.fundamental_period, r.fundamental_publish_date, r.valuation_source, r.fundamental_source,
                r.valuation_updated_at, r.fundamental_updated_at, r.completeness_score, r.source,
            )
            for r in rows
        ]
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
        return len(rows)

    def run(
        self,
        start_date: str,
        end_date: str,
        limit_per_day: int | None = None,
        offset: int = 0,
        *,
        trade_dates_override: Sequence[str] | None = None,
        daily_basic_maps: Mapping[str, Mapping[str, Dict[str, float | None]]] | None = None,
    ) -> dict:
        trade_dates = (
            list(trade_dates_override)
            if trade_dates_override is not None
            else self.fetch_trade_dates(start_date, end_date)
        )
        codes = self.fetch_stock_codes(limit=limit_per_day, offset=offset)
        if not codes:
            return {
                "start_date": start_date,
                "end_date": end_date,
                "trade_dates": len(trade_dates),
                "processed_days": 0,
                "rows_synced": 0,
                "limit_per_day": limit_per_day,
                "offset": offset,
            }
        total_rows = 0
        total_days = 0
        fundamental_map = self.fetch_stock_basic_snapshot(codes)
        for trade_date in trade_dates:
            valuation_map = (
                daily_basic_maps.get(trade_date, {})
                if daily_basic_maps is not None
                else self.fetch_daily_basic_map(trade_date)
            )
            records: List[FactorInputDailyRecord] = []
            for code in codes:
                normalized_code = code.split(".")[-1]
                valuation = valuation_map.get(normalized_code, {})
                fundamental = fundamental_map.get(code, {})
                filled_fields = [
                    valuation.get("pe_tushare"), valuation.get("pb_tushare"), valuation.get("turnover_rate"),
                    valuation.get("volume_ratio"), fundamental.get("roe"), fundamental.get("revenue_yoy"),
                ]
                completeness_score = round(len([x for x in filled_fields if x is not None]) / len(filled_fields), 4)
                records.append(
                    FactorInputDailyRecord(
                        code=code,
                        trade_date=trade_date,
                        pe_tushare=valuation.get("pe_tushare"),
                        pb_tushare=valuation.get("pb_tushare"),
                        turnover_rate=valuation.get("turnover_rate"),
                        turnover_rate_f=valuation.get("turnover_rate_f"),
                        volume_ratio=valuation.get("volume_ratio"),
                        total_mv=valuation.get("total_mv"),
                        circ_mv=valuation.get("circ_mv"),
                        roe=fundamental.get("roe"),
                        roa=fundamental.get("roa"),
                        grossprofit_margin=fundamental.get("grossprofit_margin"),
                        netprofit_margin=fundamental.get("netprofit_margin"),
                        revenue_yoy=fundamental.get("revenue_yoy"),
                        profit_yoy=fundamental.get("profit_yoy"),
                        fundamental_period=fundamental.get("fundamental_period"),
                        valuation_updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        fundamental_updated_at=fundamental.get("fundamental_updated_at"),
                        completeness_score=completeness_score,
                    )
                )
            total_rows += self.save_records(records)
            total_days += 1
        return {
            "start_date": start_date,
            "end_date": end_date,
            "trade_dates": len(trade_dates),
            "processed_days": total_days,
            "rows_synced": total_rows,
            "limit_per_day": limit_per_day,
            "offset": offset,
        }
