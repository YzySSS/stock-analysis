from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence

import tushare as ts

from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger


@dataclass
class FactorInputDailyRecord:
    code: str
    trade_date: str
    pe_tushare: float | None = None
    pb_tushare: float | None = None
    roe: float | None = None
    roa: float | None = None
    grossprofit_margin: float | None = None
    netprofit_margin: float | None = None
    revenue_yoy: float | None = None
    profit_yoy: float | None = None
    fundamental_period: str | None = None
    source: str = "tushare_stock_basic_snapshot"


class FactorInputHistorySync:
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        self.pro = ts.pro_api(self.token)
        self.task_logger = TaskRunLogger()

    def ensure_table(self) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS factor_input_daily (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(16) NOT NULL,
            trade_date DATE NOT NULL,
            pe_tushare DECIMAL(12,4) DEFAULT NULL,
            pb_tushare DECIMAL(12,4) DEFAULT NULL,
            roe DECIMAL(12,4) DEFAULT NULL,
            roa DECIMAL(12,4) DEFAULT NULL,
            grossprofit_margin DECIMAL(12,4) DEFAULT NULL,
            netprofit_margin DECIMAL(12,4) DEFAULT NULL,
            revenue_yoy DECIMAL(12,4) DEFAULT NULL,
            profit_yoy DECIMAL(12,4) DEFAULT NULL,
            fundamental_period VARCHAR(16) DEFAULT NULL,
            source VARCHAR(32) DEFAULT 'tushare_stock_basic_snapshot',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_factor_input_daily (code, trade_date),
            KEY idx_factor_input_trade_date (trade_date),
            KEY idx_factor_input_code (code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

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
        df = self.pro.daily_basic(trade_date=ts_trade_date, fields="ts_code,pe,pb")
        result: Dict[str, Dict[str, float | None]] = {}
        for _, row in df.iterrows():
            code = str(row["ts_code"]).split(".")[0]
            pe = row["pe"]
            pb = row["pb"]
            result[code] = {
                "pe_tushare": float(pe) if pe == pe else None,
                "pb_tushare": float(pb) if pb == pb else None,
            }
        return result

    def fetch_stock_basic_snapshot(self, codes: Sequence[str] | None = None) -> Dict[str, Dict[str, object]]:
        sql = """
        SELECT code, roe, roa, grossprofit_margin, netprofit_margin, revenue_yoy, profit_yoy, fundamental_period
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
            }
            for row in rows
        }

    def save_records(self, records: Iterable[FactorInputDailyRecord]) -> int:
        rows = list(records)
        if not rows:
            return 0
        sql = """
        INSERT INTO factor_input_daily (
            code, trade_date, pe_tushare, pb_tushare, roe, roa,
            grossprofit_margin, netprofit_margin, revenue_yoy, profit_yoy,
            fundamental_period, source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            pe_tushare = VALUES(pe_tushare),
            pb_tushare = VALUES(pb_tushare),
            roe = VALUES(roe),
            roa = VALUES(roa),
            grossprofit_margin = VALUES(grossprofit_margin),
            netprofit_margin = VALUES(netprofit_margin),
            revenue_yoy = VALUES(revenue_yoy),
            profit_yoy = VALUES(profit_yoy),
            fundamental_period = VALUES(fundamental_period),
            source = VALUES(source)
        """
        data = [
            (
                r.code, r.trade_date, r.pe_tushare, r.pb_tushare, r.roe, r.roa,
                r.grossprofit_margin, r.netprofit_margin, r.revenue_yoy, r.profit_yoy,
                r.fundamental_period, r.source,
            )
            for r in rows
        ]
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
        return len(rows)

    def run(self, start_date: str, end_date: str, limit_per_day: int | None = None, offset: int = 0) -> dict:
        self.ensure_table()
        trade_dates = self.fetch_trade_dates(start_date, end_date)
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
            valuation_map = self.fetch_daily_basic_map(trade_date)
            records: List[FactorInputDailyRecord] = []
            for code in codes:
                normalized_code = code.split(".")[-1]
                valuation = valuation_map.get(normalized_code, {})
                fundamental = fundamental_map.get(code, {})
                records.append(
                    FactorInputDailyRecord(
                        code=code,
                        trade_date=trade_date,
                        pe_tushare=valuation.get("pe_tushare"),
                        pb_tushare=valuation.get("pb_tushare"),
                        roe=fundamental.get("roe"),
                        roa=fundamental.get("roa"),
                        grossprofit_margin=fundamental.get("grossprofit_margin"),
                        netprofit_margin=fundamental.get("netprofit_margin"),
                        revenue_yoy=fundamental.get("revenue_yoy"),
                        profit_yoy=fundamental.get("profit_yoy"),
                        fundamental_period=fundamental.get("fundamental_period"),
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
