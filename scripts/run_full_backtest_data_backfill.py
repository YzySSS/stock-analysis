from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import tushare as ts

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_ingestion.daily_kline_sync import DailyKlineSync
from app.data_ingestion.factor_input_history_sync import FactorInputDailyRecord, FactorInputHistorySync
from app.shared.db import mysql_conn
from app.shared.task_log import TaskRunLogger

TASK_NAME = "full_backtest_data_backfill"


def active_stock_count() -> int:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS c FROM stock_basic WHERE instrument_type='stock' AND is_delisted=0")
            return int((cursor.fetchone() or {}).get("c") or 0)


def active_stock_codes() -> list[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT code
                FROM stock_basic
                WHERE instrument_type='stock' AND is_delisted=0
                ORDER BY code
            """)
            return [row["code"] for row in cursor.fetchall() or []]


def trade_dates_from_kline(start_date: str, end_date: str) -> list[str]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT trade_date
                FROM daily_kline
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date
                """,
                (start_date, end_date),
            )
            return [str(row["trade_date"]) for row in cursor.fetchall() or []]


def coverage(table: str, start_date: str, end_date: str) -> dict[str, Any]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) rows_n, COUNT(DISTINCT code) codes,
                       MIN(trade_date) min_d, MAX(trade_date) max_d
                FROM {table}
                WHERE trade_date BETWEEN %s AND %s
                """,
                (start_date, end_date),
            )
            return cursor.fetchone() or {}


def coverage_distribution(table: str, start_date: str, end_date: str) -> dict[str, Any]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) trade_days,
                       MIN(codes) min_codes,
                       MAX(codes) max_codes,
                       SUM(codes >= 5200) days_ge_5200,
                       SUM(codes >= 5180) days_ge_5180,
                       SUM(codes >= 5000) days_ge_5000
                FROM (
                    SELECT trade_date, COUNT(DISTINCT code) codes
                    FROM {table}
                    WHERE trade_date BETWEEN %s AND %s
                    GROUP BY trade_date
                ) t
                """,
                (start_date, end_date),
            )
            return cursor.fetchone() or {}


def run_kline_history(start_date: str, end_date: str, batch_size: int, pause_seconds: float) -> dict[str, Any]:
    total_codes = active_stock_count()
    sync = DailyKlineSync()
    total_rows = 0
    success_codes = 0
    failed_codes: list[dict[str, str]] = []
    batches = 0
    for offset in range(0, total_codes, batch_size):
        result = sync.run(
            start_date=start_date,
            end_date=end_date,
            limit=batch_size,
            offset=offset,
            instrument_type="stock",
            pause_seconds=0.03,
            relogin_every=20,
        )
        batch = {
            "stage": "daily_kline_batch",
            "offset": offset,
            "limit": batch_size,
            "rows_synced": int(result.get("rows_synced") or 0),
            "success_codes": int(result.get("success_codes") or 0),
            "failed_count": len(result.get("failed_codes") or []),
        }
        print(json.dumps(batch, ensure_ascii=False), flush=True)
        total_rows += batch["rows_synced"]
        success_codes += batch["success_codes"]
        failed_codes.extend(result.get("failed_codes") or [])
        batches += 1
        if pause_seconds > 0:
            time.sleep(pause_seconds)
    return {
        "total_codes": total_codes,
        "batches": batches,
        "rows_synced": total_rows,
        "success_codes": success_codes,
        "failed_count": len(failed_codes),
        "failed_codes": failed_codes[:100],
    }


def fetch_daily_basic_map(pro: Any, trade_date: str) -> dict[str, dict[str, float | None]]:
    df = pro.daily_basic(
        trade_date=trade_date.replace("-", ""),
        fields="ts_code,pe,pb,turnover_rate,turnover_rate_f,volume_ratio,total_mv,circ_mv",
    )
    result: dict[str, dict[str, float | None]] = {}
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


def run_factor_history(start_date: str, end_date: str, pause_seconds: float) -> dict[str, Any]:
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN 未配置")
    pro = ts.pro_api(token)
    helper = FactorInputHistorySync(token=token)
    trade_dates = trade_dates_from_kline(start_date, end_date)
    codes = active_stock_codes()
    fundamental_map = helper.fetch_stock_basic_snapshot(codes)
    total_rows = 0
    failed_dates: list[dict[str, str]] = []
    for index, trade_date in enumerate(trade_dates, start=1):
        try:
            valuation_map = fetch_daily_basic_map(pro, trade_date)
            records: list[FactorInputDailyRecord] = []
            for code in codes:
                normalized_code = code.split(".")[-1]
                valuation = valuation_map.get(normalized_code, {})
                fundamental = fundamental_map.get(code, {})
                filled_fields = [
                    valuation.get("pe_tushare"),
                    valuation.get("pb_tushare"),
                    valuation.get("turnover_rate"),
                    valuation.get("volume_ratio"),
                    fundamental.get("roe"),
                    fundamental.get("revenue_yoy"),
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
            rows = helper.save_records(records)
            total_rows += rows
            print(json.dumps({"stage": "factor_input_day", "index": index, "trade_date": trade_date, "rows_synced": rows}, ensure_ascii=False), flush=True)
            if pause_seconds > 0:
                time.sleep(pause_seconds)
        except Exception as exc:
            failed_dates.append({"trade_date": trade_date, "error": str(exc)[:300]})
            print(json.dumps({"stage": "factor_input_error", "trade_date": trade_date, "error": str(exc)[:300]}, ensure_ascii=False), flush=True)
    return {
        "trade_dates": len(trade_dates),
        "rows_synced": total_rows,
        "failed_count": len(failed_dates),
        "failed_dates": failed_dates[:100],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Full backtest core data backfill: daily_kline + factor_input_daily")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--kline-batch-size", type=int, default=200)
    parser.add_argument("--kline-pause-seconds", type=float, default=1.0)
    parser.add_argument("--factor-pause-seconds", type=float, default=0.25)
    parser.add_argument("--skip-kline", action="store_true")
    parser.add_argument("--skip-factor", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_id = f"full_backtest_data_backfill_{args.start_date.replace('-', '')}_{args.end_date.replace('-', '')}_{datetime.now().strftime('%H%M%S')}"
    logger = TaskRunLogger()
    metadata = vars(args)
    logger.start(TASK_NAME, run_id, metadata)
    started = datetime.now()
    payload: dict[str, Any] = {**metadata, "run_id": run_id}
    try:
        if not args.skip_kline:
            payload["daily_kline"] = run_kline_history(args.start_date, args.end_date, args.kline_batch_size, args.kline_pause_seconds)
            payload["daily_kline_coverage"] = coverage("daily_kline", args.start_date, args.end_date)
            payload["daily_kline_distribution"] = coverage_distribution("daily_kline", args.start_date, args.end_date)
        if not args.skip_factor:
            payload["factor_input_daily"] = run_factor_history(args.start_date, args.end_date, args.factor_pause_seconds)
            payload["factor_input_coverage"] = coverage("factor_input_daily", args.start_date, args.end_date)
            payload["factor_input_distribution"] = coverage_distribution("factor_input_daily", args.start_date, args.end_date)
        payload["elapsed_seconds"] = round((datetime.now() - started).total_seconds(), 2)
        logger.finish(TASK_NAME, run_id, "success", "full backtest data backfill completed", payload)
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)
    except Exception as exc:
        payload["elapsed_seconds"] = round((datetime.now() - started).total_seconds(), 2)
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], payload)
        raise


if __name__ == "__main__":
    main()
