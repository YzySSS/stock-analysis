from __future__ import annotations

import argparse
import json
import math
import os
import signal
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import akshare as ak
import pandas as pd
import tushare as ts

from app.market_timing.calibration import (
    DAILY_WEIGHTS,
    MODEL_ID,
    MODEL_NAME,
    MODEL_VERSION,
    calibrate_indicator_score,
    compose_timing_state,
    score_signal,
    signal_label,
)
from app.shared.db import mysql_conn
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.shared.task_log import TaskRunLogger

TASK_NAME = "market_timing_daily_update"
LOCK_NAME = "market_timing_daily_update_lock"
TREND_INDEX_CODES = ("000300.SH", "000852.SH", "000688.SH")

INDEX_OPTION_UNDERLYINGS = {
    "OP000300.SH": {"code": "000300.SH", "name": "沪深300"},
    "OP000016.SH": {"code": "000016.SH", "name": "上证50"},
    "OP000852.SH": {"code": "000852.SH", "name": "中证1000"},
}


class MarketTimingSourceTimeout(TimeoutError):
    pass


class MarketTimingTotalTimeout(TimeoutError):
    pass


@contextmanager
def _hard_deadline(label: str, timeout_seconds: float):
    timeout = max(float(timeout_seconds), 0.0)
    if timeout <= 0:
        yield
        return

    timeout_message = f"{label} exceeded {timeout:g}s hard timeout"
    is_main_thread = threading.current_thread() is threading.main_thread()
    has_posix_alarm = is_main_thread and all(
        hasattr(signal, attribute)
        for attribute in ("SIGALRM", "ITIMER_REAL", "setitimer")
    )

    if has_posix_alarm:
        previous_handler = signal.getsignal(signal.SIGALRM)

        def handle_timeout(_signum, _frame):
            raise MarketTimingSourceTimeout(timeout_message)

        signal.signal(signal.SIGALRM, handle_timeout)
        signal.setitimer(signal.ITIMER_REAL, timeout)
        try:
            yield
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)
        return

    # Windows has no SIGALRM/setitimer.  A timer-raised SIGINT wakes the
    # CPython main thread during common blocking waits, while the handler below
    # distinguishes the synthetic timeout from a user's Ctrl+C.
    if is_main_thread and hasattr(signal, "SIGINT") and hasattr(signal, "raise_signal"):
        expired = threading.Event()
        active = threading.Event()
        active.set()
        previous_handler = signal.getsignal(signal.SIGINT)

        def handle_timeout(signum, frame):
            if expired.is_set():
                raise MarketTimingSourceTimeout(timeout_message)
            if callable(previous_handler):
                previous_handler(signum, frame)
                return
            if previous_handler == signal.SIG_IGN:
                return
            raise KeyboardInterrupt

        def expire() -> None:
            if not active.is_set():
                return
            expired.set()
            signal.raise_signal(signal.SIGINT)

        timer = threading.Timer(timeout, expire)
        timer.daemon = True
        signal.signal(signal.SIGINT, handle_timeout)
        timer.start()
        try:
            yield
        finally:
            active.clear()
            timer.cancel()
            try:
                timer.join()
            finally:
                signal.signal(signal.SIGINT, previous_handler)
        return

    # Signal handlers can only be installed by the main thread.  Keep worker
    # thread usage portable and report an overrun deterministically when the
    # wrapped operation returns.
    started_at = time.monotonic()
    try:
        yield
    except BaseException as exc:
        if time.monotonic() - started_at >= timeout:
            raise MarketTimingSourceTimeout(timeout_message) from exc
        raise
    if time.monotonic() - started_at >= timeout:
        raise MarketTimingSourceTimeout(timeout_message)


def _remaining_source_timeout(source_timeout_seconds: float, total_deadline: float | None) -> float:
    source_timeout = max(float(source_timeout_seconds), 0.0)
    if total_deadline is None:
        return source_timeout
    remaining = total_deadline - time.monotonic()
    if remaining <= 0:
        raise MarketTimingTotalTimeout("market timing run exceeded total hard timeout")
    if source_timeout <= 0:
        return remaining
    return min(source_timeout, remaining)


def _ensure_total_deadline(total_deadline: float | None) -> None:
    if total_deadline is not None and time.monotonic() >= total_deadline:
        raise MarketTimingTotalTimeout("market timing run exceeded total hard timeout")


def _parse_date(value: str | None) -> str:
    if value:
        return value.replace("/", "-")
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
            row = cursor.fetchone() or {}
            if row.get("trade_date"):
                return str(row["trade_date"])
    return (datetime.now().date() - timedelta(days=1)).isoformat()


def _ts_date(value: str) -> str:
    return value.replace("-", "")


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _date_only(value: Any):
    if value is None:
        return None
    return pd.to_datetime(value).date()


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def _round(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None else None


def _clamp(value: float, low: float = 0, high: float = 100) -> float:
    return max(low, min(high, value))


def _signal(score: float | None) -> int:
    return score_signal(score)


def _signal_label(signal: int) -> str:
    return signal_label(signal)


def _norm_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _bs_price(spot: float, strike: float, years: float, rate: float, vol: float, call_put: str) -> float:
    if spot <= 0 or strike <= 0 or years <= 0 or vol <= 0:
        return 0.0
    sqrt_t = math.sqrt(years)
    d1 = (math.log(spot / strike) + (rate + 0.5 * vol * vol) * years) / (vol * sqrt_t)
    d2 = d1 - vol * sqrt_t
    discount = math.exp(-rate * years)
    if call_put == "C":
        return spot * _norm_cdf(d1) - strike * discount * _norm_cdf(d2)
    return strike * discount * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def _implied_vol(price: float, spot: float, strike: float, years: float, rate: float, call_put: str) -> float | None:
    if price <= 0 or spot <= 0 or strike <= 0 or years <= 0:
        return None
    intrinsic = max(spot - strike, 0) if call_put == "C" else max(strike - spot, 0)
    if price < intrinsic * 0.98:
        return None
    low = 0.0001
    high = 3.0
    for _ in range(80):
        mid = (low + high) / 2
        model_price = _bs_price(spot, strike, years, rate, mid, call_put)
        if model_price > price:
            high = mid
        else:
            low = mid
    result = (low + high) / 2
    if result <= 0 or result >= 2.99:
        return None
    return result


def _percentile(values: list[float], value: float | None) -> float | None:
    clean = [item for item in values if item is not None and not math.isnan(item)]
    if value is None or not clean:
        return None
    return sum(1 for item in clean if item <= value) / len(clean)


def _safe_call(
    label: str,
    func,
    *,
    source_timeout_seconds: float,
    total_deadline: float | None,
) -> tuple[Any | None, str | None]:
    timeout = _remaining_source_timeout(source_timeout_seconds, total_deadline)
    try:
        with _hard_deadline(label, timeout):
            return func(), None
    except MarketTimingSourceTimeout as exc:
        if total_deadline is not None and time.monotonic() >= total_deadline:
            raise MarketTimingTotalTimeout("market timing run exceeded total hard timeout") from exc
        return None, f"{label}: {str(exc)[:220]}"
    except Exception as exc:
        return None, f"{label}: {str(exc)[:220]}"


def _fetch_index_daily(pro, index_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    df = pro.index_daily(
        ts_code=index_code,
        start_date=_ts_date(start_date),
        end_date=_ts_date(end_date),
        fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
    )
    if df is None or df.empty:
        return []
    rows = df.sort_values("trade_date").to_dict("records")
    return rows


def _fetch_index_closes(pro, index_codes: list[str], trade_date: str) -> dict[str, float]:
    start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y%m%d")
    end = _ts_date(trade_date)
    closes: dict[str, float] = {}
    for code in index_codes:
        try:
            df = pro.index_daily(ts_code=code, start_date=start, end_date=end, fields="ts_code,trade_date,close")
        except MarketTimingSourceTimeout:
            raise
        except Exception:
            continue
        if df is None or df.empty:
            continue
        latest = df.sort_values("trade_date").iloc[-1]
        close = _float(latest.get("close"))
        if close is not None:
            closes[code] = close
    return closes


def _fetch_index_valuation(pro, index_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    df = pro.index_dailybasic(
        ts_code=index_code,
        start_date=_ts_date(start_date),
        end_date=_ts_date(end_date),
        fields="ts_code,trade_date,pe,pe_ttm,pb,turnover_rate,total_mv,float_mv",
    )
    if df is None or df.empty:
        return []
    return df.sort_values("trade_date").to_dict("records")


def _fetch_margin(pro, start_date: str, end_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for exchange_id in ("SSE", "SZSE"):
        df = pro.margin(
            exchange_id=exchange_id,
            start_date=_ts_date(start_date),
            end_date=_ts_date(end_date),
            fields="trade_date,exchange_id,rzye,rzmre,rzche,rqye,rqmcl,rzrqye",
        )
        if df is not None and not df.empty:
            rows.extend(df.to_dict("records"))
    return rows


def _fetch_option_daily(pro, trade_date: str) -> list[dict[str, Any]]:
    df = pro.opt_daily(
        trade_date=_ts_date(trade_date),
        fields="ts_code,trade_date,exchange,close,settle,vol,amount,oi",
    )
    if df is None or df.empty:
        return []
    return df.to_dict("records")


def _fetch_option_basic(pro) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for exchange in ("SSE", "SZSE", "CFFEX"):
        try:
            df = pro.opt_basic(exchange=exchange, fields="ts_code,call_put")
        except MarketTimingSourceTimeout:
            raise
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for row in df.to_dict("records"):
            ts_code = str(row.get("ts_code") or "")
            call_put = str(row.get("call_put") or "").upper()
            if ts_code and call_put in {"C", "P"}:
                mapping[ts_code] = call_put
    return mapping


def _fetch_cffex_option_basic_rows(pro) -> list[dict[str, Any]]:
    df = pro.opt_basic(
        exchange="CFFEX",
        fields="ts_code,opt_code,call_put,exercise_price,maturity_date,delist_date",
    )
    if df is None or df.empty:
        return []
    return df.to_dict("records")


def _infer_call_put(ts_code: str, call_put_by_code: dict[str, str]) -> str | None:
    if ts_code in call_put_by_code:
        return call_put_by_code[ts_code]
    text = ts_code.upper()
    if "-C-" in text:
        return "C"
    if "-P-" in text:
        return "P"
    return None


def _fetch_futures_holding(pro, trade_date: str) -> list[dict[str, Any]]:
    df = pro.fut_holding(
        exchange="CFFEX",
        trade_date=_ts_date(trade_date),
        fields="trade_date,symbol,broker,vol,vol_chg,long_hld,long_chg,short_hld,short_chg",
    )
    if df is None or df.empty:
        return []
    return df.to_dict("records")


def _fetch_qvix_rows(trade_date: str) -> list[dict[str, Any]]:
    target_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
    qvix_sources = [
        ("300etf_qvix", "300ETF QVIX", ak.index_option_300etf_qvix),
        ("50etf_qvix", "50ETF QVIX", ak.index_option_50etf_qvix),
        ("500etf_qvix", "500ETF QVIX", ak.index_option_500etf_qvix),
        ("1000index_qvix", "中证1000 QVIX", ak.index_option_1000index_qvix),
    ]
    rows: list[dict[str, Any]] = []
    for code, name, fetcher in qvix_sources:
        try:
            df = fetcher()
        except MarketTimingSourceTimeout:
            raise
        except Exception:
            continue
        if df is None or df.empty:
            continue
        data = df.copy()
        data["date"] = pd.to_datetime(data["date"]).dt.date
        data = data[data["date"] <= target_date].sort_values("date")
        data = data.dropna(subset=["close"])
        if data.empty:
            continue
        latest = data.iloc[-1]
        rows.append(
            {
                "trade_date": str(latest["date"]),
                "qvix_code": code,
                "qvix_name": name,
                "open": _float(latest.get("open")),
                "high": _float(latest.get("high")),
                "low": _float(latest.get("low")),
                "close": _float(latest.get("close")),
                "history_count": int(len(data)),
                "percentile_252": _percentile(
                    [item for item in data["close"].tail(252).map(_float).tolist() if item is not None],
                    _float(latest.get("close")),
                ),
            }
        )
    return rows


def _fetch_bond_yield(pro, trade_date: str) -> tuple[float | None, dict[str, Any]]:
    # yc_cb is often permission-gated. Keep this best-effort so the rest of the
    # timing model can still run when the account lacks bond curve access.
    attempts = [
        {"trade_date": _ts_date(trade_date), "curve_type": "0"},
        {"trade_date": _ts_date(trade_date)},
    ]
    last_error = None
    for kwargs in attempts:
        try:
            df = pro.yc_cb(**kwargs)
        except MarketTimingSourceTimeout:
            raise
        except Exception as exc:
            last_error = str(exc)[:220]
            continue
        if df is None or df.empty:
            continue
        records = df.to_dict("records")
        candidates: list[tuple[float, dict[str, Any]]] = []
        for row in records:
            maturity = None
            for key in ("maturity", "curve_term", "term", "years"):
                maturity = _float(row.get(key))
                if maturity is not None:
                    break
            rate = None
            for key in ("yield", "yield_rate", "rate", "yield_cb", "yld"):
                rate = _float(row.get(key))
                if rate is not None:
                    break
            if maturity is not None and rate is not None:
                candidates.append((abs(maturity - 10), row))
        if candidates:
            row = sorted(candidates, key=lambda item: item[0])[0][1]
            for key in ("yield", "yield_rate", "rate", "yield_cb", "yld"):
                rate = _float(row.get(key))
                if rate is not None:
                    return rate, {"row": row, "kwargs": kwargs}

    start_date = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=20)).strftime("%Y%m%d")
    end_date = _ts_date(trade_date)
    try:
        df = ak.bond_china_yield(start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            rows = df.copy()
            rows["日期"] = pd.to_datetime(rows["日期"]).dt.date
            target_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
            treasury = rows[
                rows["曲线名称"].astype(str).str.contains("国债收益率曲线", na=False)
                & (rows["日期"] <= target_date)
            ].sort_values("日期")
            if not treasury.empty:
                latest = treasury.iloc[-1]
                rate = _float(latest.get("10年"))
                if rate is not None:
                    return rate, {
                        "row": latest.to_dict(),
                        "source": "akshare.bond_china_yield",
                        "tushare_error": last_error,
                    }
    except MarketTimingSourceTimeout:
        raise
    except Exception as exc:
        last_error = f"{last_error or ''}; akshare.bond_china_yield: {str(exc)[:180]}".strip("; ")

    return None, {"error": last_error or "yc_cb and akshare bond yield returned empty or unknown schema"}


def _local_amount_pressure(trade_date: str) -> tuple[float | None, dict[str, Any]]:
    sql = """
    SELECT
        SUM(CASE WHEN cur.close > prev.close THEN cur.amount ELSE 0 END) AS up_amount,
        SUM(CASE WHEN cur.close < prev.close THEN cur.amount ELSE 0 END) AS down_amount,
        SUM(cur.amount) AS total_amount,
        COUNT(*) AS total_count
    FROM daily_kline cur
    INNER JOIN (
        SELECT d1.code, d1.close
        FROM daily_kline d1
        INNER JOIN (
            SELECT code, MAX(trade_date) AS prev_date
            FROM daily_kline
            WHERE trade_date < %s
            GROUP BY code
        ) p ON d1.code = p.code AND d1.trade_date = p.prev_date
    ) prev ON cur.code = prev.code
    INNER JOIN stock_basic sb ON cur.code = sb.code
    WHERE cur.trade_date = %s AND sb.instrument_type='stock' AND sb.is_delisted=0
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date, trade_date))
            row = cursor.fetchone() or {}
    up_amount = _float(row.get("up_amount")) or 0
    down_amount = _float(row.get("down_amount")) or 0
    total_amount = _float(row.get("total_amount")) or 0
    pressure = (up_amount - down_amount) / total_amount if total_amount else None
    return pressure, {
        "up_amount": up_amount,
        "down_amount": down_amount,
        "total_amount": total_amount,
        "total_count": int(row.get("total_count") or 0),
    }


def _save_index_daily(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
    INSERT INTO market_index_daily (
        trade_date, index_code, open, high, low, close, pre_close, pct_chg, vol, amount
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
        pre_close=VALUES(pre_close), pct_chg=VALUES(pct_chg), vol=VALUES(vol), amount=VALUES(amount)
    """
    values = [
        (
            _iso_date(row.get("trade_date")),
            row.get("ts_code"),
            _float(row.get("open")),
            _float(row.get("high")),
            _float(row.get("low")),
            _float(row.get("close")),
            _float(row.get("pre_close")),
            _float(row.get("pct_chg")),
            _float(row.get("vol")),
            _float(row.get("amount")),
        )
        for row in rows
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)


def _save_index_valuation(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
    INSERT INTO market_index_valuation_daily (
        trade_date, index_code, pe, pe_ttm, pb, turnover_rate, total_mv, float_mv
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        pe=VALUES(pe), pe_ttm=VALUES(pe_ttm), pb=VALUES(pb), turnover_rate=VALUES(turnover_rate),
        total_mv=VALUES(total_mv), float_mv=VALUES(float_mv)
    """
    values = [
        (
            _iso_date(row.get("trade_date")),
            row.get("ts_code"),
            _float(row.get("pe")),
            _float(row.get("pe_ttm")),
            _float(row.get("pb")),
            _float(row.get("turnover_rate")),
            _float(row.get("total_mv")),
            _float(row.get("float_mv")),
        )
        for row in rows
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)


def _save_margin(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    sql = """
    INSERT INTO market_margin_daily (
        trade_date, exchange_id, rzye, rzmre, rzche, rqye, rqmcl, rzrqye
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        rzye=VALUES(rzye), rzmre=VALUES(rzmre), rzche=VALUES(rzche),
        rqye=VALUES(rqye), rqmcl=VALUES(rqmcl), rzrqye=VALUES(rzrqye)
    """
    values = [
        (
            _iso_date(row.get("trade_date")),
            row.get("exchange_id") or "UNKNOWN",
            _float(row.get("rzye")),
            _float(row.get("rzmre")),
            _float(row.get("rzche")),
            _float(row.get("rqye")),
            _float(row.get("rqmcl")),
            _float(row.get("rzrqye")),
        )
        for row in rows
    ]
    by_date: dict[str, dict[str, float]] = {}
    for row in rows:
        trade_date = _iso_date(row.get("trade_date"))
        if not trade_date:
            continue
        item = by_date.setdefault(trade_date, {"rzye": 0.0, "rzmre": 0.0, "rzche": 0.0, "rqye": 0.0, "rqmcl": 0.0, "rzrqye": 0.0})
        for key in item:
            item[key] += _float(row.get(key)) or 0.0
    aggregate_values = [
        (trade_date, "ALL", item["rzye"], item["rzmre"], item["rzche"], item["rqye"], item["rqmcl"], item["rzrqye"])
        for trade_date, item in by_date.items()
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values + aggregate_values)
    return [{"trade_date": trade_date, "exchange_id": "ALL", **item} for trade_date, item in sorted(by_date.items())]


def _save_bond_yield(trade_date: str, yield_rate: float | None, metadata: dict[str, Any]) -> None:
    sql = """
    INSERT INTO market_bond_yield_daily (
        trade_date, curve_name, maturity_years, yield_rate, metadata_json
    ) VALUES (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        yield_rate=VALUES(yield_rate), metadata_json=VALUES(metadata_json)
    """
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date, "China Treasury", 10, yield_rate, json.dumps(metadata, ensure_ascii=False, default=str)))


def _save_option_pcr(trade_date: str, option_rows: list[dict[str, Any]], call_put_by_code: dict[str, str]) -> list[dict[str, Any]]:
    if not option_rows:
        return []
    groups: dict[str, dict[str, Any]] = {}
    unknown_count = 0
    for row in option_rows:
        call_put = _infer_call_put(str(row.get("ts_code") or ""), call_put_by_code)
        if call_put not in {"C", "P"}:
            unknown_count += 1
            continue
        exchange = str(row.get("exchange") or "UNKNOWN")
        for key in (exchange, "ALL"):
            item = groups.setdefault(
                key,
                {
                    "trade_date": trade_date,
                    "exchange": key,
                    "call_volume": 0.0,
                    "put_volume": 0.0,
                    "call_oi": 0.0,
                    "put_oi": 0.0,
                    "contract_count": 0,
                    "unknown_count": 0,
                },
            )
            if call_put == "C":
                item["call_volume"] += _float(row.get("vol")) or 0.0
                item["call_oi"] += _float(row.get("oi")) or 0.0
            else:
                item["put_volume"] += _float(row.get("vol")) or 0.0
                item["put_oi"] += _float(row.get("oi")) or 0.0
            item["contract_count"] += 1
    for item in groups.values():
        item["unknown_count"] = unknown_count
        item["volume_pcr"] = item["put_volume"] / item["call_volume"] if item["call_volume"] else None
        item["oi_pcr"] = item["put_oi"] / item["call_oi"] if item["call_oi"] else None
    rows = sorted(groups.values(), key=lambda item: item["exchange"])
    sql = """
    INSERT INTO market_option_pcr_daily (
        trade_date, exchange, call_volume, put_volume, volume_pcr,
        call_oi, put_oi, oi_pcr, contract_count, metadata_json
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        call_volume=VALUES(call_volume), put_volume=VALUES(put_volume), volume_pcr=VALUES(volume_pcr),
        call_oi=VALUES(call_oi), put_oi=VALUES(put_oi), oi_pcr=VALUES(oi_pcr),
        contract_count=VALUES(contract_count), metadata_json=VALUES(metadata_json)
    """
    values = [
        (
            item["trade_date"],
            item["exchange"],
            item["call_volume"],
            item["put_volume"],
            item["volume_pcr"],
            item["call_oi"],
            item["put_oi"],
            item["oi_pcr"],
            item["contract_count"],
            json.dumps({"unknown_count": item["unknown_count"]}, ensure_ascii=False),
        )
        for item in rows
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
    return rows


def _symbol_family(symbol: str | None) -> str | None:
    text = str(symbol or "").upper()
    for prefix in ("IF", "IH", "IC", "IM"):
        if text.startswith(prefix):
            return prefix
    return None


def _save_futures_holding(trade_date: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        family = _symbol_family(row.get("symbol"))
        if not family:
            continue
        for key in (family, "ALL"):
            item = groups.setdefault(
                key,
                {
                    "trade_date": trade_date,
                    "symbol_family": key,
                    "long_holding": 0.0,
                    "short_holding": 0.0,
                    "long_change": 0.0,
                    "short_change": 0.0,
                    "row_count": 0,
                },
            )
            item["long_holding"] += _float(row.get("long_hld")) or 0.0
            item["short_holding"] += _float(row.get("short_hld")) or 0.0
            item["long_change"] += _float(row.get("long_chg")) or 0.0
            item["short_change"] += _float(row.get("short_chg")) or 0.0
            item["row_count"] += 1
    for item in groups.values():
        item["net_holding"] = item["long_holding"] - item["short_holding"]
        total = item["long_holding"] + item["short_holding"]
        item["net_holding_ratio"] = item["net_holding"] / total if total else None
        item["net_change"] = item["long_change"] - item["short_change"]
    aggregates = sorted(groups.values(), key=lambda item: item["symbol_family"])
    sql = """
    INSERT INTO market_futures_holding_daily (
        trade_date, symbol_family, long_holding, short_holding, net_holding,
        net_holding_ratio, long_change, short_change, net_change, row_count, metadata_json
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        long_holding=VALUES(long_holding), short_holding=VALUES(short_holding),
        net_holding=VALUES(net_holding), net_holding_ratio=VALUES(net_holding_ratio),
        long_change=VALUES(long_change), short_change=VALUES(short_change),
        net_change=VALUES(net_change), row_count=VALUES(row_count), metadata_json=VALUES(metadata_json)
    """
    values = [
        (
            item["trade_date"],
            item["symbol_family"],
            item["long_holding"],
            item["short_holding"],
            item["net_holding"],
            item["net_holding_ratio"],
            item["long_change"],
            item["short_change"],
            item["net_change"],
            item["row_count"],
            json.dumps({"source_symbols": ["IF", "IH", "IC", "IM"]}, ensure_ascii=False),
        )
        for item in aggregates
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
    return aggregates


def _save_qvix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    sql = """
    INSERT INTO market_option_qvix_daily (
        trade_date, qvix_code, qvix_name, open, high, low, close, metadata_json
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        qvix_name=VALUES(qvix_name), open=VALUES(open), high=VALUES(high),
        low=VALUES(low), close=VALUES(close), metadata_json=VALUES(metadata_json)
    """
    values = [
        (
            item["trade_date"],
            item["qvix_code"],
            item["qvix_name"],
            item["open"],
            item["high"],
            item["low"],
            item["close"],
            json.dumps(
                {
                    "history_count": item.get("history_count"),
                    "percentile_252": item.get("percentile_252"),
                },
                ensure_ascii=False,
                default=str,
            ),
        )
        for item in rows
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
    return rows


def _build_iv_skew_rows(
    trade_date: str,
    option_rows: list[dict[str, Any]],
    option_basic_rows: list[dict[str, Any]],
    underlying_prices: dict[str, float],
    risk_free_rate: float | None,
) -> list[dict[str, Any]]:
    if not option_rows or not option_basic_rows:
        return []
    daily = pd.DataFrame(option_rows)
    basic = pd.DataFrame(option_basic_rows)
    if daily.empty or basic.empty:
        return []
    cols = ["ts_code", "opt_code", "call_put", "exercise_price", "maturity_date", "delist_date"]
    basic = basic[[col for col in cols if col in basic.columns]].copy()
    merged = daily.merge(basic, on="ts_code", how="inner")
    if merged.empty:
        return []
    target_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
    merged["underlying_code"] = merged["opt_code"].map(lambda value: INDEX_OPTION_UNDERLYINGS.get(str(value), {}).get("code"))
    merged["underlying_name"] = merged["opt_code"].map(lambda value: INDEX_OPTION_UNDERLYINGS.get(str(value), {}).get("name"))
    merged = merged[merged["underlying_code"].notna()].copy()
    if merged.empty:
        return []
    merged["spot_price"] = merged["underlying_code"].map(underlying_prices)
    merged["maturity_date_raw"] = merged.get("maturity_date", merged.get("delist_date"))
    merged["maturity_date"] = pd.to_datetime(merged["maturity_date_raw"], errors="coerce").dt.date
    merged["days_to_maturity"] = merged["maturity_date"].map(lambda value: (value - target_date).days if value else None)
    merged["option_price"] = merged["settle"].map(_float)
    merged.loc[merged["option_price"].isna(), "option_price"] = merged["close"].map(_float)
    merged["strike"] = merged["exercise_price"].map(_float)
    merged["vol_num"] = merged["vol"].map(_float).fillna(0)
    merged["oi_num"] = merged["oi"].map(_float).fillna(0)
    merged = merged[
        (merged["spot_price"].notna())
        & (merged["days_to_maturity"] >= 7)
        & (merged["days_to_maturity"] <= 120)
        & (merged["option_price"] > 0)
        & (merged["strike"] > 0)
        & (merged["vol_num"] >= 10)
        & (merged["oi_num"] >= 10)
        & (merged["call_put"].isin(["C", "P"]))
    ].copy()
    if merged.empty:
        return []
    rate = (risk_free_rate or 1.7) / 100
    merged["years"] = merged["days_to_maturity"] / 365
    merged["moneyness"] = merged["strike"] / merged["spot_price"]
    merged["iv"] = merged.apply(
        lambda row: _implied_vol(
            float(row["option_price"]),
            float(row["spot_price"]),
            float(row["strike"]),
            float(row["years"]),
            rate,
            str(row["call_put"]),
        ),
        axis=1,
    )
    merged = merged[(merged["iv"].notna()) & (merged["iv"] >= 0.03) & (merged["iv"] <= 1.5)].copy()
    if merged.empty:
        return []

    rows: list[dict[str, Any]] = []
    for underlying_code, group in merged.groupby("underlying_code"):
        nearest_days = int(group["days_to_maturity"].min())
        selected = group[group["days_to_maturity"] == nearest_days].copy()
        if selected.empty:
            continue
        spot = _float(selected["spot_price"].iloc[0])
        if spot is None:
            continue
        atm = selected.assign(distance=(selected["moneyness"] - 1).abs()).sort_values(["distance", "vol_num"], ascending=[True, False]).head(6)
        put_pool = selected[(selected["call_put"] == "P") & (selected["moneyness"] < 1)].copy()
        call_pool = selected[(selected["call_put"] == "C") & (selected["moneyness"] > 1)].copy()
        if put_pool.empty:
            put_pool = selected[selected["call_put"] == "P"].copy()
        if call_pool.empty:
            call_pool = selected[selected["call_put"] == "C"].copy()
        put_pool = put_pool.assign(target_distance=(put_pool["moneyness"] - 0.97).abs()).sort_values(["target_distance", "vol_num"], ascending=[True, False]).head(3)
        call_pool = call_pool.assign(target_distance=(call_pool["moneyness"] - 1.03).abs()).sort_values(["target_distance", "vol_num"], ascending=[True, False]).head(3)
        if put_pool.empty or call_pool.empty:
            continue
        put_iv = float(put_pool["iv"].mean())
        call_iv = float(call_pool["iv"].mean())
        atm_iv = float(atm["iv"].mean()) if not atm.empty else None
        skew_value = put_iv - call_iv
        rows.append(
            {
                "trade_date": trade_date,
                "underlying_code": underlying_code,
                "underlying_name": str(selected["underlying_name"].iloc[0] or underlying_code),
                "maturity_date": str(selected["maturity_date"].iloc[0]),
                "days_to_maturity": nearest_days,
                "spot_price": spot,
                "atm_iv": atm_iv,
                "put_iv": put_iv,
                "call_iv": call_iv,
                "skew_value": skew_value,
                "sample_count": int(len(selected)),
                "metadata_json": {
                    "rate": rate,
                    "put_contracts": put_pool[["ts_code", "strike", "iv", "vol_num", "moneyness"]].to_dict("records"),
                    "call_contracts": call_pool[["ts_code", "strike", "iv", "vol_num", "moneyness"]].to_dict("records"),
                },
            }
        )
    if rows:
        avg_skew = mean([row["skew_value"] for row in rows])
        avg_put_iv = mean([row["put_iv"] for row in rows])
        avg_call_iv = mean([row["call_iv"] for row in rows])
        rows.append(
            {
                "trade_date": trade_date,
                "underlying_code": "ALL",
                "underlying_name": "CFFEX 指数期权",
                "maturity_date": None,
                "days_to_maturity": None,
                "spot_price": None,
                "atm_iv": mean([row["atm_iv"] for row in rows if row.get("atm_iv") is not None]) if any(row.get("atm_iv") is not None for row in rows) else None,
                "put_iv": avg_put_iv,
                "call_iv": avg_call_iv,
                "skew_value": avg_skew,
                "sample_count": sum(row["sample_count"] for row in rows),
                "metadata_json": {"underlying_count": len(rows), "rate": rate},
            }
        )
    return rows


def _save_iv_skew(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    sql = """
    INSERT INTO market_option_iv_skew_daily (
        trade_date, underlying_code, underlying_name, maturity_date, days_to_maturity,
        spot_price, atm_iv, put_iv, call_iv, skew_value, sample_count, metadata_json
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        underlying_name=VALUES(underlying_name), maturity_date=VALUES(maturity_date),
        days_to_maturity=VALUES(days_to_maturity), spot_price=VALUES(spot_price),
        atm_iv=VALUES(atm_iv), put_iv=VALUES(put_iv), call_iv=VALUES(call_iv),
        skew_value=VALUES(skew_value), sample_count=VALUES(sample_count),
        metadata_json=VALUES(metadata_json)
    """
    values = [
        (
            row["trade_date"],
            row["underlying_code"],
            row["underlying_name"],
            row["maturity_date"],
            row["days_to_maturity"],
            row["spot_price"],
            row["atm_iv"],
            row["put_iv"],
            row["call_iv"],
            row["skew_value"],
            row["sample_count"],
            json.dumps(row["metadata_json"], ensure_ascii=False, default=str),
        )
        for row in rows
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
    return rows


def _load_recent_option_pcr_rows(trade_date: str) -> list[dict[str, Any]]:
    sql = """
    SELECT *
    FROM market_option_pcr_daily
    WHERE trade_date = (
        SELECT MAX(trade_date)
        FROM market_option_pcr_daily
        WHERE trade_date <= %s
    )
    ORDER BY exchange
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date,))
            return cursor.fetchall() or []


def _load_recent_futures_holding_rows(trade_date: str) -> list[dict[str, Any]]:
    sql = """
    SELECT *
    FROM market_futures_holding_daily
    WHERE trade_date = (
        SELECT MAX(trade_date)
        FROM market_futures_holding_daily
        WHERE trade_date <= %s
    )
    ORDER BY symbol_family
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date,))
            return cursor.fetchall() or []


def _load_recent_iv_skew_rows(trade_date: str) -> list[dict[str, Any]]:
    sql = """
    SELECT *
    FROM market_option_iv_skew_daily
    WHERE trade_date = (
        SELECT MAX(trade_date)
        FROM market_option_iv_skew_daily
        WHERE trade_date <= %s
    )
    ORDER BY underlying_code
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date,))
            return cursor.fetchall() or []


def _latest_by_date(rows: list[dict[str, Any]], trade_date: str) -> dict[str, Any] | None:
    eligible = [row for row in rows if (_iso_date(row.get("trade_date")) or "") <= trade_date]
    return eligible[-1] if eligible else None


def _source_status_for_row(row: dict[str, Any] | None, trade_date: str) -> str:
    if not row:
        return "待数据"
    source_date = _iso_date(row.get("trade_date"))
    return "已接入" if source_date == trade_date else "沿用最近收盘"


def _source_date_meta(row: dict[str, Any] | None, trade_date: str) -> dict[str, Any]:
    source_date = _iso_date((row or {}).get("trade_date"))
    if not source_date or source_date == trade_date:
        return {}
    return {"source_trade_date": source_date, "target_trade_date": trade_date, "fallback": "latest_close"}


def _load_calibration_history(trade_date: str, limit: int = 120) -> dict[str, list[float]]:
    queries = {
        "iv_skew": (
            """
            SELECT skew_value AS value
            FROM market_option_iv_skew_daily
            WHERE underlying_code = 'ALL' AND trade_date <= %s AND skew_value IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (trade_date, limit),
        ),
        "futures_holding_net": (
            """
            SELECT net_holding_ratio AS value
            FROM market_futures_holding_daily
            WHERE symbol_family = 'ALL' AND trade_date <= %s AND net_holding_ratio IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (trade_date, limit),
        ),
    }
    history: dict[str, list[float]] = {}
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            for indicator_id, (sql, params) in queries.items():
                cursor.execute(sql, params)
                values = [
                    float(row["value"])
                    for row in reversed(cursor.fetchall() or [])
                    if row.get("value") is not None
                ]
                history[indicator_id] = values
    return history


def _trend_component(rows: list[dict[str, Any]], trade_date: str) -> dict[str, Any] | None:
    eligible = [
        row
        for row in rows
        if (_iso_date(row.get("trade_date")) or "") <= trade_date and _float(row.get("close")) is not None
    ]
    if len(eligible) < 20:
        return None
    closes = [_float(row.get("close")) for row in eligible]
    closes = [value for value in closes if value is not None]
    if len(closes) < 20:
        return None
    close = closes[-1]
    ma20 = mean(closes[-20:])
    std20 = pstdev(closes[-20:]) if len(closes[-20:]) > 1 else 0
    band_pos = ((close - ma20) / (2 * std20)) if std20 else 0.0
    score = _clamp(50 + band_pos * 22 + (8 if close >= ma20 else -8))
    return {
        "trade_date": _iso_date(eligible[-1].get("trade_date")),
        "close": close,
        "ma20": ma20,
        "std20": std20,
        "band_pos": band_pos,
        "score": score,
    }


def _build_indicators(
    trade_date: str,
    index_code: str,
    index_rows: list[dict[str, Any]],
    trend_index_rows: dict[str, list[dict[str, Any]]],
    valuation_rows: list[dict[str, Any]],
    margin_rows: list[dict[str, Any]],
    option_pcr_rows: list[dict[str, Any]],
    futures_holding_rows: list[dict[str, Any]],
    qvix_rows: list[dict[str, Any]],
    iv_skew_rows: list[dict[str, Any]],
    bond_yield: float | None,
    bond_meta: dict[str, Any],
    fetch_errors: list[str],
    calibration_history: dict[str, list[float]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    indicators: list[dict[str, Any]] = []
    latest_index = _latest_by_date(index_rows, trade_date)
    latest_val = _latest_by_date(valuation_rows, trade_date)
    latest_margin = _latest_by_date(margin_rows, trade_date)
    latest_option_pcr = next((row for row in option_pcr_rows if row.get("exchange") == "ALL"), None)
    latest_futures_holding = next((row for row in futures_holding_rows if row.get("symbol_family") == "ALL"), None)
    latest_iv_skew = next((row for row in iv_skew_rows if row.get("underlying_code") == "ALL"), None)
    qvix_valid = [row for row in qvix_rows if _float(row.get("close")) is not None]
    pe_values = [_float(row.get("pe_ttm") if row.get("pe_ttm") is not None else row.get("pe")) for row in valuation_rows]
    pe_values = [item for item in pe_values if item is not None and item > 0]
    margin_buy_values = [_float(row.get("rzmre")) for row in margin_rows]
    margin_buy_values = [item for item in margin_buy_values if item is not None and item > 0]

    def add(
        dimension: str,
        indicator_id: str,
        name: str,
        value: float | None,
        value_label: str,
        score: float | None,
        source: str,
        meta: dict[str, Any] | None = None,
        status: str = "已接入",
    ) -> None:
        metadata = dict(meta or {})
        source_trade_date = _iso_date(metadata.get("source_trade_date"))
        effective_score, calibration = calibrate_indicator_score(
            indicator_id,
            score,
            value,
            history_values=calibration_history.get(indicator_id, []),
            source_status=status,
            source_trade_date=source_trade_date,
            target_trade_date=trade_date,
        )
        metadata["calibration"] = calibration
        sig = _signal(effective_score)
        indicators.append(
            {
                "trade_date": trade_date,
                "index_code": index_code,
                "model_id": MODEL_ID,
                "version": MODEL_VERSION,
                "dimension": dimension,
                "indicator_id": indicator_id,
                "indicator_name": name,
                "value": _round(value, 6),
                "value_label": value_label,
                "score": _round(effective_score, 4),
                "signal": sig,
                "signal_label": _signal_label(sig),
                "source_status": status,
                "source": source,
                "metadata_json": metadata,
            }
        )

    primary_trend = _trend_component(index_rows, trade_date)
    if latest_index and primary_trend:
        add(
            "technical",
            "index_bollinger",
            "沪深300布林带",
            primary_trend["band_pos"],
            f"{primary_trend['band_pos']:.2f}σ",
            primary_trend["score"],
            "tushare.index_daily",
            {
                "close": primary_trend["close"],
                "ma20": primary_trend["ma20"],
                "std20": primary_trend["std20"],
                "source_trade_date": primary_trend["trade_date"],
                "target_trade_date": trade_date,
            },
            "已接入" if primary_trend["trade_date"] == trade_date else "沿用最近收盘",
        )
    else:
        add("technical", "index_bollinger", "指数布林带", None, "-", None, "tushare.index_daily", {"reason": "指数日线不足 20 条"}, "待数据")

    multi_components: list[dict[str, Any]] = []
    for trend_code in TREND_INDEX_CODES:
        component = _trend_component(trend_index_rows.get(trend_code, []), trade_date)
        if component:
            multi_components.append({"index_code": trend_code, **component})
    if len(multi_components) >= 2:
        multi_score = mean(component["score"] for component in multi_components)
        multi_band_pos = mean(component["band_pos"] for component in multi_components)
        component_dates = [str(component["trade_date"]) for component in multi_components]
        oldest_component_date = min(component_dates)
        add(
            "technical",
            "multi_index_trend",
            "多指数趋势确认",
            multi_band_pos,
            " / ".join(
                f"{component['index_code']} {component['score']:.1f}"
                for component in multi_components
            ),
            multi_score,
            "tushare.index_daily",
            {
                "components": multi_components,
                "source_trade_date": oldest_component_date,
                "target_trade_date": trade_date,
            },
            "已接入" if all(item == trade_date for item in component_dates) else "沿用最近收盘",
        )
    else:
        add(
            "technical",
            "multi_index_trend",
            "多指数趋势确认",
            None,
            "-",
            None,
            "tushare.index_daily",
            {"reason": "沪深300、中证1000、科创50中可用指数少于两个"},
            "待数据",
        )

    pe_ttm = _float((latest_val or {}).get("pe_ttm")) or _float((latest_val or {}).get("pe"))
    pe_pct = _percentile(pe_values[-252:], pe_ttm)
    if pe_ttm is not None and pe_pct is not None:
        score = _clamp(100 - pe_pct * 100)
        add(
            "valuation",
            "index_pe_percentile",
            "指数估值分位",
            pe_pct,
            f"PE_TTM {pe_ttm:.2f} / 分位 {pe_pct * 100:.1f}%",
            score,
            "tushare.index_dailybasic",
            {
                "pe_ttm": pe_ttm,
                "sample_count": min(len(pe_values), 252),
                **_source_date_meta(latest_val, trade_date),
            },
            _source_status_for_row(latest_val, trade_date),
        )
    else:
        add("valuation", "index_pe_percentile", "指数估值分位", None, "-", None, "tushare.index_dailybasic", {"reason": "指数估值数据为空"}, "待数据")

    if pe_ttm and bond_yield is not None and pe_ttm > 0:
        erp = 100 / pe_ttm - bond_yield
        score = _clamp(50 + (erp - 2.5) * 18)
        add(
            "valuation",
            "erp",
            "ERP/风险溢价",
            erp,
            f"{erp:.2f}%",
            score,
            "tushare.index_dailybasic+tushare.yc_cb+akshare.bond_china_yield",
            {
                "pe_ttm": pe_ttm,
                "bond_yield": bond_yield,
                "bond_meta": bond_meta,
                **_source_date_meta(latest_val, trade_date),
            },
            _source_status_for_row(latest_val, trade_date),
        )
    else:
        add("valuation", "erp", "ERP/风险溢价", None, "-", None, "tushare.index_dailybasic+tushare.yc_cb+akshare.bond_china_yield", {"reason": "缺 PE_TTM 或十年国债收益率", "bond_meta": bond_meta}, "待数据")

    if latest_margin and margin_buy_values:
        rzmre = _float(latest_margin.get("rzmre"))
        avg20 = mean(margin_buy_values[-20:]) if margin_buy_values else None
        ratio = (rzmre / avg20 - 1) if rzmre is not None and avg20 else None
        score = _clamp(50 + (ratio or 0) * 80)
        add(
            "capital",
            "margin_buy_ratio",
            "融资买入额",
            ratio,
            f"较20日均值 {(ratio or 0) * 100:.1f}%",
            score,
            "tushare.margin",
            {"rzmre": rzmre, "avg20": avg20, **_source_date_meta(latest_margin, trade_date)},
            _source_status_for_row(latest_margin, trade_date),
        )
    else:
        add("capital", "margin_buy_ratio", "融资买入额", None, "-", None, "tushare.margin", {"reason": "两融数据为空"}, "待数据")

    if latest_option_pcr:
        volume_pcr = _float(latest_option_pcr.get("volume_pcr"))
        oi_pcr = _float(latest_option_pcr.get("oi_pcr"))
        if volume_pcr is not None:
            # Treat very high put/call as fear pressure and very low PCR as risk-on.
            pcr_component = _clamp(50 - (volume_pcr - 0.9) * 70)
            oi_component = _clamp(50 - ((oi_pcr or 0.9) - 0.9) * 45) if oi_pcr is not None else pcr_component
            score = pcr_component * 0.7 + oi_component * 0.3
            status = _source_status_for_row(latest_option_pcr, trade_date)
            add(
                "sentiment",
                "option_pcr",
                "期权 PCR",
                volume_pcr,
                f"成交PCR {volume_pcr:.2f}" + (f" / 持仓PCR {oi_pcr:.2f}" if oi_pcr is not None else ""),
                score,
                "tushare.opt_daily+opt_basic",
                {
                    "call_volume": latest_option_pcr.get("call_volume"),
                    "put_volume": latest_option_pcr.get("put_volume"),
                    "call_oi": latest_option_pcr.get("call_oi"),
                    "put_oi": latest_option_pcr.get("put_oi"),
                    "contract_count": latest_option_pcr.get("contract_count"),
                    **_source_date_meta(latest_option_pcr, trade_date),
                },
                status,
            )
        else:
            add("sentiment", "option_pcr", "期权 PCR", None, "-", None, "tushare.opt_daily+opt_basic", {"reason": "认购成交量为空"}, "待数据")
    else:
        add("sentiment", "option_pcr", "期权 PCR", None, "-", None, "tushare.opt_daily+opt_basic", {"reason": "期权行情为空"}, "待数据")

    if qvix_valid:
        qvix_scores = []
        qvix_meta = []
        for row in qvix_valid:
            pct = _float(row.get("percentile_252"))
            close = _float(row.get("close"))
            if close is None:
                continue
            score_item = _clamp(100 - (pct * 100 if pct is not None else min(close, 50) * 2))
            qvix_scores.append(score_item)
            qvix_meta.append(
                {
                    "qvix_code": row.get("qvix_code"),
                    "qvix_name": row.get("qvix_name"),
                    "trade_date": row.get("trade_date"),
                    "close": close,
                    "percentile_252": pct,
                    "score": score_item,
                }
            )
        if qvix_scores:
            avg_qvix = mean([_float(row.get("close")) or 0 for row in qvix_valid])
            avg_pct = mean([item["percentile_252"] for item in qvix_meta if item.get("percentile_252") is not None]) if any(item.get("percentile_252") is not None for item in qvix_meta) else None
            score = mean(qvix_scores)
            qvix_source_date = max(
                (_iso_date(row.get("trade_date")) or "")
                for row in qvix_valid
            ) or None
            add(
                "sentiment",
                "qvix_volatility",
                "QVIX 波动率",
                avg_qvix,
                f"均值 {avg_qvix:.2f}" + (f" / 分位 {avg_pct * 100:.1f}%" if avg_pct is not None else ""),
                score,
                "akshare.qvix",
                {
                    "items": qvix_meta,
                    "source_trade_date": qvix_source_date,
                    "target_trade_date": trade_date,
                },
                "已接入" if qvix_source_date == trade_date else "沿用最近收盘",
            )
        else:
            add("sentiment", "qvix_volatility", "QVIX 波动率", None, "-", None, "akshare.qvix", {"reason": "QVIX 分数为空"}, "待数据")
    else:
        add("sentiment", "qvix_volatility", "QVIX 波动率", None, "-", None, "akshare.qvix", {"reason": "QVIX 数据为空"}, "待数据")

    if latest_iv_skew:
        skew_value = _float(latest_iv_skew.get("skew_value"))
        put_iv = _float(latest_iv_skew.get("put_iv"))
        call_iv = _float(latest_iv_skew.get("call_iv"))
        if skew_value is not None:
            score = _clamp(50 - skew_value * 220)
            status = _source_status_for_row(latest_iv_skew, trade_date)
            add(
                "sentiment",
                "iv_skew",
                "IV 偏斜",
                skew_value,
                f"Put-Call {skew_value * 100:.1f}pct" + (f" / Put {put_iv * 100:.1f}% Call {call_iv * 100:.1f}%" if put_iv is not None and call_iv is not None else ""),
                score,
                "tushare.opt_daily+opt_basic+self_calc",
                {
                    "put_iv": put_iv,
                    "call_iv": call_iv,
                    "atm_iv": latest_iv_skew.get("atm_iv"),
                    "sample_count": latest_iv_skew.get("sample_count"),
                    **_source_date_meta(latest_iv_skew, trade_date),
                },
                status,
            )
        else:
            add("sentiment", "iv_skew", "IV 偏斜", None, "-", None, "tushare.opt_daily+opt_basic+self_calc", {"reason": "IV skew 为空"}, "待数据")
    else:
        add("sentiment", "iv_skew", "IV 偏斜", None, "-", None, "tushare.opt_daily+opt_basic+self_calc", {"reason": "CFFEX 指数期权 IV 样本不足"}, "待数据")

    if latest_futures_holding:
        net_ratio = _float(latest_futures_holding.get("net_holding_ratio"))
        net_change = _float(latest_futures_holding.get("net_change"))
        long_holding = _float(latest_futures_holding.get("long_holding")) or 0
        short_holding = _float(latest_futures_holding.get("short_holding")) or 0
        change_base = long_holding + short_holding
        net_change_ratio = net_change / change_base if net_change is not None and change_base else None
        if net_ratio is not None:
            score = _clamp(50 + net_ratio * 220 + (net_change_ratio or 0) * 800)
            status = _source_status_for_row(latest_futures_holding, trade_date)
            add(
                "sentiment",
                "futures_holding_net",
                "股指期货多空持仓",
                net_ratio,
                f"净多占比 {net_ratio * 100:.1f}%",
                score,
                "tushare.fut_holding",
                {
                    "long_holding": long_holding,
                    "short_holding": short_holding,
                    "net_holding": latest_futures_holding.get("net_holding"),
                    "net_change": net_change,
                    "net_change_ratio": net_change_ratio,
                    "row_count": latest_futures_holding.get("row_count"),
                    **_source_date_meta(latest_futures_holding, trade_date),
                },
                status,
            )
        else:
            add("sentiment", "futures_holding_net", "股指期货多空持仓", None, "-", None, "tushare.fut_holding", {"reason": "多空持仓为空"}, "待数据")
    else:
        add("sentiment", "futures_holding_net", "股指期货多空持仓", None, "-", None, "tushare.fut_holding", {"reason": "中金所股指期货持仓为空"}, "待数据")

    amount_pressure, amount_meta = _local_amount_pressure(trade_date)
    if amount_pressure is not None:
        score = _clamp(50 + amount_pressure * 90)
        add("sentiment", "up_down_amount_pressure", "上涨/下跌成交额差", amount_pressure, f"{amount_pressure * 100:.1f}%", score, "daily_kline", amount_meta)
    else:
        add("sentiment", "up_down_amount_pressure", "上涨/下跌成交额差", None, "-", None, "daily_kline", amount_meta, "待数据")

    coverage = {
        "index_daily": "已接入" if latest_index else "待数据",
        "multi_index_trend": "已接入" if len(multi_components) >= 2 else "待数据",
        "index_dailybasic": "已接入" if latest_val else "待数据",
        "margin": "已接入" if latest_margin else "待数据",
        "option_pcr": _source_status_for_row(latest_option_pcr, trade_date),
        "qvix": "已接入" if qvix_valid else "待数据",
        "iv_skew": _source_status_for_row(latest_iv_skew, trade_date),
        "fut_holding": _source_status_for_row(latest_futures_holding, trade_date),
        "yc_cb": "已接入" if bond_yield is not None else "待权限",
        "bond_yield_10y": "已接入" if bond_yield is not None else "待数据",
        "local_amount_pressure": "已接入" if amount_pressure is not None else "待数据",
        "fetch_errors": fetch_errors,
    }
    return indicators, coverage


def _save_indicators(indicators: list[dict[str, Any]]) -> None:
    sql = """
    INSERT INTO market_timing_indicator_daily (
        trade_date, index_code, model_id, version, dimension, indicator_id, indicator_name, value, value_label,
        score, signal_value, signal_label, source_status, source, metadata_json
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        version=VALUES(version), dimension=VALUES(dimension), indicator_name=VALUES(indicator_name), value=VALUES(value),
        value_label=VALUES(value_label), score=VALUES(score), signal_value=VALUES(signal_value),
        signal_label=VALUES(signal_label), source_status=VALUES(source_status),
        source=VALUES(source), metadata_json=VALUES(metadata_json)
    """
    values = [
        (
            item["trade_date"],
            item["index_code"],
            item.get("model_id") or MODEL_ID,
            item.get("version") or MODEL_VERSION,
            item["dimension"],
            item["indicator_id"],
            item["indicator_name"],
            item["value"],
            item["value_label"],
            item["score"],
            item["signal"],
            item["signal_label"],
            item["source_status"],
            item["source"],
            json.dumps(item["metadata_json"], ensure_ascii=False, default=str),
        )
        for item in indicators
    ]
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)


def _save_signal(trade_date: str, index_code: str, indicators: list[dict[str, Any]], coverage: dict[str, Any]) -> dict[str, Any]:
    composition = compose_timing_state(indicators, weights=DAILY_WEIGHTS)
    valid = [item for item in indicators if item.get("score") is not None]
    reasons = [
        f"{item['dimension_label']} {item['score']:.1f}分，{item['signal_label']}"
        for item in composition["dimensions"]
    ]
    risk_notes = []
    if coverage.get("bond_yield_10y") != "已接入":
        risk_notes.append("十年国债收益率未接入，ERP 暂不参与总分")
    if coverage.get("multi_index_trend") != "已接入":
        risk_notes.append("多指数趋势覆盖不足，趋势维度按现有宽基降级")
    if composition["confidence"] < 0.75:
        risk_notes.append("部分因子缺数据，择时置信度降低")
    stale_count = sum(1 for item in valid if item.get("source_status") == "沿用最近收盘")
    if stale_count:
        risk_notes.append(f"{stale_count} 个因子沿用最近收盘，已进行时效衰减")
    limited_calibration = []
    for item in valid:
        calibration = (item.get("metadata_json") or {}).get("calibration") or {}
        if item.get("indicator_id") in {"iv_skew", "futures_holding_net"} and int(calibration.get("history_count") or 0) < 40:
            limited_calibration.append(str(item.get("indicator_name") or item.get("indicator_id")))
    if limited_calibration:
        risk_notes.append(f"{'、'.join(limited_calibration)}历史不足40日，滚动校准仍采用渐进置信度")

    coverage_payload = dict(coverage)
    coverage_payload.update(
        {
            "model_id": MODEL_ID,
            "version": MODEL_VERSION,
            "dimension_vote_sum": composition["dimension_vote_sum"],
            "dimension_scores": composition["dimension_scores"],
            "dimension_signals": composition["dimension_signals"],
        }
    )

    sql = """
    INSERT INTO market_timing_signal_daily (
        trade_date, index_code, model_id, model_name, version, combined_signal,
        timing_score, state, state_label, position_upper, confidence,
        reasons_json, risk_notes_json, coverage_json, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        model_name=VALUES(model_name), version=VALUES(version), combined_signal=VALUES(combined_signal),
        timing_score=VALUES(timing_score), state=VALUES(state), state_label=VALUES(state_label),
        position_upper=VALUES(position_upper), confidence=VALUES(confidence),
        reasons_json=VALUES(reasons_json), risk_notes_json=VALUES(risk_notes_json),
        coverage_json=VALUES(coverage_json), source=VALUES(source)
    """
    payload = {
        "trade_date": trade_date,
        "index_code": index_code,
        "model_id": MODEL_ID,
        "model_name": MODEL_NAME,
        "version": MODEL_VERSION,
        "combined_signal": composition["combined_signal"],
        "timing_score": composition["timing_score"],
        "state": composition["state"],
        "state_label": composition["state_label"],
        "position_upper": composition["position_upper"],
        "confidence": composition["confidence"],
        "reasons": reasons,
        "risk_notes": risk_notes,
        "coverage": coverage_payload,
        "dimensions": composition["dimensions"],
        "dimension_vote_sum": composition["dimension_vote_sum"],
    }
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    trade_date,
                    index_code,
                    MODEL_ID,
                    MODEL_NAME,
                    MODEL_VERSION,
                    payload["combined_signal"],
                    payload["timing_score"],
                    payload["state"],
                    payload["state_label"],
                    payload["position_upper"],
                    payload["confidence"],
                    json.dumps(reasons, ensure_ascii=False),
                    json.dumps(risk_notes, ensure_ascii=False),
                    json.dumps(coverage_payload, ensure_ascii=False, default=str),
                    "market_timing_v19_calibrated_sources",
                ),
            )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Update market timing raw data and daily factor scores.")
    parser.add_argument("--trade-date", help="YYYY-MM-DD; default latest daily_kline trade_date")
    parser.add_argument("--index-code", default="000300.SH")
    parser.add_argument("--lookback-days", type=int, default=420)
    parser.add_argument("--source-timeout-seconds", type=float, default=45.0)
    parser.add_argument("--total-timeout-seconds", type=float, default=300.0)
    args = parser.parse_args()

    if args.source_timeout_seconds <= 0:
        parser.error("--source-timeout-seconds must be greater than 0")
    if args.total_timeout_seconds <= 0:
        parser.error("--total-timeout-seconds must be greater than 0")

    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock_handle is None:
        print(json.dumps({"status": "skipped", "reason": "lock_unavailable", "lock_name": LOCK_NAME}, ensure_ascii=False))
        return

    logger = TaskRunLogger()
    logger_started = False
    trade_date: str | None = None
    run_id: str | None = None
    total_deadline = time.monotonic() + args.total_timeout_seconds
    try:
        trade_date = _parse_date(args.trade_date)
        start_date = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=args.lookback_days)).strftime("%Y-%m-%d")
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise RuntimeError("TUSHARE_TOKEN 未配置")
        pro = ts.pro_api(token)
        run_id = f"market_timing_{trade_date.replace('-', '')}_{args.index_code.replace('.', '')}"
        run_metadata = {
            "trade_date": trade_date,
            "index_code": args.index_code,
            "trend_index_codes": list(dict.fromkeys((args.index_code, *TREND_INDEX_CODES))),
            "model_id": MODEL_ID,
            "version": MODEL_VERSION,
            "lookback_days": args.lookback_days,
            "source_timeout_seconds": args.source_timeout_seconds,
            "total_timeout_seconds": args.total_timeout_seconds,
        }
        logger.start(TASK_NAME, run_id, run_metadata)
        logger_started = True
        fetch_errors: list[str] = []

        def safe_call(label: str, func):
            return _safe_call(
                label,
                func,
                source_timeout_seconds=args.source_timeout_seconds,
                total_deadline=total_deadline,
            )

        trend_index_rows: dict[str, list[dict[str, Any]]] = {}
        trend_codes = tuple(dict.fromkeys((args.index_code, *TREND_INDEX_CODES)))
        for trend_code in trend_codes:
            rows, error = safe_call(
                f"index_daily:{trend_code}",
                lambda code=trend_code: _fetch_index_daily(pro, code, start_date, trade_date),
            )
            if error:
                fetch_errors.append(error)
                rows = []
            trend_index_rows[trend_code] = rows or []
        index_rows = trend_index_rows.get(args.index_code, [])
        valuation_rows, error = safe_call("index_dailybasic", lambda: _fetch_index_valuation(pro, args.index_code, start_date, trade_date))
        if error:
            fetch_errors.append(error)
            valuation_rows = []
        margin_source_rows, error = safe_call("margin", lambda: _fetch_margin(pro, start_date, trade_date))
        if error:
            fetch_errors.append(error)
            margin_source_rows = []
        option_rows, error = safe_call("opt_daily", lambda: _fetch_option_daily(pro, trade_date))
        if error:
            fetch_errors.append(error)
            option_rows = []
        option_basic, error = safe_call("opt_basic", lambda: _fetch_option_basic(pro))
        if error:
            fetch_errors.append(error)
            option_basic = {}
        cffex_option_basic_rows, error = safe_call("opt_basic_cffex", lambda: _fetch_cffex_option_basic_rows(pro))
        if error:
            fetch_errors.append(error)
            cffex_option_basic_rows = []
        futures_source_rows, error = safe_call("fut_holding", lambda: _fetch_futures_holding(pro, trade_date))
        if error:
            fetch_errors.append(error)
            futures_source_rows = []
        qvix_source_rows, error = safe_call("qvix", lambda: _fetch_qvix_rows(trade_date))
        if error:
            fetch_errors.append(error)
            qvix_source_rows = []
        bond_result, error = safe_call("yc_cb", lambda: _fetch_bond_yield(pro, trade_date))
        if error:
            fetch_errors.append(error)
            bond_yield, bond_meta = None, {"error": error}
        else:
            bond_yield, bond_meta = bond_result or (None, {})

        _ensure_total_deadline(total_deadline)
        _save_index_daily(
            [
                row
                for rows in trend_index_rows.values()
                for row in rows
            ]
        )
        _save_index_valuation(valuation_rows)
        margin_rows = _save_margin(margin_source_rows)
        option_pcr_rows = _save_option_pcr(trade_date, option_rows, option_basic)
        futures_holding_rows = _save_futures_holding(trade_date, futures_source_rows)
        qvix_rows = _save_qvix(qvix_source_rows)
        _save_bond_yield(trade_date, bond_yield, bond_meta)
        underlying_prices, error = safe_call(
            "index_closes",
            lambda: _fetch_index_closes(pro, [item["code"] for item in INDEX_OPTION_UNDERLYINGS.values()], trade_date),
        )
        if error:
            fetch_errors.append(error)
            underlying_prices = {}
        iv_skew_rows = _save_iv_skew(
            _build_iv_skew_rows(trade_date, option_rows, cffex_option_basic_rows, underlying_prices, bond_yield)
        )
        if not option_pcr_rows:
            option_pcr_rows = _load_recent_option_pcr_rows(trade_date)
        if not futures_holding_rows:
            futures_holding_rows = _load_recent_futures_holding_rows(trade_date)
        if not iv_skew_rows:
            iv_skew_rows = _load_recent_iv_skew_rows(trade_date)
        calibration_history = _load_calibration_history(trade_date)
        _ensure_total_deadline(total_deadline)
        indicators, coverage = _build_indicators(
            trade_date,
            args.index_code,
            index_rows,
            trend_index_rows,
            valuation_rows,
            margin_rows,
            option_pcr_rows,
            futures_holding_rows,
            qvix_rows,
            iv_skew_rows,
            bond_yield,
            bond_meta,
            fetch_errors,
            calibration_history,
        )
        _save_indicators(indicators)
        payload = _save_signal(trade_date, args.index_code, indicators, coverage)
        payload["indicator_count"] = len(indicators)
        payload["fetch_errors"] = fetch_errors
        payload["source_timeout_seconds"] = args.source_timeout_seconds
        payload["total_timeout_seconds"] = args.total_timeout_seconds
        logger.finish(TASK_NAME, run_id, "success", f"market timing updated, score={payload['timing_score']}", payload)
        print(json.dumps(payload, ensure_ascii=False, default=str))
    except Exception as exc:
        if logger_started and run_id:
            error_code = "upstream_timeout" if isinstance(exc, (MarketTimingSourceTimeout, MarketTimingTotalTimeout)) else None
            logger.finish(
                TASK_NAME,
                run_id,
                "failed",
                str(exc)[:500],
                {
                    "trade_date": trade_date,
                    "index_code": args.index_code,
                    "source_timeout_seconds": args.source_timeout_seconds,
                    "total_timeout_seconds": args.total_timeout_seconds,
                },
                error_code=error_code,
            )
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock_handle)
        if release_error:
            print(f"warning: failed to release {LOCK_NAME}: {release_error}", file=sys.stderr)


if __name__ == "__main__":
    main()
