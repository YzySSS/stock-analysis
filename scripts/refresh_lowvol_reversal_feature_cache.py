#!/usr/bin/env python3
"""Refresh cached historical window features for lowvol_reversal.

The backtest candidate loader originally recalculated 5/20/60-day windows from
`daily_kline` for every backtest trade date. That is correct but slow. This
script materializes those point-in-time features once per code/trade_date so
backtests and full-candidate IC analysis can reuse them.

Implementation note: this uses Python rolling windows instead of SQL window
functions. On the current MySQL host, filtering a CTE after window calculation
still caused large scans; fetching a bounded lookback range and grouping in
Python is more predictable for incremental refreshes.
"""

from __future__ import annotations

import argparse
import math
from collections import defaultdict, deque
from datetime import datetime, timedelta
from statistics import mean, stdev
from typing import Any, Sequence

from app.shared.db import mysql_conn


UPSERT_SQL = """
INSERT INTO lowvol_reversal_feature_daily (
    code, trade_date, ma20, ma60, close_5d, close_20d, prev_close_1d,
    max_close_20, min_close_20, avg_amount_20, kline_count_20, kline_count_60,
    std_return_20, pct_chg_1d, turnover_rate_5d_avg
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    ma20=VALUES(ma20),
    ma60=VALUES(ma60),
    close_5d=VALUES(close_5d),
    close_20d=VALUES(close_20d),
    prev_close_1d=VALUES(prev_close_1d),
    max_close_20=VALUES(max_close_20),
    min_close_20=VALUES(min_close_20),
    avg_amount_20=VALUES(avg_amount_20),
    kline_count_20=VALUES(kline_count_20),
    kline_count_60=VALUES(kline_count_60),
    std_return_20=VALUES(std_return_20),
    pct_chg_1d=VALUES(pct_chg_1d),
    turnover_rate_5d_avg=VALUES(turnover_rate_5d_avg),
    updated_at=CURRENT_TIMESTAMP
"""


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def avg(values: list[float]) -> float | None:
    return round(mean(values), 6) if values else None


def std(values: list[float]) -> float | None:
    return round(stdev(values), 10) if len(values) >= 2 else None


def refresh(
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int = 180,
    codes: Sequence[str] | None = None,
) -> dict[str, Any]:
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required for bounded feature refresh")

    lookback_start = (datetime.strptime(start_date, "%Y-%m-%d").date() - timedelta(days=lookback_days)).isoformat()

    normalized_codes = sorted({str(code) for code in (codes or []) if code})
    code_filter = ""
    code_params: list[Any] = []
    if normalized_codes:
        code_filter = f" AND code IN ({','.join(['%s'] * len(normalized_codes))})"
        code_params = normalized_codes

    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT code, trade_date, close, amount
                FROM daily_kline
                WHERE trade_date BETWEEN %s AND %s
                {code_filter}
                ORDER BY code, trade_date
                """,
                [lookback_start, end_date, *code_params],
            )
            kline_rows = cursor.fetchall() or []
            cursor.execute(
                f"""
                SELECT code, trade_date, turnover_rate
                FROM factor_input_daily
                WHERE trade_date BETWEEN %s AND %s
                  AND turnover_rate IS NOT NULL
                {code_filter}
                ORDER BY code, trade_date
                """,
                [lookback_start, end_date, *code_params],
            )
            turnover_rows = cursor.fetchall() or []

    turnover_by_key: dict[tuple[str, str], float | None] = {}
    turnover_windows: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=5))
    for row in turnover_rows:
        code = row["code"]
        trade_date = str(row["trade_date"])
        value = to_float(row.get("turnover_rate"))
        if value is not None:
            turnover_windows[code].append(value)
        turnover_by_key[(code, trade_date)] = avg(list(turnover_windows[code]))

    payload: list[tuple[Any, ...]] = []
    close_windows: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=60))
    amount_windows: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=20))
    return_windows: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=20))
    prev_close_by_code: dict[str, float] = {}

    for row in kline_rows:
        code = row["code"]
        trade_date = str(row["trade_date"])
        close = to_float(row.get("close"))
        amount = to_float(row.get("amount"))
        if close is None:
            continue

        closes_before = list(close_windows[code])
        prev_close = prev_close_by_code.get(code)
        pct_chg = ((close - prev_close) / prev_close * 100) if prev_close and prev_close > 0 else None
        ret = (close / prev_close - 1) if prev_close and prev_close > 0 else None

        close_windows[code].append(close)
        if amount is not None:
            amount_windows[code].append(amount)
        if ret is not None:
            return_windows[code].append(ret)
        prev_close_by_code[code] = close

        if trade_date < start_date or trade_date > end_date:
            continue

        closes = list(close_windows[code])
        closes20 = closes[-20:]
        closes60 = closes[-60:]
        amounts20 = list(amount_windows[code])[-20:]
        payload.append(
            (
                code,
                trade_date,
                avg(closes20),
                avg(closes60),
                round(closes_before[-5], 6) if len(closes_before) >= 5 else None,
                round(closes_before[-19], 6) if len(closes_before) >= 19 else None,
                round(prev_close, 6) if prev_close is not None else None,
                round(max(closes20), 6) if closes20 else None,
                round(min(closes20), 6) if closes20 else None,
                avg(amounts20),
                len(closes20),
                len(closes60),
                std(list(return_windows[code])[-20:]),
                round(pct_chg, 8) if pct_chg is not None else None,
                turnover_by_key.get((code, trade_date)),
            )
        )

    affected = 0
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            if payload:
                affected = cursor.executemany(UPSERT_SQL, payload)
            cursor.execute(
                """
                SELECT COUNT(*) AS total_rows, MIN(trade_date) AS min_trade_date, MAX(trade_date) AS max_trade_date
                FROM lowvol_reversal_feature_daily
                """
            )
            summary = cursor.fetchone() or {}
        conn.commit()

    return {
        "affected_rows": affected,
        "payload_rows": len(payload),
        "filtered_codes": len(normalized_codes),
        **summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh lowvol reversal historical feature cache")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--lookback-days", type=int, default=180)
    parser.add_argument("--codes", help="Optional comma-separated code list for a bounded targeted refresh")
    args = parser.parse_args()
    result = refresh(
        args.start_date,
        args.end_date,
        args.lookback_days,
        [code.strip() for code in args.codes.split(",") if code.strip()] if args.codes else None,
    )
    print(result)


if __name__ == "__main__":
    main()
