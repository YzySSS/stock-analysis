#!/usr/bin/env python3
"""Analyze lowvol_reversal factor Rank IC on the full historical candidate pool.

This is intentionally separate from the backtest result tables: backtest_pick only
stores selected rows, while IC should be computed over the full valid cross-section
for each trade date.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Iterable

from app.backtest.service import BacktestService
from app.stock_selection.selector import StockSelector


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def pct_return(entry: Any, exit_price: Any) -> float | None:
    entry_value = to_float(entry)
    exit_value = to_float(exit_price)
    if entry_value is None or exit_value is None or entry_value <= 0:
        return None
    return (exit_value - entry_value) / entry_value * 100


def ranks(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    output = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        average_rank = (i + j) / 2 + 1
        for k in range(i, j + 1):
            output[indexed[k][0]] = average_rank
        i = j + 1
    return output


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    mx = mean(xs)
    my = mean(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def spearman(pairs: Iterable[tuple[Any, Any]]) -> float | None:
    cleaned = [(to_float(x), to_float(y)) for x, y in pairs]
    cleaned = [(x, y) for x, y in cleaned if x is not None and y is not None]
    if len(cleaned) < 20:
        return None
    return pearson(ranks([x for x, _ in cleaned]), ranks([y for _, y in cleaned]))


def summarize_values(values: list[float]) -> dict[str, Any]:
    values = [v for v in values if v is not None and math.isfinite(v)]
    if not values:
        return {"count": 0, "mean": None, "std": None, "positive_rate": None, "icir": None}
    std = pstdev(values) if len(values) > 1 else 0.0
    return {
        "count": len(values),
        "mean": round(mean(values), 6),
        "std": round(std, 6),
        "positive_rate": round(sum(1 for v in values if v > 0) / len(values) * 100, 2),
        "icir": round(mean(values) / std, 6) if std else None,
    }


def quintile_returns(rows: list[dict[str, Any]], factor: str) -> list[dict[str, Any]]:
    valid = [row for row in rows if to_float(row.get(factor)) is not None and to_float(row.get("return_1d_pct")) is not None]
    if len(valid) < 20:
        return []
    valid.sort(key=lambda row: float(row[factor]))
    buckets: list[dict[str, Any]] = []
    for index in range(5):
        start = int(len(valid) * index / 5)
        end = int(len(valid) * (index + 1) / 5)
        bucket = valid[start:end]
        returns = [float(row["return_1d_pct"]) for row in bucket]
        buckets.append({
            "bucket": index + 1,
            "count": len(bucket),
            "avg_return_1d_pct": round(mean(returns), 6) if returns else None,
        })
    return buckets


def analyze(args: argparse.Namespace) -> dict[str, Any]:
    service = BacktestService()
    selector = StockSelector(
        strategy_id="lowvol_reversal",
        strategy_overrides={"max_picks": args.max_picks, "score_threshold": args.score_threshold},
    )
    trade_dates = service._fetch_trade_dates(args.start_date, args.end_date)
    if args.max_days:
        trade_dates = trade_dates[: args.max_days]

    daily: list[dict[str, Any]] = []
    monthly_ic: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    all_rows: list[dict[str, Any]] = []
    factor_names = ["score", "turnover", "lowvol", "reversal"]

    for offset, trade_date in enumerate(trade_dates, start=1):
        candidates = service._load_candidates(selector, trade_date, args.instrument_type)
        context = selector.strategy.prepare_context({"candidates": candidates})
        factor_rows = selector.strategy.compute_factors(context)
        scored = selector.strategy.score(factor_rows)
        codes = [row.get("code") for row in scored if row.get("code")]
        future = service._fetch_future_bars(codes, trade_date, lookahead=2)

        rows: list[dict[str, Any]] = []
        for row in scored:
            code = row.get("code")
            bars = future.get(code) or []
            exit_bar = bars[1] if len(bars) > 1 else None
            ret = pct_return(row.get("open"), exit_bar.get("open") if exit_bar else None)
            if ret is None:
                continue
            factors = row.get("factors") or {}
            record = {
                "trade_date": trade_date,
                "code": code,
                "return_1d_pct": ret,
                "score": to_float(row.get("score")),
                "turnover": to_float(factors.get("turnover")),
                "lowvol": to_float(factors.get("lowvol")),
                "reversal": to_float(factors.get("reversal")),
            }
            rows.append(record)

        ic_row: dict[str, Any] = {
            "trade_date": trade_date,
            "candidate_count": len(candidates),
            "scored_count": len(scored),
            "labeled_count": len(rows),
        }
        ym = trade_date[:7]
        for factor in factor_names:
            value = spearman((row.get(factor), row.get("return_1d_pct")) for row in rows)
            ic_row[f"{factor}_rank_ic"] = round(value, 6) if value is not None else None
            if value is not None:
                monthly_ic[ym][factor].append(value)
        if args.include_quintiles:
            ic_row["quintiles"] = {factor: quintile_returns(rows, factor) for factor in factor_names}
        daily.append(ic_row)
        all_rows.extend(rows)
        print(f"[{offset}/{len(trade_dates)}] {trade_date} labeled={len(rows)} score_ic={ic_row.get('score_rank_ic')}")

    monthly_summary = []
    for ym in sorted(monthly_ic):
        item = {"ym": ym}
        for factor in factor_names:
            item[factor] = summarize_values(monthly_ic[ym][factor])
        monthly_summary.append(item)

    overall = {}
    for factor in factor_names:
        overall[factor] = summarize_values([row.get(f"{factor}_rank_ic") for row in daily if row.get(f"{factor}_rank_ic") is not None])
        overall[f"{factor}_pooled_spearman"] = round(
            spearman((row.get(factor), row.get("return_1d_pct")) for row in all_rows) or 0,
            6,
        )

    return {
        "strategy_id": "lowvol_reversal",
        "strategy_version": selector.strategy_meta.get("version"),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "return_mode": "1d_open_to_next_open",
        "trade_days": len(trade_dates),
        "overall": overall,
        "monthly": monthly_summary,
        "daily": daily,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze full-candidate Rank IC for lowvol_reversal")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-05-10")
    parser.add_argument("--instrument-type", default="stock")
    parser.add_argument("--max-picks", type=int, default=3)
    parser.add_argument("--score-threshold", type=float, default=60)
    parser.add_argument("--max-days", type=int, default=None, help="Optional smoke-test cap from the start date")
    parser.add_argument("--include-quintiles", action="store_true")
    parser.add_argument("--output", default="reports/lowvol_factor_ic.json")
    args = parser.parse_args()

    result = analyze(args)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
