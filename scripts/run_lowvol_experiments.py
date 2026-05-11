#!/usr/bin/env python3
"""Run lowvol_reversal v2.1 follow-up experiments offline.

Experiments:
- baseline_v21: current low turnover + low volatility + 5d reversal.
- g1_no_reversal: remove reversal, rank by lowvol + turnover only.
- g2_momentum_5d: reverse the failed reversal direction; prefer stronger 5d momentum.
- g3_reversal_market_strong: use reversal only when market breadth is positive; otherwise neutral.
- g4_reversal_as_filter: don't score reversal; filter extreme 5d drops only.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any

from app.backtest.service import BacktestService
from app.stock_selection.selector import StockSelector


EXPERIMENTS = {
    "baseline_v21": "current v2.1 equal-weight low turnover + lowvol + 5d reversal",
    "g1_no_reversal": "remove reversal; score lowvol + turnover",
    "g2_momentum_5d": "prefer positive 5d momentum instead of 5d reversal",
    "g3_reversal_market_strong": "enable reversal only when market breadth is positive",
    "g4_reversal_as_filter": "use reversal as risk filter only; score lowvol + turnover",
}


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
    return round((exit_value - entry_value) / entry_value * 100, 4)


def percentile_scores(values: list[float | None], higher_is_better: bool = True) -> list[float]:
    non_missing = sorted(v for v in values if v is not None and math.isfinite(v))
    if not values:
        return []
    if not non_missing:
        return [50.0 for _ in values]
    median = non_missing[len(non_missing) // 2]
    filled = [v if v is not None and math.isfinite(v) else median for v in values]
    if not higher_is_better:
        filled = [-v for v in filled]
    n = len(filled)
    if n == 1:
        return [50.0]
    indexed = sorted(enumerate(filled), key=lambda pair: pair[1])
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        percentile = ((i + j) / 2) / (n - 1) * 100
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = percentile
        i = j + 1
    return [round(score, 4) for score in ranks]


def ranks(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda x: x[1])
    out = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            out[indexed[k][0]] = avg
        i = j + 1
    return out


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 20 or len(xs) != len(ys):
        return None
    mx = mean(xs); my = mean(ys)
    vx = sum((x - mx) ** 2 for x in xs); vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def spearman(rows: list[dict[str, Any]], x_field: str, y_field: str = "return_1d_pct") -> float | None:
    pairs = [(to_float(r.get(x_field)), to_float(r.get(y_field))) for r in rows]
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    if len(pairs) < 20:
        return None
    return pearson(ranks([x for x, _ in pairs]), ranks([y for _, y in pairs]))


def is_valid_candidate(item: dict[str, Any]) -> bool:
    if item.get("is_st"):
        return False
    if int(item.get("kline_count_20") or 0) < 15:
        return False
    if int(item.get("kline_count_60") or 0) < 60:
        return False
    listed_days = item.get("listed_days")
    if listed_days is not None and int(listed_days) < 60:
        return False
    close = to_float(item.get("close"))
    if close is None or close <= 2:
        return False
    avg_amount = to_float(item.get("avg_amount_20"))
    if avg_amount is None or avg_amount < 5_000_000:
        return False
    pct_chg_1d = to_float(item.get("pct_chg_1d"))
    if pct_chg_1d is not None and pct_chg_1d <= -9.5:
        return False
    turnover_rate = to_float(item.get("turnover_rate"))
    if turnover_rate is not None and turnover_rate >= 20:
        return False
    return True


def factorize(candidates: list[dict[str, Any]], experiment: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    valid = [item for item in candidates if is_valid_candidate(item)]
    breadth_values = [to_float(item.get("pct_chg_1d")) for item in valid]
    breadth_values = [v for v in breadth_values if v is not None]
    positive_breadth_pct = (sum(1 for v in breadth_values if v > 0) / len(breadth_values) * 100) if breadth_values else None
    market_strong = bool(positive_breadth_pct is not None and positive_breadth_pct >= 50)

    rows = []
    for item in valid:
        close = to_float(item.get("close"))
        close_5d = to_float(item.get("close_5d"))
        ma60 = to_float(item.get("ma60"))
        return_5d = (close - close_5d) / close_5d if close is not None and close_5d and close_5d > 0 else None
        trend_ok = bool(close is not None and ma60 is not None and close > ma60)
        rows.append({
            "item": item,
            "raw_turnover": to_float(item.get("turnover_rate_5d_avg")) or to_float(item.get("turnover_rate")),
            "raw_lowvol": to_float(item.get("std_return_20")),
            "raw_return_5d": return_5d if trend_ok else None,
            "trend_ok": trend_ok,
        })

    turnover_scores = percentile_scores([r["raw_turnover"] for r in rows], higher_is_better=False)
    lowvol_scores = percentile_scores([r["raw_lowvol"] for r in rows], higher_is_better=False)
    reversal_scores = percentile_scores([r["raw_return_5d"] for r in rows], higher_is_better=False)
    momentum_scores = percentile_scores([r["raw_return_5d"] for r in rows], higher_is_better=True)

    scored = []
    for idx, row in enumerate(rows):
        item = row["item"]
        turnover = turnover_scores[idx]
        lowvol = lowvol_scores[idx]
        reversal = reversal_scores[idx] if row["trend_ok"] else 50.0
        momentum = momentum_scores[idx] if row["trend_ok"] else 50.0
        return_5d = row["raw_return_5d"]

        if experiment == "g1_no_reversal":
            score = 0.5 * turnover + 0.5 * lowvol
            factors = {"turnover": turnover, "lowvol": lowvol}
        elif experiment == "g2_momentum_5d":
            score = (turnover + lowvol + momentum) / 3
            factors = {"turnover": turnover, "lowvol": lowvol, "momentum": momentum}
        elif experiment == "g3_reversal_market_strong":
            gated_reversal = reversal if market_strong else 50.0
            score = (turnover + lowvol + gated_reversal) / 3
            factors = {"turnover": turnover, "lowvol": lowvol, "reversal": gated_reversal}
        elif experiment == "g4_reversal_as_filter":
            if return_5d is not None and return_5d <= -0.08:
                continue
            score = 0.5 * turnover + 0.5 * lowvol
            factors = {"turnover": turnover, "lowvol": lowvol, "reversal_filter_return_5d": return_5d}
        else:
            score = (turnover + lowvol + reversal) / 3
            factors = {"turnover": turnover, "lowvol": lowvol, "reversal": reversal}

        scored.append({
            **item,
            "score": round(score, 4),
            "factors": factors,
            "raw_return_5d": return_5d,
            "market_strong": market_strong,
            "positive_breadth_pct": positive_breadth_pct,
        })

    scored.sort(key=lambda x: (-float(x.get("score") or 0), -float((x.get("factors") or {}).get("lowvol") or 0), -float(x.get("avg_amount_20") or 0), str(x.get("code") or "")))
    return scored, {"positive_breadth_pct": positive_breadth_pct, "market_strong": market_strong, "valid_count": len(valid)}


def run(args: argparse.Namespace) -> dict[str, Any]:
    service = BacktestService()
    selector = StockSelector(strategy_id="lowvol_reversal", strategy_overrides={"max_picks": args.max_picks, "score_threshold": args.score_threshold})
    trade_dates = service._fetch_trade_dates(args.start_date, args.end_date)
    if args.max_days:
        trade_dates = trade_dates[:args.max_days]

    state = {name: {"daily": [], "trades": [], "picks": [], "daily_ic": []} for name in EXPERIMENTS}
    for idx, trade_date in enumerate(trade_dates, start=1):
        candidates = service._load_candidates(selector, trade_date, args.instrument_type)
        codes = [row.get("code") for row in candidates if row.get("code")]
        future = service._fetch_future_bars(codes, trade_date, lookahead=2)
        return_by_code = {}
        for row in candidates:
            code = row.get("code")
            bars = future.get(code) or []
            exit_bar = bars[1] if len(bars) > 1 else None
            ret = pct_return(row.get("open"), exit_bar.get("open") if exit_bar else None)
            if ret is not None:
                return_by_code[code] = ret

        for name in EXPERIMENTS:
            scored, diagnostics = factorize(candidates, name)
            labeled = []
            for row in scored:
                ret = return_by_code.get(row.get("code"))
                if ret is None:
                    continue
                labeled.append({**row, "return_1d_pct": ret})
            ic = spearman(labeled, "score")
            selected = [row for row in labeled if float(row.get("score") or 0) >= args.score_threshold][:args.max_picks]
            returns = [row["return_1d_pct"] for row in selected]
            daily_return = round(mean(returns), 4) if returns else None
            state[name]["daily"].append({"trade_date": trade_date, "daily_return_pct": daily_return, "pick_count": len(selected), **diagnostics})
            state[name]["trades"].extend({"trade_date": trade_date, "code": row.get("code"), "name": row.get("name"), "score": row.get("score"), "return_1d_pct": row.get("return_1d_pct"), "factors": row.get("factors")} for row in selected)
            state[name]["daily_ic"].append({"trade_date": trade_date, "score_rank_ic": round(ic, 6) if ic is not None else None, "labeled_count": len(labeled)})
        print(f"[{idx}/{len(trade_dates)}] {trade_date}", flush=True)

    result = {"start_date": args.start_date, "end_date": args.end_date, "experiments": {}}
    for name, data in state.items():
        equity = 1.0; peak = 1.0; max_dd = 0.0
        for row in data["daily"]:
            if row.get("daily_return_pct") is not None:
                equity *= 1 + float(row["daily_return_pct"]) / 100
            peak = max(peak, equity)
            max_dd = min(max_dd, (equity - peak) / peak * 100 if peak else 0)
        trade_returns = [t["return_1d_pct"] for t in data["trades"] if t.get("return_1d_pct") is not None]
        ic_values = [x["score_rank_ic"] for x in data["daily_ic"] if x.get("score_rank_ic") is not None]
        monthly = {}
        for row in data["daily"]:
            ym = row["trade_date"][:7]
            monthly.setdefault(ym, []).append(row.get("daily_return_pct"))
        result["experiments"][name] = {
            "description": EXPERIMENTS[name],
            "summary": {
                "trade_days": len(data["daily"]),
                "trade_count": len(trade_returns),
                "total_return_pct": round((equity - 1) * 100, 4),
                "avg_trade_return_pct": round(mean(trade_returns), 4) if trade_returns else None,
                "win_rate_pct": round(sum(1 for v in trade_returns if v > 0) / len(trade_returns) * 100, 4) if trade_returns else None,
                "max_drawdown_pct": round(max_dd, 4),
                "mean_score_rank_ic": round(mean(ic_values), 6) if ic_values else None,
                "positive_ic_rate_pct": round(sum(1 for v in ic_values if v > 0) / len(ic_values) * 100, 2) if ic_values else None,
            },
            "monthly_return_pct": {ym: round(sum(v for v in vals if v is not None), 4) for ym, vals in monthly.items()},
            "daily": data["daily"],
            "daily_ic": data["daily_ic"],
            "trades": data["trades"],
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run lowvol follow-up experiments")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-05-10")
    parser.add_argument("--instrument-type", default="stock")
    parser.add_argument("--max-picks", type=int, default=3)
    parser.add_argument("--score-threshold", type=float, default=60)
    parser.add_argument("--max-days", type=int)
    parser.add_argument("--output", default="reports/lowvol_experiments_20260511.json")
    args = parser.parse_args()
    result = run(args)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    print(f"wrote {output}")
    for name, payload in result["experiments"].items():
        print(name, payload["summary"])


if __name__ == "__main__":
    main()
