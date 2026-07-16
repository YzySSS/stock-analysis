#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]

import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.error_learning.models import SelectionTrackingRecord
from app.error_learning.tracker import SelectionResultTracker
from app.shared.db import mysql_conn
from app.stock_selection.review_rules import ReviewRuleConfig, evaluate_review_rules


def _to_date(value: Any) -> Optional[date]:
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


def _to_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _avg(values: Iterable[Optional[float]]) -> Optional[float]:
    valid = [value for value in values if value is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 4)


def _summary(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    returns = [_to_float(row.get("price_change_pct")) for row in records]
    valid_returns = [value for value in returns if value is not None]
    if not records:
        return {
            "count": 0,
            "valid_return_count": 0,
            "avg_return_pct": None,
            "median_return_pct": None,
            "win_rate_pct": None,
            "best_return_pct": None,
            "worst_return_pct": None,
            "avg_max_gain_pct": None,
            "avg_max_drawdown_pct": None,
        }
    sorted_returns = sorted(valid_returns)
    median = None
    if sorted_returns:
        mid = len(sorted_returns) // 2
        if len(sorted_returns) % 2:
            median = sorted_returns[mid]
        else:
            median = (sorted_returns[mid - 1] + sorted_returns[mid]) / 2
    return {
        "count": len(records),
        "valid_return_count": len(valid_returns),
        "avg_return_pct": _avg(valid_returns),
        "median_return_pct": round(median, 4) if median is not None else None,
        "win_rate_pct": round(
            sum(1 for value in valid_returns if value > 0) / len(valid_returns) * 100,
            4,
        )
        if valid_returns
        else None,
        "best_return_pct": round(max(valid_returns), 4) if valid_returns else None,
        "worst_return_pct": round(min(valid_returns), 4) if valid_returns else None,
        "avg_max_gain_pct": _avg(row.get("max_gain_pct") for row in records),
        "avg_max_drawdown_pct": _avg(row.get("max_drawdown_pct") for row in records),
    }


def _combo_summary(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rule_names = tuple(sorted(row.get("review_rule_hits") or {}))
        key = "+".join(rule_names) if rule_names else "none"
        grouped[key].append(row)
    return {key: _summary(value) for key, value in sorted(grouped.items())}


def _fetch_as_of_close(code: str, trade_date: str) -> Optional[float]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT close
                FROM daily_kline
                WHERE code = %s AND trade_date <= %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (code, trade_date),
            )
            row = cursor.fetchone()
    return _to_float((row or {}).get("close"))


def _prior_return_as_of(
    prior: SelectionTrackingRecord,
    current_selection_date: str,
) -> Optional[float]:
    base_price = _to_float(prior.selected_open_price) or _to_float(prior.selected_close_price)
    if base_price is None or base_price <= 0:
        return None
    as_of_close = _fetch_as_of_close(prior.code, current_selection_date)
    if as_of_close is None:
        return _to_float(prior.price_change_pct)
    return round((as_of_close - base_price) / base_price * 100, 4)


def _record_to_row(record: SelectionTrackingRecord) -> Dict[str, Any]:
    return {
        **asdict(record),
        "factor_scores": record.factor_scores or {},
        "sentiment_context": record.sentiment_context or {},
        "reason_summary": record.reason_summary or [],
        "risk_summary": record.risk_summary or [],
    }


def _load_records(args: argparse.Namespace) -> List[SelectionTrackingRecord]:
    tracker = SelectionResultTracker()
    records = tracker.build_latest_selection_snapshot(
        limit=args.limit,
        instrument_type="stock",
        strategy_id=args.strategy_id,
        latest_only=False,
    )
    result = []
    for record in records:
        if not record.include_in_stats:
            continue
        if args.start_date and record.selection_date < args.start_date:
            continue
        if args.end_date and record.selection_date > args.end_date:
            continue
        result.append(record)
    result.sort(key=lambda item: (item.selection_date or "", item.selection_datetime or "", item.rank_no or 9999, item.code))
    return result


def analyze(args: argparse.Namespace) -> Dict[str, Any]:
    records = _load_records(args)
    config = ReviewRuleConfig(
        repeat_loss_window_days=args.repeat_window_days,
        price_confirm_weak_threshold=args.price_confirm_weak_threshold,
        price_confirm_watch_threshold=args.price_confirm_watch_threshold,
        low_match_recognition_threshold=args.low_match_recognition_threshold,
    )

    previous_by_code: Dict[str, List[SelectionTrackingRecord]] = defaultdict(list)
    rows: List[Dict[str, Any]] = []
    for record in records:
        current_date = _to_date(record.selection_date)
        prior_selection = None
        prior_return = None
        if current_date:
            for prior in reversed(previous_by_code[record.code]):
                prior_date = _to_date(prior.selection_date)
                if not prior_date:
                    continue
                if (current_date - prior_date).days > config.repeat_loss_window_days:
                    continue
                prior_return = _prior_return_as_of(prior, record.selection_date)
                prior_selection = prior
                break

        hits = evaluate_review_rules(
            factor_scores=record.factor_scores,
            sentiment_context=record.sentiment_context,
            reason_summary=record.reason_summary,
            risk_summary=record.risk_summary,
            prior_selection_date=prior_selection.selection_date if prior_selection else None,
            prior_return_pct=prior_return,
            config=config,
        )
        row = _record_to_row(record)
        row["review_rule_hits"] = hits
        rows.append(row)
        previous_by_code[record.code].append(record)

    by_rule = {}
    rule_names = sorted({name for row in rows for name in row["review_rule_hits"]})
    for rule_name in rule_names:
        hit_rows = [row for row in rows if rule_name in row["review_rule_hits"]]
        clean_rows = [row for row in rows if rule_name not in row["review_rule_hits"]]
        by_rule[rule_name] = {
            "hit": _summary(hit_rows),
            "without_hit": _summary(clean_rows),
            "examples": [
                {
                    "selection_date": row.get("selection_date"),
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "rank_no": row.get("rank_no"),
                    "return_pct": row.get("price_change_pct"),
                    "rule_reason": row["review_rule_hits"][rule_name].get("reason"),
                }
                for row in hit_rows[:10]
            ],
        }

    hard_rule_names = {"repeat_prev_loss", "low_match"}
    all_flagged_rows = [row for row in rows if row["review_rule_hits"]]
    hard_flagged_rows = [
        row for row in rows if hard_rule_names.intersection(row["review_rule_hits"].keys())
    ]
    recommended_filter_rows = [
        row
        for row in rows
        if "low_match" in row["review_rule_hits"]
        or (
            "price_confirm_weak" in row["review_rule_hits"]
            and "repeat_prev_loss" in row["review_rule_hits"]
        )
    ]
    no_flag_rows = [row for row in rows if not row["review_rule_hits"]]
    without_all_flagged = [row for row in rows if not row["review_rule_hits"]]
    without_hard_flagged = [
        row for row in rows if not hard_rule_names.intersection(row["review_rule_hits"].keys())
    ]
    without_recommended_filter = [
        row
        for row in rows
        if row not in recommended_filter_rows
    ]

    return {
        "strategy_id": args.strategy_id,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "config": asdict(config),
        "baseline": _summary(rows),
        "all_flagged": _summary(all_flagged_rows),
        "no_flag": _summary(no_flag_rows),
        "without_all_flagged": _summary(without_all_flagged),
        "hard_flagged": _summary(hard_flagged_rows),
        "without_hard_flagged": _summary(without_hard_flagged),
        "recommended_filter": _summary(recommended_filter_rows),
        "without_recommended_filter": _summary(without_recommended_filter),
        "combination_summary": _combo_summary(rows),
        "by_rule": by_rule,
        "rows": [
            {
                "selection_date": row.get("selection_date"),
                "code": row.get("code"),
                "name": row.get("name"),
                "rank_no": row.get("rank_no"),
                "score": row.get("score"),
                "return_pct": row.get("price_change_pct"),
                "max_gain_pct": row.get("max_gain_pct"),
                "max_drawdown_pct": row.get("max_drawdown_pct"),
                "rules": row.get("review_rule_hits"),
            }
            for row in rows
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze A-share sentiment review rules on tracked selection results.")
    parser.add_argument("--strategy-id", default="a_share_sentiment")
    parser.add_argument("--start-date", default="2026-06-01")
    parser.add_argument("--end-date", default="2026-07-31")
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--repeat-window-days", type=int, default=30)
    parser.add_argument("--price-confirm-weak-threshold", type=float, default=35.0)
    parser.add_argument("--price-confirm-watch-threshold", type=float, default=45.0)
    parser.add_argument("--low-match-recognition-threshold", type=float, default=58.0)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    result = analyze(args)
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main()
