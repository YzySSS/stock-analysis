from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.orchestration.strategy_factor_ci_schema import ensure_strategy_factor_ci_schema
from app.shared.db import mysql_conn
from app.shared.strategy_loader import StrategyLoader
from app.shared.task_log import TaskRunLogger
from app.stock_selection.selector import StockSelector
from app.strategies.service import StrategyService


TASK_NAME = "strategy_factor_ci_daily_update"


def build_run_id() -> str:
    return f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _to_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _mean(values: Iterable[float]) -> Optional[float]:
    clean = [value for value in values if math.isfinite(value)]
    return sum(clean) / len(clean) if clean else None


def _pearson(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    dx = [x - mean_x for x in xs]
    dy = [y - mean_y for y in ys]
    denom_x = math.sqrt(sum(item * item for item in dx))
    denom_y = math.sqrt(sum(item * item for item in dy))
    if denom_x <= 0 or denom_y <= 0:
        return None
    return sum(a * b for a, b in zip(dx, dy)) / (denom_x * denom_y)


def _ranks(values: List[float]) -> List[float]:
    indexed = sorted(enumerate(values), key=lambda pair: pair[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        rank = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = rank
        i = j + 1
    return ranks


def _spearman(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    return _pearson(_ranks(xs), _ranks(ys))


def fetch_trade_dates(limit: int = 260) -> List[str]:
    sql = """
    SELECT DISTINCT trade_date
    FROM daily_kline
    ORDER BY trade_date DESC
    LIMIT %s
    """
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (limit,))
            rows = cursor.fetchall() or []
    return sorted(str(row["trade_date"]) for row in rows)


def latest_eligible_trade_date(horizon_days: int) -> Optional[str]:
    dates = fetch_trade_dates(limit=max(30, horizon_days + 10))
    if len(dates) <= horizon_days:
        return None
    return dates[-(horizon_days + 1)]


def fetch_forward_returns(trade_date: str, horizon_days: int) -> tuple[Dict[str, float], Optional[str]]:
    dates = fetch_trade_dates(limit=260)
    if trade_date not in dates:
        return {}, None
    target_index = dates.index(trade_date) + horizon_days
    if target_index >= len(dates):
        return {}, None
    target_date = dates[target_index]
    sql = """
    SELECT d0.code, d0.close AS base_close, d1.close AS target_close
    FROM daily_kline d0
    INNER JOIN daily_kline d1 ON d0.code = d1.code
    WHERE d0.trade_date = %s
      AND d1.trade_date = %s
      AND d0.close IS NOT NULL
      AND d1.close IS NOT NULL
      AND d0.close > 0
    """
    returns: Dict[str, float] = {}
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (trade_date, target_date))
            for row in cursor.fetchall() or []:
                base_close = _to_float(row.get("base_close"))
                target_close = _to_float(row.get("target_close"))
                if base_close and target_close is not None:
                    returns[str(row["code"])] = (target_close / base_close - 1) * 100
    return returns, target_date


def runtime_strategy_ids(requested: Optional[List[str]] = None) -> List[str]:
    loader = StrategyLoader()
    allowed = set(StrategyService.RUNTIME_READY_IDS)
    strategy_ids = [
        item.get("id")
        for item in loader.registry.get("strategies", [])
        if item.get("id") in allowed and bool(item.get("executable", True))
    ]
    if requested:
        requested_set = set(requested)
        strategy_ids = [strategy_id for strategy_id in strategy_ids if strategy_id in requested_set]
    return [strategy_id for strategy_id in strategy_ids if strategy_id]


def factor_names(strategy_id: str) -> Dict[str, str]:
    config = StrategyLoader().load_config(strategy_id)
    return {
        key: str(value.get("name") or key)
        for key, value in (config.get("factors", {}) or {}).items()
    }


def compute_strategy_factor_ci(
    strategy_id: str,
    trade_date: str,
    horizon_days: int,
    instrument_type: str,
) -> Dict[str, Any]:
    selector = StockSelector(
        strategy_id=strategy_id,
        strategy_overrides={"as_of_datetime": f"{trade_date} 15:30:00"},
    )
    data_bundle = selector.load_candidates_from_mysql(candidate_limit=None, instrument_type=instrument_type)
    context = selector.strategy.prepare_context(data_bundle)
    factor_rows = selector.strategy.compute_factors(context)
    returns, target_date = fetch_forward_returns(trade_date, horizon_days)
    names = factor_names(strategy_id)
    factor_keys = sorted({key for item in factor_rows for key in (item.get("factors") or {}).keys()})
    payload: List[Dict[str, Any]] = []
    for factor_key in factor_keys:
        total = len(factor_rows)
        xs: List[float] = []
        ys: List[float] = []
        present_values: List[float] = []
        for item in factor_rows:
            factor_value = _to_float((item.get("factors") or {}).get(factor_key))
            if factor_value is None:
                continue
            present_values.append(factor_value)
            forward_return = returns.get(str(item.get("code")))
            if forward_return is None:
                continue
            xs.append(factor_value)
            ys.append(forward_return)
        present = len(present_values)
        coverage = round((present / total) * 100, 4) if total else None
        ic = _pearson(xs, ys)
        rank_ic = _spearman(xs, ys)
        payload.append(
            {
                "strategy_id": strategy_id,
                "instrument_type": instrument_type,
                "trade_date": trade_date,
                "horizon_days": horizon_days,
                "factor_key": factor_key,
                "factor_name": names.get(factor_key, factor_key),
                "sample_size": total,
                "valid_sample_size": len(xs),
                "coverage": coverage,
                "missing_rate": round(100 - coverage, 4) if coverage is not None else None,
                "factor_mean": _mean(present_values),
                "forward_return_mean_pct": _mean(ys),
                "ic": ic,
                "rank_ic": rank_ic,
                "ci": rank_ic,
                "metadata_json": {
                    "target_trade_date": target_date,
                    "return_label": f"close_to_close_{horizon_days}d",
                    "candidate_count_before_strategy_filters": len(data_bundle.get("candidates") or []),
                    "ci_definition": "rank_ic_factor_score_vs_forward_return",
                },
            }
        )
    return {
        "strategy_id": strategy_id,
        "trade_date": trade_date,
        "target_trade_date": target_date,
        "factor_rows": len(factor_rows),
        "records": payload,
    }


def save_records(records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0
    sql = """
    INSERT INTO strategy_factor_ci_daily (
        strategy_id, instrument_type, trade_date, horizon_days, factor_key, factor_name,
        sample_size, valid_sample_size, coverage, missing_rate, factor_mean,
        forward_return_mean_pct, ic, rank_ic, ci, source, metadata_json, computed_at
    )
    VALUES (
        %(strategy_id)s, %(instrument_type)s, %(trade_date)s, %(horizon_days)s, %(factor_key)s, %(factor_name)s,
        %(sample_size)s, %(valid_sample_size)s, %(coverage)s, %(missing_rate)s, %(factor_mean)s,
        %(forward_return_mean_pct)s, %(ic)s, %(rank_ic)s, %(ci)s, 'daily_full_sample', %(metadata_json)s, %(computed_at)s
    )
    ON DUPLICATE KEY UPDATE
        factor_name = VALUES(factor_name),
        sample_size = VALUES(sample_size),
        valid_sample_size = VALUES(valid_sample_size),
        coverage = VALUES(coverage),
        missing_rate = VALUES(missing_rate),
        factor_mean = VALUES(factor_mean),
        forward_return_mean_pct = VALUES(forward_return_mean_pct),
        ic = VALUES(ic),
        rank_ic = VALUES(rank_ic),
        ci = VALUES(ci),
        source = VALUES(source),
        metadata_json = VALUES(metadata_json),
        computed_at = VALUES(computed_at)
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for record in records:
        rows.append({
            **record,
            "metadata_json": json.dumps(record.get("metadata_json") or {}, ensure_ascii=False),
            "computed_at": now,
        })
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
    return len(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily full-sample strategy factor CI update")
    parser.add_argument("--trade-date", default=None, help="Base trade date. Defaults to latest date with forward return labels.")
    parser.add_argument("--horizon-days", type=int, default=1)
    parser.add_argument("--instrument-type", default="stock")
    parser.add_argument("--strategy-id", action="append", default=None, help="Run only selected strategy. Can be repeated.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    ensure_strategy_factor_ci_schema()
    trade_date = args.trade_date or latest_eligible_trade_date(args.horizon_days)
    run_id = build_run_id()
    logger = TaskRunLogger()
    metadata = {
        "trade_date": trade_date,
        "horizon_days": args.horizon_days,
        "instrument_type": args.instrument_type,
        "strategy_ids": args.strategy_id,
    }
    logger.start(TASK_NAME, run_id, metadata)
    try:
        if not trade_date:
            payload = {**metadata, "saved_records": 0, "message": "no eligible trade date"}
            logger.finish(TASK_NAME, run_id, "success", "no eligible trade date", payload)
            print(json.dumps(payload, ensure_ascii=False))
            return
        strategy_ids = runtime_strategy_ids(args.strategy_id)
        saved_records = 0
        strategy_summaries = []
        for strategy_id in strategy_ids:
            summary = compute_strategy_factor_ci(
                strategy_id=strategy_id,
                trade_date=trade_date,
                horizon_days=args.horizon_days,
                instrument_type=args.instrument_type,
            )
            saved = save_records(summary["records"])
            saved_records += saved
            strategy_summaries.append({
                "strategy_id": strategy_id,
                "factor_rows": summary["factor_rows"],
                "records": saved,
                "target_trade_date": summary["target_trade_date"],
            })
        payload = {
            **metadata,
            "trade_date": trade_date,
            "strategy_count": len(strategy_ids),
            "saved_records": saved_records,
            "strategies": strategy_summaries,
        }
        logger.finish(TASK_NAME, run_id, "success", f"saved_records={saved_records}", payload)
        print(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.finish(TASK_NAME, run_id, "failed", str(exc)[:500], metadata)
        raise


if __name__ == "__main__":
    main()
