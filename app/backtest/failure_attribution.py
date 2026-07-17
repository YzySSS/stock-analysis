from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from math import sqrt
from statistics import mean, median
from typing import Any, Iterable, Sequence

from app.backtest.failure_attribution_repository import StrategyFailureAttributionRepository


REPORT_VERSION = "strategy_failure_attribution_v1"
HORIZON_EXIT_FIELDS = {
    1: "exit_price_1d",
    3: "exit_price_3d",
    5: "exit_price_5d",
    10: "exit_price_10d",
}


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _as_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def compound_pct(values: Iterable[float]) -> float:
    equity = 1.0
    for value in values:
        equity *= 1 + float(value) / 100
    return round((equity - 1) * 100, 4)


@dataclass(frozen=True)
class TransactionCosts:
    commission_bps: float
    stamp_tax_bps: float
    slippage_bps: float

    @property
    def buy_rate(self) -> float:
        return (self.commission_bps + self.slippage_bps) / 10_000

    @property
    def sell_rate(self) -> float:
        return (self.commission_bps + self.stamp_tax_bps + self.slippage_bps) / 10_000

    @property
    def round_trip_drag_pct(self) -> float:
        return round((1 - (1 - self.sell_rate) / (1 + self.buy_rate)) * 100, 6)

    def gross_return_pct(self, entry_price: Any, exit_price: Any) -> float | None:
        entry = _as_float(entry_price)
        exit_value = _as_float(exit_price)
        if entry is None or exit_value is None or entry <= 0 or exit_value <= 0:
            return None
        return (exit_value / entry - 1) * 100

    def net_return_pct(self, entry_price: Any, exit_price: Any) -> float | None:
        entry = _as_float(entry_price)
        exit_value = _as_float(exit_price)
        if entry is None or exit_value is None or entry <= 0 or exit_value <= 0:
            return None
        effective_entry = entry * (1 + self.buy_rate)
        effective_exit = exit_value * (1 - self.sell_rate)
        return (effective_exit / effective_entry - 1) * 100


def summarize_values(values: Sequence[float | None]) -> dict[str, Any]:
    clean = [number for value in values if (number := _as_float(value)) is not None]
    return {
        "sample_count": len(clean),
        "missing_count": len(values) - len(clean),
        "mean_pct": round(mean(clean), 6) if clean else None,
        "median_pct": round(median(clean), 6) if clean else None,
        "win_rate_pct": round(sum(value > 0 for value in clean) / len(clean) * 100, 4) if clean else None,
    }


def _average_ranks(values: Sequence[float]) -> list[float]:
    ordered = sorted(range(len(values)), key=lambda index: values[index])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index
        while end + 1 < len(ordered) and values[ordered[end + 1]] == values[ordered[index]]:
            end += 1
        average_rank = (index + end) / 2 + 1
        for cursor in range(index, end + 1):
            ranks[ordered[cursor]] = average_rank
        index = end + 1
    return ranks


def spearman_correlation(pairs: Sequence[tuple[Any, Any]]) -> float | None:
    clean: list[tuple[float, float]] = []
    for left, right in pairs:
        left_value = _as_float(left)
        right_value = _as_float(right)
        if left_value is not None and right_value is not None:
            clean.append((left_value, right_value))
    if len(clean) < 3:
        return None
    left_ranks = _average_ranks([left for left, _ in clean])
    right_ranks = _average_ranks([right for _, right in clean])
    left_mean = mean(left_ranks)
    right_mean = mean(right_ranks)
    covariance = sum(
        (left - left_mean) * (right - right_mean)
        for left, right in zip(left_ranks, right_ranks)
    )
    left_variance = sum((value - left_mean) ** 2 for value in left_ranks)
    right_variance = sum((value - right_mean) ** 2 for value in right_ranks)
    if left_variance <= 0 or right_variance <= 0:
        return None
    return round(covariance / sqrt(left_variance * right_variance), 6)


def non_overlapping_cohorts(
    signal_dates: Sequence[date],
    rows: Sequence[dict[str, Any]],
    horizon: int,
    costs: TransactionCosts,
) -> list[dict[str, Any]]:
    exit_field = HORIZON_EXIT_FIELDS[horizon]
    rows_by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_date[_as_date(row["trade_date"])].append(row)
    cohorts: list[dict[str, Any]] = []
    for offset in range(horizon):
        scheduled_dates = signal_dates[offset::horizon]
        gross_periods: list[float] = []
        net_periods: list[float] = []
        missing_trades = 0
        incomplete_rebalances = 0
        for signal_date in scheduled_dates:
            date_rows = rows_by_date.get(signal_date, [])
            if not date_rows:
                gross_periods.append(0.0)
                net_periods.append(0.0)
                continue
            gross = [costs.gross_return_pct(row.get("entry_price"), row.get(exit_field)) for row in date_rows]
            net = [costs.net_return_pct(row.get("entry_price"), row.get(exit_field)) for row in date_rows]
            missing_trades += sum(value is None for value in gross)
            valid_gross = [value for value in gross if value is not None]
            valid_net = [value for value in net if value is not None]
            if len(valid_gross) != len(date_rows) or len(valid_net) != len(date_rows):
                incomplete_rebalances += 1
            if not valid_gross or not valid_net:
                continue
            gross_periods.append(mean(valid_gross))
            net_periods.append(mean(valid_net))
        cohorts.append(
            {
                "offset": offset,
                "scheduled_rebalance_count": len(scheduled_dates),
                "valid_rebalance_count": len(gross_periods),
                "incomplete_rebalance_count": incomplete_rebalances,
                "missing_trade_returns": missing_trades,
                "gross_compound_pct": compound_pct(gross_periods),
                "net_compound_pct": compound_pct(net_periods),
            }
        )
    return cohorts


def retention_diagnostics(
    signal_dates: Sequence[date],
    rows: Sequence[dict[str, Any]],
    costs: TransactionCosts,
) -> dict[str, Any]:
    codes_by_date: dict[date, set[str]] = defaultdict(set)
    rows_by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        signal_date = _as_date(row["trade_date"])
        codes_by_date[signal_date].add(str(row["code"]))
        rows_by_date[signal_date].append(row)
    overlaps: list[int] = []
    retention_rates: list[float] = []
    replacement_cycles = 0
    previous: set[str] = set()
    approximate_daily_returns: list[float] = []
    for index, signal_date in enumerate(signal_dates):
        current = codes_by_date.get(signal_date, set())
        if index == 0:
            new_count = len(current)
        else:
            overlap = len(previous & current)
            overlaps.append(overlap)
            retention_rates.append(overlap / len(previous) if previous else 0.0)
            new_count = len(current - previous)
        replacement_cycles += new_count
        date_rows = rows_by_date.get(signal_date, [])
        gross_values = [
            costs.gross_return_pct(row.get("entry_price"), row.get("exit_price_1d"))
            for row in date_rows
        ]
        valid_gross = [value for value in gross_values if value is not None]
        gross_period = mean(valid_gross) if valid_gross else 0.0
        position_count = max(len(current), 1)
        cost_fraction = min(new_count / position_count, 1.0) if current else 0.0
        approximate_factor = (1 + gross_period / 100) * (
            ((1 - costs.sell_rate) / (1 + costs.buy_rate)) ** cost_fraction
        )
        approximate_daily_returns.append((approximate_factor - 1) * 100)
        previous = current
    return {
        "adjacent_pair_count": len(overlaps),
        "average_overlap_count": round(mean(overlaps), 6) if overlaps else None,
        "average_retention_pct": round(mean(retention_rates) * 100, 4) if retention_rates else None,
        "full_churn_round_trips": len(rows),
        "replacement_round_trips": replacement_cycles,
        "replacement_reduction_pct": round((1 - replacement_cycles / len(rows)) * 100, 4) if rows else None,
        "retention_aware_cost_approx_compound_pct": compound_pct(approximate_daily_returns),
        "approximation_note": "保留股延续持仓，仅对新进入仓位计一次完整往返成本；不改变原始每日选股与毛收益。",
    }


class StrategyFailureAttributionService:
    def __init__(self, repository: StrategyFailureAttributionRepository | None = None) -> None:
        self.repository = repository or StrategyFailureAttributionRepository()

    @staticmethod
    def _costs(run: dict[str, Any]) -> TransactionCosts:
        request = _as_json(run.get("request_json"))
        commission = _as_float(request.get("commission_bps"))
        stamp_tax = _as_float(request.get("stamp_tax_bps"))
        slippage = _as_float(request.get("slippage_bps"))
        return TransactionCosts(
            commission_bps=commission if commission is not None else float(run.get("commission_bps") or 0),
            stamp_tax_bps=stamp_tax if stamp_tax is not None else float(run.get("stamp_tax_bps") or 0),
            slippage_bps=slippage if slippage is not None else float(run.get("slippage_bps") or 0),
        )

    @staticmethod
    def _regimes(market_rows: Sequence[dict[str, Any]]) -> dict[date, str]:
        rows = [row for row in market_rows if _as_float(row.get("close")) is not None]
        closes = [float(row["close"]) for row in rows]
        regimes: dict[date, str] = {}
        for index in range(59, len(rows)):
            current = closes[index]
            moving_average_60 = mean(closes[index - 59 : index + 1])
            return_20 = current / closes[index - 20] - 1
            state = "uptrend" if current > moving_average_60 and return_20 > 0 else (
                "downtrend" if current < moving_average_60 and return_20 < 0 else "mixed"
            )
            regimes[_as_date(rows[index]["trade_date"])] = state
        return regimes

    @staticmethod
    def _factor_rows(rows: Sequence[dict[str, Any]], costs: TransactionCosts) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        for row in rows:
            factor_json = _as_json(row.get("factor_json"))
            factors = factor_json.get("factors") or {}
            raw_metrics = factor_json.get("raw_metrics") or {}
            item = dict(row)
            item["factors"] = factors
            item["raw_metrics"] = raw_metrics
            item["base_score"] = raw_metrics.get("base_score_before_price_preference")
            for horizon, exit_field in HORIZON_EXIT_FIELDS.items():
                item[f"gross_{horizon}d"] = costs.gross_return_pct(row.get("entry_price"), row.get(exit_field))
                item[f"net_{horizon}d"] = costs.net_return_pct(row.get("entry_price"), row.get(exit_field))
            parsed.append(item)
        return parsed

    @staticmethod
    def _daily_compound(
        signal_dates: Sequence[date],
        rows: Sequence[dict[str, Any]],
        return_field: str,
    ) -> float:
        rows_by_date: dict[date, list[float]] = defaultdict(list)
        for row in rows:
            value = _as_float(row.get(return_field))
            if value is not None:
                rows_by_date[_as_date(row["trade_date"])].append(value)
        daily = [mean(rows_by_date[signal_date]) if rows_by_date.get(signal_date) else 0.0 for signal_date in signal_dates]
        return compound_pct(daily)

    @staticmethod
    def _market_regime_summary(
        rows: Sequence[dict[str, Any]],
        regimes: dict[date, str],
    ) -> dict[str, Any]:
        grouped: dict[str, dict[str, list[float | None]]] = defaultdict(lambda: defaultdict(list))
        missing_dates: set[date] = set()
        for row in rows:
            signal_date = _as_date(row["trade_date"])
            state = regimes.get(signal_date)
            if state is None:
                missing_dates.add(signal_date)
                continue
            for horizon in HORIZON_EXIT_FIELDS:
                grouped[state][f"gross_{horizon}d"].append(row.get(f"gross_{horizon}d"))
                grouped[state][f"net_{horizon}d"].append(row.get(f"net_{horizon}d"))
        return {
            "covered_trade_rows": sum(
                len(values.get("gross_1d", [])) for values in grouped.values()
            ),
            "missing_signal_dates": [str(value) for value in sorted(missing_dates)],
            "states": {
                state: {
                    f"{horizon}d": {
                        "gross": summarize_values(values[f"gross_{horizon}d"]),
                        "net": summarize_values(values[f"net_{horizon}d"]),
                    }
                    for horizon in (1, 3, 5)
                }
                for state, values in sorted(grouped.items())
            },
            "definition": "沪深300信号日收盘价相对60日均线，并结合过去20日方向；只使用信号日及此前数据。",
        }

    @staticmethod
    def _classification(report: dict[str, Any]) -> dict[str, Any]:
        gross = float(report["one_day_portfolio"]["gross_compound_pct"])
        net = float(report["one_day_portfolio"]["net_compound_pct"])
        score_ic = report["factor_information_coefficient"]["1d"].get("score")
        retention = report["turnover"]["average_retention_pct"]
        if gross <= 0 and (score_ic is None or score_ic <= 0):
            failure_mode = "factor_direction_failure"
        elif gross > 0 and net < 0 and retention is not None and retention < 25:
            failure_mode = "weak_signal_destroyed_by_turnover_costs"
        elif net < 0:
            failure_mode = "negative_net_edge"
        else:
            failure_mode = "not_failed_by_primary_net_return_check"
        cohort_checks = {}
        for horizon in (3, 5, 10):
            cohorts = report["non_overlapping_cohorts"][f"{horizon}d"]
            cohort_checks[f"{horizon}d_all_offsets_positive"] = bool(cohorts) and all(
                row["net_compound_pct"] > 0
                and row["missing_trade_returns"] == 0
                and row["incomplete_rebalance_count"] == 0
                and row["valid_rebalance_count"] == row["scheduled_rebalance_count"]
                for row in cohorts
            )
        return {
            "failure_mode": failure_mode,
            "current_version_decision": (
                "reject_as_effective_strategy" if net < 0 else "no_promotion_from_historical_attribution"
            ),
            "cohort_robustness_checks": cohort_checks,
            "execution_only_revision_supported": any(cohort_checks.values()),
            "next_research_action": (
                "freeze_old_evidence_and_define_an_independent_signal_family"
                if not any(cohort_checks.values())
                else "freeze_a_separate_execution_version_for_new_prospective_validation"
            ),
            "anti_overfitting_note": "不按历史最佳 offset、月份、排名或阈值回调参数；所有 offset 必须同时通过才视为执行假设稳健。",
        }

    def build_report(self, run_id: str, benchmark_index_code: str = "000300.SH") -> dict[str, Any]:
        run = self.repository.load_run(run_id)
        if not run:
            raise ValueError(f"backtest run not found: {run_id}")
        if str(run.get("status")) != "success":
            raise ValueError(f"backtest run must be successful: {run_id}")
        raw_signal_dates = self.repository.load_signal_dates(run_id)
        signal_dates = [_as_date(row["trade_date"]) for row in raw_signal_dates]
        if not signal_dates:
            raise ValueError(f"backtest run has no daily summaries: {run_id}")
        raw_rows = self.repository.load_trade_rows(run_id)
        costs = self._costs(run)
        rows = self._factor_rows(raw_rows, costs)
        market_rows = self.repository.load_market_rows(benchmark_index_code, max(signal_dates))
        regimes = self._regimes(market_rows)

        horizon_summary = {}
        for horizon in HORIZON_EXIT_FIELDS:
            horizon_summary[f"{horizon}d"] = {
                "gross": summarize_values([row[f"gross_{horizon}d"] for row in rows]),
                "net": summarize_values([row[f"net_{horizon}d"] for row in rows]),
            }
        factor_keys = ("score", "base_score", "turnover", "lowvol", "reversal")
        factor_ic = {}
        for horizon in (1, 3, 5):
            factor_ic[f"{horizon}d"] = {}
            for factor in factor_keys:
                pairs = []
                for row in rows:
                    factor_value = row.get(factor)
                    if factor in {"turnover", "lowvol", "reversal"}:
                        factor_value = (row.get("factors") or {}).get(factor)
                    pairs.append((factor_value, row.get(f"gross_{horizon}d")))
                factor_ic[f"{horizon}d"][factor] = spearman_correlation(pairs)

        score_values = [value for row in rows if (value := _as_float(row.get("score"))) is not None]

        report = {
            "report_version": REPORT_VERSION,
            "run": {
                "run_id": run_id,
                "strategy_id": run.get("strategy_id"),
                "strategy_version": run.get("strategy_version"),
                "start_date": str(run.get("start_date")),
                "end_date": str(run.get("end_date")),
                "sample_days": len(signal_dates),
                "trade_rows": len(rows),
                "research_only": True,
            },
            "costs": {
                "commission_bps_each_side": costs.commission_bps,
                "stamp_tax_bps_sell": costs.stamp_tax_bps,
                "slippage_bps_each_side": costs.slippage_bps,
                "round_trip_drag_pct": costs.round_trip_drag_pct,
            },
            "trade_horizons": horizon_summary,
            "one_day_portfolio": {
                "gross_compound_pct": self._daily_compound(signal_dates, rows, "gross_1d"),
                "net_compound_pct": self._daily_compound(signal_dates, rows, "net_1d"),
            },
            "factor_information_coefficient": factor_ic,
            "rank_summary": {
                str(rank): {
                    f"{horizon}d_gross": summarize_values(
                        [row[f"gross_{horizon}d"] for row in rows if int(row.get("rank_no") or 0) == rank]
                    )
                    for horizon in (1, 3, 5)
                }
                for rank in (1, 2, 3)
            },
            "turnover": retention_diagnostics(signal_dates, rows, costs),
            "non_overlapping_cohorts": {
                f"{horizon}d": non_overlapping_cohorts(signal_dates, rows, horizon, costs)
                for horizon in (3, 5, 10)
            },
            "selected_exposure": {
                "unique_codes": len({str(row.get("code")) for row in rows}),
                "score_min": min(score_values, default=None),
                "score_median": median(score_values) if score_values else None,
                "score_max": max(score_values, default=None),
                "top_industries": Counter(
                    str((row.get("raw_metrics") or {}).get("industry") or "未知") for row in rows
                ).most_common(8),
            },
            "market_regimes": self._market_regime_summary(rows, regimes),
            "limitations": [
                "这是已观察历史冻结 run 的失败归因，不是新的样本外验证。",
                "3/5/10 日收益按入场日起对应交易栏位收盘计算；缺失退出价保持缺失，不补零。",
                "市场阶段需要60个指数交易日预热，预热不足日期单独列出。",
                "保留持仓成本结果是反事实近似，不改变原始选股序列。",
            ],
        }
        report["classification"] = self._classification(report)
        return report
