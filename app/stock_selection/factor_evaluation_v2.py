from __future__ import annotations

import hashlib
import json
import math
import random
from collections import defaultdict
from datetime import date, datetime, time
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from app.shared.db import mysql_conn, mysql_read_conn


SPEC_PATH = Path(__file__).resolve().parent / "specs" / "strategy_factor_evaluation_v2.json"
PROTOCOL_ID = "strategy_factor_evaluation_v2"
DEFAULT_BENCHMARK_CODE = "000300.SH"
DEFAULT_ROUND_TRIP_COST_PCT = 0.25


def mature_horizon_cutoffs(
    market_dates: Sequence[date],
    horizons: Sequence[int],
) -> list[tuple[int, date]]:
    """Return the latest signal date whose requested horizon is observable."""

    ordered_dates = sorted(set(market_dates))
    result: list[tuple[int, date]] = []
    for horizon in sorted({int(value) for value in horizons if int(value) > 0}):
        cutoff_index = len(ordered_dates) - 1 - horizon
        if cutoff_index >= 0:
            result.append((horizon, ordered_dates[cutoff_index]))
    return result


def missing_factor_rows(
    factor_rows: Sequence[Mapping[str, Any]],
    existing_outcomes: set[tuple[int, int]],
    horizon_days: int,
) -> list[Mapping[str, Any]]:
    """Keep only factor snapshots without a materialized horizon outcome."""

    return [
        row
        for row in factor_rows
        if (int(row["id"]), int(horizon_days)) not in existing_outcomes
    ]


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _json_value(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list, tuple)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback


def _to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _mean(values: Iterable[float]) -> float | None:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return sum(clean) / len(clean) if clean else None


def _std(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    avg = sum(values) / len(values)
    return math.sqrt(sum((value - avg) ** 2 for value in values) / (len(values) - 1))


def _ranks(values: Sequence[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda pair: pair[1])
    ranks = [0.0] * len(values)
    start = 0
    while start < len(indexed):
        end = start
        while end + 1 < len(indexed) and indexed[end + 1][1] == indexed[start][1]:
            end += 1
        rank = (start + end) / 2 + 1
        for index in range(start, end + 1):
            ranks[indexed[index][0]] = rank
        start = end + 1
    return ranks


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    centered_x = [value - mean_x for value in xs]
    centered_y = [value - mean_y for value in ys]
    denominator = math.sqrt(
        sum(value * value for value in centered_x)
        * sum(value * value for value in centered_y)
    )
    if denominator <= 0:
        return None
    return sum(x * y for x, y in zip(centered_x, centered_y)) / denominator


def spearman(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    return pearson(_ranks(xs), _ranks(ys))


def newey_west_mean_t(values: Sequence[float]) -> tuple[float | None, float | None, int]:
    """Return HAC t-stat, normal-approximation p-value and lag."""

    clean = [float(value) for value in values if math.isfinite(float(value))]
    count = len(clean)
    if count < 3:
        return None, None, 0
    avg = sum(clean) / count
    lag = min(
        count - 1,
        max(0, int(math.floor(4 * (count / 100) ** (2 / 9)))),
    )
    centered = [value - avg for value in clean]
    long_run_variance = sum(value * value for value in centered) / count
    for offset in range(1, lag + 1):
        covariance = (
            sum(
                centered[index] * centered[index - offset]
                for index in range(offset, count)
            )
            / count
        )
        weight = 1 - offset / (lag + 1)
        long_run_variance += 2 * weight * covariance
    variance_of_mean = max(long_run_variance, 0.0) / count
    if variance_of_mean <= 0:
        return None, None, lag
    t_stat = avg / math.sqrt(variance_of_mean)
    p_value = math.erfc(abs(t_stat) / math.sqrt(2))
    return t_stat, p_value, lag


def moving_block_bootstrap_mean(
    values: Sequence[float],
    *,
    iterations: int = 1000,
    seed: int = 20260724,
) -> tuple[float | None, float | None, int]:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    count = len(clean)
    if count < 3:
        return None, None, 0
    block_size = max(2, min(count, round(count ** (1 / 3))))
    rng = random.Random(seed)
    samples: list[float] = []
    for _ in range(max(100, int(iterations))):
        draw: list[float] = []
        while len(draw) < count:
            start = rng.randrange(count)
            for offset in range(block_size):
                draw.append(clean[(start + offset) % count])
                if len(draw) >= count:
                    break
        samples.append(sum(draw) / count)
    samples.sort()
    lower_index = max(0, int(0.025 * (len(samples) - 1)))
    upper_index = min(len(samples) - 1, int(0.975 * (len(samples) - 1)))
    return samples[lower_index], samples[upper_index], block_size


def benjamini_hochberg(p_values: Mapping[str, float | None]) -> dict[str, float | None]:
    valid = sorted(
        (
            (key, float(value))
            for key, value in p_values.items()
            if value is not None and math.isfinite(float(value))
        ),
        key=lambda pair: pair[1],
    )
    result: dict[str, float | None] = {key: None for key in p_values}
    total = len(valid)
    running = 1.0
    for reverse_index in range(total - 1, -1, -1):
        key, value = valid[reverse_index]
        rank = reverse_index + 1
        running = min(running, value * total / rank)
        result[key] = min(1.0, running)
    return result


def load_factor_evaluation_spec() -> dict[str, Any]:
    with SPEC_PATH.open("r", encoding="utf-8") as handle:
        spec = json.load(handle)
    if spec.get("protocol_id") != PROTOCOL_ID:
        raise RuntimeError("strategy factor evaluation spec protocol_id mismatch")
    return spec


def factor_evaluation_spec_hash() -> str:
    return _sha256(load_factor_evaluation_spec())


def maturity_state(
    *,
    observation_days: int,
    valid_rows: int,
    market_states: int = 0,
) -> str:
    if observation_days >= 60 and valid_rows >= 2000 and market_states >= 2:
        return "research_candidate"
    if observation_days >= 30 and valid_rows >= 1000:
        return "provisional"
    if observation_days >= 10 and valid_rows >= 500:
        return "directional_hint"
    return "data_only"


def _group_result(
    records: Sequence[tuple[float, float]],
    *,
    preferred_groups: int = 5,
    fallback_groups: int = 3,
    minimum_per_group: int = 20,
) -> tuple[list[dict[str, Any]], float | None, float | None]:
    if len(records) >= preferred_groups * minimum_per_group:
        group_count = preferred_groups
    elif len(records) >= fallback_groups * minimum_per_group:
        group_count = fallback_groups
    else:
        return [], None, None

    ordered = sorted(records, key=lambda pair: pair[0])
    groups: list[dict[str, Any]] = []
    for group_index in range(group_count):
        start = round(group_index * len(ordered) / group_count)
        end = round((group_index + 1) * len(ordered) / group_count)
        chunk = ordered[start:end]
        if not chunk:
            continue
        factors = [item[0] for item in chunk]
        returns = [item[1] for item in chunk]
        groups.append(
            {
                "group_no": group_index + 1,
                "group_label": f"Q{group_index + 1}",
                "sample_size": len(chunk),
                "factor_min": min(factors),
                "factor_max": max(factors),
                "average_return_pct": _mean(returns),
                "median_return_pct": median(returns),
                "win_rate": sum(value > 0 for value in returns) / len(returns),
                "cost_adjusted_return_pct": _mean(
                    value - DEFAULT_ROUND_TRIP_COST_PCT for value in returns
                ),
            }
        )
    group_returns = [
        float(item["average_return_pct"])
        for item in groups
        if item.get("average_return_pct") is not None
    ]
    monotonicity = (
        spearman(list(range(1, len(group_returns) + 1)), group_returns)
        if len(group_returns) >= 3
        else None
    )
    top_bottom = (
        group_returns[-1] - group_returns[0] if len(group_returns) == group_count else None
    )
    return groups, monotonicity, top_bottom


def evaluate_factor_records(
    records: Sequence[Mapping[str, Any]],
    *,
    strategy_id: str,
    strategy_version: str,
    scope_name: str,
    horizon_days: int,
    factor_keys: Sequence[str],
    computed_at: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate immutable factor rows carrying matured forward labels."""

    computed_at = computed_at or datetime.now()
    normalized: list[dict[str, Any]] = []
    for raw in records:
        outcome = _to_float(raw.get("forward_return_pct"))
        factors = _json_value(raw.get("factor_json"), {})
        contributions = _json_value(raw.get("contribution_json"), {})
        if not isinstance(factors, Mapping):
            factors = {}
        if not isinstance(contributions, Mapping):
            contributions = {}
        normalized.append(
            {
                **dict(raw),
                "trade_date": str(raw.get("trade_date") or "")[:10],
                "forward_return_pct": outcome,
                "factor_json": dict(factors),
                "contribution_json": dict(contributions),
                "score": _to_float(raw.get("score")),
            }
        )

    factor_results: list[dict[str, Any]] = []
    group_results: list[dict[str, Any]] = []
    p_values: dict[str, float | None] = {}
    for factor_key in factor_keys:
        present_values: list[float] = []
        valid_pairs: list[tuple[float, float]] = []
        pairs_by_date: dict[str, list[tuple[float, float]]] = defaultdict(list)
        market_states: set[str] = set()
        for row in normalized:
            value = _to_float(row["factor_json"].get(factor_key))
            if value is not None:
                present_values.append(value)
            outcome = row.get("forward_return_pct")
            if value is None or outcome is None:
                continue
            valid_pairs.append((value, float(outcome)))
            pairs_by_date[row["trade_date"]].append((value, float(outcome)))
            state = str(row.get("market_state") or "").strip()
            if state:
                market_states.add(state)

        daily_pearson: list[float] = []
        daily_rank: list[float] = []
        daily_details: list[dict[str, Any]] = []
        for trade_day in sorted(pairs_by_date):
            pairs = pairs_by_date[trade_day]
            xs = [pair[0] for pair in pairs]
            ys = [pair[1] for pair in pairs]
            pearson_value = (
                None
                if scope_name == "selected_top_k"
                else pearson(xs, ys)
            )
            rank_value = (
                None
                if scope_name == "selected_top_k"
                else spearman(xs, ys)
            )
            if pearson_value is not None:
                daily_pearson.append(pearson_value)
            if rank_value is not None:
                daily_rank.append(rank_value)
            daily_details.append(
                {
                    "trade_date": trade_day,
                    "sample_size": len(pairs),
                    "pearson_ic": pearson_value,
                    "rank_ic": rank_value,
                }
            )

        rank_mean = _mean(daily_rank)
        rank_std = _std(daily_rank)
        rank_ir = (
            rank_mean / rank_std
            if rank_mean is not None and rank_std is not None and rank_std > 0
            else None
        )
        nw_t, p_value, nw_lag = newey_west_mean_t(daily_rank)
        bootstrap_low, bootstrap_high, block_size = moving_block_bootstrap_mean(
            daily_rank
        )
        groups, monotonicity, top_bottom = (
            ([], None, None)
            if scope_name == "selected_top_k"
            else _group_result(valid_pairs)
        )
        observation_days = len(pairs_by_date)
        maturity = (
            "data_only"
            if scope_name == "selected_top_k"
            else maturity_state(
                observation_days=observation_days,
                valid_rows=len(valid_pairs),
                market_states=len(market_states),
            )
        )
        if maturity == "research_candidate" and bootstrap_low is not None and bootstrap_low > 0:
            validation_status = "research_candidate_positive"
        elif maturity in {"directional_hint", "provisional", "research_candidate"}:
            validation_status = "directional_evidence_only"
        else:
            validation_status = "insufficient_evidence"

        sample_dates = sorted(pairs_by_date)
        evaluation_id = (
            f"{strategy_id}:{strategy_version}:{scope_name}:"
            f"{horizon_days}:{factor_key}:{sample_dates[-1] if sample_dates else 'none'}"
        )[:128]
        total_rows = len(normalized)
        coverage = len(present_values) / total_rows if total_rows else None
        result = {
            "protocol_id": PROTOCOL_ID,
            "spec_hash": factor_evaluation_spec_hash(),
            "evaluation_id": evaluation_id,
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "scope_name": scope_name,
            "factor_key": factor_key,
            "horizon_days": int(horizon_days),
            "sample_start_date": sample_dates[0] if sample_dates else None,
            "sample_end_date": sample_dates[-1] if sample_dates else None,
            "observation_days": observation_days,
            "sample_size": total_rows,
            "valid_sample_size": len(valid_pairs),
            "coverage": coverage,
            "missing_rate": 1 - coverage if coverage is not None else None,
            "factor_mean": _mean(present_values),
            "factor_std": _std(present_values),
            "pearson_ic_mean": _mean(daily_pearson),
            "rank_ic_mean": rank_mean,
            "rank_ic_std": rank_std,
            "rank_ic_ir": rank_ir,
            "positive_ic_ratio": (
                sum(value > 0 for value in daily_rank) / len(daily_rank)
                if daily_rank
                else None
            ),
            "newey_west_t": nw_t,
            "p_value": p_value,
            "bootstrap_ci_low": bootstrap_low,
            "bootstrap_ci_high": bootstrap_high,
            "group_count": len(groups) or None,
            "monotonicity_score": monotonicity,
            "top_bottom_return_pct": top_bottom,
            "fdr_q_value": None,
            "maturity_state": maturity,
            "validation_status": validation_status,
            "details_json": {
                "daily_ic": daily_details,
                "newey_west_lag": nw_lag,
                "bootstrap_block_size": block_size,
                "bootstrap_iterations": 1000 if daily_rank else 0,
                "market_state_count": len(market_states),
                "return_label": f"next_open_to_close_{horizon_days}d",
                "selected_only_cross_sectional_ic_allowed": scope_name != "selected_top_k",
            },
            "computed_at": computed_at,
        }
        factor_results.append(result)
        p_values[factor_key] = p_value
        for item in groups:
            group_results.append(
                {
                    "evaluation_id": evaluation_id,
                    "factor_key": factor_key,
                    "horizon_days": int(horizon_days),
                    **item,
                    "details_json": {
                        "scope_name": scope_name,
                        "cost_pct": DEFAULT_ROUND_TRIP_COST_PCT,
                    },
                }
            )

    q_values = benjamini_hochberg(p_values)
    for result in factor_results:
        result["fdr_q_value"] = q_values.get(result["factor_key"])

    ablation_results = _evaluate_ablation(
        normalized,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        scope_name=scope_name,
        horizon_days=horizon_days,
        factor_keys=factor_keys,
    )
    return {
        "evaluations": factor_results,
        "groups": group_results,
        "ablations": ablation_results,
    }


def _daily_rank_ic(
    rows: Sequence[Mapping[str, Any]],
    score_key: str,
) -> float | None:
    values: list[float] = []
    by_date: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in rows:
        score = _to_float(row.get(score_key))
        outcome = _to_float(row.get("forward_return_pct"))
        if score is None or outcome is None:
            continue
        by_date[str(row.get("trade_date") or "")].append((score, outcome))
    for pairs in by_date.values():
        rank_value = spearman(
            [pair[0] for pair in pairs],
            [pair[1] for pair in pairs],
        )
        if rank_value is not None:
            values.append(rank_value)
    return _mean(values)


def _evaluate_ablation(
    rows: Sequence[Mapping[str, Any]],
    *,
    strategy_id: str,
    strategy_version: str,
    scope_name: str,
    horizon_days: int,
    factor_keys: Sequence[str],
) -> list[dict[str, Any]]:
    if scope_name == "selected_top_k":
        return []
    enriched: list[dict[str, Any]] = []
    for row in rows:
        score = _to_float(row.get("score"))
        outcome = _to_float(row.get("forward_return_pct"))
        if score is None or outcome is None:
            continue
        contributions = row.get("contribution_json") or {}
        factors = row.get("factor_json") or {}
        enriched.append(
            {
                **dict(row),
                "_baseline_score": score,
                "_factors": factors,
                "_contributions": contributions,
            }
        )
    baseline_ic = _daily_rank_ic(enriched, "_baseline_score")
    if not enriched:
        return []
    sample_end = max(str(row.get("trade_date") or "") for row in enriched)
    result: list[dict[str, Any]] = []
    for factor_key in factor_keys:
        ablated_rows: list[dict[str, Any]] = []
        correlations: list[float] = []
        factor_values = [
            _to_float((row.get("_factors") or {}).get(factor_key))
            for row in enriched
        ]
        for other_key in factor_keys:
            if other_key == factor_key:
                continue
            xs: list[float] = []
            ys: list[float] = []
            for row, factor_value in zip(enriched, factor_values):
                other_value = _to_float((row.get("_factors") or {}).get(other_key))
                if factor_value is not None and other_value is not None:
                    xs.append(factor_value)
                    ys.append(other_value)
            correlation = spearman(xs, ys)
            if correlation is not None:
                correlations.append(abs(correlation))
        for row in enriched:
            contribution = _to_float(
                (row.get("_contributions") or {}).get(factor_key)
            )
            ablated_rows.append(
                {
                    **row,
                    "_ablated_score": (
                        row["_baseline_score"] - contribution
                        if contribution is not None
                        else row["_baseline_score"]
                    ),
                }
            )
        ablated_ic = _daily_rank_ic(ablated_rows, "_ablated_score")
        delta = (
            baseline_ic - ablated_ic
            if baseline_ic is not None and ablated_ic is not None
            else None
        )
        if len(enriched) < 500:
            conclusion = "insufficient_evidence"
        elif delta is not None and delta > 0.005:
            conclusion = "incremental_positive"
        elif delta is not None and delta < -0.005:
            conclusion = "possible_negative_or_suppressor"
        else:
            conclusion = "redundant_or_neutral"
        evaluation_id = (
            f"{strategy_id}:{strategy_version}:{scope_name}:"
            f"{horizon_days}:ablation:{sample_end}"
        )[:128]
        result.append(
            {
                "evaluation_id": evaluation_id,
                "factor_key": factor_key,
                "horizon_days": int(horizon_days),
                "sample_size": len(enriched),
                "baseline_rank_ic": baseline_ic,
                "ablated_rank_ic": ablated_ic,
                "rank_ic_delta": delta,
                "baseline_top_bottom_return_pct": None,
                "ablated_top_bottom_return_pct": None,
                "top_bottom_delta_pct": None,
                "redundancy_max_abs_corr": max(correlations) if correlations else None,
                "conclusion": conclusion,
                "details_json": {
                    "scope_name": scope_name,
                    "method": "leave_one_contribution_out",
                    "selected_only_history_excluded": True,
                },
            }
        )
    return result


class StrategyFactorEvaluationRepository:
    def __init__(
        self,
        *,
        connection_factory=None,
        read_connection_factory=None,
    ) -> None:
        self._connection_factory = connection_factory or mysql_conn
        self._read_connection_factory = read_connection_factory or mysql_read_conn

    def persist_snapshot(
        self,
        *,
        snapshot_id: str,
        source_snapshot_id: str | None,
        strategy_id: str,
        strategy_version: str,
        strategy_config_hash: str,
        trade_date: date | str,
        decision_as_of: datetime,
        expected_entity_count: int,
        trace_rows: Sequence[Mapping[str, Any]],
        source_lineage: Sequence[Mapping[str, Any]],
        trace_mode: str = "full_forward_trace",
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        rows = [dict(row) for row in trace_rows]
        if not snapshot_id or not strategy_id or not strategy_version:
            raise ValueError("snapshot_id, strategy_id and strategy_version are required")
        if not strategy_config_hash:
            raise ValueError("strategy_config_hash is required")
        if not rows:
            return {
                "status": "skipped",
                "reason": "empty_factor_trace",
                "snapshot_id": snapshot_id,
            }
        payload_hash = _sha256(
            {
                "snapshot_id": snapshot_id,
                "rows": rows,
                "source_lineage": source_lineage,
                "trace_mode": trace_mode,
            }
        )
        pre_filter_count = sum(bool(row.get("in_pre_filter", True)) for row in rows)
        eligible_count = sum(bool(row.get("in_eligible_pool")) for row in rows)
        selected_count = sum(bool(row.get("is_selected")) for row in rows)
        earliest_execution = datetime.combine(
            decision_as_of.date(),
            time(9, 30),
        )
        if decision_as_of.time() >= time(9, 30):
            earliest_execution = None

        manifest_sql = """
        INSERT INTO strategy_factor_snapshot_manifest (
            protocol_id, spec_hash, snapshot_id, source_snapshot_id,
            strategy_id, strategy_version, strategy_config_hash,
            trade_date, decision_as_of, earliest_execution_at,
            expected_entity_count, pre_filter_count, eligible_count,
            selected_count, trace_mode, maturity_state,
            source_lineage_json, metadata_json, payload_hash
        ) VALUES (
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,'data_only',
            %s,%s,%s
        )
        """
        row_sql = """
        INSERT INTO strategy_factor_snapshot (
            manifest_id, snapshot_id, strategy_id, strategy_version,
            trade_date, decision_as_of, code, name, industry, theme_name,
            candidate_lane, in_pre_filter, in_eligible_pool, is_selected,
            hard_gate_pass, signal_grade, score, factor_json,
            contribution_json, gate_json, rejection_reasons_json,
            market_context_json, data_lineage_json, payload_hash
        ) VALUES (
            %s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s
        )
        """
        with self._connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, payload_hash
                    FROM strategy_factor_snapshot_manifest
                    WHERE snapshot_id=%s
                    """,
                    (snapshot_id,),
                )
                existing = cursor.fetchone()
                if existing:
                    if str(existing.get("payload_hash") or "") != payload_hash:
                        raise RuntimeError(
                            f"immutable factor snapshot mismatch: {snapshot_id}"
                        )
                    return {
                        "status": "unchanged",
                        "snapshot_id": snapshot_id,
                        "manifest_id": int(existing["id"]),
                        "row_count": len(rows),
                    }

                cursor.execute(
                    manifest_sql,
                    (
                        PROTOCOL_ID,
                        factor_evaluation_spec_hash(),
                        snapshot_id,
                        source_snapshot_id,
                        strategy_id,
                        strategy_version,
                        strategy_config_hash,
                        trade_date,
                        decision_as_of,
                        earliest_execution,
                        int(expected_entity_count),
                        pre_filter_count,
                        eligible_count,
                        selected_count,
                        trace_mode,
                        _canonical_json(source_lineage),
                        _canonical_json(dict(metadata or {})),
                        payload_hash,
                    ),
                )
                manifest_id = int(cursor.lastrowid)
                payload: list[tuple[Any, ...]] = []
                for row in rows:
                    row_payload = {
                        "code": row.get("code"),
                        "factor_json": row.get("factor_json") or {},
                        "contribution_json": row.get("contribution_json") or {},
                        "gate_json": row.get("gate_json") or {},
                        "rejection_reasons": row.get("rejection_reasons") or [],
                        "market_context": row.get("market_context") or {},
                    }
                    payload.append(
                        (
                            manifest_id,
                            snapshot_id,
                            strategy_id,
                            strategy_version,
                            trade_date,
                            decision_as_of,
                            row.get("code"),
                            row.get("name"),
                            row.get("industry"),
                            row.get("theme_name"),
                            row.get("candidate_lane"),
                            int(bool(row.get("in_pre_filter", True))),
                            int(bool(row.get("in_eligible_pool"))),
                            int(bool(row.get("is_selected"))),
                            (
                                None
                                if row.get("hard_gate_pass") is None
                                else int(bool(row.get("hard_gate_pass")))
                            ),
                            row.get("signal_grade"),
                            _to_float(row.get("score")),
                            _canonical_json(row.get("factor_json") or {}),
                            _canonical_json(row.get("contribution_json") or {}),
                            _canonical_json(row.get("gate_json") or {}),
                            _canonical_json(row.get("rejection_reasons") or []),
                            _canonical_json(row.get("market_context") or {}),
                            _canonical_json(
                                {
                                    "source_snapshot_id": source_snapshot_id,
                                    "trace_mode": trace_mode,
                                }
                            ),
                            _sha256(row_payload),
                        )
                    )
                for start in range(0, len(payload), 250):
                    cursor.executemany(row_sql, payload[start : start + 250])
        return {
            "status": "created",
            "snapshot_id": snapshot_id,
            "manifest_id": manifest_id,
            "row_count": len(rows),
            "pre_filter_count": pre_filter_count,
            "eligible_count": eligible_count,
            "selected_count": selected_count,
        }

    def backfill_selected_only_history(
        self,
        *,
        strategy_id: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        conditions = [
            "m.status='ready'",
            "m.quality_status='passed'",
            "f.snapshot_id IS NULL",
        ]
        params: list[Any] = []
        if strategy_id:
            conditions.append("m.strategy_id=%s")
            params.append(strategy_id)
        params.append(max(1, int(limit)))
        sql = f"""
        SELECT m.*, c.code, c.name, c.industry, c.score,
               c.trade_grade_state, c.opinion_sector_name,
               c.factor_json, c.explain_json, c.source_lineage_json
        FROM sentiment_candidate_snapshot_manifest m
        INNER JOIN sentiment_candidate_snapshot c
            ON c.snapshot_id=m.snapshot_id AND c.is_selected=1
        LEFT JOIN strategy_factor_snapshot_manifest f
            ON f.snapshot_id=m.snapshot_id
        WHERE {' AND '.join(conditions)}
        ORDER BY m.decision_as_of, c.rank_no, c.code
        LIMIT %s
        """
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                raw_rows = cursor.fetchall() or []

        grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in raw_rows:
            grouped[str(row["snapshot_id"])].append(row)
        created = 0
        unchanged = 0
        skipped = 0
        for snapshot_id, snapshot_rows in grouped.items():
            manifest = snapshot_rows[0]
            trace_rows: list[dict[str, Any]] = []
            for row in snapshot_rows:
                factors = _json_value(row.get("factor_json"), {})
                explain = _json_value(row.get("explain_json"), {})
                contract = (
                    explain.get("_selection_contract")
                    if isinstance(explain, Mapping)
                    else {}
                ) or {}
                trace_rows.append(
                    {
                        "code": row.get("code"),
                        "name": row.get("name"),
                        "industry": row.get("industry"),
                        "theme_name": row.get("opinion_sector_name"),
                        "candidate_lane": (
                            (explain.get("candidate_lane") if isinstance(explain, Mapping) else None)
                            or (
                                (explain.get("raw_metrics") or {}).get("candidate_lane")
                                if isinstance(explain, Mapping)
                                and isinstance(explain.get("raw_metrics"), Mapping)
                                else None
                            )
                        ),
                        "in_pre_filter": True,
                        "in_eligible_pool": True,
                        "is_selected": True,
                        "hard_gate_pass": True,
                        "signal_grade": row.get("trade_grade_state"),
                        "score": row.get("score"),
                        "factor_json": factors if isinstance(factors, Mapping) else {},
                        "contribution_json": (
                            contract.get("score_breakdown")
                            if isinstance(contract, Mapping)
                            else {}
                        )
                        or (
                            explain.get("factor_contributions")
                            if isinstance(explain, Mapping)
                            else {}
                        )
                        or {},
                        "gate_json": (
                            contract.get("gate_results")
                            if isinstance(contract, Mapping)
                            else {}
                        )
                        or {},
                        "rejection_reasons": [],
                        "market_context": {
                            "historical_scope": "selected_top_k_only",
                        },
                    }
                )
            result = self.persist_snapshot(
                snapshot_id=snapshot_id,
                source_snapshot_id=snapshot_id,
                strategy_id=str(manifest["strategy_id"]),
                strategy_version=str(manifest["strategy_version"]),
                strategy_config_hash=str(manifest["strategy_config_hash"]),
                trade_date=manifest["trade_date"],
                decision_as_of=manifest["decision_as_of"],
                expected_entity_count=int(manifest.get("candidate_count") or len(trace_rows)),
                trace_rows=trace_rows,
                source_lineage=_json_value(
                    snapshot_rows[0].get("source_lineage_json"),
                    [],
                ),
                trace_mode="selected_only_historical",
                metadata={
                    "cross_sectional_ic_allowed": False,
                    "backfill_source": "immutable_sentiment_candidate_snapshot",
                },
            )
            if result["status"] == "created":
                created += 1
            elif result["status"] == "unchanged":
                unchanged += 1
            else:
                skipped += 1
        return {
            "status": "success",
            "snapshot_groups": len(grouped),
            "created": created,
            "unchanged": unchanged,
            "skipped": skipped,
            "rows_read": len(raw_rows),
        }

    def refresh_outcomes(
        self,
        *,
        horizons: Sequence[int] = (1, 3, 5, 10, 20),
        manifest_limit: int = 20,
    ) -> dict[str, Any]:
        horizon_values = sorted({int(value) for value in horizons if int(value) > 0})
        if not horizon_values:
            raise ValueError("at least one positive horizon is required")
        horizon_placeholders = ",".join(["%s"] * len(horizon_values))
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM daily_kline
                    ORDER BY trade_date
                    """
                )
                market_dates = [
                    row["trade_date"] for row in (cursor.fetchall() or [])
                ]
                maturity_cutoffs = mature_horizon_cutoffs(
                    market_dates,
                    horizon_values,
                )
                if not maturity_cutoffs:
                    return {
                        "status": "success",
                        "manifest_count": 0,
                        "outcomes_written": 0,
                        "pending": 0,
                        "blocked": 0,
                    }
                maturity_clauses: list[str] = []
                maturity_params: list[Any] = []
                for horizon, cutoff_date in maturity_cutoffs:
                    maturity_clauses.append(
                        """
                        (
                            m.trade_date <= %s
                            AND EXISTS (
                                SELECT 1
                                FROM strategy_factor_snapshot ms
                                WHERE ms.manifest_id=m.id
                                  AND ms.in_eligible_pool=1
                                  AND NOT EXISTS (
                                      SELECT 1
                                      FROM strategy_factor_outcome mo
                                      WHERE mo.factor_snapshot_id=ms.id
                                        AND mo.horizon_days=%s
                                  )
                            )
                        )
                        """
                    )
                    maturity_params.extend((cutoff_date, horizon))
                cursor.execute(
                    f"""
                    SELECT m.*
                    FROM strategy_factor_snapshot_manifest m
                    WHERE EXISTS (
                        SELECT 1
                        FROM strategy_factor_snapshot s
                        WHERE s.manifest_id=m.id
                          AND s.in_eligible_pool=1
                    )
                      AND ({" OR ".join(maturity_clauses)})
                    ORDER BY m.trade_date, m.id
                    LIMIT %s
                    """,
                    (
                        *maturity_params,
                        max(1, int(manifest_limit)),
                    ),
                )
                manifests = cursor.fetchall() or []
        date_index = {value: index for index, value in enumerate(market_dates)}
        inserted_or_updated = 0
        pending = 0
        blocked = 0
        for manifest in manifests:
            signal_date = manifest["trade_date"]
            index = date_index.get(signal_date)
            if index is None:
                continue
            with self._read_connection_factory() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT *
                        FROM strategy_factor_snapshot
                        WHERE manifest_id=%s AND in_eligible_pool=1
                        ORDER BY code
                        """,
                        (manifest["id"],),
                    )
                    factor_rows = cursor.fetchall() or []
                    cursor.execute(
                        f"""
                        SELECT o.factor_snapshot_id, o.horizon_days
                        FROM strategy_factor_outcome o
                        INNER JOIN strategy_factor_snapshot os
                            ON os.id=o.factor_snapshot_id
                        WHERE os.manifest_id=%s
                          AND o.horizon_days IN ({horizon_placeholders})
                        """,
                        (manifest["id"], *horizon_values),
                    )
                    existing_outcomes = {
                        (
                            int(row["factor_snapshot_id"]),
                            int(row["horizon_days"]),
                        )
                        for row in (cursor.fetchall() or [])
                    }
            for horizon in horizon_values:
                missing_rows = missing_factor_rows(
                    factor_rows,
                    existing_outcomes,
                    horizon,
                )
                if not missing_rows:
                    continue
                exit_index = index + int(horizon)
                entry_index = index + 1
                if entry_index >= len(market_dates) or exit_index >= len(market_dates):
                    pending += len(missing_rows)
                    continue
                entry_date = market_dates[entry_index]
                exit_date = market_dates[exit_index]
                outcome_rows = self._build_outcomes_for_horizon(
                    factor_rows=missing_rows,
                    signal_date=signal_date,
                    entry_date=entry_date,
                    exit_date=exit_date,
                    horizon_days=int(horizon),
                )
                if not outcome_rows:
                    continue
                with self._connection_factory(dict_cursor=False) as conn:
                    with conn.cursor() as cursor:
                        cursor.executemany(
                            """
                            INSERT INTO strategy_factor_outcome (
                                factor_snapshot_id, snapshot_id, strategy_id,
                                strategy_version, code, signal_trade_date,
                                horizon_days, entry_trade_date, exit_trade_date,
                                entry_price, exit_price, gross_return_pct,
                                cost_adjusted_return_pct, benchmark_code,
                                benchmark_return_pct, benchmark_excess_return_pct,
                                industry_excess_return_pct, mfe_pct, mae_pct,
                                execution_status, block_reason, label_hash,
                                metadata_json, computed_at
                            ) VALUES (
                                %s,%s,%s,
                                %s,%s,%s,
                                %s,%s,%s,
                                %s,%s,%s,
                                %s,%s,
                                %s,%s,
                                %s,%s,%s,
                                %s,%s,%s,
                                %s,%s
                            )
                            ON DUPLICATE KEY UPDATE
                                entry_trade_date=VALUES(entry_trade_date),
                                exit_trade_date=VALUES(exit_trade_date),
                                entry_price=VALUES(entry_price),
                                exit_price=VALUES(exit_price),
                                gross_return_pct=VALUES(gross_return_pct),
                                cost_adjusted_return_pct=VALUES(cost_adjusted_return_pct),
                                benchmark_return_pct=VALUES(benchmark_return_pct),
                                benchmark_excess_return_pct=VALUES(benchmark_excess_return_pct),
                                mfe_pct=VALUES(mfe_pct),
                                mae_pct=VALUES(mae_pct),
                                execution_status=VALUES(execution_status),
                                block_reason=VALUES(block_reason),
                                label_hash=VALUES(label_hash),
                                metadata_json=VALUES(metadata_json),
                                computed_at=VALUES(computed_at)
                            """,
                            outcome_rows,
                        )
                inserted_or_updated += len(outcome_rows)
                blocked += sum(row[19] == "blocked" for row in outcome_rows)
        return {
            "status": "success",
            "manifest_count": len(manifests),
            "outcomes_written": inserted_or_updated,
            "pending": pending,
            "blocked": blocked,
        }

    def _build_outcomes_for_horizon(
        self,
        *,
        factor_rows: Sequence[Mapping[str, Any]],
        signal_date: date,
        entry_date: date,
        exit_date: date,
        horizon_days: int,
    ) -> list[tuple[Any, ...]]:
        codes = [str(row["code"]) for row in factor_rows]
        if not codes:
            return []
        price_by_code: dict[str, dict[str, Any]] = defaultdict(dict)
        placeholders = ",".join(["%s"] * len(codes))
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT k.code, k.trade_date, k.open, k.high, k.low, k.close,
                           af.adj_factor
                    FROM daily_kline k
                    LEFT JOIN adj_factor_daily af
                      ON af.code=k.code AND af.trade_date=k.trade_date
                    WHERE k.code IN ({placeholders})
                      AND k.trade_date BETWEEN %s AND %s
                    ORDER BY k.code, k.trade_date
                    """,
                    [*codes, entry_date, exit_date],
                )
                price_rows = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT trade_date, open, high, low, close
                    FROM market_index_daily
                    WHERE index_code=%s AND trade_date IN (%s,%s)
                    """,
                    (DEFAULT_BENCHMARK_CODE, entry_date, exit_date),
                )
                benchmark_rows = {
                    row["trade_date"]: row for row in (cursor.fetchall() or [])
                }
        for row in price_rows:
            code = str(row["code"])
            price_by_code[code].setdefault("rows", []).append(row)
            if row["trade_date"] == entry_date:
                price_by_code[code]["entry"] = row
            if row["trade_date"] == exit_date:
                price_by_code[code]["exit"] = row

        benchmark_entry = benchmark_rows.get(entry_date)
        benchmark_exit = benchmark_rows.get(exit_date)
        benchmark_return = None
        if benchmark_entry and benchmark_exit:
            entry_value = _to_float(benchmark_entry.get("open"))
            exit_value = _to_float(benchmark_exit.get("close"))
            if entry_value and exit_value is not None:
                benchmark_return = (exit_value / entry_value - 1) * 100

        now = datetime.now()
        result: list[tuple[Any, ...]] = []
        for factor_row in factor_rows:
            code = str(factor_row["code"])
            prices = price_by_code.get(code) or {}
            entry = prices.get("entry")
            exit_row = prices.get("exit")
            status = "ready"
            block_reason = None
            gross_return = None
            cost_adjusted = None
            mfe = None
            mae = None
            entry_price = _to_float(entry.get("open")) if entry else None
            exit_price = _to_float(exit_row.get("close")) if exit_row else None
            entry_adj = _to_float(entry.get("adj_factor")) if entry else None
            exit_adj = _to_float(exit_row.get("adj_factor")) if exit_row else None
            if not entry or not entry_price or entry_price <= 0:
                status = "blocked"
                block_reason = "no_next_tradable_open"
            elif not exit_row or exit_price is None or exit_price <= 0:
                status = "blocked"
                block_reason = "no_horizon_exit_quote"
            else:
                adjusted = bool(entry_adj and exit_adj)
                entry_basis = entry_price * entry_adj if adjusted else entry_price
                exit_basis = exit_price * exit_adj if adjusted else exit_price
                gross_return = (exit_basis / entry_basis - 1) * 100
                cost_adjusted = gross_return - DEFAULT_ROUND_TRIP_COST_PCT
                highs: list[float] = []
                lows: list[float] = []
                for price_row in prices.get("rows") or []:
                    row_adj = _to_float(price_row.get("adj_factor"))
                    high = _to_float(price_row.get("high"))
                    low = _to_float(price_row.get("low"))
                    if high is not None:
                        highs.append(high * row_adj if adjusted and row_adj else high)
                    if low is not None:
                        lows.append(low * row_adj if adjusted and row_adj else low)
                if highs:
                    mfe = (max(highs) / entry_basis - 1) * 100
                if lows:
                    mae = (min(lows) / entry_basis - 1) * 100

            excess = (
                gross_return - benchmark_return
                if gross_return is not None and benchmark_return is not None
                else None
            )
            label_payload = {
                "factor_snapshot_id": factor_row["id"],
                "horizon_days": horizon_days,
                "entry_date": entry_date,
                "exit_date": exit_date,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "gross_return_pct": gross_return,
                "benchmark_return_pct": benchmark_return,
                "execution_status": status,
            }
            metadata = {
                "entry_rule": "next_tradable_open",
                "exit_rule": f"market_trading_day_{horizon_days}_close",
                "adjustment_status": (
                    "adj_factor_total_return"
                    if entry_adj and exit_adj
                    else "raw_price_explicit_fallback"
                ),
                "round_trip_cost_pct": DEFAULT_ROUND_TRIP_COST_PCT,
                "industry_excess_status": "not_available",
            }
            result.append(
                (
                    factor_row["id"],
                    factor_row["snapshot_id"],
                    factor_row["strategy_id"],
                    factor_row["strategy_version"],
                    code,
                    signal_date,
                    horizon_days,
                    entry_date,
                    exit_date,
                    entry_price,
                    exit_price,
                    gross_return,
                    cost_adjusted,
                    DEFAULT_BENCHMARK_CODE,
                    benchmark_return,
                    excess,
                    None,
                    mfe,
                    mae,
                    status,
                    block_reason,
                    _sha256(label_payload),
                    _canonical_json(metadata),
                    now,
                )
            )
        return result

    def run_evaluations(
        self,
        *,
        strategy_id: str,
        strategy_version: str,
        horizons: Sequence[int] = (1, 3, 5, 10, 20),
    ) -> dict[str, Any]:
        factor_keys = load_factor_evaluation_spec()["factor_keys"]
        written = 0
        summaries: list[dict[str, Any]] = []
        for horizon in horizons:
            for scope_name, scope_sql in (
                ("eligible_pool", "s.in_eligible_pool=1"),
                ("selected_top_k", "s.is_selected=1"),
            ):
                with self._read_connection_factory() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            f"""
                            SELECT s.trade_date, s.score, s.factor_json,
                                   s.contribution_json,
                                   JSON_UNQUOTE(
                                       JSON_EXTRACT(
                                           s.market_context_json,
                                           '$.market_regime'
                                       )
                                   ) AS market_state,
                                   o.cost_adjusted_return_pct AS forward_return_pct
                            FROM strategy_factor_snapshot s
                            INNER JOIN strategy_factor_snapshot_manifest m
                                ON m.id=s.manifest_id
                            INNER JOIN strategy_factor_outcome o
                                ON o.factor_snapshot_id=s.id
                               AND o.horizon_days=%s
                               AND o.execution_status='ready'
                            WHERE s.strategy_id=%s
                              AND s.strategy_version=%s
                              AND {scope_sql}
                              AND (
                                  m.trace_mode='full_forward_trace'
                                  OR %s='selected_top_k'
                              )
                            ORDER BY s.trade_date, s.code
                            """,
                            (
                                int(horizon),
                                strategy_id,
                                strategy_version,
                                scope_name,
                            ),
                        )
                        records = cursor.fetchall() or []
                computed = evaluate_factor_records(
                    records,
                    strategy_id=strategy_id,
                    strategy_version=strategy_version,
                    scope_name=scope_name,
                    horizon_days=int(horizon),
                    factor_keys=factor_keys,
                )
                self._save_evaluation_payload(computed)
                written += len(computed["evaluations"])
                summaries.append(
                    {
                        "horizon_days": int(horizon),
                        "scope_name": scope_name,
                        "record_count": len(records),
                        "evaluation_count": len(computed["evaluations"]),
                    }
                )
        self._refresh_manifest_maturity(strategy_id, strategy_version)
        return {
            "status": "success",
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "evaluations_written": written,
            "summaries": summaries,
        }

    def _save_evaluation_payload(self, payload: Mapping[str, Any]) -> None:
        evaluations = list(payload.get("evaluations") or [])
        groups = list(payload.get("groups") or [])
        ablations = list(payload.get("ablations") or [])
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                for evaluation in evaluations:
                    cursor.execute(
                        """
                        INSERT INTO strategy_factor_evaluation (
                            protocol_id, spec_hash, evaluation_id, strategy_id,
                            strategy_version, scope_name, factor_key,
                            horizon_days, sample_start_date, sample_end_date,
                            observation_days, sample_size, valid_sample_size,
                            coverage, missing_rate, factor_mean, factor_std,
                            pearson_ic_mean, rank_ic_mean, rank_ic_std,
                            rank_ic_ir, positive_ic_ratio, newey_west_t,
                            p_value, bootstrap_ci_low, bootstrap_ci_high,
                            group_count, monotonicity_score,
                            top_bottom_return_pct, fdr_q_value, maturity_state,
                            validation_status, details_json, computed_at
                        ) VALUES (
                            %s,%s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,
                            %s,%s,%s,
                            %s,%s,%s
                        )
                        ON DUPLICATE KEY UPDATE
                            observation_days=VALUES(observation_days),
                            sample_size=VALUES(sample_size),
                            valid_sample_size=VALUES(valid_sample_size),
                            coverage=VALUES(coverage),
                            missing_rate=VALUES(missing_rate),
                            factor_mean=VALUES(factor_mean),
                            factor_std=VALUES(factor_std),
                            pearson_ic_mean=VALUES(pearson_ic_mean),
                            rank_ic_mean=VALUES(rank_ic_mean),
                            rank_ic_std=VALUES(rank_ic_std),
                            rank_ic_ir=VALUES(rank_ic_ir),
                            positive_ic_ratio=VALUES(positive_ic_ratio),
                            newey_west_t=VALUES(newey_west_t),
                            p_value=VALUES(p_value),
                            bootstrap_ci_low=VALUES(bootstrap_ci_low),
                            bootstrap_ci_high=VALUES(bootstrap_ci_high),
                            group_count=VALUES(group_count),
                            monotonicity_score=VALUES(monotonicity_score),
                            top_bottom_return_pct=VALUES(top_bottom_return_pct),
                            fdr_q_value=VALUES(fdr_q_value),
                            maturity_state=VALUES(maturity_state),
                            validation_status=VALUES(validation_status),
                            details_json=VALUES(details_json),
                            computed_at=VALUES(computed_at)
                        """,
                        (
                            evaluation["protocol_id"],
                            evaluation["spec_hash"],
                            evaluation["evaluation_id"],
                            evaluation["strategy_id"],
                            evaluation["strategy_version"],
                            evaluation["scope_name"],
                            evaluation["factor_key"],
                            evaluation["horizon_days"],
                            evaluation["sample_start_date"],
                            evaluation["sample_end_date"],
                            evaluation["observation_days"],
                            evaluation["sample_size"],
                            evaluation["valid_sample_size"],
                            evaluation["coverage"],
                            evaluation["missing_rate"],
                            evaluation["factor_mean"],
                            evaluation["factor_std"],
                            evaluation["pearson_ic_mean"],
                            evaluation["rank_ic_mean"],
                            evaluation["rank_ic_std"],
                            evaluation["rank_ic_ir"],
                            evaluation["positive_ic_ratio"],
                            evaluation["newey_west_t"],
                            evaluation["p_value"],
                            evaluation["bootstrap_ci_low"],
                            evaluation["bootstrap_ci_high"],
                            evaluation["group_count"],
                            evaluation["monotonicity_score"],
                            evaluation["top_bottom_return_pct"],
                            evaluation["fdr_q_value"],
                            evaluation["maturity_state"],
                            evaluation["validation_status"],
                            _canonical_json(evaluation["details_json"]),
                            evaluation["computed_at"],
                        ),
                    )
                for group in groups:
                    cursor.execute(
                        """
                        INSERT INTO strategy_factor_group_result (
                            evaluation_id, factor_key, horizon_days, group_no,
                            group_label, sample_size, factor_min, factor_max,
                            average_return_pct, median_return_pct, win_rate,
                            cost_adjusted_return_pct, details_json
                        ) VALUES (
                            %s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,
                            %s,%s
                        )
                        ON DUPLICATE KEY UPDATE
                            sample_size=VALUES(sample_size),
                            factor_min=VALUES(factor_min),
                            factor_max=VALUES(factor_max),
                            average_return_pct=VALUES(average_return_pct),
                            median_return_pct=VALUES(median_return_pct),
                            win_rate=VALUES(win_rate),
                            cost_adjusted_return_pct=VALUES(cost_adjusted_return_pct),
                            details_json=VALUES(details_json)
                        """,
                        (
                            group["evaluation_id"],
                            group["factor_key"],
                            group["horizon_days"],
                            group["group_no"],
                            group["group_label"],
                            group["sample_size"],
                            group["factor_min"],
                            group["factor_max"],
                            group["average_return_pct"],
                            group["median_return_pct"],
                            group["win_rate"],
                            group["cost_adjusted_return_pct"],
                            _canonical_json(group["details_json"]),
                        ),
                    )
                for ablation in ablations:
                    cursor.execute(
                        """
                        INSERT INTO strategy_factor_ablation_result (
                            evaluation_id, factor_key, horizon_days, sample_size,
                            baseline_rank_ic, ablated_rank_ic, rank_ic_delta,
                            baseline_top_bottom_return_pct,
                            ablated_top_bottom_return_pct,
                            top_bottom_delta_pct, redundancy_max_abs_corr,
                            conclusion, details_json
                        ) VALUES (
                            %s,%s,%s,%s,
                            %s,%s,%s,
                            %s,
                            %s,
                            %s,%s,
                            %s,%s
                        )
                        ON DUPLICATE KEY UPDATE
                            sample_size=VALUES(sample_size),
                            baseline_rank_ic=VALUES(baseline_rank_ic),
                            ablated_rank_ic=VALUES(ablated_rank_ic),
                            rank_ic_delta=VALUES(rank_ic_delta),
                            redundancy_max_abs_corr=VALUES(redundancy_max_abs_corr),
                            conclusion=VALUES(conclusion),
                            details_json=VALUES(details_json)
                        """,
                        (
                            ablation["evaluation_id"],
                            ablation["factor_key"],
                            ablation["horizon_days"],
                            ablation["sample_size"],
                            ablation["baseline_rank_ic"],
                            ablation["ablated_rank_ic"],
                            ablation["rank_ic_delta"],
                            ablation["baseline_top_bottom_return_pct"],
                            ablation["ablated_top_bottom_return_pct"],
                            ablation["top_bottom_delta_pct"],
                            ablation["redundancy_max_abs_corr"],
                            ablation["conclusion"],
                            _canonical_json(ablation["details_json"]),
                        ),
                    )

    def _refresh_manifest_maturity(
        self,
        strategy_id: str,
        strategy_version: str,
    ) -> None:
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_factor_snapshot_manifest m
                    SET maturity_state=COALESCE((
                        SELECT CASE
                            WHEN MAX(e.maturity_state='research_candidate')=1
                                THEN 'research_candidate'
                            WHEN MAX(e.maturity_state='provisional')=1
                                THEN 'provisional'
                            WHEN MAX(e.maturity_state='directional_hint')=1
                                THEN 'directional_hint'
                            ELSE 'data_only'
                        END
                        FROM strategy_factor_evaluation e
                        WHERE e.strategy_id=m.strategy_id
                          AND e.strategy_version=m.strategy_version
                    ), 'data_only')
                    WHERE m.strategy_id=%s AND m.strategy_version=%s
                    """,
                    (strategy_id, strategy_version),
                )

    def latest_summary(
        self,
        strategy_id: str,
        *,
        strategy_version: str | None = None,
        horizon_days: int = 5,
        scope_name: str = "eligible_pool",
    ) -> dict[str, Any]:
        conditions = [
            "strategy_id=%s",
            "horizon_days=%s",
            "scope_name=%s",
        ]
        params: list[Any] = [strategy_id, int(horizon_days), scope_name]
        if strategy_version:
            conditions.append("strategy_version=%s")
            params.append(strategy_version)
        sql = f"""
        SELECT *
        FROM strategy_factor_evaluation
        WHERE {' AND '.join(conditions)}
          AND computed_at=(
              SELECT MAX(computed_at)
              FROM strategy_factor_evaluation
              WHERE {' AND '.join(conditions)}
          )
        ORDER BY factor_key
        """
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, [*params, *params])
                evaluations = cursor.fetchall() or []
                groups: list[Mapping[str, Any]] = []
                if evaluations:
                    evaluation_ids = [
                        str(row["evaluation_id"]) for row in evaluations
                    ]
                    placeholders = ",".join(["%s"] * len(evaluation_ids))
                    cursor.execute(
                        f"""
                        SELECT *
                        FROM strategy_factor_group_result
                        WHERE evaluation_id IN ({placeholders})
                        ORDER BY factor_key, group_no
                        """,
                        evaluation_ids,
                    )
                    groups = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT *
                    FROM strategy_factor_ablation_result
                    WHERE evaluation_id=(
                        SELECT evaluation_id
                        FROM strategy_factor_ablation_result
                        WHERE evaluation_id LIKE %s
                        ORDER BY evaluation_id DESC
                        LIMIT 1
                    )
                    ORDER BY factor_key
                    """,
                    (
                        f"{strategy_id}:{strategy_version or '%'}:"
                        f"{scope_name}:{int(horizon_days)}:ablation:%",
                    ),
                )
                ablations = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT trace_mode, COUNT(*) AS snapshot_days,
                           SUM(pre_filter_count) AS pre_filter_rows,
                           SUM(eligible_count) AS eligible_rows,
                           SUM(selected_count) AS selected_rows,
                           MIN(trade_date) AS first_trade_date,
                           MAX(trade_date) AS last_trade_date
                    FROM strategy_factor_snapshot_manifest
                    WHERE strategy_id=%s
                      AND (%s IS NULL OR strategy_version=%s)
                    GROUP BY trace_mode
                    ORDER BY trace_mode
                    """,
                    (strategy_id, strategy_version, strategy_version),
                )
                trace_summary = cursor.fetchall() or []
        decoded = []
        for row in evaluations:
            item = dict(row)
            item["details_json"] = _json_value(item.get("details_json"), {})
            for key, value in list(item.items()):
                if hasattr(value, "as_tuple"):
                    item[key] = float(value)
                elif isinstance(value, (date, datetime)):
                    item[key] = value.isoformat()
            decoded.append(item)

        def decode_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
            decoded_rows: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["details_json"] = _json_value(item.get("details_json"), {})
                for key, value in list(item.items()):
                    if hasattr(value, "as_tuple"):
                        item[key] = float(value)
                    elif isinstance(value, (date, datetime)):
                        item[key] = value.isoformat()
                decoded_rows.append(item)
            return decoded_rows

        return {
            "protocol_id": PROTOCOL_ID,
            "spec_hash": factor_evaluation_spec_hash(),
            "status": "ready" if decoded else "collecting",
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "horizon_days": int(horizon_days),
            "scope_name": scope_name,
            "evaluations": decoded,
            "groups": decode_rows(groups),
            "ablations": decode_rows(ablations),
            "trace_summary": [
                {
                    key: (
                        value.isoformat()
                        if isinstance(value, (date, datetime))
                        else int(value)
                        if key.endswith("_rows") or key == "snapshot_days"
                        else value
                    )
                    for key, value in dict(row).items()
                }
                for row in trace_summary
            ],
            "guardrails": {
                "research_only": True,
                "automatic_weight_change": False,
                "automatic_strategy_promotion": False,
                "selected_only_cross_sectional_ic": False,
            },
        }
