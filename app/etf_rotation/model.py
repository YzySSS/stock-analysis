from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Mapping

from app.market_timing.leadership_cycle import (
    classify_cycle,
    compute_price_metrics,
)


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _clip(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _weighted_score(values: dict[str, float], weights: dict[str, Any]) -> float:
    return round(sum(values[key] * float(weights[key]) for key in weights), 6)


def _date_text(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    return str(value or "")[:10]


def _percentile_scores(values: dict[str, float | None]) -> dict[str, float]:
    present = sorted((value, key) for key, value in values.items() if value is not None)
    if not present:
        return {key: 50.0 for key in values}
    if len(present) == 1:
        return {key: 50.0 for key in values}

    scores: dict[str, float] = {}
    index = 0
    while index < len(present):
        end = index + 1
        while end < len(present) and present[end][0] == present[index][0]:
            end += 1
        average_rank = (index + end - 1) / 2
        score = average_rank / (len(present) - 1) * 100
        for _, key in present[index:end]:
            scores[key] = round(score, 6)
        index = end
    return {key: scores.get(key, 50.0) for key in values}


def _compound_return_pct(rows: list[dict[str, Any]], days: int) -> float | None:
    changes = [
        value
        for row in rows[-days:]
        if (value := _number(row.get("pct_change"))) is not None
    ]
    if len(changes) < days:
        return None
    result = 1.0
    for value in changes:
        result *= 1 + value / 100
    return (result - 1) * 100


def _price_return_pct(rows: list[dict[str, Any]], days: int) -> float | None:
    if len(rows) < days + 1:
        return None
    latest = _number(rows[-1].get("close"))
    previous = _number(rows[-(days + 1)].get("close"))
    if latest is None or previous in {None, 0}:
        return None
    return (latest / previous - 1) * 100


def _provider_pct_change(row: Mapping[str, Any]) -> float | None:
    return _number(row.get("pct_chg", row.get("pct_change")))


def _unit_adjustments(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Detect ETF unit splits/consolidations from price-vs-provider returns."""

    adjustments = []
    for index, (previous, current) in enumerate(zip(rows, rows[1:]), start=1):
        previous_close = _number(previous.get("close"))
        current_close = _number(current.get("close"))
        declared_change = _provider_pct_change(current)
        if (
            previous_close in {None, 0}
            or current_close in {None, 0}
            or declared_change is None
        ):
            continue
        actual_ratio = current_close / previous_close
        expected_ratio = 1 + declared_change / 100
        if actual_ratio <= 0 or expected_ratio <= 0:
            continue
        unit_factor = expected_ratio / actual_ratio
        if 0.77 <= unit_factor <= 1.30:
            continue
        previous_share = _number(previous.get("fund_share_10k"))
        current_share = _number(current.get("fund_share_10k"))
        share_ratio = (
            current_share / previous_share
            if previous_share not in {None, 0} and current_share is not None
            else None
        )
        share_aligned = (
            share_ratio is not None
            and abs(share_ratio / unit_factor - 1) <= 0.25
        )
        adjustments.append(
            {
                "index": index,
                "trade_date": _date_text(current.get("trade_date")),
                "unit_factor": unit_factor,
                "share_ratio": share_ratio,
                "share_aligned": share_aligned,
            }
        )
    return adjustments


def _fund_return_pct(
    rows: list[dict[str, Any]],
    days: int,
) -> tuple[float | None, str, list[dict[str, Any]]]:
    window = rows[-(days + 1) :]
    adjustments = _unit_adjustments(window)
    changes = [_provider_pct_change(row) for row in rows[-days:]]
    if len(changes) == days and all(value is not None for value in changes):
        result = 1.0
        for value in changes:
            result *= 1 + float(value) / 100
        return (result - 1) * 100, "provider_pct_chg_compounded", adjustments
    if adjustments:
        return None, "blocked_raw_price_adjustment_break", adjustments
    return _price_return_pct(rows, days), "raw_close_fallback", adjustments


def _adjusted_share_change_pct(
    rows: list[dict[str, Any]],
    days: int,
) -> tuple[float | None, str, list[dict[str, Any]]]:
    window = rows[-(days + 1) :]
    share_points = [
        (index, value)
        for index, row in enumerate(window)
        if (value := _number(row.get("fund_share_10k"))) is not None
    ]
    if len(share_points) < 2 or share_points[0][1] == 0:
        return None, "missing_share_history", []
    start_index, start_share = share_points[0]
    end_index, end_share = share_points[-1]
    adjustments = [
        item
        for item in _unit_adjustments(window)
        if start_index < int(item["index"]) <= end_index
    ]
    unit_factor = math.prod(float(item["unit_factor"]) for item in adjustments)
    normalized_start = start_share * unit_factor
    if normalized_start == 0:
        return None, "missing_share_history", adjustments
    if adjustments and all(item["share_aligned"] for item in adjustments):
        basis = "price_and_share_confirmed_unit_adjusted_fund_share"
    elif adjustments:
        basis = "price_inferred_unit_adjusted_fund_share"
    else:
        basis = "raw_fund_share_no_adjustment"
    return (end_share / normalized_start - 1) * 100, basis, adjustments


def aggregate_sector_history(
    sector_rows: list[dict[str, Any]],
    sectors: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    aliases_by_sector = {
        item["sector_id"]: set(item["fund_flow_industries"])
        for item in sectors
    }
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = {
        sector_id: defaultdict(list) for sector_id in aliases_by_sector
    }
    for row in sector_rows:
        industry = str(row.get("industry_name") or row.get("industry") or "")
        trade_date = _date_text(row.get("trade_date"))
        if not industry or not trade_date:
            continue
        for sector_id, aliases in aliases_by_sector.items():
            if industry in aliases:
                grouped[sector_id][trade_date].append(row)

    histories: dict[str, list[dict[str, Any]]] = {}
    for item in sectors:
        sector_id = item["sector_id"]
        aliases = aliases_by_sector[sector_id]
        rows: list[dict[str, Any]] = []
        for trade_date, members in sorted(grouped[sector_id].items()):
            net_values = [
                value
                for member in members
                if (value := _number(member.get("net_amount"))) is not None
            ]
            pct_values = [
                value
                for member in members
                if (value := _number(
                    member.get("pct_change", member.get("pct_chg"))
                )) is not None
            ]
            present_aliases = sorted(
                {
                    str(member.get("industry_name") or member.get("industry") or "")
                    for member in members
                }
            )
            rows.append(
                {
                    "trade_date": trade_date,
                    "net_amount": _mean(net_values),
                    "pct_change": _mean(pct_values),
                    "present_aliases": present_aliases,
                    "alias_coverage": len(present_aliases) / len(aliases),
                }
            )
        histories[sector_id] = rows
    return histories


def _sector_cycle_evidence(
    rows: list[dict[str, Any]],
    *,
    minimum_history_days: int,
) -> dict[str, Any]:
    level = 100.0
    series = []
    for row in rows:
        pct_change = _number(row.get("pct_change"))
        if pct_change is None or pct_change <= -100:
            continue
        level *= 1 + pct_change / 100
        series.append(
            {
                "trade_date": row["trade_date"],
                "value": level,
            }
        )
    metrics = compute_price_metrics(
        series,
        minimum_days=minimum_history_days,
    )
    cycle = classify_cycle(metrics)
    return {
        "status": metrics.get("status"),
        "cycle_state": cycle["cycle_state"],
        "cycle_label": cycle["cycle_label"],
        "metrics": metrics,
        "reasons": cycle["reasons"],
    }


def _sector_scores(
    *,
    histories: dict[str, list[dict[str, Any]]],
    opinion_scores: dict[str, dict[str, Any]],
    spec: dict[str, Any],
    trade_date: str,
) -> dict[str, dict[str, Any]]:
    contract = spec["data_contract"]
    weights = spec["scoring"]["sector_weights"]
    lookback = int(contract["sector_lookback_trade_days"])
    persistence_days = int(contract["sector_persistence_trade_days"])
    cycle_history_days = int(contract["minimum_sector_cycle_history_days"])
    missing_score = float(contract["missing_numeric_score"])

    dates = sorted(
        {
            row["trade_date"]
            for rows in histories.values()
            for row in rows
            if row["trade_date"] <= trade_date
        }
    )[-lookback:]
    daily_percentiles: dict[str, dict[str, float]] = {}
    for current_date in dates:
        daily_values = {
            sector_id: next(
                (
                    _number(row.get("net_amount"))
                    for row in reversed(rows)
                    if row["trade_date"] == current_date
                ),
                None,
            )
            for sector_id, rows in histories.items()
        }
        daily_percentiles[current_date] = _percentile_scores(daily_values)

    latest_flow_values = {
        sector_id: next(
            (
                _number(row.get("net_amount"))
                for row in reversed(rows)
                if row["trade_date"] == trade_date
            ),
            None,
        )
        for sector_id, rows in histories.items()
    }
    latest_flow_scores = _percentile_scores(latest_flow_values)

    results: dict[str, dict[str, Any]] = {}
    for item in spec["sectors"]:
        sector_id = item["sector_id"]
        all_rows = [
            row
            for row in histories.get(sector_id, [])
            if row["trade_date"] <= trade_date
        ]
        cycle_evidence = _sector_cycle_evidence(
            all_rows,
            minimum_history_days=cycle_history_days,
        )
        rows = all_rows[-lookback:]
        latest = rows[-1] if rows else {}
        recent = rows[-persistence_days:]
        positive_count = sum(
            1 for row in recent if (_number(row.get("net_amount")) or 0) > 0
        )
        strong_count = sum(
            1
            for row in recent
            if daily_percentiles.get(row["trade_date"], {}).get(sector_id, 50) >= 60
        )
        persistence_score = (
            50 * positive_count / len(recent) + 50 * strong_count / len(recent)
            if recent
            else missing_score
        )
        return_5d = _compound_return_pct(rows, 5)
        return_20d = _compound_return_pct(rows, 20)
        trend_score = (
            _clip(50 + 3 * return_5d + 1.5 * return_20d)
            if return_5d is not None and return_20d is not None
            else missing_score
        )
        opinion = opinion_scores.get(sector_id) or {}
        opinion_score = _number(opinion.get("score"))
        components = {
            "flow_strength": latest_flow_scores.get(sector_id, missing_score),
            "flow_persistence": persistence_score,
            "price_trend": trend_score,
            "opinion": opinion_score if opinion_score is not None else missing_score,
        }
        results[sector_id] = {
            "score": _weighted_score(components, weights),
            "components": {key: round(value, 6) for key, value in components.items()},
            "history_days": len(rows),
            "latest_trade_date": latest.get("trade_date"),
            "latest_mean_net_amount": _number(latest.get("net_amount")),
            "latest_alias_coverage": _number(latest.get("alias_coverage")),
            "return_5d_pct": return_5d,
            "return_20d_pct": return_20d,
            "opinion": opinion,
            "cycle_evidence": cycle_evidence,
        }
    return results


def _etf_scores(
    *,
    fund_rows_by_code: dict[str, list[dict[str, Any]]],
    spec: dict[str, Any],
    trade_date: str,
) -> dict[str, dict[str, Any]]:
    contract = spec["data_contract"]
    weights = spec["scoring"]["etf_weights"]
    lookback = int(contract["etf_lookback_trade_days"])
    liquidity_days = int(contract["liquidity_lookback_trade_days"])
    share_days = int(contract["share_change_lookback_trade_days"])
    missing_score = float(contract["missing_numeric_score"])

    prepared: dict[str, dict[str, Any]] = {}
    for item in spec["sectors"]:
        ts_code = item["etf"]["ts_code"]
        rows = sorted(
            (
                {**row, "trade_date": _date_text(row.get("trade_date"))}
                for row in fund_rows_by_code.get(ts_code, [])
                if _date_text(row.get("trade_date")) <= trade_date
            ),
            key=lambda row: row["trade_date"],
        )[-lookback:]
        latest = rows[-1] if rows else {}
        amount_values = [
            value
            for row in rows[-liquidity_days:]
            if (value := _number(row.get("amount_yuan"))) is not None
        ]
        average_amount = _mean(amount_values)
        ret5, return_5d_basis, adjustments5 = _fund_return_pct(rows, 5)
        ret20, return_20d_basis, adjustments20 = _fund_return_pct(rows, 20)
        trend_score = (
            _clip(50 + 3 * ret5 + 1.5 * ret20)
            if ret5 is not None and ret20 is not None
            else missing_score
        )
        share_change, share_change_basis, share_adjustments = (
            _adjusted_share_change_pct(rows, share_days)
        )
        premium_discount = _number(latest.get("premium_discount_pct"))
        prepared[ts_code] = {
            "rows": rows,
            "latest": latest,
            "average_amount_20d_yuan": average_amount,
            "return_5d_pct": ret5,
            "return_20d_pct": ret20,
            "return_5d_basis": return_5d_basis,
            "return_20d_basis": return_20d_basis,
            "unit_adjustments": adjustments20 or adjustments5,
            "trend_score": trend_score,
            "share_change_20d_pct": share_change,
            "share_change_basis": share_change_basis,
            "share_unit_adjustments": share_adjustments,
            "share_change_score": (
                _clip(50 + 2 * share_change)
                if share_change is not None
                else missing_score
            ),
            "premium_discount_pct": premium_discount,
            "tracking_score": (
                _clip(100 - 25 * abs(premium_discount))
                if premium_discount is not None
                else missing_score
            ),
        }

    liquidity_scores = _percentile_scores(
        {
            ts_code: item["average_amount_20d_yuan"]
            for ts_code, item in prepared.items()
        }
    )
    for ts_code, item in prepared.items():
        components = {
            "liquidity": liquidity_scores[ts_code],
            "price_trend": item["trend_score"],
            "share_change": item["share_change_score"],
            "tracking": item["tracking_score"],
        }
        item["components"] = {
            key: round(value, 6) for key, value in components.items()
        }
        item["score"] = _weighted_score(components, weights)
        item["history_days"] = len(item["rows"])
    return prepared


def build_rotation_candidates(
    *,
    spec: dict[str, Any],
    trade_date: str,
    sector_rows: list[dict[str, Any]],
    fund_rows_by_code: dict[str, list[dict[str, Any]]],
    opinion_scores: dict[str, dict[str, Any]],
    timing_signal: dict[str, Any] | None,
) -> dict[str, Any]:
    contract = spec["data_contract"]
    scoring = spec["scoring"]
    histories = aggregate_sector_history(sector_rows, spec["sectors"])
    sector_scores = _sector_scores(
        histories=histories,
        opinion_scores=opinion_scores,
        spec=spec,
        trade_date=trade_date,
    )
    etf_scores = _etf_scores(
        fund_rows_by_code=fund_rows_by_code,
        spec=spec,
        trade_date=trade_date,
    )

    timing_state = str((timing_signal or {}).get("state") or "missing")
    timing_caps = spec["risk_overlay"]["timing_state_max_selections"]
    allowed_cycle_states = set(
        spec["risk_overlay"]["sector_cycle_allowed_states"]
    )
    selection_cap = min(
        int(spec["maximum_selections"]),
        int(timing_caps.get(timing_state, timing_caps["missing"])),
    )
    timing_aligned = _date_text((timing_signal or {}).get("trade_date")) == trade_date
    if not timing_aligned:
        timing_state = "missing"
        selection_cap = 0

    combined_weights = scoring["combined_weights"]
    candidates: list[dict[str, Any]] = []
    signal_date = date.fromisoformat(trade_date)
    for sector in spec["sectors"]:
        sector_id = sector["sector_id"]
        etf = sector["etf"]
        sector_item = sector_scores[sector_id]
        etf_item = etf_scores[etf["ts_code"]]
        latest = etf_item["latest"]
        list_date = date.fromisoformat(
            f"{etf['list_date'][:4]}-{etf['list_date'][4:6]}-{etf['list_date'][6:8]}"
        )
        listing_days = (signal_date - list_date).days
        premium_discount = etf_item["premium_discount_pct"]
        gates = {
            "timing_aligned": timing_aligned,
            "sector_latest_aligned": sector_item["latest_trade_date"] == trade_date,
            "sector_history_sufficient": sector_item["history_days"]
            >= int(contract["minimum_sector_history_days"]),
            "sector_alias_coverage_complete": (
                sector_item["latest_alias_coverage"] is not None
                and sector_item["latest_alias_coverage"] >= 1
            ),
            "sector_cycle_evidence_ready": (
                sector_item["cycle_evidence"]["status"] == "ready"
            ),
            "sector_cycle_allows_entry": (
                sector_item["cycle_evidence"]["cycle_state"]
                in allowed_cycle_states
            ),
            "opinion_available": _number(sector_item["opinion"].get("score")) is not None,
            "opinion_aligned": _date_text(
                sector_item["opinion"].get("trade_date")
            ) == trade_date,
            "opinion_alias_coverage_complete": (
                _number(sector_item["opinion"].get("alias_coverage")) is not None
                and _number(sector_item["opinion"].get("alias_coverage")) >= 1
            ),
            "etf_latest_aligned": _date_text(latest.get("trade_date")) == trade_date,
            "etf_history_sufficient": etf_item["history_days"]
            >= int(contract["minimum_etf_history_days"]),
            "etf_return_adjustment_safe": (
                etf_item["return_5d_pct"] is not None
                and etf_item["return_20d_pct"] is not None
                and not str(etf_item["return_20d_basis"]).startswith("blocked_")
            ),
            "listing_age_sufficient": listing_days
            >= int(contract["minimum_listing_days"]),
            "liquidity_sufficient": (
                etf_item["average_amount_20d_yuan"] is not None
                and etf_item["average_amount_20d_yuan"]
                >= float(contract["minimum_average_amount_20d_yuan"])
            ),
            "share_history_available": etf_item["share_change_20d_pct"] is not None,
            "nav_available": premium_discount is not None,
            "nav_fresh": (
                latest.get("nav_date") is not None
                and 0
                <= (
                    signal_date
                    - date.fromisoformat(_date_text(latest.get("nav_date")))
                ).days
                <= int(contract["maximum_nav_staleness_calendar_days"])
            ),
            "premium_discount_within_limit": (
                premium_discount is not None
                and abs(premium_discount)
                <= float(contract["maximum_absolute_premium_discount_pct"])
            ),
        }
        data_completeness_gates = (
            "sector_latest_aligned",
            "sector_history_sufficient",
            "sector_alias_coverage_complete",
            "sector_cycle_evidence_ready",
            "opinion_available",
            "opinion_aligned",
            "opinion_alias_coverage_complete",
            "etf_latest_aligned",
            "etf_history_sufficient",
            "etf_return_adjustment_safe",
            "share_history_available",
            "nav_available",
            "nav_fresh",
        )
        data_complete = all(gates[name] for name in data_completeness_gates)
        hard_gate_pass = all(gates.values())
        combined_score = round(
            sector_item["score"] * float(combined_weights["sector"])
            + etf_item["score"] * float(combined_weights["etf"]),
            6,
        )
        score_gate_pass = (
            sector_item["score"] >= float(scoring["minimum_sector_score"])
            and combined_score >= float(scoring["minimum_combined_score"])
        )
        candidates.append(
            {
                "sector_id": sector_id,
                "sector_name": sector["sector_name"],
                "ts_code": etf["ts_code"],
                "fund_name": etf["name"],
                "sector_score": sector_item["score"],
                "etf_score": etf_item["score"],
                "combined_score": combined_score,
                "data_complete": data_complete,
                "hard_gate_pass": hard_gate_pass,
                "is_eligible": hard_gate_pass and score_gate_pass,
                "is_selected": False,
                "gates": gates,
                "sector_components": sector_item["components"],
                "etf_components": etf_item["components"],
                "latest_close": _number(latest.get("close")),
                "average_amount_20d_yuan": etf_item["average_amount_20d_yuan"],
                "share_change_20d_pct": etf_item["share_change_20d_pct"],
                "premium_discount_pct": premium_discount,
                "evidence": {
                    "listing_days": listing_days,
                    "sector_history_days": sector_item["history_days"],
                    "etf_history_days": etf_item["history_days"],
                    "sector_latest_mean_net_amount": sector_item["latest_mean_net_amount"],
                    "sector_alias_coverage": sector_item["latest_alias_coverage"],
                    "sector_return_5d_pct": sector_item["return_5d_pct"],
                    "sector_return_20d_pct": sector_item["return_20d_pct"],
                    "sector_cycle_state": sector_item["cycle_evidence"][
                        "cycle_state"
                    ],
                    "sector_cycle_label": sector_item["cycle_evidence"][
                        "cycle_label"
                    ],
                    "sector_cycle_metrics": sector_item["cycle_evidence"][
                        "metrics"
                    ],
                    "sector_cycle_reasons": sector_item["cycle_evidence"][
                        "reasons"
                    ],
                    "etf_return_5d_pct": etf_item["return_5d_pct"],
                    "etf_return_20d_pct": etf_item["return_20d_pct"],
                    "etf_return_5d_basis": etf_item["return_5d_basis"],
                    "etf_return_20d_basis": etf_item["return_20d_basis"],
                    "etf_unit_adjustments": etf_item["unit_adjustments"],
                    "share_change_basis": etf_item["share_change_basis"],
                    "share_unit_adjustments": etf_item[
                        "share_unit_adjustments"
                    ],
                    "opinion": sector_item["opinion"],
                    "score_gate_pass": score_gate_pass,
                },
            }
        )

    # Universe readiness covers evidence availability. Investability failures
    # exclude that ETF, but do not erase complete evidence for the other ETFs.
    complete_count = sum(1 for item in candidates if item["data_complete"])
    complete_ratio = complete_count / len(candidates) if candidates else 0.0
    universe_ready = complete_ratio >= float(
        contract["minimum_complete_universe_ratio"]
    )
    if not universe_ready:
        selection_cap = 0

    candidates.sort(
        key=lambda item: (
            not item["is_eligible"],
            -item["combined_score"],
            item["sector_id"],
        )
    )
    selected = 0
    for rank, candidate in enumerate(candidates, start=1):
        candidate["rank_no"] = rank
        if candidate["is_eligible"] and selected < selection_cap:
            candidate["is_selected"] = True
            selected += 1
    return {
        "trade_date": trade_date,
        "timing_state": timing_state,
        "timing_aligned": timing_aligned,
        "selection_cap": selection_cap,
        "candidate_count": len(candidates),
        "complete_candidate_count": complete_count,
        "complete_universe_ratio": round(complete_ratio, 6),
        "universe_ready": universe_ready,
        "eligible_count": sum(1 for item in candidates if item["is_eligible"]),
        "selected_count": selected,
        "candidates": candidates,
    }
