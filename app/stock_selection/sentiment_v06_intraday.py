from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Mapping, Sequence

from app.shared.market_clock import to_shanghai_wall_clock


INTRADAY_FEATURE_VERSION = "sentiment-v06-intraday-v1"


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    return to_shanghai_wall_clock(parsed)


def _pct(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return (numerator / denominator - 1.0) * 100.0


def _path_drawdown(prices: Sequence[float]) -> float | None:
    peak: float | None = None
    worst: float | None = None
    for price in prices:
        peak = price if peak is None else max(peak, price)
        drawdown = _pct(price, peak)
        if drawdown is not None:
            worst = drawdown if worst is None else min(worst, drawdown)
    return worst


def _window_vwap(rows: Sequence[Mapping[str, Any]], reference_price: float) -> tuple[float | None, str]:
    if len(rows) < 2:
        return None, "insufficient_samples"
    first_amount = _number(rows[0].get("amount"))
    last_amount = _number(rows[-1].get("amount"))
    first_volume = _number(rows[0].get("volume"))
    last_volume = _number(rows[-1].get("volume"))
    if None in {first_amount, last_amount, first_volume, last_volume}:
        return None, "cumulative_amount_or_volume_missing"
    amount_delta = float(last_amount) - float(first_amount)
    volume_delta = float(last_volume) - float(first_volume)
    if amount_delta <= 0 or volume_delta <= 0:
        return None, "nonpositive_cumulative_delta"
    raw = amount_delta / volume_delta
    candidates = ((raw, "volume_in_shares"), (raw / 100.0, "volume_in_lots"))
    plausible = [
        (value, unit)
        for value, unit in candidates
        if reference_price * 0.2 <= value <= reference_price * 5.0
    ]
    if not plausible:
        return None, "unit_not_inferable"
    value, unit = min(plausible, key=lambda pair: abs(pair[0] - reference_price))
    return round(value, 6), unit


def summarize_intraday_path(
    rows: Sequence[Mapping[str, Any]],
    *,
    decision_as_of: Any,
    event_available_at: Any = None,
    minimum_samples: int = 3,
    quote_ttl_seconds: int = 180,
) -> dict[str, Any]:
    """Build an auditable path feature set from quotes known by the decision.

    Source and receive clocks are checked independently. A later-received quote
    is retained in diagnostics but excluded from every computed price feature.
    """

    decision_time = _datetime(decision_as_of)
    event_time = _datetime(event_available_at)
    accepted: list[dict[str, Any]] = []
    excluded_after_decision = 0
    excluded_stale = 0
    for raw in rows:
        quote_time = _datetime(raw.get("quote_time") or raw.get("quote_minute"))
        received_at = _datetime(raw.get("received_at"))
        if decision_time is not None and (
            (quote_time is not None and quote_time > decision_time)
            or (received_at is not None and received_at > decision_time)
        ):
            excluded_after_decision += 1
            continue
        if bool(raw.get("is_stale")):
            excluded_stale += 1
            continue
        price = _number(raw.get("latest_price") or raw.get("price"))
        if quote_time is None or received_at is None or price is None or price <= 0:
            continue
        accepted.append({**dict(raw), "_quote_time": quote_time, "_received_at": received_at, "_price": price})
    accepted.sort(key=lambda row: (row["_quote_time"], row["_received_at"]))

    prices = [float(row["_price"]) for row in accepted]
    first = accepted[0] if accepted else None
    last = accepted[-1] if accepted else None
    sample_count = len(accepted)
    first_price = prices[0] if prices else None
    last_price = prices[-1] if prices else None
    high_price = max(prices) if prices else None
    low_price = min(prices) if prices else None
    short_return = (
        _pct(float(last_price), float(first_price))
        if last_price is not None and first_price is not None
        else None
    )
    high_drawdown = (
        _pct(float(last_price), float(high_price))
        if last_price is not None and high_price is not None
        else None
    )
    low_repair = (
        _pct(float(last_price), float(low_price))
        if last_price is not None and low_price is not None
        else None
    )
    worst_path_drawdown = _path_drawdown(prices)
    vwap, volume_unit = (
        _window_vwap(accepted, float(last_price))
        if last_price is not None
        else (None, "missing_price")
    )
    vwap_deviation = (
        _pct(float(last_price), float(vwap))
        if last_price is not None and vwap is not None
        else None
    )
    quote_age_seconds = (
        max((decision_time - last["_quote_time"]).total_seconds(), 0.0)
        if decision_time is not None and last is not None
        else None
    )
    window_minutes = (
        max((last["_quote_time"] - first["_quote_time"]).total_seconds() / 60.0, 0.0)
        if first is not None and last is not None
        else 0.0
    )
    source_values = {
        str(row.get("source") or "") for row in accepted if str(row.get("source") or "")
    }
    trade_dates = {
        str(row.get("trade_date") or "")[:10]
        for row in accepted
        if str(row.get("trade_date") or "")
    }
    receive_order_valid = all(
        row["_received_at"] >= row["_quote_time"] for row in accepted
    )
    trade_date_matches = bool(
        decision_time is None
        or not trade_dates
        or trade_dates == {decision_time.date().isoformat()}
    )
    clock_consistent = (
        len(source_values) <= 1
        and len(trade_dates) <= 1
        and receive_order_valid
        and trade_date_matches
    )
    receive_age_seconds = (
        max((decision_time - last["_received_at"]).total_seconds(), 0.0)
        if decision_time is not None and last is not None
        else None
    )
    if sample_count < minimum_samples:
        data_status = "insufficient_samples"
    elif quote_age_seconds is None:
        data_status = "clock_missing"
    elif (
        quote_age_seconds > quote_ttl_seconds
        or receive_age_seconds is None
        or receive_age_seconds > quote_ttl_seconds
    ):
        data_status = "stale"
    elif not clock_consistent:
        data_status = "mixed_clock"
    else:
        data_status = "complete"

    if data_status != "complete":
        path_state = "unknown"
    elif (
        short_return is not None
        and high_drawdown is not None
        and short_return >= 0
        and high_drawdown >= -1.2
        and (vwap_deviation is None or vwap_deviation >= -0.3)
    ):
        path_state = "sustained_support"
    elif high_drawdown is not None and high_drawdown <= -2.5:
        path_state = "spike_fade"
    elif low_repair is not None and short_return is not None and low_repair >= 2.0 and short_return >= -0.5:
        path_state = "repair"
    else:
        path_state = "weak_or_unconfirmed"

    amount_delta = None
    if first is not None and last is not None:
        first_amount = _number(first.get("amount"))
        last_amount = _number(last.get("amount"))
        if first_amount is not None and last_amount is not None:
            amount_delta = max(last_amount - first_amount, 0.0)

    pre_event_rows = (
        [row for row in accepted if row["_quote_time"] < event_time]
        if event_time is not None
        else []
    )
    pre_event_price = (
        float(pre_event_rows[-1]["_price"]) if pre_event_rows else None
    )
    event_response = (
        _pct(float(last_price), pre_event_price)
        if last_price is not None and pre_event_price is not None
        else None
    )

    return {
        "feature_version": INTRADAY_FEATURE_VERSION,
        "data_status": data_status,
        "path_state": path_state,
        "sample_count": sample_count,
        "minimum_samples": minimum_samples,
        "window_minutes": round(window_minutes, 4),
        "first_quote_time": first["_quote_time"].strftime("%Y-%m-%d %H:%M:%S") if first else None,
        "last_quote_time": last["_quote_time"].strftime("%Y-%m-%d %H:%M:%S") if last else None,
        "last_received_at": last["_received_at"].strftime("%Y-%m-%d %H:%M:%S") if last else None,
        "quote_age_seconds": round(quote_age_seconds, 4) if quote_age_seconds is not None else None,
        "receive_age_seconds": round(receive_age_seconds, 4)
        if receive_age_seconds is not None
        else None,
        "first_price": round(first_price, 6) if first_price is not None else None,
        "last_price": round(last_price, 6) if last_price is not None else None,
        "high_price": round(high_price, 6) if high_price is not None else None,
        "low_price": round(low_price, 6) if low_price is not None else None,
        "short_return_pct": round(short_return, 4) if short_return is not None else None,
        "high_drawdown_pct": round(high_drawdown, 4) if high_drawdown is not None else None,
        "worst_path_drawdown_pct": round(worst_path_drawdown, 4) if worst_path_drawdown is not None else None,
        "low_repair_pct": round(low_repair, 4) if low_repair is not None else None,
        "window_vwap": vwap,
        "window_vwap_unit": volume_unit,
        "vwap_deviation_pct": round(vwap_deviation, 4) if vwap_deviation is not None else None,
        "amount_delta": round(amount_delta, 4) if amount_delta is not None else None,
        "event_available_at": event_time.strftime("%Y-%m-%d %H:%M:%S")
        if event_time
        else None,
        "pre_event_quote_time": pre_event_rows[-1]["_quote_time"].strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if pre_event_rows
        else None,
        "pre_event_price": round(pre_event_price, 6)
        if pre_event_price is not None
        else None,
        "event_response_pct": round(event_response, 4)
        if event_response is not None
        else None,
        "source": next(iter(source_values), None) if len(source_values) == 1 else None,
        "trade_date": next(iter(trade_dates), None) if len(trade_dates) == 1 else None,
        "clock_consistent": clock_consistent,
        "receive_order_valid": receive_order_valid,
        "trade_date_matches_decision": trade_date_matches,
        "excluded_after_decision": excluded_after_decision,
        "excluded_stale": excluded_stale,
    }
