from __future__ import annotations

import copy
import hashlib
import json
import math
from collections import Counter
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from app.shared.market_clock import to_shanghai_wall_clock


SPEC_PATH = (
    Path(__file__).resolve().parent
    / "specs"
    / "sentiment_v06_forward_evaluation_v1.json"
)
EXPECTED_SPEC_SHA256 = (
    "037f57554eaa79dbf849d6f78e84714ec0235ec1e67b9c1bd1af816864582eb5"
)


@lru_cache(maxsize=1)
def _read_spec() -> tuple[dict[str, Any], str]:
    payload = SPEC_PATH.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if digest != EXPECTED_SPEC_SHA256:
        raise RuntimeError(
            "sentiment_v06_forward_evaluation_v1 checksum mismatch; create a "
            "new protocol version instead of editing the preregistered file"
        )
    spec = json.loads(payload.decode("utf-8"))
    if spec.get("spec_id") != "sentiment_v06_forward_evaluation_v1":
        raise RuntimeError("unexpected sentiment v0.6 evaluation spec_id")
    if spec.get("strategy_id") != "a_share_sentiment_v06":
        raise RuntimeError("unexpected sentiment v0.6 evaluation strategy_id")
    return spec, digest


def load_sentiment_v06_evaluation_spec() -> dict[str, Any]:
    """Return a defensive copy of the frozen, not-yet-activated protocol."""

    spec, _digest = _read_spec()
    return copy.deepcopy(spec)


def sentiment_v06_evaluation_spec_hash() -> str:
    return _read_spec()[1]


def evaluation_denominator_counts(
    decisions: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """Count all decision and execution states, including negative space.

    This helper intentionally has no profitability filter. Empty, failed,
    untriggered and unfilled observations remain visible denominators rather
    than disappearing from an eventual report.
    """

    counts: Counter[str] = Counter()
    for decision in decisions:
        counts["all_decisions"] += 1
        status = str(decision.get("status") or "unknown").strip().lower()
        if status in {"failed", "data_failed", "no_data"}:
            counts["data_failed_decisions"] += 1
        recalled = max(int(decision.get("recalled_count") or 0), 0)
        scored = max(int(decision.get("scored_count") or 0), 0)
        displayed = max(int(decision.get("displayed_count") or 0), 0)
        conditions_met = max(int(decision.get("conditions_met_count") or 0), 0)
        filled = max(int(decision.get("filled_count") or 0), 0)
        untriggered = max(int(decision.get("untriggered_count") or 0), 0)
        unfilled = max(int(decision.get("unfilled_count") or 0), 0)
        counts["all_recalled_candidates"] += recalled
        counts["all_scored_candidates"] += scored
        counts["all_displayed_candidates"] += displayed
        counts["conditions_met_candidates"] += conditions_met
        counts["filled_simulated_entries"] += filled
        counts["untriggered_candidates"] += untriggered
        counts["unfilled_candidates"] += unfilled
        if recalled == 0:
            counts["empty_decisions"] += 1
    spec = load_sentiment_v06_evaluation_spec()
    return {
        key: int(counts.get(key, 0))
        for key in spec.get("denominators") or []
    }


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


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _limit_rate(code: str, name: str) -> float:
    if "ST" in name.upper():
        return 5.0
    if code.startswith(("sz.300", "sh.688")):
        return 20.0
    if code.startswith("bj."):
        return 30.0
    return 10.0


def _sell_block_reason(bar: Mapping[str, Any]) -> str | None:
    if bool(bar.get("is_suspended")):
        return "suspended_or_no_quote"
    if bool(bar.get("sell_blocked") or bar.get("limit_down_untradable")):
        return "sell_blocked_limit_down"
    open_price = _number(bar.get("open"))
    high = _number(bar.get("high"))
    low = _number(bar.get("low"))
    close = _number(bar.get("close"))
    if None in {open_price, high, low, close} or min(
        float(open_price), float(high), float(low), float(close)
    ) <= 0:
        return "suspended_or_no_quote"
    prev_close = _number(bar.get("prev_close"))
    if prev_close is None or prev_close <= 0:
        return None
    limit_price = prev_close * (1.0 - _limit_rate(
        str(bar.get("code") or ""), str(bar.get("name") or "")
    ) / 100.0)
    one_price_down = (
        abs(float(open_price) - float(high)) < 1e-9
        and abs(float(high) - float(low)) < 1e-9
        and abs(float(low) - float(close)) < 1e-9
        and float(close) <= limit_price * 1.002
    )
    return "sell_blocked_limit_down" if one_price_down else None


def _buy_block_reason(quote: Mapping[str, Any]) -> str | None:
    if bool(quote.get("buy_blocked") or quote.get("limit_up_untradable")):
        return "buy_blocked_limit_up"
    price = _number(quote.get("latest_price", quote.get("price")))
    open_price = _number(quote.get("open_price", quote.get("open")))
    high = _number(quote.get("high_price", quote.get("high")))
    low = _number(quote.get("low_price", quote.get("low")))
    prev_close = _number(quote.get("pre_close", quote.get("prev_close")))
    if None in {price, open_price, high, low, prev_close} or prev_close <= 0:
        return None
    limit_price = prev_close * (
        1.0
        + _limit_rate(
            str(quote.get("code") or ""), str(quote.get("name") or "")
        )
        / 100.0
    )
    one_price_up = (
        abs(float(open_price) - float(high)) < 1e-9
        and abs(float(high) - float(low)) < 1e-9
        and abs(float(low) - float(price)) < 1e-9
        and float(price) >= limit_price * 0.998
    )
    return "buy_blocked_limit_up" if one_price_up else None


def simulate_sentiment_v06_execution(
    *,
    decision_as_of: Any,
    valid_until: Any,
    entry_quotes: Sequence[Mapping[str, Any]],
    daily_bars: Sequence[Mapping[str, Any]],
    horizon_trade_days: int = 3,
    observed_through: Any = None,
) -> dict[str, Any]:
    """Simulate the frozen V0.6 execution contract without inventing fills.

    The entry session counts as holding session one. T+1 remains the earliest
    sell session. A blocked target exit carries forward to the next executable
    opening quote and stays non-terminal while no such quote exists.
    """

    decision = _datetime(decision_as_of)
    expiry = _datetime(valid_until)
    observed_time = _datetime(observed_through)
    if decision is None or expiry is None or horizon_trade_days < 1:
        return {
            "status": "data_failed",
            "terminal": True,
            "reason": "invalid_execution_contract",
            "events": [],
        }

    events: list[dict[str, Any]] = []
    entry: dict[str, Any] | None = None
    execution_spec = load_sentiment_v06_evaluation_spec()["execution"]
    observation_cutoff = (
        min(expiry, observed_time) if observed_time is not None else expiry
    )
    minimum_entry_time = decision + timedelta(
        seconds=max(int(execution_spec.get("minimum_entry_delay_seconds") or 0), 0)
    )
    for raw in sorted(
        (dict(row) for row in entry_quotes),
        key=lambda row: str(row.get("quote_time") or ""),
    ):
        quote_time = _datetime(raw.get("quote_time"))
        received_at = _datetime(raw.get("received_at"))
        if (
            quote_time is None
            or received_at is None
            or quote_time <= decision
            or quote_time > observation_cutoff
            or received_at > observation_cutoff
            or received_at < quote_time
        ):
            continue
        if quote_time < minimum_entry_time:
            events.append(
                {
                    "event_type": "entry_deferred",
                    "event_time": quote_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "price": _number(raw.get("latest_price", raw.get("price"))),
                    "executable": False,
                    "block_reason": "minimum_decision_latency",
                }
            )
            continue
        price = _number(raw.get("latest_price", raw.get("price")))
        block_reason = None
        if bool(raw.get("is_suspended")) or price is None or price <= 0:
            block_reason = "suspended_or_no_quote"
        else:
            block_reason = _buy_block_reason(raw)
        events.append(
            {
                "event_type": "entry_blocked" if block_reason else "entry_filled",
                "event_time": quote_time.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "executable": block_reason is None,
                "block_reason": block_reason,
            }
        )
        if block_reason is None:
            entry = {**raw, "_quote_time": quote_time, "_price": float(price)}
            break

    if entry is None:
        expired = observed_time is not None and observed_time >= expiry
        return {
            "status": "unfilled" if expired else "pending_entry",
            "terminal": expired,
            "reason": "entry_window_expired" if expired else "entry_window_open",
            "events": events,
        }

    entry_date = entry["_quote_time"].date()
    bars = sorted(
        (
            {**dict(row), "_trade_date": parsed_date}
            for row in daily_bars
            if (parsed_date := _date(row.get("trade_date"))) is not None
            and parsed_date >= entry_date
            and (observed_time is None or parsed_date <= observed_time.date())
        ),
        key=lambda row: row["_trade_date"],
    )
    deduped_bars: list[dict[str, Any]] = []
    for bar in bars:
        if not deduped_bars or deduped_bars[-1]["_trade_date"] != bar["_trade_date"]:
            deduped_bars.append(bar)
    bars = deduped_bars
    if not bars or bars[0]["_trade_date"] != entry_date:
        return {
            "status": "data_failed",
            "terminal": True,
            "reason": "entry_session_bar_missing",
            "entry_trade_date": entry_date.isoformat(),
            "entry_price": entry["_price"],
            "events": events,
        }
    entry_low = _number(bars[0].get("low"))
    entry_high = _number(bars[0].get("high"))
    if (
        entry_low is None
        or entry_high is None
        or not entry_low <= entry["_price"] <= entry_high
    ):
        return {
            "status": "data_failed",
            "terminal": True,
            "reason": "entry_price_outside_daily_range",
            "entry_trade_date": entry_date.isoformat(),
            "entry_price": entry["_price"],
            "events": events,
        }

    target_index = max(horizon_trade_days - 1, 1)
    if len(bars) <= target_index:
        return {
            "status": "pending_horizon",
            "terminal": False,
            "reason": "evaluation_horizon_not_mature",
            "entry_trade_date": entry_date.isoformat(),
            "entry_price": entry["_price"],
            "events": events,
        }

    exit_bar: dict[str, Any] | None = None
    exit_price: float | None = None
    exit_index: int | None = None
    for index in range(target_index, len(bars)):
        bar = bars[index]
        block_reason = _sell_block_reason(bar)
        if block_reason:
            events.append(
                {
                    "event_type": "exit_blocked",
                    "trade_date": bar["_trade_date"].isoformat(),
                    "executable": False,
                    "block_reason": block_reason,
                }
            )
            continue
        candidate_price = _number(bar.get("close" if index == target_index else "open"))
        low = _number(bar.get("low"))
        high = _number(bar.get("high"))
        if (
            candidate_price is None
            or low is None
            or high is None
            or not low <= candidate_price <= high
        ):
            return {
                "status": "data_failed",
                "terminal": True,
                "reason": "exit_price_outside_daily_range",
                "entry_trade_date": entry_date.isoformat(),
                "entry_price": entry["_price"],
                "events": events,
            }
        exit_bar = bar
        exit_price = candidate_price
        exit_index = index
        events.append(
            {
                "event_type": "exit_filled",
                "trade_date": bar["_trade_date"].isoformat(),
                "price": candidate_price,
                "executable": True,
                "block_reason": None,
                "delayed_after_block": index > target_index,
            }
        )
        break

    if exit_bar is None or exit_price is None or exit_index is None:
        return {
            "status": "holding_blocked",
            "terminal": False,
            "reason": "exit_still_blocked",
            "entry_trade_date": entry_date.isoformat(),
            "entry_price": entry["_price"],
            "events": events,
        }

    path = bars[: exit_index + 1]
    highs = [value for row in path if (value := _number(row.get("high"))) is not None]
    lows = [value for row in path if (value := _number(row.get("low"))) is not None]
    entry_price = float(entry["_price"])
    gross_return = (exit_price / entry_price - 1.0) * 100.0
    cost_pct = float(execution_spec["default_round_trip_cost_pct"])
    return {
        "status": "complete",
        "terminal": True,
        "reason": "first_executable_exit_after_horizon",
        "entry_trade_date": entry_date.isoformat(),
        "entry_time": entry["_quote_time"].strftime("%Y-%m-%d %H:%M:%S"),
        "entry_price": round(entry_price, 6),
        "target_exit_trade_date": bars[target_index]["_trade_date"].isoformat(),
        "exit_trade_date": exit_bar["_trade_date"].isoformat(),
        "exit_price": round(exit_price, 6),
        "gross_return_pct": round(gross_return, 6),
        "net_return_pct": round(gross_return - cost_pct, 6),
        "implementation_cost_pct": cost_pct,
        "cost_includes_slippage": bool(
            execution_spec.get("round_trip_cost_includes_slippage")
        ),
        "mfe_pct": round((max(highs) / entry_price - 1.0) * 100.0, 6)
        if highs
        else None,
        "mae_pct": round((min(lows) / entry_price - 1.0) * 100.0, 6)
        if lows
        else None,
        "events": events,
    }


def preserve_first_terminal_outcome(
    existing: Mapping[str, Any] | None,
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep the first terminal evaluation immutable across later refreshes."""

    if existing and existing.get("terminal") is True:
        return {**copy.deepcopy(dict(existing)), "immutable_reused": True}
    return {**copy.deepcopy(dict(candidate)), "immutable_reused": False}
