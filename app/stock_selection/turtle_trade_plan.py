from __future__ import annotations

import copy
import hashlib
import json
import math
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from app.shared.db import mysql_conn


SPEC_PATH = (
    Path(__file__).resolve().parent
    / "specs"
    / "turtle_selection_risk_v1.json"
)
EXPECTED_SPEC_SHA256 = "b27e5de516acf09bba85e28bc9e08dd1ccfb072b93dfb295e33c804fb3bae107"
SUPPORTED_STRATEGY_IDS = frozenset(
    {
        "a_share_sentiment",
        "a_share_sentiment_v05",
    }
)

STATE_LABELS = {
    "no_trade": "暂不交易",
    "watch": "等待触发",
    "pullback_ready": "回踩待确认",
    "breakout_ready": "突破待确认",
    "holding": "持仓管理",
    "reduce": "降低仓位",
    "exit": "退出",
}


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _positive(value: Any) -> float | None:
    number = _to_float(value)
    return number if number is not None and number > 0 else None


def _first_defined(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _round_price(value: float | None) -> float | None:
    return round(float(value), 3) if value is not None else None


def _round_shares(value: float) -> int:
    return max(int(math.floor(max(value, 0.0) / 100.0) * 100), 0)


@lru_cache(maxsize=1)
def _read_spec() -> tuple[dict[str, Any], str]:
    payload = SPEC_PATH.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if digest != EXPECTED_SPEC_SHA256:
        raise RuntimeError(
            "turtle_selection_risk_v1 spec checksum mismatch; "
            "create a new spec/version instead of changing the frozen file"
        )
    spec = json.loads(payload.decode("utf-8"))
    if spec.get("spec_id") != "turtle_selection_risk_v1":
        raise RuntimeError("unexpected turtle trade-plan spec_id")
    if spec.get("trade_plan_version") != "selection_trade_plan_v4_turtle_risk":
        raise RuntimeError("unexpected turtle trade-plan version")
    return spec, digest


def load_turtle_trade_plan_spec() -> dict[str, Any]:
    """Return the frozen machine contract without exposing cached mutable state."""

    spec, _digest = _read_spec()
    return copy.deepcopy(spec)


def turtle_trade_plan_spec_hash() -> str:
    return _read_spec()[1]


def _valid_history_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    valid: list[dict[str, Any]] = []
    for raw in rows:
        high = _positive(raw.get("high"))
        low = _positive(raw.get("low"))
        close = _positive(raw.get("close"))
        if high is None or low is None or close is None or high < low:
            continue
        valid.append(
            {
                "trade_date": raw.get("trade_date"),
                "open": _positive(raw.get("open")),
                "high": high,
                "low": low,
                "close": close,
            }
        )
    return valid


def calculate_wilder_n(
    rows: Sequence[Mapping[str, Any]],
    *,
    lookback_days: int = 20,
) -> float | None:
    """Calculate the original Turtle/Wilder N using only completed bars."""

    history = _valid_history_rows(rows)
    if len(history) < lookback_days + 1:
        return None

    true_ranges: list[float] = []
    previous_close = float(history[0]["close"])
    for row in history[1:]:
        high = float(row["high"])
        low = float(row["low"])
        true_ranges.append(
            max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )
        )
        previous_close = float(row["close"])

    if len(true_ranges) < lookback_days:
        return None
    n_value = sum(true_ranges[:lookback_days]) / lookback_days
    for true_range in true_ranges[lookback_days:]:
        n_value = ((lookback_days - 1) * n_value + true_range) / lookback_days
    return n_value if n_value > 0 else None


def _selection_raw_metrics(
    item: Mapping[str, Any],
    explicit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    explain = item.get("explain")
    explain_raw = (
        explain.get("raw_metrics")
        if isinstance(explain, Mapping)
        and isinstance(explain.get("raw_metrics"), Mapping)
        else {}
    )
    strategy_raw = (
        item.get("strategy_raw_metrics")
        if isinstance(item.get("strategy_raw_metrics"), Mapping)
        else {}
    )
    merged = {
        **dict(explain_raw),
        **dict(strategy_raw),
        **dict(explicit or {}),
    }
    for key in (
        "code",
        "name",
        "industry",
        "trade_date",
        "selected_price",
        "selected_price_source",
        "selected_price_quote_time",
        "trade_grade_state",
        "trade_grade_reason",
        "signal_grade",
        "grade_state",
    ):
        merged_value = merged.get(key)
        item_value = item.get(key)
        if (
            (merged_value is None or merged_value == "")
            and item_value is not None
            and item_value != ""
        ):
            merged[key] = item.get(key)
    return merged


def _fetch_completed_technical_rows(
    item: Mapping[str, Any],
    raw_metrics: Mapping[str, Any],
    *,
    connection_factory: Callable[..., Any] | None = None,
) -> list[dict[str, Any]]:
    """Load only bars available at the frozen selection decision."""

    code = str(item.get("code") or raw_metrics.get("code") or "").strip()
    if not code:
        return []
    cutoff = _first_defined(
        raw_metrics.get("selected_price_trade_date"),
        raw_metrics.get("trade_date"),
        item.get("trade_date"),
        str(raw_metrics.get("selected_price_quote_time") or "")[:10],
    )
    clock_mode = str(
        raw_metrics.get("selection_clock_mode")
        or raw_metrics.get("decision_clock_mode")
        or ""
    ).strip().lower()
    intraday_modes = {
        "auction",
        "opening_auction",
        "preopen",
        "09:25",
        "intraday",
        "trading",
    }
    sql = """
        SELECT trade_date, open, high, low, close
        FROM daily_kline
        WHERE code = %s
    """
    params: list[Any] = [code]
    if cutoff:
        operator = "<" if clock_mode in intraday_modes else "<="
        sql += f" AND trade_date {operator} %s"
        params.append(str(cutoff)[:10])
    sql += " ORDER BY trade_date DESC LIMIT 40"
    try:
        connect = connection_factory or mysql_conn
        with connect(dict_cursor=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall() or []
    except Exception:
        return []
    return list(reversed(rows))


def _moving_average(values: Sequence[float], lookback_days: int) -> float | None:
    if len(values) < lookback_days:
        return None
    return sum(values[-lookback_days:]) / lookback_days


def _price_tick(raw_metrics: Mapping[str, Any]) -> float:
    tick = _positive(raw_metrics.get("price_tick"))
    return tick if tick is not None else 0.01


def _limit_down_pct(
    code: str,
    name: str,
    raw_metrics: Mapping[str, Any],
) -> tuple[float, str]:
    explicit = _positive(
        raw_metrics.get("price_limit_pct")
        or raw_metrics.get("limit_down_pct")
    )
    if explicit is not None:
        return explicit, "explicit_input"
    normalized_name = name.upper()
    normalized_code = code.lower()
    if "ST" in normalized_name:
        return 5.0, "st_name_assumption"
    if normalized_code.startswith("bj."):
        return 30.0, "beijing_board_assumption"
    digits = normalized_code.split(".")[-1]
    if digits.startswith(("300", "301", "688", "689")):
        return 20.0, "growth_or_star_board_assumption"
    return 10.0, "main_board_assumption"


def _drawdown_guard(
    drawdown_pct: float | None,
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    config = spec["account_drawdown"]
    if drawdown_pct is None:
        return {
            "status": "not_evaluated",
            "drawdown_pct": None,
            "risk_multiplier": 1.0,
            "allow_new_entry": True,
            "allow_add": True,
            "position_cap_pct": None,
            "cooldown_trade_days": int(config["cooldown_trade_days"]),
            "warning": "未提供账户权益回撤；只输出标准化股数，不代表真实账户可买股数",
        }
    if drawdown_pct >= float(config["minimal_risk_below_pct"]):
        return {
            "status": "cooldown",
            "drawdown_pct": round(drawdown_pct, 4),
            "risk_multiplier": 0.0,
            "allow_new_entry": False,
            "allow_add": False,
            "position_cap_pct": 0.0,
            "cooldown_trade_days": int(config["cooldown_trade_days"]),
            "warning": "账户回撤达到熔断线，禁止新增并进入冷静期",
        }
    if drawdown_pct >= float(config["half_risk_below_pct"]):
        return {
            "status": "minimal_risk",
            "drawdown_pct": round(drawdown_pct, 4),
            "risk_multiplier": float(config["minimal_risk_multiplier"]),
            "allow_new_entry": True,
            "allow_add": False,
            "position_cap_pct": float(config["minimal_risk_position_cap_pct"]),
            "cooldown_trade_days": 0,
            "warning": "账户回撤较大，只允许极小试仓且禁止加仓",
        }
    if drawdown_pct >= float(config["full_risk_below_pct"]):
        return {
            "status": "half_risk",
            "drawdown_pct": round(drawdown_pct, 4),
            "risk_multiplier": 0.5,
            "allow_new_entry": True,
            "allow_add": False,
            "position_cap_pct": None,
            "cooldown_trade_days": 0,
            "warning": "账户回撤进入降风险区，单位风险减半并禁止加仓",
        }
    return {
        "status": "normal",
        "drawdown_pct": round(drawdown_pct, 4),
        "risk_multiplier": 1.0,
        "allow_new_entry": True,
        "allow_add": True,
        "position_cap_pct": None,
        "cooldown_trade_days": 0,
        "warning": None,
    }


def _market_constraint(
    item: Mapping[str, Any],
    raw_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    state = str(
        raw_metrics.get("market_timing_state")
        or item.get("market_timing_state")
        or ""
    ).strip().lower()
    target = _to_float(
        _first_defined(
            raw_metrics.get("market_target_position_pct"),
            item.get("market_target_position_pct"),
        )
    )
    upper = _to_float(
        _first_defined(
            raw_metrics.get("market_position_upper_pct"),
            item.get("market_position_upper_pct"),
        )
    )
    return {
        "status": "evaluated" if state else "not_evaluated",
        "state": state or None,
        "target_position_pct": target,
        "position_upper_pct": upper,
        "allow_new_entry": state != "cash",
        "warning": (
            None
            if state
            else "未绑定市场择时快照；实际交易前仍须服从当日市场仓位上限"
        ),
    }


def _industry_constraint(
    item: Mapping[str, Any],
    raw_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    industry = str(item.get("industry") or raw_metrics.get("industry") or "").strip()
    state = str(
        raw_metrics.get("industry_state")
        or item.get("industry_state")
        or ""
    ).strip().lower()
    return {
        "status": "evaluated" if state else "not_evaluated",
        "industry": industry or None,
        "state": state or None,
        "allow_new_entry": state != "decay",
        "warning": (
            None
            if state
            else "行业状态尚未版本化接入；当前只冻结正式行业名称"
        ),
    }


def _empty_plan(
    *,
    strategy_id: str,
    item: Mapping[str, Any],
    raw_metrics: Mapping[str, Any],
    spec: Mapping[str, Any],
    spec_hash: str,
    reason: str,
) -> dict[str, Any]:
    reference_price = _positive(
        raw_metrics.get("selected_price")
        or item.get("selected_price")
        or item.get("realtime_price")
        or item.get("close")
    )
    decision_time = (
        raw_metrics.get("decision_time")
        or raw_metrics.get("decision_as_of")
        or raw_metrics.get("selected_price_quote_time")
        or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    return {
        "version": spec["trade_plan_version"],
        "spec_id": spec["spec_id"],
        "spec_hash": spec_hash,
        "status": spec["status"],
        "strategy_id": strategy_id,
        "decision_time": str(decision_time),
        "data_cutoff": str(
            raw_metrics.get("data_cutoff")
            or raw_metrics.get("selected_price_quote_time")
            or decision_time
        ),
        "earliest_execution_policy": "not_allowed",
        "state": "no_trade",
        "state_label": STATE_LABELS["no_trade"],
        "reference_price": _round_price(reference_price),
        "n20": None,
        "entry": None,
        "risk": {
            "status": "not_sized",
            "reason": reason,
            "risk_per_unit_pct": float(spec["risk"]["risk_per_unit_equity"]) * 100,
            "unit_shares": None,
            "shares_per_reference_equity": 0,
            "reference_equity": float(spec["risk"]["reference_equity"]),
        },
        "add_levels": [],
        "exits": None,
        "reasons": [reason],
        "warnings": ["研究影子计划，不自动下单，不改变正式选股分级"],
    }


def build_turtle_selection_trade_plan(
    item: Mapping[str, Any],
    *,
    strategy_id: str,
    raw_metrics: Mapping[str, Any] | None = None,
    technical_rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Build the frozen P1 research plan without mutating formal selection semantics."""

    if strategy_id not in SUPPORTED_STRATEGY_IDS:
        return None

    raw_metrics = raw_metrics or {}
    spec, spec_hash = _read_spec()
    history = _valid_history_rows(technical_rows or ())
    minimum_rows = int(spec["volatility"]["minimum_history_rows"])
    if len(history) < minimum_rows:
        return _empty_plan(
            strategy_id=strategy_id,
            item=item,
            raw_metrics=raw_metrics,
            spec=spec,
            spec_hash=spec_hash,
            reason=f"完整日线不足 {minimum_rows} 根，无法计算冻结的 N20",
        )

    reference_price = _positive(
        raw_metrics.get("selected_price")
        or item.get("selected_price")
        or item.get("realtime_price")
        or raw_metrics.get("realtime_price")
        or item.get("latest_price")
        or raw_metrics.get("latest_price")
        or item.get("close")
        or raw_metrics.get("close")
    )
    if reference_price is None:
        return _empty_plan(
            strategy_id=strategy_id,
            item=item,
            raw_metrics=raw_metrics,
            spec=spec,
            spec_hash=spec_hash,
            reason="缺少可审计参考价格，无法生成买卖点",
        )

    lookback = int(spec["volatility"]["lookback_days"])
    n20 = calculate_wilder_n(history, lookback_days=lookback)
    if n20 is None:
        return _empty_plan(
            strategy_id=strategy_id,
            item=item,
            raw_metrics=raw_metrics,
            spec=spec,
            spec_hash=spec_hash,
            reason="N20 计算失败，研究计划 fail-closed",
        )

    closes = [float(row["close"]) for row in history]
    highs = [float(row["high"]) for row in history]
    lows = [float(row["low"]) for row in history]
    tick = _price_tick(raw_metrics)
    breakout_days = int(spec["entry"]["breakout_lookback_days"])
    trend_exit_days = int(spec["exit"]["trend_exit_lookback_days"])
    high20 = max(highs[-breakout_days:])
    low10 = min(lows[-trend_exit_days:])
    ma10 = _moving_average(closes, 10)
    ma20 = _moving_average(closes, 20)
    previous_ma20 = (
        sum(closes[-21:-1]) / 20
        if len(closes) >= 21
        else None
    )
    ma20_slope = (
        ma20 - previous_ma20
        if ma20 is not None and previous_ma20 is not None
        else None
    )

    chip_support = _positive(raw_metrics.get("chip_his_low"))
    support_candidates = [
        value
        for value in (ma10, ma20, chip_support)
        if value is not None and value <= reference_price + n20 * float(spec["entry"]["pullback_zone_n"])
    ]
    support = max(support_candidates) if support_candidates else ma20
    breakout_trigger = high20 + tick
    breakout_distance_n = (breakout_trigger - reference_price) / n20
    max_chase_n = float(spec["entry"]["max_chase_n"])
    pullback_zone_n = float(spec["entry"]["pullback_zone_n"])

    market_constraint = _market_constraint(item, raw_metrics)
    industry_constraint = _industry_constraint(item, raw_metrics)
    account_drawdown = _to_float(
        _first_defined(
            raw_metrics.get("account_drawdown_pct"),
            item.get("account_drawdown_pct"),
        )
    )
    account_guard = _drawdown_guard(account_drawdown, spec)

    explicit_blockers: list[str] = []
    if not market_constraint["allow_new_entry"]:
        explicit_blockers.append("市场择时状态为 cash")
    if not industry_constraint["allow_new_entry"]:
        explicit_blockers.append("行业状态已进入 decay")
    if not account_guard["allow_new_entry"]:
        explicit_blockers.append("账户回撤达到新增仓位熔断线")
    if bool(raw_metrics.get("is_suspended") or item.get("is_suspended")):
        explicit_blockers.append("股票停牌或不可交易")
    if bool(raw_metrics.get("is_limit_up_locked") or item.get("is_limit_up_locked")):
        explicit_blockers.append("一字涨停或涨停封死，不能假设成交")

    grade_state = str(
        item.get("trade_grade_state")
        or item.get("grade_state")
        or item.get("signal_grade")
        or raw_metrics.get("trade_grade_state")
        or raw_metrics.get("grade_state")
        or raw_metrics.get("signal_grade")
        or ""
    ).strip().lower()
    state = "watch"
    state_reason = "选股逻辑成立，等待突破或回踩触发"
    setup = "breakout_20d"
    if explicit_blockers:
        state = "no_trade"
        state_reason = "；".join(explicit_blockers)
    elif grade_state == "watch":
        state = "watch"
        state_reason = "当前正式分级仍为观察级，V4 只输出条件计划"
    elif -max_chase_n <= breakout_distance_n <= max_chase_n:
        state = "breakout_ready"
        state_reason = "参考价位于 20 日突破触发上下 0.5N 内"
    elif breakout_distance_n < -max_chase_n:
        state = "watch"
        state_reason = "价格已超过突破位 0.5N，禁止追价"
    elif (
        support is not None
        and ma20 is not None
        and reference_price >= ma20
        and (ma20_slope is None or ma20_slope >= 0)
        and abs(reference_price - support) <= pullback_zone_n * n20
    ):
        state = "pullback_ready"
        state_reason = "趋势未坏且价格回到支撑 ±0.25N 区域"
        setup = "pullback_reclaim"

    if setup == "pullback_reclaim" and support is not None:
        entry_low = max(support - pullback_zone_n * n20, tick)
        entry_high = support + pullback_zone_n * n20
        entry_trigger = entry_high
        planned_entry = entry_high
    else:
        entry_low = breakout_trigger
        entry_high = breakout_trigger + max_chase_n * n20
        entry_trigger = breakout_trigger
        planned_entry = breakout_trigger

    volatility_stop = planned_entry - float(spec["risk"]["initial_stop_n"]) * n20
    structure_stop = low10 - tick
    valid_stops = [
        value
        for value in (volatility_stop, structure_stop)
        if 0 < value < planned_entry
    ]
    initial_stop = max(valid_stops) if valid_stops else volatility_stop
    stop_distance_n = (planned_entry - initial_stop) / n20
    if stop_distance_n < float(spec["risk"]["minimum_stop_distance_n"]):
        initial_stop = volatility_stop
        stop_distance_n = float(spec["risk"]["initial_stop_n"])
    initial_stop = max(initial_stop, tick)
    risk_per_share = max(planned_entry - initial_stop, tick)

    risk_config = spec["risk"]
    reference_equity = float(risk_config["reference_equity"])
    risk_multiplier = float(account_guard["risk_multiplier"])
    normal_risk_budget = reference_equity * float(risk_config["risk_per_unit_equity"])
    limit_down_pct, limit_source = _limit_down_pct(
        str(item.get("code") or raw_metrics.get("code") or ""),
        str(item.get("name") or raw_metrics.get("name") or ""),
        raw_metrics,
    )
    one_limit_loss_per_share = planned_entry * limit_down_pct / 100
    two_limit_loss_per_share = planned_entry * (
        1 - (1 - limit_down_pct / 100) ** 2
    )
    risk_sized_shares = _round_shares(normal_risk_budget * risk_multiplier / risk_per_share)
    one_limit_cap_shares = _round_shares(
        reference_equity
        * float(risk_config["one_limit_down_stress_cap_equity"])
        / max(one_limit_loss_per_share, tick)
    )
    two_limit_cap_shares = _round_shares(
        reference_equity
        * float(risk_config["two_limit_down_stress_cap_equity"])
        / max(two_limit_loss_per_share, tick)
    )
    shares_per_reference_equity = min(
        risk_sized_shares,
        one_limit_cap_shares,
        two_limit_cap_shares,
    )

    actual_equity = _positive(
        raw_metrics.get("account_equity")
        or item.get("account_equity")
    )
    unit_shares: int | None = None
    if actual_equity is not None:
        equity_scale = actual_equity / reference_equity
        unit_shares = _round_shares(shares_per_reference_equity * equity_scale)
    if shares_per_reference_equity <= 0 and state != "no_trade":
        state = "no_trade"
        state_reason = "按单元风险和跌停压力约束后不足一手"

    one_stress_pct = (
        shares_per_reference_equity
        * one_limit_loss_per_share
        / reference_equity
        * 100
    )
    two_stress_pct = (
        shares_per_reference_equity
        * two_limit_loss_per_share
        / reference_equity
        * 100
    )
    add_levels = []
    if (
        state != "no_trade"
        and account_guard["allow_add"]
        and int(risk_config["maximum_event_units"]) > 1
    ):
        add_levels.append(
            _round_price(
                planned_entry
                + float(risk_config["add_interval_n"]) * n20
            )
        )

    clock_mode = str(
        raw_metrics.get("selection_clock_mode")
        or raw_metrics.get("decision_clock_mode")
        or ""
    ).strip().lower()
    earliest_execution_policy = (
        "same_day_final_open"
        if clock_mode in {"auction", "opening_auction", "preopen", "09:25"}
        else "next_trading_day_open"
    )
    decision_time = (
        raw_metrics.get("decision_time")
        or raw_metrics.get("decision_as_of")
        or raw_metrics.get("selected_price_quote_time")
        or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    warnings = [
        "研究影子计划，不自动下单，不改变正式选股分级",
        "买入区间被触及不等于一定成交，涨跌停/停牌须按实际可成交性记录",
    ]
    for constraint in (market_constraint, industry_constraint, account_guard):
        warning = constraint.get("warning")
        if warning:
            warnings.append(str(warning))

    return {
        "version": spec["trade_plan_version"],
        "spec_id": spec["spec_id"],
        "spec_hash": spec_hash,
        "status": spec["status"],
        "strategy_id": strategy_id,
        "decision_time": str(decision_time),
        "data_cutoff": str(
            raw_metrics.get("data_cutoff")
            or raw_metrics.get("selected_price_quote_time")
            or decision_time
        ),
        "earliest_execution_policy": earliest_execution_policy,
        "state": state,
        "state_label": STATE_LABELS[state],
        "state_reason": state_reason,
        "reference_price": _round_price(reference_price),
        "n20": _round_price(n20),
        "technical": {
            "trade_date": str(history[-1].get("trade_date") or ""),
            "ma10": _round_price(ma10),
            "ma20": _round_price(ma20),
            "ma20_slope": _round_price(ma20_slope),
            "previous_20_day_high": _round_price(high20),
            "previous_10_day_low": _round_price(low10),
            "source": "daily_kline_completed_bars",
        },
        "entry": {
            "setup": setup,
            "trigger": _round_price(entry_trigger),
            "zone_low": _round_price(entry_low),
            "zone_high": _round_price(entry_high),
            "max_chase_n": max_chase_n,
            "expires_after_trade_days": int(spec["entry"]["expires_after_trade_days"]),
            "trigger_required": True,
        },
        "risk": {
            "status": "sized" if shares_per_reference_equity > 0 else "not_sized",
            "planned_entry": _round_price(planned_entry),
            "initial_stop": _round_price(initial_stop),
            "volatility_stop": _round_price(volatility_stop),
            "structure_stop": _round_price(structure_stop),
            "stop_distance_n": round(stop_distance_n, 4),
            "risk_per_share": _round_price(risk_per_share),
            "risk_per_unit_pct": float(risk_config["risk_per_unit_equity"]) * 100,
            "unit_shares": unit_shares,
            "shares_per_reference_equity": shares_per_reference_equity,
            "reference_equity": reference_equity,
            "price_limit_down_pct": limit_down_pct,
            "price_limit_source": limit_source,
            "one_limit_down_stress_pct": round(one_stress_pct, 4),
            "two_limit_down_stress_pct": round(two_stress_pct, 4),
        },
        "add_levels": add_levels,
        "exits": {
            "initial_stop": _round_price(initial_stop),
            "trailing_stop_rule": "highest_close_since_entry_minus_2n",
            "trend_exit": _round_price(low10 - tick),
            "trend_exit_lookback_days": trend_exit_days,
            "time_exit_trade_days": int(spec["exit"]["time_exit_trade_days"]),
            "time_exit_minimum_progress_n": float(spec["exit"]["minimum_progress_n"]),
            "time_exit_progress_price_basis": str(
                spec["exit"]["minimum_progress_price_basis"]
            ),
            "evaluation_censor_trade_days": int(
                spec["exit"]["evaluation_censor_trade_days"]
            ),
            "optional_partial_take_profit": _round_price(
                planned_entry
                + float(spec["exit"]["optional_partial_take_profit_r"])
                * risk_per_share
            ),
            "event_invalid_conditions": [
                "催化过期或被证伪",
                "正式行业/热点主题资金连续转负",
                "股票—事件关系证据失效",
                "市场或账户风险触发强制降仓",
            ],
        },
        "market_constraint": market_constraint,
        "industry_constraint": industry_constraint,
        "account_guard": account_guard,
        "portfolio_guard": {
            "maximum_portfolio_heat_pct": float(
                spec["portfolio"]["maximum_portfolio_heat"]
            )
            * 100,
            "maximum_single_sector_heat_pct": float(
                spec["portfolio"]["maximum_single_sector_heat"]
            )
            * 100,
            "status": "constraint_only",
        },
        "reasons": [
            state_reason,
            f"N20={n20:.3f}；计划止损使用 2N 与前 {trend_exit_days} 日低点的有效约束",
            "只允许盈利后加仓，禁止向下摊平",
        ],
        "warnings": warnings,
    }


def attach_turtle_research_shadow(
    item: Mapping[str, Any],
    *,
    strategy_id: str | None = None,
    raw_metrics: Mapping[str, Any] | None = None,
    technical_rows: Sequence[Mapping[str, Any]] | None = None,
    connection_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Attach V4 after formal selection without changing frozen strategy output.

    The active V2/V3 plan stays byte-for-byte semantically intact.  V4 is an
    explicitly separate research envelope materialized into downstream
    snapshots and forward evidence.
    """

    existing = item.get("trade_plan")
    active_plan = copy.deepcopy(dict(existing)) if isinstance(existing, Mapping) else {}
    normalized_strategy_id = str(
        strategy_id or item.get("strategy_id") or active_plan.get("strategy_id") or ""
    ).strip()
    if normalized_strategy_id not in SUPPORTED_STRATEGY_IDS:
        return active_plan
    # Sentiment selectors always emit a versioned active plan.  Refuse to
    # synthesize an execution envelope around an unknown/legacy payload.
    if not str(active_plan.get("version") or "").strip():
        return active_plan

    current_shadow = active_plan.get("research_shadow")
    if (
        isinstance(current_shadow, Mapping)
        and current_shadow.get("version") == "selection_trade_plan_v4_turtle_risk"
        and current_shadow.get("spec_hash") == turtle_trade_plan_spec_hash()
    ):
        return active_plan

    metrics = _selection_raw_metrics(item, raw_metrics)
    normalized_item = dict(item)
    normalized_item["trade_plan"] = active_plan
    if not normalized_item.get("industry") and metrics.get("industry"):
        normalized_item["industry"] = metrics.get("industry")
    rows = (
        list(technical_rows)
        if technical_rows is not None
        else _fetch_completed_technical_rows(
            normalized_item,
            metrics,
            connection_factory=connection_factory,
        )
    )
    shadow = build_turtle_selection_trade_plan(
        normalized_item,
        strategy_id=normalized_strategy_id,
        raw_metrics=metrics,
        technical_rows=rows,
    )
    if shadow is not None:
        active_plan["research_shadow"] = shadow
        constrain_turtle_plan_to_selection_grade(
            active_plan,
            grade_state=str(
                item.get("trade_grade_state")
                or item.get("signal_grade")
                or item.get("grade_state")
                or metrics.get("trade_grade_state")
                or ""
            ),
            grade_reason=str(
                item.get("trade_grade_reason")
                or item.get("grade_reason")
                or metrics.get("trade_grade_reason")
                or ""
            )
            or None,
        )
    return active_plan


def constrain_turtle_plan_to_selection_grade(
    trade_plan: dict[str, Any] | None,
    *,
    grade_state: str | None,
    grade_reason: str | None = None,
) -> dict[str, Any] | None:
    """Prevent the execution layer from upgrading the formal selection grade."""

    if not isinstance(trade_plan, dict):
        return trade_plan
    shadow = trade_plan.get("research_shadow")
    if not isinstance(shadow, dict):
        return trade_plan
    normalized_grade = str(grade_state or "").strip().lower()
    if normalized_grade in {"tradable", "trade", "buy"}:
        return trade_plan
    if shadow.get("state") == "no_trade":
        return trade_plan
    shadow["state"] = "watch"
    shadow["state_label"] = STATE_LABELS["watch"]
    shadow["state_reason"] = (
        f"正式选股仍为观察级：{grade_reason}"
        if grade_reason
        else "正式选股仍为观察级，交易风险层不得抬级"
    )
    reasons = list(shadow.get("reasons") or [])
    reasons = [
        value
        for value in reasons
        if not str(value).startswith("正式选股仍为观察级")
    ]
    reasons.insert(0, shadow["state_reason"])
    shadow["reasons"] = reasons
    return trade_plan
