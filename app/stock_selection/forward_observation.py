from __future__ import annotations

import hashlib
import json
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal
from statistics import mean
from typing import Any, Callable, Iterable

from app.shared.db import mysql_conn
from app.stock_selection.trade_plan_events import (
    TradePlanEventRepository,
    immutable_trade_plan_id,
)


ConnectionFactory = Callable[..., AbstractContextManager]
SUPPORTED_ACTION_TYPES = {"viewed", "saved", "bought", "skipped", "sold"}


class ForwardProtocolDriftError(RuntimeError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True, separators=(",", ":"))


def _from_json(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _date_text(value: Any) -> str | None:
    return str(value)[:10] if value else None


def _datetime_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).replace("T", " ")
    return text[:19] if len(text) >= 19 else text


def _time_text(value: Any) -> str:
    if isinstance(value, timedelta):
        seconds = int(value.total_seconds())
        return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"
    if isinstance(value, dt_time):
        return value.strftime("%H:%M:%S")
    text = str(value or "")
    return text if len(text) == 8 else f"{text}:00" if len(text) == 5 else text


def strategy_config_hash(strategy_meta: dict[str, Any], strategy_config: dict[str, Any]) -> str:
    material = _json({"strategy_meta": strategy_meta, "strategy_config": strategy_config})
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ForwardProtocolSpec:
    protocol_id: str
    strategy_id: str
    strategy_version: str
    protocol_version: str
    execution_time: str
    timezone: str
    entry_rule: str
    horizons: tuple[int, ...]
    benchmark_codes: tuple[str, ...]
    minimum_observation_days: int
    minimum_candidate_count: int
    immutable_tag: str
    implementation_commit: str
    strategy_config_hash: str
    ai_policy: str
    strategy_snapshot: dict[str, Any]
    request: dict[str, Any]
    started_on: str
    campaign_id: str | None = None
    observation_source: str = "scheduled_forward"

    def immutable_material(self) -> dict[str, Any]:
        return asdict(self)


def _ai_status(result: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    explicit_mode = str(result.get("ai_mode") or "").strip()
    if explicit_mode:
        return explicit_mode, {
            "mode": explicit_mode,
            "source": "immutable_candidate_snapshot",
            "input_snapshot_id": result.get("input_snapshot_id"),
        }
    progressive = result.get("progressive_rerank") or {}
    sector = progressive.get("sector_deepseek") or {}
    stock = progressive.get("stock_deepseek") or {}
    sector_items = sector.get("items") or []
    stock_items = stock.get("items") or []
    phases = sorted(
        {
            str(item.get("selection_phase"))
            for item in (result.get("results") or [])
            if item.get("selection_phase")
        }
    )
    if not progressive.get("enabled"):
        mode = "disabled"
    elif sector.get("available") and stock.get("available") and sector_items and stock_items:
        mode = "progressive_ai"
    elif sector_items or stock_items:
        mode = "partial_ai"
    else:
        mode = "local_fallback"
    return mode, {
        "mode": mode,
        "selection_phases": phases,
        "sector": {
            "enabled": sector.get("enabled"),
            "available": sector.get("available"),
            "model": sector.get("model"),
            "error": sector.get("error"),
            "item_count": len(sector_items),
        },
        "stock": {
            "enabled": stock.get("enabled"),
            "available": stock.get("available"),
            "model": stock.get("model"),
            "error": stock.get("error"),
            "item_count": len(stock_items),
        },
    }


def _pick_signal_price(item: dict[str, Any]) -> float | None:
    for key in ("realtime_price", "selected_price", "latest_price", "close"):
        value = _float(item.get(key))
        if value is not None and value > 0:
            return value
    return None


def _pick_theme(item: dict[str, Any]) -> str | None:
    context = item.get("sentiment_context") or {}
    return (
        item.get("opinion_sector_name")
        or item.get("theme_name")
        or item.get("sector_name")
        or context.get("sector_name")
    )


def build_turtle_forward_outcome(
    *,
    raw_item: dict[str, Any],
    price_path: list[dict[str, Any]],
    strategy_id: str,
    observation_id: str,
    signal_trade_date: str,
    source_snapshot_id: str | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Evaluate the frozen V4 plan without relabelling the selection protocol.

    Daily OHLC cannot identify the exact intraday order of trigger and stop, so
    the shared backtest resolver uses the conservative stop-first assumption.
    This evaluation is gross of trading costs and remains research-only.
    """

    trade_plan = raw_item.get("trade_plan")
    if not isinstance(trade_plan, dict):
        return {"status": "not_applicable"}, []
    shadow = trade_plan.get("research_shadow")
    if not isinstance(shadow, dict):
        return {"status": "not_applicable"}, []

    from app.backtest.service import BacktestRequest, BacktestService

    code = str(raw_item.get("code") or "")
    spec_hash = str(shadow.get("spec_hash") or "")
    plan_version = str(
        shadow.get("version") or "selection_trade_plan_v4_turtle_risk"
    )
    plan_id = immutable_trade_plan_id(
        source_kind="forward",
        source_id=observation_id,
        code=code,
        spec_hash=spec_hash,
    )
    request = BacktestRequest(
        strategy_id=strategy_id,
        start_date=signal_trade_date,
        end_date=signal_trade_date,
        return_mode="turtle_selection_risk_v1",
        trade_strategy_id="turtle_selection_risk_v1",
        use_adjusted_price=True,
        apply_execution_constraints=True,
    )
    trade, rejection = BacktestService()._resolve_turtle_trade(
        run_id=f"forward_{observation_id}",
        strategy_id=strategy_id,
        signal_date=signal_trade_date,
        pick={"code": code, "trade_plan": trade_plan},
        bars=price_path,
        request=request,
    )
    pending_reasons = {
        "turtle_entry_pending",
        "turtle_holding_pending",
        "turtle_stop_pending_limit_down",
        "turtle_time_exit_pending_limit_down",
        "turtle_missing_holding_bars",
    }
    censor_reasons = {"turtle_open_at_evaluation_horizon"}
    status = (
        "complete"
        if trade and trade.get("return_3d_pct") is not None
        else "blocked"
        if trade
        else "censored"
        if rejection in censor_reasons
        else "pending"
        if rejection in pending_reasons
        else "not_triggered"
    )
    evaluation = {
        "status": status,
        "plan_id": plan_id,
        "trade_strategy_id": "turtle_selection_risk_v1",
        "trade_plan_version": plan_version,
        "spec_hash": spec_hash or None,
        "evaluation_method": "daily_ohlc_conservative_stop_first_v1",
        "return_basis": "adjusted_gross_before_costs",
        "rejection_reason": rejection,
        "trade": trade,
    }

    decision_time = str(
        shadow.get("decision_time")
        or raw_item.get("selected_price_quote_time")
        or f"{signal_trade_date} 15:00:00"
    ).replace("T", " ")[:19]
    common = {
        "plan_id": plan_id,
        "selection_result_id": None,
        "snapshot_id": source_snapshot_id,
        "code": code,
        "trade_plan_version": plan_version,
        "spec_hash": spec_hash or None,
    }
    events: list[dict[str, Any]] = [
        {
            **common,
            "event_time": decision_time,
            "event_type": "plan_created",
            "planned_price": (shadow.get("entry") or {}).get("trigger"),
            "observed_price": shadow.get("reference_price"),
            "executable": False,
            "block_reason": None,
            "metadata": {
                "observation_id": observation_id,
                "strategy_id": strategy_id,
                "signal_trade_date": signal_trade_date,
                "state": shadow.get("state"),
                "event_time_semantics": "frozen_decision_time",
            },
        }
    ]
    for trace in (trade or {}).get("event_trace") or []:
        event_trade_date = str(trace.get("trade_date") or signal_trade_date)[:10]
        events.append(
            {
                **common,
                "event_time": f"{event_trade_date} 15:00:00",
                "event_type": trace.get("event_type"),
                "planned_price": trace.get("planned_price"),
                "observed_price": trace.get("observed_price"),
                "executable": trace.get("executable"),
                "block_reason": trace.get("block_reason"),
                "metadata": {
                    "observation_id": observation_id,
                    "strategy_id": strategy_id,
                    "signal_trade_date": signal_trade_date,
                    "reason": trace.get("reason"),
                    "event_time_semantics": "observed_by_daily_close",
                },
            }
        )
    if (
        not trade
        and rejection
        and rejection not in pending_reasons
    ):
        last_trade_date = (
            _date_text(price_path[-1].get("trade_date"))
            if price_path
            else signal_trade_date
        )
        event_type = {
            "turtle_no_trade": "plan_blocked",
            "turtle_chase_gap": "entry_cancelled",
            "turtle_entry_not_triggered": "entry_expired",
            "turtle_open_at_evaluation_horizon": "evaluation_censored",
        }.get(rejection, "evaluation_blocked")
        events.append(
            {
                **common,
                "event_time": f"{last_trade_date} 15:00:00",
                "event_type": event_type,
                "planned_price": (shadow.get("entry") or {}).get("trigger"),
                "observed_price": (
                    _float(price_path[-1].get("close"))
                    if price_path
                    else shadow.get("reference_price")
                ),
                "executable": False,
                "block_reason": rejection,
                "metadata": {
                    "observation_id": observation_id,
                    "strategy_id": strategy_id,
                    "signal_trade_date": signal_trade_date,
                    "event_time_semantics": "observed_by_daily_close",
                },
            }
        )
    return evaluation, events


class ForwardObservationRepository:
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    def ensure_protocol(self, spec: ForwardProtocolSpec) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM strategy_forward_protocol WHERE protocol_id=%s",
                    (spec.protocol_id,),
                )
                existing = cursor.fetchone()
                if existing:
                    self._assert_protocol_unchanged(existing, spec)
                    return existing
                cursor.execute(
                    """
                    INSERT INTO strategy_forward_protocol (
                        protocol_id, campaign_id, strategy_id, strategy_version, protocol_version,
                        status, observation_source, execution_time, timezone, entry_rule,
                        horizons_json, benchmark_codes_json,
                        minimum_observation_days, minimum_candidate_count, immutable_tag,
                        implementation_commit, strategy_config_hash, ai_policy,
                        strategy_snapshot_json, request_json, started_on, locked_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,'active',%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()
                    )
                    """,
                    (
                        spec.protocol_id,
                        spec.campaign_id,
                        spec.strategy_id,
                        spec.strategy_version,
                        spec.protocol_version,
                        spec.observation_source,
                        spec.execution_time,
                        spec.timezone,
                        spec.entry_rule,
                        _json(list(spec.horizons)),
                        _json(list(spec.benchmark_codes)),
                        spec.minimum_observation_days,
                        spec.minimum_candidate_count,
                        spec.immutable_tag,
                        spec.implementation_commit,
                        spec.strategy_config_hash,
                        spec.ai_policy,
                        _json(spec.strategy_snapshot),
                        _json(spec.request),
                        spec.started_on,
                    ),
                )
                cursor.execute(
                    "SELECT * FROM strategy_forward_protocol WHERE protocol_id=%s",
                    (spec.protocol_id,),
                )
                return cursor.fetchone() or {}

    @staticmethod
    def _assert_protocol_unchanged(existing: dict[str, Any], spec: ForwardProtocolSpec) -> None:
        actual = {
            "protocol_id": str(existing.get("protocol_id") or ""),
            "campaign_id": existing.get("campaign_id"),
            "strategy_id": str(existing.get("strategy_id") or ""),
            "strategy_version": str(existing.get("strategy_version") or ""),
            "protocol_version": str(existing.get("protocol_version") or ""),
            "execution_time": _time_text(existing.get("execution_time")),
            "timezone": str(existing.get("timezone") or ""),
            "entry_rule": str(existing.get("entry_rule") or ""),
            "horizons": tuple(int(value) for value in (_from_json(existing.get("horizons_json"), []) or [])),
            "benchmark_codes": tuple(str(value) for value in (_from_json(existing.get("benchmark_codes_json"), []) or [])),
            "minimum_observation_days": int(existing.get("minimum_observation_days") or 0),
            "minimum_candidate_count": int(existing.get("minimum_candidate_count") or 0),
            "immutable_tag": str(existing.get("immutable_tag") or ""),
            "implementation_commit": str(existing.get("implementation_commit") or ""),
            "strategy_config_hash": str(existing.get("strategy_config_hash") or ""),
            "ai_policy": str(existing.get("ai_policy") or ""),
            "strategy_snapshot": _from_json(existing.get("strategy_snapshot_json"), {}) or {},
            "request": _from_json(existing.get("request_json"), {}) or {},
            "started_on": _date_text(existing.get("started_on")),
            "observation_source": str(
                existing.get("observation_source") or "scheduled_forward"
            ),
        }
        expected = spec.immutable_material()
        expected["execution_time"] = _time_text(expected["execution_time"])
        if _json(actual) != _json(expected):
            changed = sorted(key for key in expected if _json(actual.get(key)) != _json(expected.get(key)))
            raise ForwardProtocolDriftError(
                f"forward protocol {spec.protocol_id} is immutable; changed fields: {', '.join(changed)}"
            )

    def latest_data_trade_date(self) -> date | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
                value = (cursor.fetchone() or {}).get("trade_date")
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value)[:10]) if value else None

    def reserve_observation(
        self,
        *,
        observation_id: str,
        protocol_id: str,
        signal_trade_date: date,
        request: dict[str, Any],
        campaign_id: str | None = None,
        observation_source: str = "scheduled_forward",
        source_snapshot_id: str | None = None,
        paired_input_hash: str | None = None,
    ) -> dict[str, Any]:
        request_json = _json(request)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT IGNORE INTO strategy_forward_observation (
                        observation_id, protocol_id, campaign_id, signal_trade_date,
                        source_snapshot_id, paired_input_hash, status, observation_source,
                        request_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,'pending_submission',%s,%s)
                    """,
                    (
                        observation_id,
                        protocol_id,
                        campaign_id,
                        signal_trade_date,
                        source_snapshot_id,
                        paired_input_hash,
                        observation_source,
                        request_json,
                    ),
                )
                cursor.execute(
                    "SELECT * FROM strategy_forward_observation WHERE protocol_id=%s AND signal_trade_date=%s",
                    (protocol_id, signal_trade_date),
                )
                row = cursor.fetchone() or {}
        if row and _json(_from_json(row.get("request_json"), {}) or {}) != request_json:
            raise ForwardProtocolDriftError(
                f"observation request drift for {protocol_id} on {signal_trade_date}"
            )
        expected_lineage = {
            "campaign_id": campaign_id,
            "observation_source": observation_source,
            "source_snapshot_id": source_snapshot_id,
            "paired_input_hash": paired_input_hash,
        }
        if row:
            changed = [
                key
                for key, expected in expected_lineage.items()
                if row.get(key) != expected
            ]
            if changed:
                raise ForwardProtocolDriftError(
                    f"observation lineage drift for {protocol_id} on "
                    f"{signal_trade_date}: {', '.join(changed)}"
                )
        return row

    def get_observation(self, observation_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM strategy_forward_observation WHERE observation_id=%s",
                    (observation_id,),
                )
                return cursor.fetchone()

    def find_selection_run_for_observation(self, observation_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM selection_run
                    WHERE JSON_UNQUOTE(JSON_EXTRACT(request_json, '$.forward_observation_id'))=%s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (observation_id,),
                )
                return cursor.fetchone()

    def attach_selection_run(self, observation_id: str, selection_run_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_forward_observation
                    SET selection_run_id=%s, status='queued', submitted_at=COALESCE(submitted_at, NOW()),
                        error_code=NULL, error_message=NULL
                    WHERE observation_id=%s
                      AND (selection_run_id IS NULL OR selection_run_id=%s)
                      AND status IN ('pending_submission','queued','running','failed')
                    """,
                    (selection_run_id, observation_id, selection_run_id),
                )
                cursor.execute(
                    "SELECT * FROM strategy_forward_observation WHERE observation_id=%s",
                    (observation_id,),
                )
                row = cursor.fetchone() or {}
        if row.get("selection_run_id") != selection_run_id:
            raise RuntimeError(f"observation {observation_id} is already linked to another selection run")
        return row

    def mark_running(self, observation_id: str, selection_run_id: str) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_forward_observation
                    SET status='running'
                    WHERE observation_id=%s AND selection_run_id=%s AND status IN ('queued','running')
                    """,
                    (observation_id, selection_run_id),
                )

    def finalize_success(
        self,
        *,
        protocol_id: str,
        observation_id: str,
        selection_run_id: str | None,
        result: dict[str, Any],
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                self._finalize_success_with_cursor(
                    cursor,
                    protocol_id=protocol_id,
                    observation_id=observation_id,
                    selection_run_id=selection_run_id,
                    result=result,
                )

    def finalize_paired_success(
        self,
        records: Iterable[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Persist a paired observation atomically without writing selection_result."""

        items = [dict(record) for record in records]
        if len(items) < 2:
            raise ValueError("paired forward observation requires at least two strategy records")
        with self._connect() as conn:
            with conn.cursor() as cursor:
                for record in items:
                    request_json = _json(record.get("request") or {})
                    cursor.execute(
                        """
                        INSERT IGNORE INTO strategy_forward_observation (
                            observation_id, protocol_id, campaign_id, signal_trade_date,
                            source_snapshot_id, paired_input_hash, status,
                            observation_source, request_json
                        ) VALUES (%s,%s,%s,%s,%s,%s,'pending_submission',%s,%s)
                        """,
                        (
                            record["observation_id"],
                            record["protocol_id"],
                            record.get("campaign_id"),
                            record["signal_trade_date"],
                            record.get("source_snapshot_id"),
                            record.get("paired_input_hash"),
                            record.get("observation_source") or "automatic_observation",
                            request_json,
                        ),
                    )
                    cursor.execute(
                        """
                        SELECT *
                        FROM strategy_forward_observation
                        WHERE observation_id=%s
                        FOR UPDATE
                        """,
                        (record["observation_id"],),
                    )
                    existing = cursor.fetchone() or {}
                    if not existing:
                        raise RuntimeError(
                            f"forward observation not found: {record['observation_id']}"
                        )
                    expected = {
                        "protocol_id": record["protocol_id"],
                        "campaign_id": record.get("campaign_id"),
                        "observation_source": (
                            record.get("observation_source")
                            or "automatic_observation"
                        ),
                        "source_snapshot_id": record.get("source_snapshot_id"),
                        "paired_input_hash": record.get("paired_input_hash"),
                    }
                    changed = [
                        key
                        for key, value in expected.items()
                        if existing.get(key) != value
                    ]
                    if (
                        _json(_from_json(existing.get("request_json"), {}) or {})
                        != request_json
                    ):
                        changed.append("request")
                    if changed:
                        raise ForwardProtocolDriftError(
                            f"paired observation lineage drift for "
                            f"{record['observation_id']}: {', '.join(changed)}"
                        )

                for record in items:
                    self._finalize_success_with_cursor(
                        cursor,
                        protocol_id=str(record["protocol_id"]),
                        observation_id=str(record["observation_id"]),
                        selection_run_id=None,
                        result=dict(record.get("result") or {}),
                    )

                placeholders = ",".join(["%s"] * len(items))
                cursor.execute(
                    f"""
                    SELECT *
                    FROM strategy_forward_observation
                    WHERE observation_id IN ({placeholders})
                    ORDER BY protocol_id
                    """,
                    tuple(str(record["observation_id"]) for record in items),
                )
                return cursor.fetchall() or []

    def _finalize_success_with_cursor(
        self,
        cursor: Any,
        *,
        protocol_id: str,
        observation_id: str,
        selection_run_id: str | None,
        result: dict[str, Any],
    ) -> None:
        result_json = _json(result)
        items = list(result.get("results") or [])
        ai_mode, ai_status = _ai_status(result)
        data_as_of_candidates = []
        pick_values = []
        for index, item in enumerate(items, start=1):
            context = item.get("sentiment_context") or {}
            as_of = (
                item.get("opinion_as_of_datetime")
                or context.get("as_of")
                or item.get("realtime_quote_time")
                or item.get("selected_price_quote_time")
            )
            if as_of:
                data_as_of_candidates.append(_datetime_text(as_of))
            pick_values.append(
                (
                    observation_id,
                    protocol_id,
                    item.get("trade_date"),
                    item.get("code"),
                    item.get("name"),
                    int(item.get("rank_no") or index),
                    _float(item.get("score")),
                    _pick_signal_price(item),
                    _pick_theme(item),
                    item.get("trade_grade_state") or context.get("trade_grade_state"),
                    item.get("selection_phase"),
                    _datetime_text(as_of),
                    _json(item),
                )
            )

        cursor.execute(
            "SELECT * FROM strategy_forward_observation WHERE observation_id=%s FOR UPDATE",
            (observation_id,),
        )
        observation = cursor.fetchone() or {}
        if not observation:
            raise RuntimeError(f"forward observation not found: {observation_id}")
        if observation.get("protocol_id") != protocol_id:
            raise RuntimeError("forward observation protocol mismatch")
        if selection_run_id is not None and observation.get("selection_run_id") not in {
            None,
            selection_run_id,
        }:
            raise RuntimeError("forward observation selection run mismatch")
        if observation.get("status") == "success":
            if _json(_from_json(observation.get("result_json"), {}) or {}) != result_json:
                raise ForwardProtocolDriftError(
                    "a completed forward observation cannot be rewritten"
                )
            return
        signal_trade_date = observation.get("signal_trade_date")
        if pick_values:
            normalized_values = [
                (
                    value[0],
                    value[1],
                    observation.get("campaign_id"),
                    observation.get("observation_source")
                    or "scheduled_forward",
                    observation.get("source_snapshot_id"),
                    signal_trade_date,
                    *value[3:],
                )
                for value in pick_values
            ]
            cursor.executemany(
                """
                INSERT INTO strategy_forward_pick (
                    observation_id, protocol_id, campaign_id, observation_source,
                    source_snapshot_id, signal_trade_date, code, name, rank_no,
                    score, signal_price, theme_name, trade_grade_state,
                    selection_phase, opinion_as_of_at, raw_json
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s
                )
                """,
                normalized_values,
            )
        cursor.execute(
            """
            UPDATE strategy_forward_observation
            SET selection_run_id=COALESCE(selection_run_id, %s),
                status='success', result_count=%s,
                data_as_of_at=%s, ai_mode=%s, ai_status_json=%s, result_json=%s,
                error_code=NULL, error_message=NULL,
                submitted_at=COALESCE(submitted_at, NOW()), completed_at=NOW()
            WHERE observation_id=%s
            """,
            (
                selection_run_id,
                len(items),
                max(data_as_of_candidates) if data_as_of_candidates else None,
                ai_mode,
                _json(ai_status),
                result_json,
                observation_id,
            ),
        )

    def mark_failed(
        self,
        *,
        observation_id: str,
        selection_run_id: str | None,
        error_code: str,
        error_message: str,
    ) -> None:
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_forward_observation
                    SET selection_run_id=COALESCE(selection_run_id, %s), status='failed',
                        error_code=%s, error_message=%s, completed_at=NOW()
                    WHERE observation_id=%s AND status <> 'success'
                    """,
                    (selection_run_id, error_code, error_message[:1000], observation_id),
                )

    def open_observations(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM strategy_forward_observation
                    WHERE status IN ('pending_submission','queued','running')
                    ORDER BY id ASC LIMIT %s
                    """,
                    (limit,),
                )
                return cursor.fetchall() or []

    def pending_outcome_picks(self, limit: int = 500) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT p.*, pr.horizons_json, pr.benchmark_codes_json,
                           pr.entry_rule, pr.strategy_id
                    FROM strategy_forward_pick p
                    INNER JOIN strategy_forward_protocol pr ON pr.protocol_id=p.protocol_id
                    WHERE p.outcome_status <> 'complete'
                    ORDER BY p.signal_trade_date, p.rank_no, p.id
                    LIMIT %s
                    """,
                    (limit,),
                )
                return cursor.fetchall() or []

    def load_price_path(
        self,
        code: str,
        signal_trade_date: str,
        limit: int = 25,
        entry_rule: str = "next_tradable_open",
    ) -> list[dict[str, Any]]:
        if entry_rule not in {"next_tradable_open", "same_day_open"}:
            raise ValueError(f"unsupported forward entry rule: {entry_rule}")
        operator = ">=" if entry_rule == "same_day_open" else ">"
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT dk.trade_date, dk.open, dk.high, dk.low, dk.close,
                           (
                               SELECT prev.close
                               FROM daily_kline prev
                               WHERE prev.code=dk.code
                                 AND prev.trade_date < dk.trade_date
                               ORDER BY prev.trade_date DESC
                               LIMIT 1
                           ) AS prev_close,
                           af.adj_factor
                    FROM daily_kline dk
                    LEFT JOIN adj_factor_daily af ON af.code=dk.code AND af.trade_date=dk.trade_date
                    WHERE dk.code=%s AND dk.trade_date{operator}%s
                    ORDER BY dk.trade_date ASC
                    LIMIT %s
                    """,
                    (code, signal_trade_date, limit),
                )
                return cursor.fetchall() or []

    def load_benchmark_path(
        self,
        benchmark_codes: Iterable[str],
        start_date: str,
        end_date: str,
    ) -> dict[str, list[dict[str, Any]]]:
        codes = [str(code) for code in benchmark_codes]
        if not codes:
            return {}
        placeholders = ",".join(["%s"] * len(codes))
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT index_code, trade_date, open, close
                    FROM market_index_daily
                    WHERE index_code IN ({placeholders}) AND trade_date BETWEEN %s AND %s
                    ORDER BY index_code, trade_date
                    """,
                    (*codes, start_date, end_date),
                )
                rows = cursor.fetchall() or []
        grouped: dict[str, list[dict[str, Any]]] = {code: [] for code in codes}
        for row in rows:
            grouped.setdefault(str(row.get("index_code")), []).append(row)
        return grouped

    def update_pick_outcome(self, pick_id: int, outcome: dict[str, Any]) -> None:
        returns = outcome.get("returns") or {}
        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE strategy_forward_pick
                    SET entry_trade_date=%s, entry_price=%s, price_adjustment_mode=%s,
                        return_1d_pct=%s, return_3d_pct=%s, return_5d_pct=%s, return_20d_pct=%s,
                        max_favorable_5d_pct=%s, max_adverse_5d_pct=%s,
                        max_favorable_20d_pct=%s, max_adverse_20d_pct=%s,
                        outcome_status=%s, outcome_json=%s, last_outcome_trade_date=%s,
                        outcome_updated_at=NOW()
                    WHERE id=%s
                    """,
                    (
                        outcome.get("entry_trade_date"),
                        outcome.get("entry_price"),
                        outcome.get("price_adjustment_mode"),
                        returns.get("1"),
                        returns.get("3"),
                        returns.get("5"),
                        returns.get("20"),
                        outcome.get("max_favorable_5d_pct"),
                        outcome.get("max_adverse_5d_pct"),
                        outcome.get("max_favorable_20d_pct"),
                        outcome.get("max_adverse_20d_pct"),
                        outcome.get("status"),
                        _json(outcome),
                        outcome.get("last_outcome_trade_date"),
                        pick_id,
                    ),
                )

    def record_action(
        self,
        *,
        observation_id: str,
        code: str,
        action_type: str,
        action_price: float | None = None,
        note: str | None = None,
        source: str = "user",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_action = str(action_type or "").strip().lower()
        if normalized_action not in SUPPORTED_ACTION_TYPES:
            raise ValueError(f"unsupported forward action: {normalized_action}")
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT protocol_id FROM strategy_forward_pick WHERE observation_id=%s AND code=%s",
                    (observation_id, code),
                )
                pick = cursor.fetchone() or {}
                if not pick:
                    raise ValueError("forward observation pick not found")
                cursor.execute(
                    """
                    INSERT INTO strategy_forward_action (
                        observation_id, protocol_id, code, action_type, action_at,
                        action_price, note, source, metadata_json
                    ) VALUES (%s,%s,%s,%s,NOW(),%s,%s,%s,%s)
                    """,
                    (
                        observation_id,
                        pick.get("protocol_id"),
                        code,
                        normalized_action,
                        action_price,
                        note[:500] if note else None,
                        source,
                        _json(metadata or {}),
                    ),
                )
                action_id = cursor.lastrowid
                cursor.execute("SELECT * FROM strategy_forward_action WHERE id=%s", (action_id,))
                return cursor.fetchone() or {}

    def evidence_rows(self, strategy_id: str) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM strategy_forward_protocol
                    WHERE strategy_id=%s ORDER BY started_on DESC, id DESC LIMIT 1
                    """,
                    (strategy_id,),
                )
                protocol = cursor.fetchone() or {}
                if not protocol:
                    return {}, [], [], []
                protocol_id = protocol.get("protocol_id")
                cursor.execute(
                    """
                    SELECT observation_id, campaign_id, observation_source,
                           signal_trade_date, selection_run_id, source_snapshot_id,
                           paired_input_hash, status, result_count, data_as_of_at,
                           ai_mode, ai_status_json, error_code, error_message,
                           submitted_at, completed_at, created_at
                    FROM strategy_forward_observation
                    WHERE protocol_id=%s ORDER BY signal_trade_date, id
                    """,
                    (protocol_id,),
                )
                observations = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT observation_id, campaign_id, observation_source,
                           source_snapshot_id, signal_trade_date, code, name,
                           rank_no, score, theme_name, trade_grade_state,
                           entry_trade_date, entry_price,
                           price_adjustment_mode, return_1d_pct, return_3d_pct,
                           return_5d_pct, return_20d_pct, max_favorable_5d_pct,
                           max_adverse_5d_pct, max_favorable_20d_pct, max_adverse_20d_pct,
                           outcome_status, outcome_json
                    FROM strategy_forward_pick
                    WHERE protocol_id=%s ORDER BY signal_trade_date, rank_no, id
                    """,
                    (protocol_id,),
                )
                picks = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT observation_id, code, action_type, action_at, action_price,
                           note, source
                    FROM strategy_forward_action
                    WHERE protocol_id=%s
                    ORDER BY action_at, id
                    """,
                    (protocol_id,),
                )
                actions = cursor.fetchall() or []
        return protocol, observations, picks, actions


def _adjusted_value(bar: dict[str, Any], price_key: str, entry_factor: float | None) -> tuple[float | None, bool]:
    price = _float(bar.get(price_key))
    factor = _float(bar.get("adj_factor"))
    if price is None:
        return None, False
    if entry_factor is not None and factor is not None:
        return price * factor / entry_factor, True
    return price, False


def build_forward_outcome(
    price_path: list[dict[str, Any]],
    *,
    horizons: Iterable[int],
    benchmark_paths: dict[str, list[dict[str, Any]]] | None = None,
    entry_rule: str = "next_tradable_open",
) -> dict[str, Any]:
    if entry_rule not in {"next_tradable_open", "same_day_open"}:
        raise ValueError(f"unsupported forward entry rule: {entry_rule}")
    requested_horizons = sorted({int(value) for value in horizons if int(value) > 0})
    if not price_path:
        return {
            "status": "pending",
            "entry_trade_date": None,
            "entry_price": None,
            "returns": {str(value): None for value in requested_horizons},
            "horizons": {},
            "price_adjustment_mode": None,
            "last_outcome_trade_date": None,
        }

    entry = price_path[0]
    entry_price = _float(entry.get("open"))
    entry_factor = _float(entry.get("adj_factor"))
    if entry_price is None or entry_price <= 0:
        return {
            "status": "pending",
            "entry_trade_date": _date_text(entry.get("trade_date")),
            "entry_price": None,
            "returns": {str(value): None for value in requested_horizons},
            "horizons": {},
            "price_adjustment_mode": None,
            "last_outcome_trade_date": _date_text(price_path[-1].get("trade_date")),
        }

    horizon_details: dict[str, Any] = {}
    returns: dict[str, float | None] = {}
    used_adjusted = True
    benchmark_paths = benchmark_paths or {}
    entry_date = _date_text(entry.get("trade_date"))
    for horizon in requested_horizons:
        key = str(horizon)
        if len(price_path) < horizon:
            returns[key] = None
            continue
        exit_bar = price_path[horizon - 1]
        exit_price, adjusted = _adjusted_value(exit_bar, "close", entry_factor)
        used_adjusted = used_adjusted and adjusted
        return_pct = round((exit_price / entry_price - 1) * 100, 4) if exit_price is not None else None
        returns[key] = return_pct
        exit_date = _date_text(exit_bar.get("trade_date"))
        benchmark_returns: dict[str, float | None] = {}
        excess_returns: dict[str, float | None] = {}
        for benchmark_code, rows in benchmark_paths.items():
            by_date = {_date_text(row.get("trade_date")): row for row in rows}
            benchmark_entry = by_date.get(entry_date)
            benchmark_exit = by_date.get(exit_date)
            benchmark_open = _float((benchmark_entry or {}).get("open"))
            benchmark_close = _float((benchmark_exit or {}).get("close"))
            benchmark_return = (
                round((benchmark_close / benchmark_open - 1) * 100, 4)
                if benchmark_open and benchmark_close is not None
                else None
            )
            benchmark_returns[benchmark_code] = benchmark_return
            excess_returns[benchmark_code] = (
                round(return_pct - benchmark_return, 4)
                if return_pct is not None and benchmark_return is not None
                else None
            )
        horizon_details[key] = {
            "exit_trade_date": exit_date,
            "exit_close": _float(exit_bar.get("close")),
            "return_pct": return_pct,
            "benchmark_returns_pct": benchmark_returns,
            "excess_returns_pct": excess_returns,
            "adjusted": adjusted,
        }

    def excursion(days: int) -> tuple[float | None, float | None]:
        if len(price_path) < days:
            return None, None
        highs: list[float] = []
        lows: list[float] = []
        nonlocal used_adjusted
        for bar in price_path[:days]:
            high, high_adjusted = _adjusted_value(bar, "high", entry_factor)
            low, low_adjusted = _adjusted_value(bar, "low", entry_factor)
            if high is not None:
                highs.append(high)
            if low is not None:
                lows.append(low)
            used_adjusted = used_adjusted and high_adjusted and low_adjusted
        favorable = round((max(highs) / entry_price - 1) * 100, 4) if highs else None
        adverse = round((min(lows) / entry_price - 1) * 100, 4) if lows else None
        return favorable, adverse

    mfe5, mae5 = excursion(5)
    mfe20, mae20 = excursion(20)
    max_horizon = max(requested_horizons, default=0)
    status = "complete" if max_horizon and len(price_path) >= max_horizon else "partial"
    return {
        "status": status,
        "entry_trade_date": entry_date,
        "entry_price": round(entry_price, 4),
        "entry_rule": entry_rule,
        "returns": returns,
        "horizons": horizon_details,
        "max_favorable_5d_pct": mfe5,
        "max_adverse_5d_pct": mae5,
        "max_favorable_20d_pct": mfe20,
        "max_adverse_20d_pct": mae20,
        "price_adjustment_mode": "adjusted_total_return" if used_adjusted and entry_factor is not None else "raw_fallback",
        "path_trade_days": len(price_path),
        "last_outcome_trade_date": _date_text(price_path[-1].get("trade_date")),
    }


class ForwardObservationService:
    def __init__(
        self,
        repository: ForwardObservationRepository | None = None,
        trade_plan_events: TradePlanEventRepository | None = None,
    ) -> None:
        self.repository = repository or ForwardObservationRepository()
        self.trade_plan_events = (
            trade_plan_events or TradePlanEventRepository()
        )

    def reconcile_open_observations(self) -> dict[str, int]:
        changed = {"success": 0, "failed": 0, "waiting": 0}
        for observation in self.repository.open_observations():
            observation_id = str(observation.get("observation_id"))
            run = None
            if observation.get("selection_run_id"):
                with self.repository._connect() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM selection_run WHERE run_id=%s",
                            (observation.get("selection_run_id"),),
                        )
                        run = cursor.fetchone()
            if not run:
                run = self.repository.find_selection_run_for_observation(observation_id)
                if run:
                    self.repository.attach_selection_run(observation_id, str(run.get("run_id")))
            if not run:
                changed["waiting"] += 1
                continue
            status = str(run.get("status") or "")
            if status == "success":
                result = _from_json(run.get("result_json"), {}) or {}
                self.repository.finalize_success(
                    protocol_id=str(observation.get("protocol_id")),
                    observation_id=observation_id,
                    selection_run_id=str(run.get("run_id")),
                    result=result,
                )
                changed["success"] += 1
            elif status in {"failed", "cancelled"}:
                self.repository.mark_failed(
                    observation_id=observation_id,
                    selection_run_id=str(run.get("run_id")),
                    error_code=str(run.get("error_code") or f"selection_{status}"),
                    error_message=str(run.get("error_message") or f"selection run {status}"),
                )
                changed["failed"] += 1
            else:
                changed["waiting"] += 1
        return changed

    def refresh_outcomes(self, limit: int = 500) -> dict[str, int]:
        changed = {"processed": 0, "pending": 0, "partial": 0, "complete": 0}
        for pick in self.repository.pending_outcome_picks(limit=limit):
            horizons = _from_json(pick.get("horizons_json"), [1, 3, 5, 20]) or [1, 3, 5, 20]
            benchmark_codes = _from_json(pick.get("benchmark_codes_json"), []) or []
            price_path = self.repository.load_price_path(
                str(pick.get("code")),
                str(pick.get("signal_trade_date")),
                limit=max(max(int(value) for value in horizons), 20) + 5,
                entry_rule=str(pick.get("entry_rule") or "next_tradable_open"),
            )
            benchmark_paths: dict[str, list[dict[str, Any]]] = {}
            if price_path:
                benchmark_paths = self.repository.load_benchmark_path(
                    benchmark_codes,
                    _date_text(price_path[0].get("trade_date")) or str(pick.get("signal_trade_date")),
                    _date_text(price_path[-1].get("trade_date")) or str(pick.get("signal_trade_date")),
                )
            outcome = build_forward_outcome(
                price_path,
                horizons=horizons,
                benchmark_paths=benchmark_paths,
                entry_rule=str(pick.get("entry_rule") or "next_tradable_open"),
            )
            raw_item = _from_json(pick.get("raw_json"), {}) or {}
            turtle_evaluation, plan_events = build_turtle_forward_outcome(
                raw_item=raw_item,
                price_path=price_path,
                strategy_id=str(pick.get("strategy_id") or ""),
                observation_id=str(pick.get("observation_id") or ""),
                signal_trade_date=str(pick.get("signal_trade_date") or "")[:10],
                source_snapshot_id=pick.get("source_snapshot_id"),
            )
            if turtle_evaluation.get("status") != "not_applicable":
                outcome["trade_plan_evaluation"] = turtle_evaluation
                self.trade_plan_events.append(plan_events)
                if (
                    outcome.get("status") == "complete"
                    and turtle_evaluation.get("status") == "pending"
                ):
                    outcome["status"] = "partial"
            self.repository.update_pick_outcome(int(pick["id"]), outcome)
            status = str(outcome.get("status") or "pending")
            changed["processed"] += 1
            changed[status] = changed.get(status, 0) + 1
        return changed

    def evidence_summary(self, strategy_id: str) -> dict[str, Any]:
        protocol, observations, picks, actions = self.repository.evidence_rows(strategy_id)
        if not protocol:
            return {
                "status": "not_configured",
                "validation_status": "unvalidated",
                "strategy_id": strategy_id,
            }
        success_observations = [row for row in observations if row.get("status") == "success"]
        candidate_count = len(picks)
        observation_days = len(success_observations)
        minimum_days = int(protocol.get("minimum_observation_days") or 0)
        minimum_candidates = int(protocol.get("minimum_candidate_count") or 0)
        action_counts: dict[str, int] = {}
        actions_by_pick: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for action in actions:
            action_type = str(action.get("action_type") or "")
            if action_type:
                action_counts[action_type] = action_counts.get(action_type, 0) + 1
            key = (str(action.get("observation_id") or ""), str(action.get("code") or ""))
            actions_by_pick.setdefault(key, []).append(action)

        metrics: dict[str, Any] = {}
        for horizon in (1, 3, 5, 20):
            key = f"return_{horizon}d_pct"
            values = [_float(row.get(key)) for row in picks]
            values = [value for value in values if value is not None]
            benchmark_values: dict[str, list[float]] = {}
            excess_values: dict[str, list[float]] = {}
            for row in picks:
                outcome = _from_json(row.get("outcome_json"), {}) or {}
                detail = (outcome.get("horizons") or {}).get(str(horizon)) or {}
                for benchmark_code, value in (detail.get("benchmark_returns_pct") or {}).items():
                    number = _float(value)
                    if number is not None:
                        benchmark_values.setdefault(str(benchmark_code), []).append(number)
                for benchmark_code, value in (detail.get("excess_returns_pct") or {}).items():
                    number = _float(value)
                    if number is not None:
                        excess_values.setdefault(str(benchmark_code), []).append(number)
            metrics[str(horizon)] = {
                "sample_size": len(values),
                "average_return_pct": round(mean(values), 4) if values else None,
                "win_rate_pct": round(sum(value > 0 for value in values) / len(values) * 100, 2) if values else None,
                "average_benchmark_return_pct": {
                    code: round(mean(numbers), 4) for code, numbers in benchmark_values.items()
                },
                "average_excess_return_pct": {
                    code: round(mean(numbers), 4) for code, numbers in excess_values.items()
                },
            }
        picked_keys = {
            (str(row.get("observation_id") or ""), str(row.get("code") or "")) for row in picks
        }
        acted_keys = {key for key in actions_by_pick if key in picked_keys}
        decision_action_types = {"saved", "bought", "skipped", "sold"}
        decided_keys = {
            key
            for key, rows in actions_by_pick.items()
            if key in picked_keys and any(str(row.get("action_type")) in decision_action_types for row in rows)
        }
        bought_keys = {
            key
            for key, rows in actions_by_pick.items()
            if key in picked_keys and any(str(row.get("action_type")) == "bought" for row in rows)
        }
        sold_keys = {
            key
            for key, rows in actions_by_pick.items()
            if key in picked_keys and any(str(row.get("action_type")) == "sold" for row in rows)
        }
        bought_picks = [
            row
            for row in picks
            if (str(row.get("observation_id") or ""), str(row.get("code") or "")) in bought_keys
        ]
        bought_metrics: dict[str, Any] = {}
        for horizon in (1, 3, 5, 20):
            values = [_float(row.get(f"return_{horizon}d_pct")) for row in bought_picks]
            values = [value for value in values if value is not None]
            bought_metrics[str(horizon)] = {
                "sample_size": len(values),
                "average_return_pct": round(mean(values), 4) if values else None,
                "win_rate_pct": round(sum(value > 0 for value in values) / len(values) * 100, 2) if values else None,
            }
        preliminary_ready = observation_days >= minimum_days or candidate_count >= minimum_candidates
        return {
            "status": "preliminary_ready" if preliminary_ready else "collecting",
            "validation_status": "unvalidated",
            "strategy_id": strategy_id,
            "protocol": {
                "protocol_id": protocol.get("protocol_id"),
                "campaign_id": protocol.get("campaign_id"),
                "observation_source": protocol.get("observation_source"),
                "strategy_version": protocol.get("strategy_version"),
                "protocol_version": protocol.get("protocol_version"),
                "execution_time": _time_text(protocol.get("execution_time")),
                "timezone": protocol.get("timezone"),
                "entry_rule": protocol.get("entry_rule"),
                "horizons": _from_json(protocol.get("horizons_json"), []),
                "benchmark_codes": _from_json(protocol.get("benchmark_codes_json"), []),
                "immutable_tag": protocol.get("immutable_tag"),
                "implementation_commit": protocol.get("implementation_commit"),
                "strategy_config_hash": protocol.get("strategy_config_hash"),
                "ai_policy": protocol.get("ai_policy"),
                "started_on": _date_text(protocol.get("started_on")),
                "minimum_observation_days": minimum_days,
                "minimum_candidate_count": minimum_candidates,
            },
            "sample": {
                "observation_days": observation_days,
                "candidate_count": candidate_count,
                "zero_pick_days": sum(int(row.get("result_count") or 0) == 0 for row in success_observations),
                "failed_days": sum(row.get("status") == "failed" for row in observations),
                "observation_target_met": observation_days >= minimum_days,
                "candidate_target_met": candidate_count >= minimum_candidates,
            },
            "metrics": metrics,
            "advice_quality": {
                "forward_returns": metrics,
                "complete_20d_pick_count": sum(row.get("outcome_status") == "complete" for row in picks),
                "adjusted_pick_count": sum(row.get("price_adjustment_mode") == "adjusted_total_return" for row in picks),
                "raw_fallback_pick_count": sum(row.get("price_adjustment_mode") == "raw_fallback" for row in picks),
                "average_mfe_5d_pct": _mean_field(picks, "max_favorable_5d_pct"),
                "average_mae_5d_pct": _mean_field(picks, "max_adverse_5d_pct"),
                "average_mfe_20d_pct": _mean_field(picks, "max_favorable_20d_pct"),
                "average_mae_20d_pct": _mean_field(picks, "max_adverse_20d_pct"),
            },
            "ai_modes": {
                mode: sum(row.get("ai_mode") == mode for row in success_observations)
                for mode in sorted({str(row.get("ai_mode")) for row in success_observations if row.get("ai_mode")})
            },
            "actions": action_counts,
            "user_discipline": {
                "acted_pick_count": len(acted_keys),
                "decided_pick_count": len(decided_keys),
                "bought_pick_count": len(bought_keys),
                "sold_pick_count": len(sold_keys),
                "decision_rate_pct": round(len(decided_keys) / candidate_count * 100, 2) if candidate_count else None,
                "bought_forward_returns": bought_metrics,
            },
            "recent_observations": [
                {
                    "observation_id": row.get("observation_id"),
                    "campaign_id": row.get("campaign_id"),
                    "observation_source": row.get("observation_source"),
                    "source_snapshot_id": row.get("source_snapshot_id"),
                    "paired_input_hash": row.get("paired_input_hash"),
                    "signal_trade_date": _date_text(row.get("signal_trade_date")),
                    "status": row.get("status"),
                    "result_count": int(row.get("result_count") or 0),
                    "ai_mode": row.get("ai_mode"),
                    "selection_run_id": row.get("selection_run_id"),
                }
                for row in observations[-20:][::-1]
            ],
            "recent_picks": [
                _recent_pick_payload(row, actions_by_pick)
                for row in picks[-50:][::-1]
            ],
            "recent_actions": [
                {
                    "observation_id": row.get("observation_id"),
                    "code": row.get("code"),
                    "action_type": row.get("action_type"),
                    "action_at": _datetime_text(row.get("action_at")),
                    "action_price": _float(row.get("action_price")),
                    "note": row.get("note"),
                    "source": row.get("source"),
                }
                for row in actions[-100:][::-1]
            ],
        }


def _mean_field(rows: Iterable[dict[str, Any]], field: str) -> float | None:
    values = [_float(row.get(field)) for row in rows]
    values = [value for value in values if value is not None]
    return round(mean(values), 4) if values else None


def _recent_pick_payload(
    row: dict[str, Any],
    actions_by_pick: dict[tuple[str, str], list[dict[str, Any]]],
) -> dict[str, Any]:
    observation_id = str(row.get("observation_id") or "")
    code = str(row.get("code") or "")
    action_rows = actions_by_pick.get((observation_id, code), [])
    return {
        "observation_id": row.get("observation_id"),
        "campaign_id": row.get("campaign_id"),
        "observation_source": row.get("observation_source"),
        "source_snapshot_id": row.get("source_snapshot_id"),
        "signal_trade_date": _date_text(row.get("signal_trade_date")),
        "code": row.get("code"),
        "name": row.get("name"),
        "rank_no": row.get("rank_no"),
        "score": _float(row.get("score")),
        "theme_name": row.get("theme_name"),
        "trade_grade_state": row.get("trade_grade_state"),
        "entry_trade_date": _date_text(row.get("entry_trade_date")),
        "entry_price": _float(row.get("entry_price")),
        "return_1d_pct": _float(row.get("return_1d_pct")),
        "return_3d_pct": _float(row.get("return_3d_pct")),
        "return_5d_pct": _float(row.get("return_5d_pct")),
        "return_20d_pct": _float(row.get("return_20d_pct")),
        "outcome_status": row.get("outcome_status"),
        "last_action": action_rows[-1].get("action_type") if action_rows else None,
        "actions": [str(action.get("action_type")) for action in action_rows],
    }
