from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Mapping

from app.shared.db import mysql_conn
from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.forward_observation import (
    ForwardObservationRepository,
    ForwardProtocolDriftError,
    ForwardProtocolSpec,
    strategy_config_hash,
)
from app.stock_selection.sentiment_snapshot_materializer import (
    SentimentSnapshotMaterializationService,
)
from app.strategies.service import StrategyService


AUTOMATIC_OBSERVATION_SOURCE = "automatic_observation"
AUTOMATIC_OBSERVATION_MODE = "paired_first_n_trade_days"
SUPPORTED_OBSERVATION_ENGINE = "sentiment_snapshot_pair"


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_value(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback


def _time_text(value: Any) -> str:
    if isinstance(value, timedelta):
        seconds = int(value.total_seconds())
        return (
            f"{seconds // 3600:02d}:"
            f"{(seconds % 3600) // 60:02d}:"
            f"{seconds % 60:02d}"
        )
    if isinstance(value, dt_time):
        return value.strftime("%H:%M:%S")
    text = str(value or "").strip()
    return f"{text}:00" if len(text) == 5 else text


def _compact_id(prefix: str, material: str, *, max_length: int = 96) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", material).strip("_").lower()
    candidate = f"{prefix}_{slug}"
    if len(candidate) <= max_length:
        return candidate
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]
    return f"{candidate[: max_length - len(digest) - 1]}_{digest}"


@dataclass(frozen=True)
class AutomaticObservationPolicy:
    baseline_strategy_id: str
    candidate_strategy_id: str
    baseline_strategy_version: str
    candidate_strategy_version: str
    baseline_immutable_tag: str
    candidate_immutable_tag: str
    start_on: date
    target_trade_days: int
    execution_time: str
    timezone: str
    entry_rule: str
    horizons: tuple[int, ...]
    benchmark_codes: tuple[str, ...]
    max_picks: int
    minimum_realtime_coverage: float
    engine: str

    @property
    def campaign_id(self) -> str:
        return _compact_id(
            "autoobs",
            (
                f"{self.candidate_strategy_id}_{self.candidate_strategy_version}_"
                f"{self.start_on.isoformat()}_{self.execution_time}_"
                f"{self.target_trade_days}d"
            ),
        )

    def protocol_id(self, role: str, strategy_id: str, strategy_version: str) -> str:
        return _compact_id(
            "autofwd",
            (
                f"{self.campaign_id}_{role}_{strategy_id}_"
                f"{strategy_version}"
            ),
        )

    def observation_id(self, role: str, strategy_id: str, signal_date: date) -> str:
        return _compact_id(
            "autoobs",
            (
                f"{self.campaign_id}_{signal_date.strftime('%Y%m%d')}_"
                f"{role}_{strategy_id}"
            ),
        )


def discover_automatic_observation_policies(
    loader: StrategyLoader,
    *,
    today: date,
) -> list[AutomaticObservationPolicy]:
    registry = loader.registry
    defaults = dict(registry.get("automatic_observation_defaults") or {})
    if not defaults.get("enabled", False):
        return []
    if str(defaults.get("mode") or "") != AUTOMATIC_OBSERVATION_MODE:
        raise ValueError("unsupported automatic observation mode")

    baseline_strategy_id = str(defaults.get("baseline_strategy_id") or "").strip()
    if not baseline_strategy_id:
        raise ValueError("automatic observation baseline_strategy_id is required")
    baseline_meta = loader.get_strategy_meta(baseline_strategy_id)
    baseline_tag = str(baseline_meta.get("immutable_tag") or "").strip()
    if not baseline_tag:
        raise ValueError(f"baseline strategy {baseline_strategy_id} has no immutable_tag")

    policies: list[AutomaticObservationPolicy] = []
    for raw_meta in registry.get("strategies", []):
        meta = dict(raw_meta or {})
        strategy_id = str(meta.get("id") or "").strip()
        if not strategy_id or strategy_id == baseline_strategy_id:
            continue
        if str(meta.get("mode") or "") != "shadow_only":
            continue
        override = dict(meta.get("automatic_observation") or {})
        if override.get("enabled", True) is False:
            raise ValueError(
                f"shadow strategy {strategy_id} cannot opt out of the "
                "mandatory automatic observation policy"
            )
        engine = str(override.get("engine") or defaults.get("engine") or "")
        if engine != SUPPORTED_OBSERVATION_ENGINE:
            raise ValueError(
                f"strategy {strategy_id} requires unsupported observation engine {engine}"
            )
        immutable_tag = str(meta.get("immutable_tag") or "").strip()
        if not immutable_tag:
            raise ValueError(f"shadow strategy {strategy_id} has no immutable_tag")
        start_value = override.get("start_on")
        start_on = (
            date.fromisoformat(str(start_value)[:10])
            if start_value
            else today
        )
        target_trade_days = int(
            override.get("target_trade_days")
            or defaults.get("target_trade_days")
            or 5
        )
        execution_time = _time_text(
            override.get("execution_time")
            or defaults.get("execution_time")
            or "09:25:00"
        )
        if target_trade_days != 5 or execution_time != "09:25:00":
            raise ValueError(
                f"strategy {strategy_id} must use the standard 5-day 09:25 observation policy"
            )
        entry_rule = str(
            override.get("entry_rule")
            or defaults.get("entry_rule")
            or "same_day_open"
        )
        if entry_rule != "same_day_open":
            raise ValueError("09:25 automatic observation must use same_day_open")
        policies.append(
            AutomaticObservationPolicy(
                baseline_strategy_id=baseline_strategy_id,
                candidate_strategy_id=strategy_id,
                baseline_strategy_version=str(baseline_meta.get("version") or ""),
                candidate_strategy_version=str(meta.get("version") or ""),
                baseline_immutable_tag=baseline_tag,
                candidate_immutable_tag=immutable_tag,
                start_on=start_on,
                target_trade_days=target_trade_days,
                execution_time=execution_time,
                timezone=str(
                    override.get("timezone")
                    or defaults.get("timezone")
                    or "Asia/Shanghai"
                ),
                entry_rule=entry_rule,
                horizons=tuple(
                    int(value)
                    for value in (
                        override.get("horizons")
                        or defaults.get("horizons")
                        or (1, 3, 5, 20)
                    )
                ),
                benchmark_codes=tuple(
                    str(value)
                    for value in (
                        override.get("benchmark_codes")
                        or defaults.get("benchmark_codes")
                        or ("000300.SH", "000905.SH", "000852.SH")
                    )
                ),
                max_picks=int(
                    override.get("max_picks")
                    or defaults.get("max_picks")
                    or 3
                ),
                minimum_realtime_coverage=float(
                    override.get("minimum_realtime_coverage")
                    or defaults.get("minimum_realtime_coverage")
                    or 0.90
                ),
                engine=engine,
            )
        )
    return policies


class AutomaticObservationCampaignRepository:
    def __init__(self, connection_factory=None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    def quote_readiness(
        self,
        *,
        signal_date: date,
        execution_time: str,
        minimum_coverage: float,
    ) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS expected_entities
                    FROM stock_basic
                    WHERE instrument_type='stock'
                      AND COALESCE(is_delisted, 0)=0
                    """
                )
                expected = int(
                    (cursor.fetchone() or {}).get("expected_entities") or 0
                )
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT realtime.code) AS actual_entities,
                           MAX(realtime.quote_time) AS latest_quote_time
                    FROM stock_realtime_snapshot realtime
                    INNER JOIN stock_basic sb ON sb.code=realtime.code
                    WHERE realtime.trade_date=%s
                      AND COALESCE(realtime.is_stale, 0)=0
                      AND sb.instrument_type='stock'
                      AND COALESCE(sb.is_delisted, 0)=0
                    """,
                    (signal_date,),
                )
                row = cursor.fetchone() or {}
        actual = int(row.get("actual_entities") or 0)
        coverage = round(actual / expected, 8) if expected else 0.0
        latest_quote_time = row.get("latest_quote_time")
        required_quote_time = datetime.fromisoformat(
            f"{signal_date.isoformat()}T{execution_time}"
        )
        ready = bool(
            latest_quote_time
            and latest_quote_time >= required_quote_time
            and coverage >= minimum_coverage
        )
        return {
            "ready": ready,
            "signal_trade_date": signal_date.isoformat(),
            "required_quote_time": required_quote_time.isoformat(sep=" "),
            "latest_quote_time": (
                str(latest_quote_time) if latest_quote_time else None
            ),
            "expected_entities": expected,
            "actual_entities": actual,
            "coverage_ratio": coverage,
            "minimum_coverage": minimum_coverage,
        }

    def ensure_campaign(
        self,
        policy: AutomaticObservationPolicy,
        *,
        implementation_commit: str,
        protocol_ids: Mapping[str, str],
    ) -> dict[str, Any]:
        metadata = {
            "mode": AUTOMATIC_OBSERVATION_MODE,
            "engine": policy.engine,
            "implementation_commit": implementation_commit,
            "manual_selection_table": "selection_result",
            "automatic_observation_tables": [
                "strategy_forward_observation",
                "strategy_forward_pick",
            ],
            "writes_selection_result": False,
        }
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT IGNORE INTO strategy_observation_campaign (
                        campaign_id, baseline_strategy_id, baseline_strategy_version,
                        candidate_strategy_id, candidate_strategy_version, status,
                        observation_source, execution_time, timezone, entry_rule,
                        target_trade_days, completed_trade_days, started_on,
                        protocol_ids_json, metadata_json
                    ) VALUES (
                        %s,%s,%s,%s,%s,'active',%s,%s,%s,%s,%s,0,%s,%s,%s
                    )
                    """,
                    (
                        policy.campaign_id,
                        policy.baseline_strategy_id,
                        policy.baseline_strategy_version,
                        policy.candidate_strategy_id,
                        policy.candidate_strategy_version,
                        AUTOMATIC_OBSERVATION_SOURCE,
                        policy.execution_time,
                        policy.timezone,
                        policy.entry_rule,
                        policy.target_trade_days,
                        policy.start_on,
                        _json(dict(protocol_ids)),
                        _json(metadata),
                    ),
                )
                cursor.execute(
                    """
                    SELECT *
                    FROM strategy_observation_campaign
                    WHERE campaign_id=%s
                    """,
                    (policy.campaign_id,),
                )
                row = cursor.fetchone() or {}
        if not row:
            raise RuntimeError(f"automatic observation campaign missing: {policy.campaign_id}")
        expected = {
            "baseline_strategy_id": policy.baseline_strategy_id,
            "baseline_strategy_version": policy.baseline_strategy_version,
            "candidate_strategy_id": policy.candidate_strategy_id,
            "candidate_strategy_version": policy.candidate_strategy_version,
            "observation_source": AUTOMATIC_OBSERVATION_SOURCE,
            "execution_time": policy.execution_time,
            "timezone": policy.timezone,
            "entry_rule": policy.entry_rule,
            "target_trade_days": policy.target_trade_days,
            "started_on": policy.start_on.isoformat(),
            "protocol_ids_json": dict(protocol_ids),
        }
        actual = {
            **row,
            "execution_time": _time_text(row.get("execution_time")),
            "started_on": str(row.get("started_on"))[:10],
            "protocol_ids_json": _json_value(row.get("protocol_ids_json"), {}),
        }
        changed = [
            key
            for key, value in expected.items()
            if actual.get(key) != value
        ]
        if changed:
            raise ForwardProtocolDriftError(
                f"automatic observation campaign drift: {', '.join(changed)}"
            )
        row["metadata_json"] = _json_value(row.get("metadata_json"), {})
        row["protocol_ids_json"] = actual["protocol_ids_json"]
        return row

    def refresh_progress(self, campaign_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS completed_trade_days,
                           MAX(signal_trade_date) AS last_signal_trade_date
                    FROM (
                        SELECT signal_trade_date
                        FROM strategy_forward_observation
                        WHERE campaign_id=%s
                          AND observation_source=%s
                          AND status='success'
                        GROUP BY signal_trade_date
                        HAVING COUNT(DISTINCT protocol_id) >= 2
                    ) paired_days
                    """,
                    (campaign_id, AUTOMATIC_OBSERVATION_SOURCE),
                )
                progress = cursor.fetchone() or {}
                completed = int(progress.get("completed_trade_days") or 0)
                last_signal = progress.get("last_signal_trade_date")
                cursor.execute(
                    """
                    UPDATE strategy_observation_campaign
                    SET completed_trade_days=%s,
                        last_signal_trade_date=%s,
                        status=CASE
                            WHEN %s >= target_trade_days THEN 'completed'
                            ELSE 'active'
                        END,
                        completed_on=CASE
                            WHEN %s >= target_trade_days
                            THEN COALESCE(completed_on, %s)
                            ELSE NULL
                        END
                    WHERE campaign_id=%s
                    """,
                    (
                        completed,
                        last_signal,
                        completed,
                        completed,
                        last_signal,
                        campaign_id,
                    ),
                )
                cursor.execute(
                    """
                    SELECT *
                    FROM strategy_observation_campaign
                    WHERE campaign_id=%s
                    """,
                    (campaign_id,),
                )
                return cursor.fetchone() or {}


class AutomaticObservationCampaignService:
    def __init__(
        self,
        *,
        loader: StrategyLoader | None = None,
        campaign_repository: AutomaticObservationCampaignRepository | None = None,
        forward_repository: ForwardObservationRepository | None = None,
        materializer: SentimentSnapshotMaterializationService | None = None,
        strategy_service: StrategyService | None = None,
    ) -> None:
        self.loader = loader or StrategyLoader()
        self.campaigns = (
            campaign_repository or AutomaticObservationCampaignRepository()
        )
        self.forward = forward_repository or ForwardObservationRepository()
        self.materializer = (
            materializer or SentimentSnapshotMaterializationService()
        )
        self.strategy_service = strategy_service or StrategyService()

    def policies(self, *, today: date) -> list[AutomaticObservationPolicy]:
        return discover_automatic_observation_policies(
            self.loader,
            today=today,
        )

    def quote_readiness(
        self,
        *,
        today: date,
        policies: list[AutomaticObservationPolicy],
    ) -> dict[str, Any]:
        if not policies:
            return {"ready": False, "reason": "no_automatic_observation_policy"}
        minimum = max(policy.minimum_realtime_coverage for policy in policies)
        execution_times = {policy.execution_time for policy in policies}
        if len(execution_times) != 1:
            raise ValueError("automatic observation policies must share one execution time")
        return self.campaigns.quote_readiness(
            signal_date=today,
            execution_time=execution_times.pop(),
            minimum_coverage=minimum,
        )

    def run(
        self,
        *,
        today: date,
        implementation_commit: str,
    ) -> dict[str, Any]:
        policies = self.policies(today=today)
        results = []
        for policy in policies:
            results.append(
                self._run_policy(
                    policy,
                    today=today,
                    implementation_commit=implementation_commit,
                )
            )
        return {
            "status": "success",
            "signal_trade_date": today.isoformat(),
            "campaign_count": len(results),
            "campaigns": results,
            "selection_result_written": False,
        }

    def _run_policy(
        self,
        policy: AutomaticObservationPolicy,
        *,
        today: date,
        implementation_commit: str,
    ) -> dict[str, Any]:
        protocol_ids = {
            "baseline": policy.protocol_id(
                "baseline",
                policy.baseline_strategy_id,
                policy.baseline_strategy_version,
            ),
            "candidate": policy.protocol_id(
                "candidate",
                policy.candidate_strategy_id,
                policy.candidate_strategy_version,
            ),
        }
        campaign = self.campaigns.ensure_campaign(
            policy,
            implementation_commit=implementation_commit,
            protocol_ids=protocol_ids,
        )
        campaign = self.campaigns.refresh_progress(policy.campaign_id)
        if str(campaign.get("status") or "") == "completed":
            return self._campaign_payload(campaign, status="completed")
        if today < policy.start_on:
            return {
                **self._campaign_payload(campaign, status="scheduled"),
                "start_on": policy.start_on.isoformat(),
            }
        if str(campaign.get("last_signal_trade_date") or "")[:10] == today.isoformat():
            return self._campaign_payload(campaign, status="deduplicated")

        materialized = self.materializer.materialize_pair(
            baseline_strategy_id=policy.baseline_strategy_id,
            candidate_strategy_id=policy.candidate_strategy_id,
            max_picks=policy.max_picks,
        )
        if materialized.get("status") != "success":
            raise RuntimeError(
                f"paired materialization failed for {policy.campaign_id}"
            )
        paired_input_hash = str(materialized.get("dual_input_hash") or "")
        runs = dict(materialized.get("runs") or {})
        expected_ids = {
            policy.baseline_strategy_id,
            policy.candidate_strategy_id,
        }
        if set(runs) != expected_ids or not paired_input_hash:
            raise RuntimeError("paired materialization returned incomplete lineage")

        frozen_commit = str(
            (_json_value(campaign.get("metadata_json"), {}) or {}).get(
                "implementation_commit"
            )
            or implementation_commit
        )
        strategy_records = (
            (
                "baseline",
                policy.baseline_strategy_id,
                policy.baseline_strategy_version,
                policy.baseline_immutable_tag,
                False,
            ),
            (
                "candidate",
                policy.candidate_strategy_id,
                policy.candidate_strategy_version,
                policy.candidate_immutable_tag,
                True,
            ),
        )
        records = []
        for role, strategy_id, strategy_version, immutable_tag, allow_shadow in strategy_records:
            snapshot_run = dict(runs[strategy_id] or {})
            snapshot_id = str(snapshot_run.get("snapshot_id") or "")
            if (
                snapshot_run.get("snapshot_status") != "ready"
                or snapshot_run.get("quality_status") != "passed"
                or not snapshot_id
            ):
                raise RuntimeError(
                    f"strategy {strategy_id} did not publish a ready paired snapshot"
                )
            spec = self._protocol_spec(
                policy,
                role=role,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                immutable_tag=immutable_tag,
                implementation_commit=frozen_commit,
                protocol_id=protocol_ids[role],
            )
            self.forward.ensure_protocol(spec)
            observation_id = policy.observation_id(role, strategy_id, today)
            result = self.strategy_service.published_sentiment_observation_result(
                strategy_id=strategy_id,
                snapshot_id=snapshot_id,
                limit=policy.max_picks,
                run_id=observation_id,
                allow_shadow=allow_shadow,
            )
            result["automatic_observation"] = {
                "campaign_id": policy.campaign_id,
                "observation_source": AUTOMATIC_OBSERVATION_SOURCE,
                "paired_input_hash": paired_input_hash,
                "signal_trade_date": today.isoformat(),
                "execution_time": policy.execution_time,
                "entry_rule": policy.entry_rule,
                "writes_selection_result": False,
            }
            records.append(
                {
                    "observation_id": observation_id,
                    "protocol_id": protocol_ids[role],
                    "campaign_id": policy.campaign_id,
                    "signal_trade_date": today,
                    "source_snapshot_id": snapshot_id,
                    "paired_input_hash": paired_input_hash,
                    "observation_source": AUTOMATIC_OBSERVATION_SOURCE,
                    "request": spec.request,
                    "result": result,
                }
            )

        observations = self.forward.finalize_paired_success(records)
        campaign = self.campaigns.refresh_progress(policy.campaign_id)
        return {
            **self._campaign_payload(campaign, status="recorded"),
            "paired_input_hash": paired_input_hash,
            "observations": [
                {
                    "observation_id": row.get("observation_id"),
                    "protocol_id": row.get("protocol_id"),
                    "source_snapshot_id": row.get("source_snapshot_id"),
                    "result_count": int(row.get("result_count") or 0),
                    "status": row.get("status"),
                }
                for row in observations
            ],
        }

    def _protocol_spec(
        self,
        policy: AutomaticObservationPolicy,
        *,
        role: str,
        strategy_id: str,
        strategy_version: str,
        immutable_tag: str,
        implementation_commit: str,
        protocol_id: str,
    ) -> ForwardProtocolSpec:
        meta = self.loader.get_strategy_meta(strategy_id)
        config = self.loader.load_config(strategy_id)
        threshold = float((config.get("selection") or {}).get("score_threshold") or 0)
        request = {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "instrument_type": "stock",
            "max_picks": policy.max_picks,
            "score_threshold": threshold,
            "save": False,
            "run_source": AUTOMATIC_OBSERVATION_SOURCE,
            "campaign_id": policy.campaign_id,
            "paired_role": role,
            "writes_selection_result": False,
        }
        return ForwardProtocolSpec(
            protocol_id=protocol_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            protocol_version="automatic_open_0925_v1",
            execution_time=policy.execution_time,
            timezone=policy.timezone,
            entry_rule=policy.entry_rule,
            horizons=policy.horizons,
            benchmark_codes=policy.benchmark_codes,
            minimum_observation_days=policy.target_trade_days,
            minimum_candidate_count=policy.target_trade_days * policy.max_picks,
            immutable_tag=immutable_tag,
            implementation_commit=implementation_commit,
            strategy_config_hash=strategy_config_hash(meta, config),
            ai_policy=(
                "advisory_only_zero_applied_adjustment"
                if role == "candidate"
                else "local_core_snapshot"
            ),
            strategy_snapshot={
                "strategy_meta": meta,
                "strategy_config": config,
                "methodology": {
                    "signal_clock": "call_auction_09_25_asia_shanghai",
                    "entry_rule": policy.entry_rule,
                    "exit_rules": {
                        "1d": "signal_day_close",
                        "3d": "third_trading_day_close_including_entry_day",
                        "5d": "fifth_trading_day_close_including_entry_day",
                        "20d": "twentieth_trading_day_close_including_entry_day",
                    },
                    "sample_policy": (
                        "retain_exactly_five_successful_paired_trade_days_"
                        "including_zero_pick_days"
                    ),
                    "manual_separation": (
                        "never_write_selection_result_or_manual_14_day_statistics"
                    ),
                },
            },
            request=request,
            started_on=policy.start_on.isoformat(),
            campaign_id=policy.campaign_id,
            observation_source=AUTOMATIC_OBSERVATION_SOURCE,
        )

    @staticmethod
    def _campaign_payload(
        campaign: Mapping[str, Any],
        *,
        status: str,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "campaign_id": campaign.get("campaign_id"),
            "baseline_strategy_id": campaign.get("baseline_strategy_id"),
            "baseline_strategy_version": campaign.get("baseline_strategy_version"),
            "candidate_strategy_id": campaign.get("candidate_strategy_id"),
            "candidate_strategy_version": campaign.get("candidate_strategy_version"),
            "completed_trade_days": int(
                campaign.get("completed_trade_days") or 0
            ),
            "target_trade_days": int(campaign.get("target_trade_days") or 0),
            "campaign_status": campaign.get("status"),
            "last_signal_trade_date": (
                str(campaign.get("last_signal_trade_date"))[:10]
                if campaign.get("last_signal_trade_date")
                else None
            ),
            "selection_result_written": False,
        }
