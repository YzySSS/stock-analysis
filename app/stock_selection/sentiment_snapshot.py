from __future__ import annotations

import hashlib
import json
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence

from app.shared.cache import CacheBackend, get_cache_backend
from app.shared.db import mysql_conn, mysql_read_conn


ConnectionFactory = Callable[..., AbstractContextManager]

MIN_COVERAGE_RATIO = 0.98
DEFAULT_CRITICAL_FIELDS = ("code", "candidate_state", "source_lineage")
LINEAGE_FIELDS = ("provider", "batch_id", "source_time", "received_at")
LATEST_SNAPSHOT_CACHE_TTL_SECONDS = 86_400
SELECTION_CONTRACT_ENVELOPE_KEY = "_selection_contract"
SELECTION_CONTRACT_FIELDS = (
    "signal_grade",
    "validation_status",
    "score_breakdown",
    "gate_results",
    "evidence_ids",
    "ai_status",
)


class SnapshotNotPublishableError(RuntimeError):
    """Raised when an immutable snapshot is not in a publishable state."""


class SnapshotIntegrityError(RuntimeError):
    """Raised when a ready manifest and its rows no longer form one snapshot."""


@dataclass(frozen=True)
class SnapshotValidation:
    coverage_ratio: float
    critical_completeness_ratio: float
    expected_entity_count: int
    covered_entity_count: int
    candidate_count: int
    errors: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.errors

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "coverage_ratio": self.coverage_ratio,
            "critical_completeness_ratio": self.critical_completeness_ratio,
            "expected_entity_count": self.expected_entity_count,
            "covered_entity_count": self.covered_entity_count,
            "candidate_count": self.candidate_count,
            "errors": list(self.errors),
        }


@dataclass(frozen=True)
class SnapshotStageResult:
    snapshot_id: str
    status: str
    quality_status: str
    validation: SnapshotValidation

    @property
    def publishable(self) -> bool:
        return self.status == "building" and self.quality_status == "pending"


@dataclass(frozen=True)
class PublishedSentimentSnapshot:
    manifest: Mapping[str, Any]
    candidates: tuple[Mapping[str, Any], ...]

    @property
    def snapshot_id(self) -> str:
        return str(self.manifest["snapshot_id"])

    def as_dict(self) -> dict[str, Any]:
        return {
            "manifest": dict(self.manifest),
            "candidates": [dict(candidate) for candidate in self.candidates],
        }


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


def _as_datetime(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    elif value is not None and str(value).strip():
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO-8601 datetime") from exc
    else:
        raise ValueError(f"{field_name} is required")

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _as_date(value: Any, *, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 date") from exc


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


def _candidate_explain_payload(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Persist the selection contract without requiring another table column.

    ``explain_json`` already belongs to the immutable candidate row.  Keep the
    transport-only fields in a namespaced envelope and lift them back to the
    candidate root on reads so API callers see the normal selection contract.
    """

    raw_explain = candidate.get("explain_json", candidate.get("explain", {}))
    decoded_explain = _json_value(raw_explain, {})
    explain = dict(decoded_explain) if isinstance(decoded_explain, Mapping) else {}
    contract = {
        field: candidate.get(field)
        for field in SELECTION_CONTRACT_FIELDS
        if field in candidate
    }
    if contract:
        explain[SELECTION_CONTRACT_ENVELOPE_KEY] = contract
    return explain


def _lineage_items(candidate: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    lineage = candidate.get("source_lineage")
    if lineage is None:
        lineage = candidate.get("source_lineage_json")
    lineage = _json_value(lineage, lineage)
    if isinstance(lineage, Mapping):
        return [lineage]
    if isinstance(lineage, (list, tuple)):
        return [item for item in lineage if isinstance(item, Mapping)]
    return []


def _is_present(value: Any) -> bool:
    return value is not None and (not isinstance(value, str) or bool(value.strip()))


def validate_sentiment_snapshot(
    candidates: Sequence[Mapping[str, Any]],
    *,
    expected_entity_count: int,
    covered_entity_count: int,
    decision_as_of: Any,
    source_manifest_ids: Sequence[Any],
    critical_fields: Sequence[str] = DEFAULT_CRITICAL_FIELDS,
    minimum_coverage_ratio: float = MIN_COVERAGE_RATIO,
) -> SnapshotValidation:
    """Validate a candidate batch without consulting mutable external state."""

    decision_time = _as_datetime(decision_as_of, field_name="decision_as_of")
    expected = int(expected_entity_count)
    covered = int(covered_entity_count)
    errors: list[str] = []

    if expected <= 0:
        errors.append("expected_entity_count must be greater than zero")
        coverage_ratio = 0.0
    else:
        coverage_ratio = min(1.0, max(0.0, covered / expected))
        if coverage_ratio < float(minimum_coverage_ratio):
            errors.append(
                f"coverage_ratio {coverage_ratio:.8f} is below {float(minimum_coverage_ratio):.8f}"
            )
    if covered < 0:
        errors.append("covered_entity_count must not be negative")
    if not source_manifest_ids:
        errors.append("source_manifest_ids must not be empty")

    complete_rows = 0
    seen_codes: set[str] = set()
    for index, candidate in enumerate(candidates):
        code = str(candidate.get("code") or "").strip()
        row_label = code or f"row[{index}]"
        row_complete = True

        if code:
            if code in seen_codes:
                errors.append(f"duplicate candidate code: {code}")
                row_complete = False
            seen_codes.add(code)

        lineage_items = _lineage_items(candidate)
        for field in critical_fields:
            if field == "source_lineage":
                if not lineage_items:
                    errors.append(f"{row_label}: source_lineage is required")
                    row_complete = False
                continue
            if not _is_present(candidate.get(field)):
                errors.append(f"{row_label}: {field} is required")
                row_complete = False

        for lineage_index, lineage in enumerate(lineage_items):
            lineage_label = f"{row_label}.source_lineage[{lineage_index}]"
            for field in LINEAGE_FIELDS:
                if not _is_present(lineage.get(field)):
                    errors.append(f"{lineage_label}: {field} is required")
                    row_complete = False
            if _is_present(lineage.get("source_time")):
                try:
                    source_time = _as_datetime(
                        lineage["source_time"],
                        field_name=f"{lineage_label}.source_time",
                    )
                    if source_time > decision_time:
                        errors.append(
                            f"{lineage_label}: source_time is after decision_as_of"
                        )
                        row_complete = False
                except ValueError as exc:
                    errors.append(str(exc))
                    row_complete = False
            if _is_present(lineage.get("received_at")):
                try:
                    _as_datetime(
                        lineage["received_at"],
                        field_name=f"{lineage_label}.received_at",
                    )
                except ValueError as exc:
                    errors.append(str(exc))
                    row_complete = False

        if row_complete:
            complete_rows += 1

    candidate_count = len(candidates)
    completeness_ratio = 1.0 if candidate_count == 0 else complete_rows / candidate_count
    if completeness_ratio < 1.0:
        errors.append(
            f"critical_completeness_ratio {completeness_ratio:.8f} is below 1.00000000"
        )

    # Preserve order for diagnostics while avoiding repeated messages.
    unique_errors = tuple(dict.fromkeys(errors))
    return SnapshotValidation(
        coverage_ratio=round(coverage_ratio, 8),
        critical_completeness_ratio=round(completeness_ratio, 8),
        expected_entity_count=expected,
        covered_entity_count=covered,
        candidate_count=candidate_count,
        errors=unique_errors,
    )


class SentimentCandidateSnapshotRepository:
    """Immutable staging, publication and consistent reads for sentiment snapshots."""

    def __init__(
        self,
        connection_factory: ConnectionFactory | None = None,
        cache_backend: CacheBackend | None = None,
        read_connection_factory: ConnectionFactory | None = None,
    ) -> None:
        self._connection_factory = connection_factory or mysql_conn
        self._read_connection_factory = (
            read_connection_factory or connection_factory or mysql_read_conn
        )
        self._cache_backend = cache_backend

    def _connect(self, *, dict_cursor: bool = True):
        return self._connection_factory(dict_cursor=dict_cursor)

    def _read_connect(self, *, dict_cursor: bool = True):
        return self._read_connection_factory(dict_cursor=dict_cursor)

    def stage_snapshot(
        self,
        *,
        snapshot_id: str,
        strategy_id: str,
        strategy_version: str,
        trade_date: Any,
        decision_as_of: Any,
        candidates: Sequence[Mapping[str, Any]],
        source_manifest_ids: Sequence[Any],
        expected_entity_count: int,
        covered_entity_count: int,
        strategy_config_hash: str,
        source_batch_set_hash: str | None = None,
        implementation_hash: str | None = None,
        news_event_set_hash: str | None = None,
        generated_at: Any | None = None,
        freshness_seconds: int | None = None,
        ai_mode: str = "local_core",
        prompt_version: str | None = None,
        model_version: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        critical_fields: Sequence[str] = DEFAULT_CRITICAL_FIELDS,
    ) -> SnapshotStageResult:
        snapshot_id = str(snapshot_id).strip()
        if not snapshot_id:
            raise ValueError("snapshot_id is required")
        if not str(strategy_id).strip() or not str(strategy_version).strip():
            raise ValueError("strategy_id and strategy_version are required")
        if not str(strategy_config_hash).strip():
            raise ValueError("strategy_config_hash is required")

        candidate_rows = [dict(candidate) for candidate in candidates]
        decision_time = _as_datetime(decision_as_of, field_name="decision_as_of")
        generated_time = _as_datetime(
            generated_at or datetime.now(timezone.utc),
            field_name="generated_at",
        )
        trade_day = _as_date(trade_date, field_name="trade_date")
        source_ids = list(source_manifest_ids)
        validation = validate_sentiment_snapshot(
            candidate_rows,
            expected_entity_count=expected_entity_count,
            covered_entity_count=covered_entity_count,
            decision_as_of=decision_time,
            source_manifest_ids=source_ids,
            critical_fields=critical_fields,
        )
        status = "building" if validation.passed else "rejected"
        quality_status = "pending" if validation.passed else "failed"
        quality_reason = None if validation.passed else "; ".join(validation.errors)[:500]

        manifest_metadata = dict(metadata or {})
        manifest_metadata["snapshot_validation"] = validation.as_dict()
        manifest_metadata["critical_fields"] = list(critical_fields)
        manifest_metadata["expected_entity_count"] = int(expected_entity_count)
        manifest_metadata["covered_entity_count"] = int(covered_entity_count)

        selected_count = sum(bool(item.get("is_selected")) for item in candidate_rows)
        tradable_count = sum(bool(item.get("is_tradable")) for item in candidate_rows)
        eligible_count = sum(
            str(item.get("candidate_state") or "eligible") == "eligible"
            for item in candidate_rows
        )
        source_hash = source_batch_set_hash or _sha256(source_ids)

        manifest_sql = """
        INSERT INTO sentiment_candidate_snapshot_manifest (
            snapshot_id, strategy_id, strategy_version, trade_date, decision_as_of,
            status, quality_status, quality_reason,
            source_manifest_ids_json, source_batch_set_hash, strategy_config_hash,
            implementation_hash, news_event_set_hash,
            candidate_count, eligible_count, selected_count, tradable_count,
            coverage_ratio, freshness_seconds, ai_mode, prompt_version, model_version,
            generated_at, metadata_json
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        """
        row_sql = """
        INSERT INTO sentiment_candidate_snapshot (
            snapshot_id, code, name, candidate_state, eligibility_reason,
            is_selected, is_tradable, rank_no, score, trade_grade_state,
            opinion_sector_type, opinion_sector_name, opinion_match_type,
            market_opinion_snapshot_id, selected_price, selected_price_source,
            selected_price_quote_time, factor_json, explain_json, trade_plan_json,
            source_lineage_json, row_hash
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
        """
        insertable_rows: list[tuple[Any, ...]] = []
        inserted_codes: set[str] = set()
        for item in candidate_rows:
            code = str(item.get("code") or "").strip()
            # A rejected diagnostic batch may contain malformed or duplicate rows.
            # Keep the immutable manifest and every uniquely addressable row without
            # violating the (snapshot_id, code) storage constraint.
            if not code or code in inserted_codes:
                continue
            inserted_codes.add(code)
            lineage = item.get("source_lineage", item.get("source_lineage_json"))
            normalized_for_hash = dict(item)
            normalized_for_hash["source_lineage"] = _json_value(lineage, lineage)
            row_hash = str(item.get("row_hash") or _sha256(normalized_for_hash))
            insertable_rows.append(
                (
                    snapshot_id,
                    code,
                    item.get("name"),
                    item.get("candidate_state") or "eligible",
                    item.get("eligibility_reason"),
                    1 if item.get("is_selected") else 0,
                    1 if item.get("is_tradable") else 0,
                    item.get("rank_no"),
                    item.get("score"),
                    item.get("trade_grade_state"),
                    item.get("opinion_sector_type"),
                    item.get("opinion_sector_name"),
                    item.get("opinion_match_type"),
                    item.get("market_opinion_snapshot_id"),
                    item.get("selected_price"),
                    item.get("selected_price_source"),
                    item.get("selected_price_quote_time"),
                    _canonical_json(item.get("factor_json", item.get("factors", {}))),
                    _canonical_json(_candidate_explain_payload(item)),
                    _canonical_json(item.get("trade_plan_json", item.get("trade_plan", {}))),
                    _canonical_json(_json_value(lineage, lineage)),
                    row_hash,
                )
            )

        with self._connect(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    manifest_sql,
                    (
                        snapshot_id,
                        str(strategy_id).strip(),
                        str(strategy_version).strip(),
                        trade_day,
                        decision_time,
                        status,
                        quality_status,
                        quality_reason,
                        _canonical_json(source_ids),
                        source_hash,
                        str(strategy_config_hash).strip(),
                        implementation_hash,
                        news_event_set_hash,
                        len(candidate_rows),
                        eligible_count,
                        selected_count,
                        tradable_count,
                        Decimal(str(validation.coverage_ratio)),
                        freshness_seconds,
                        ai_mode,
                        prompt_version,
                        model_version,
                        generated_time,
                        _canonical_json(manifest_metadata),
                    ),
                )
                if insertable_rows:
                    cursor.executemany(row_sql, insertable_rows)

        return SnapshotStageResult(
            snapshot_id=snapshot_id,
            status=status,
            quality_status=quality_status,
            validation=validation,
        )

    def publish_snapshot(self, snapshot_id: str) -> SnapshotStageResult:
        """Atomically promote one staged snapshot after re-validating stored rows."""

        snapshot_id = str(snapshot_id).strip()
        cache_payload: dict[str, Any] | None = None
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM sentiment_candidate_snapshot_manifest
                    WHERE snapshot_id=%s
                    FOR UPDATE
                    """,
                    (snapshot_id,),
                )
                manifest = cursor.fetchone()
                if not manifest:
                    raise SnapshotNotPublishableError(f"snapshot not found: {snapshot_id}")
                if manifest.get("status") == "ready" and manifest.get("quality_status") == "passed":
                    validation = self._validation_from_manifest(manifest)
                    return SnapshotStageResult(snapshot_id, "ready", "passed", validation)
                if manifest.get("status") != "building" or manifest.get("quality_status") != "pending":
                    raise SnapshotNotPublishableError(
                        f"snapshot {snapshot_id} is {manifest.get('status')}/{manifest.get('quality_status')}"
                    )

                cursor.execute(
                    """
                    SELECT *
                    FROM sentiment_candidate_snapshot
                    WHERE snapshot_id=%s
                    ORDER BY code
                    """,
                    (snapshot_id,),
                )
                stored_rows = [self._candidate_for_validation(row) for row in (cursor.fetchall() or [])]
                metadata = _json_value(manifest.get("metadata_json"), {})
                validation = validate_sentiment_snapshot(
                    stored_rows,
                    expected_entity_count=int(metadata.get("expected_entity_count") or 0),
                    covered_entity_count=int(metadata.get("covered_entity_count") or 0),
                    decision_as_of=manifest.get("decision_as_of"),
                    source_manifest_ids=_json_value(manifest.get("source_manifest_ids_json"), []),
                    critical_fields=tuple(metadata.get("critical_fields") or DEFAULT_CRITICAL_FIELDS),
                )
                if not validation.passed:
                    cursor.execute(
                        """
                        UPDATE sentiment_candidate_snapshot_manifest
                        SET status='rejected', quality_status='failed', quality_reason=%s,
                            coverage_ratio=%s
                        WHERE snapshot_id=%s AND status='building' AND quality_status='pending'
                        """,
                        (
                            "; ".join(validation.errors)[:500],
                            Decimal(str(validation.coverage_ratio)),
                            snapshot_id,
                        ),
                    )
                    return SnapshotStageResult(snapshot_id, "rejected", "failed", validation)

                cursor.execute(
                    """
                    SELECT snapshot_id
                    FROM sentiment_candidate_snapshot_manifest
                    WHERE strategy_id=%s
                      AND status='ready'
                      AND quality_status='passed'
                      AND published_at IS NOT NULL
                      AND snapshot_id<>%s
                    ORDER BY decision_as_of DESC, published_at DESC, id DESC
                    LIMIT 1
                    """,
                    (manifest.get("strategy_id"), snapshot_id),
                )
                previous = cursor.fetchone() or {}
                published_at = datetime.now(timezone.utc).replace(tzinfo=None)
                cursor.execute(
                    """
                    UPDATE sentiment_candidate_snapshot_manifest
                    SET status='ready', quality_status='passed', quality_reason=NULL,
                        coverage_ratio=%s, published_at=%s, supersedes_snapshot_id=%s
                    WHERE snapshot_id=%s AND status='building' AND quality_status='pending'
                    """,
                    (
                        Decimal(str(validation.coverage_ratio)),
                        published_at,
                        previous.get("snapshot_id"),
                        snapshot_id,
                    ),
                )
                if getattr(cursor, "rowcount", 1) != 1:
                    raise SnapshotNotPublishableError(
                        f"snapshot {snapshot_id} changed before publication"
                    )

                cache_payload = {
                    "snapshot_id": snapshot_id,
                    "strategy_id": manifest.get("strategy_id"),
                    "strategy_version": manifest.get("strategy_version"),
                    "decision_as_of": manifest.get("decision_as_of"),
                    "published_at": published_at,
                    "freshness_seconds": manifest.get("freshness_seconds"),
                    "freshness_status": metadata.get("freshness_status"),
                    "coverage_ratio": validation.coverage_ratio,
                }

        if cache_payload is not None:
            self._write_latest_pointer(str(cache_payload["strategy_id"]), cache_payload)
        return SnapshotStageResult(snapshot_id, "ready", "passed", validation)

    def stage_and_publish(self, **kwargs: Any) -> SnapshotStageResult:
        staged = self.stage_snapshot(**kwargs)
        if not staged.publishable:
            return staged
        return self.publish_snapshot(staged.snapshot_id)

    def latest_complete_manifest(
        self,
        *,
        snapshot_id: str | None = None,
        strategy_id: str | None = None,
        strategy_version: str | None = None,
        trade_date: Any | None = None,
        decision_as_of: Any | None = None,
    ) -> Mapping[str, Any] | None:
        """Read the latest ready manifest without loading candidate rows.

        This is the cross-process source of truth when a process-local cache
        has no pointer yet (for example after an API or worker restart).
        """

        conditions = [
            "status='ready'",
            "quality_status='passed'",
            "published_at IS NOT NULL",
        ]
        params: list[Any] = []
        if snapshot_id:
            conditions.append("snapshot_id=%s")
            params.append(str(snapshot_id).strip())
        if strategy_id:
            conditions.append("strategy_id=%s")
            params.append(strategy_id)
        if strategy_version:
            conditions.append("strategy_version=%s")
            params.append(strategy_version)
        if trade_date is not None:
            conditions.append("trade_date=%s")
            params.append(_as_date(trade_date, field_name="trade_date"))
        if decision_as_of is not None:
            conditions.append("decision_as_of<=%s")
            params.append(_as_datetime(decision_as_of, field_name="decision_as_of"))

        manifest_sql = f"""
        SELECT *
        FROM sentiment_candidate_snapshot_manifest
        WHERE {' AND '.join(conditions)}
        ORDER BY decision_as_of DESC, published_at DESC, id DESC
        LIMIT 1
        """
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(manifest_sql, params)
                raw_manifest = cursor.fetchone()
        return self._decode_manifest(raw_manifest) if raw_manifest else None

    def latest_complete_snapshot(
        self,
        *,
        snapshot_id: str | None = None,
        strategy_id: str | None = None,
        strategy_version: str | None = None,
        trade_date: Any | None = None,
        decision_as_of: Any | None = None,
    ) -> PublishedSentimentSnapshot | None:
        """Read one published manifest and only rows carrying its exact ID."""

        conditions = [
            "status='ready'",
            "quality_status='passed'",
            "published_at IS NOT NULL",
        ]
        params: list[Any] = []
        if snapshot_id:
            conditions.append("snapshot_id=%s")
            params.append(str(snapshot_id).strip())
        if strategy_id:
            conditions.append("strategy_id=%s")
            params.append(strategy_id)
        if strategy_version:
            conditions.append("strategy_version=%s")
            params.append(strategy_version)
        if trade_date is not None:
            conditions.append("trade_date=%s")
            params.append(_as_date(trade_date, field_name="trade_date"))
        if decision_as_of is not None:
            conditions.append("decision_as_of<=%s")
            params.append(_as_datetime(decision_as_of, field_name="decision_as_of"))

        manifest_sql = f"""
        SELECT *
        FROM sentiment_candidate_snapshot_manifest
        WHERE {' AND '.join(conditions)}
        ORDER BY decision_as_of DESC, published_at DESC, id DESC
        LIMIT 1
        """
        with self._read_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(manifest_sql, params)
                raw_manifest = cursor.fetchone()
                if not raw_manifest:
                    return None
                snapshot_id = str(raw_manifest["snapshot_id"])
                cursor.execute(
                    """
                    SELECT *
                    FROM sentiment_candidate_snapshot
                    WHERE snapshot_id=%s
                    ORDER BY is_selected DESC, rank_no IS NULL, rank_no, score DESC, code
                    """,
                    (snapshot_id,),
                )
                raw_candidates = cursor.fetchall() or []

        expected_rows = int(raw_manifest.get("candidate_count") or 0)
        if len(raw_candidates) != expected_rows:
            raise SnapshotIntegrityError(
                f"snapshot {snapshot_id} expected {expected_rows} rows but read {len(raw_candidates)}"
            )
        if any(str(row.get("snapshot_id")) != snapshot_id for row in raw_candidates):
            raise SnapshotIntegrityError(f"snapshot {snapshot_id} contains mixed row identifiers")

        manifest = self._decode_manifest(raw_manifest)
        candidates = tuple(self._decode_candidate(row) for row in raw_candidates)
        return PublishedSentimentSnapshot(manifest=manifest, candidates=candidates)

    @staticmethod
    def latest_snapshot_cache_key(strategy_id: str) -> str:
        normalized = str(strategy_id).strip()
        if not normalized:
            raise ValueError("strategy_id is required")
        return f"sentiment:snapshot:latest:{normalized}"

    def latest_snapshot_pointer(self, strategy_id: str) -> Mapping[str, Any] | None:
        """Return the lightweight cache pointer without consulting MySQL."""

        try:
            value = self._cache().get(self.latest_snapshot_cache_key(strategy_id))
        except Exception:
            return None
        return value if isinstance(value, Mapping) else None

    def _cache(self) -> CacheBackend:
        return self._cache_backend or get_cache_backend()

    def _write_latest_pointer(self, strategy_id: str, payload: Mapping[str, Any]) -> None:
        # Cache publication is deliberately outside the MySQL transaction and is
        # best-effort. MySQL remains the source of truth if Redis/memory is down.
        try:
            self._cache().set(
                self.latest_snapshot_cache_key(strategy_id),
                dict(payload),
                ttl_seconds=LATEST_SNAPSHOT_CACHE_TTL_SECONDS,
            )
        except Exception:
            return

    @staticmethod
    def _candidate_for_validation(row: Mapping[str, Any]) -> dict[str, Any]:
        candidate = dict(row)
        candidate["source_lineage"] = _json_value(row.get("source_lineage_json"), [])
        return candidate

    @staticmethod
    def _validation_from_manifest(manifest: Mapping[str, Any]) -> SnapshotValidation:
        metadata = _json_value(manifest.get("metadata_json"), {})
        stored = metadata.get("snapshot_validation") or {}
        return SnapshotValidation(
            coverage_ratio=float(stored.get("coverage_ratio") or manifest.get("coverage_ratio") or 0),
            critical_completeness_ratio=float(stored.get("critical_completeness_ratio") or 0),
            expected_entity_count=int(stored.get("expected_entity_count") or 0),
            covered_entity_count=int(stored.get("covered_entity_count") or 0),
            candidate_count=int(stored.get("candidate_count") or manifest.get("candidate_count") or 0),
            errors=tuple(stored.get("errors") or ()),
        )

    @staticmethod
    def _decode_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
        decoded = dict(manifest)
        for key, fallback in (
            ("source_manifest_ids_json", []),
            ("metadata_json", {}),
        ):
            decoded[key] = _json_value(decoded.get(key), fallback)
        metadata = decoded.get("metadata_json")
        if isinstance(metadata, Mapping):
            for key in (
                "freshness_status",
                "decision_data_version",
                "decision_clock_mode",
            ):
                if key in metadata:
                    decoded[key] = metadata[key]
        return decoded

    @staticmethod
    def _decode_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
        decoded = dict(candidate)
        for key, fallback in (
            ("factor_json", {}),
            ("explain_json", {}),
            ("trade_plan_json", {}),
            ("source_lineage_json", []),
        ):
            decoded[key] = _json_value(decoded.get(key), fallback)
        explain = decoded.get("explain_json")
        if isinstance(explain, Mapping):
            public_explain = dict(explain)
            contract = _json_value(
                public_explain.pop(SELECTION_CONTRACT_ENVELOPE_KEY, None),
                {},
            )
            decoded["explain_json"] = public_explain
            if isinstance(contract, Mapping):
                for field in SELECTION_CONTRACT_FIELDS:
                    if field in contract:
                        decoded[field] = contract[field]
        decoded["source_lineage"] = decoded["source_lineage_json"]
        return decoded
