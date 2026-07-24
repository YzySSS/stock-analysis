from __future__ import annotations

import hashlib
import inspect
import json
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Callable, Iterator, Mapping, Sequence

from app.shared.db import mysql_conn
from app.stock_selection.factor_evaluation_v2 import (
    StrategyFactorEvaluationRepository,
)
from app.stock_selection.dataset_scope import (
    filter_rows_to_code_prefixes,
    required_dataset_code_prefixes,
    sql_code_prefix_filter,
)
from app.stock_selection.repository import SelectionRepository
from app.stock_selection.selector import SENTIMENT_STRATEGY_IDS, StockSelector
from app.stock_selection.sentiment_snapshot import (
    MIN_COVERAGE_RATIO,
    SentimentCandidateSnapshotRepository,
    SnapshotStageResult,
)
from app.stock_selection.turtle_trade_plan import attach_turtle_research_shadow
from app.strategies.service import StrategyService


ConnectionFactory = Callable[..., AbstractContextManager]
SelectorFactory = Callable[[str, Mapping[str, Any], SelectionRepository], StockSelector]

# Persisted in source_batch_manifest.schema_version (VARCHAR(32)). Keep this
# identifier compact and stable so a metadata-only version string cannot abort
# an otherwise valid candidate snapshot transaction.
MATERIALIZER_VERSION = "sentiment-snapshot-mat-v1"
MATERIALIZATION_LOCK_PREFIX = "sentiment_snapshot_materialize"
SUPPORTED_STOCK_DATASETS = {
    "daily_kline",
    "factor_input_daily",
    "stock_moneyflow_daily",
    "stock_chip_daily",
    "stock_technical_feature_daily",
    "stock_realtime_snapshot",
}
OPTIONAL_SCORING_DATASETS = (
    "stock_realtime_moneyflow_snapshot",
    "stock_popularity_snapshot",
    "stock_sentiment_daily",
    "market_context_daily",
    "market_sector_fund_flow_snapshot",
    "stock_instrument_lifecycle",
    "stock_name_history",
    "stock_status_snapshot",
)
STATIC_OR_DERIVED_DATASETS = frozenset({"stock_basic", "sentiment_selection_input"})
DEFAULT_MAXIMUM_DATA_AGE_DAYS = 1.0


class SentimentSnapshotMaterializationError(RuntimeError):
    """Base class for failures that must leave the previous snapshot active."""


class SentimentSnapshotInputQualityError(SentimentSnapshotMaterializationError):
    """Raised when exact MySQL input coverage is below the publication gate."""

    def __init__(self, audit: "MaterializationInputAudit") -> None:
        self.audit = audit
        super().__init__("; ".join(audit.quality_errors) or "sentiment input audit failed")


class SentimentSnapshotConcurrentRunError(SentimentSnapshotMaterializationError):
    """Raised when another materializer owns the MySQL named lock."""


@dataclass
class SourceDatasetBatch:
    dataset_name: str
    batch_id: str
    source_time: datetime | None
    received_at: datetime | None
    actual_rows: int
    expected_entities: int
    actual_entities: int
    coverage_ratio: float
    required: bool = True
    manifest_id: int | None = None
    payload_hash: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def provider(self) -> str:
        value = self.metadata.get("providers") or self.metadata.get("provider") or "mysql"
        if isinstance(value, (list, tuple, set)):
            normalized = sorted({str(item).strip() for item in value if str(item).strip()})
            return ",".join(normalized) or "mysql"
        return str(value).strip() or "mysql"

    @property
    def lineage_ready(self) -> bool:
        return bool(
            self.manifest_id
            and self.batch_id
            and self.source_time is not None
            and self.received_at is not None
        )

    def lineage(self) -> dict[str, Any]:
        if not self.lineage_ready:
            raise ValueError(f"dataset {self.dataset_name} has incomplete lineage")
        lineage = {
            "provider": self.provider,
            "dataset_name": self.dataset_name,
            "batch_id": self.batch_id,
            "source_time": self.source_time,
            "received_at": self.received_at,
            "source_manifest_id": self.manifest_id,
            "required": self.required,
            "coverage_ratio": self.coverage_ratio,
        }
        provider_batch_ids = self.metadata.get("provider_batch_ids")
        if provider_batch_ids:
            lineage["provider_batch_ids"] = provider_batch_ids
        return lineage

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "provider": self.provider,
            "batch_id": self.batch_id,
            "manifest_id": self.manifest_id,
            "source_time": self.source_time,
            "received_at": self.received_at,
            "actual_rows": self.actual_rows,
            "expected_entities": self.expected_entities,
            "actual_entities": self.actual_entities,
            "coverage_ratio": self.coverage_ratio,
            "required": self.required,
            "payload_hash": self.payload_hash,
            "metadata": dict(self.metadata),
        }


@dataclass
class MaterializationInputAudit:
    strategy_id: str
    strategy_version: str
    decision_as_of: datetime
    reference_trade_date: date | None
    clock_mode: str
    expected_entity_count: int
    covered_entity_count: int
    minimum_coverage_ratio: float
    datasets: list[SourceDatasetBatch]
    quality_errors: list[str] = field(default_factory=list)

    @property
    def coverage_ratio(self) -> float:
        if self.expected_entity_count <= 0:
            return 0.0
        return round(
            min(1.0, max(0.0, self.covered_entity_count / self.expected_entity_count)),
            8,
        )

    @property
    def passed(self) -> bool:
        return not self.quality_errors

    @property
    def source_manifest_ids(self) -> list[int]:
        return [int(item.manifest_id) for item in self.datasets if item.manifest_id]

    @property
    def source_lineage(self) -> list[dict[str, Any]]:
        return [item.lineage() for item in self.datasets if item.lineage_ready]

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "decision_as_of": self.decision_as_of,
            "reference_trade_date": self.reference_trade_date,
            "clock_mode": self.clock_mode,
            "expected_entity_count": self.expected_entity_count,
            "covered_entity_count": self.covered_entity_count,
            "coverage_ratio": self.coverage_ratio,
            "minimum_coverage_ratio": self.minimum_coverage_ratio,
            "passed": self.passed,
            "quality_errors": list(self.quality_errors),
            "datasets": [item.as_dict() for item in self.datasets],
        }


@dataclass
class PreparedSentimentInputs:
    audit: MaterializationInputAudit
    selection_repository: SelectionRepository
    _register_read_view: Callable[[Sequence[Mapping[str, Any]]], SourceDatasetBatch]
    _commit: Callable[[], None]
    read_connection_factory: ConnectionFactory | None = None
    committed: bool = False

    def register_read_view(
        self, candidates: Sequence[Mapping[str, Any]]
    ) -> SourceDatasetBatch:
        if self.committed:
            raise RuntimeError("input transaction is already committed")
        batch = self._register_read_view(candidates)
        self.audit.datasets.append(batch)
        return batch

    def commit(self) -> None:
        if not self.committed:
            self._commit()
            self.committed = True


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


def _discard_connection(conn: Any) -> None:
    """Make a connection that may still own a named lock non-reusable.

    SQLAlchemy's pooled connection exposes ``invalidate``.  A direct PyMySQL
    connection does not, so closing it is the physical-discard fallback.  If a
    pool invalidation itself fails, close the wrapped DBAPI connection directly
    before the pool wrapper is returned by its outer context manager.
    """

    invalidate = getattr(conn, "invalidate", None)
    if callable(invalidate):
        try:
            invalidate()
            return
        except Exception:
            wrapped = getattr(conn, "_connection", None)
            dbapi_connection = getattr(wrapped, "dbapi_connection", None)
            if dbapi_connection is not None:
                dbapi_connection.close()
                return
            raise

    close = getattr(conn, "close", None)
    if not callable(close):
        raise RuntimeError("connection cannot be invalidated or physically closed")
    close()


def _as_naive_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    return datetime.fromisoformat(str(value).replace("T", " ").replace("Z", "+00:00")).replace(
        tzinfo=None
    )


def _selector_factory(
    strategy_id: str,
    overrides: Mapping[str, Any],
    repository: SelectionRepository,
) -> StockSelector:
    return StockSelector(
        strategy_id=strategy_id,
        strategy_overrides=dict(overrides),
        repository=repository,
    )


class MySQLSentimentSnapshotInputRepository:
    """Audit and freeze all formal strategy inputs in one MySQL snapshot.

    The selector borrows the same repeatable-read connection used for the
    coverage audit. No network provider is reachable from this component.
    """

    _DATED_DATASETS: dict[str, tuple[str, str | None]] = {
        "daily_kline": ("updated_at", "source"),
        "factor_input_daily": ("updated_at", "source"),
        "stock_moneyflow_daily": ("updated_at", "source"),
        "stock_chip_daily": ("updated_at", "source"),
        "stock_technical_feature_daily": ("computed_at", None),
    }

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    @contextmanager
    def open_consistent_inputs(
        self,
        *,
        strategy_meta: Mapping[str, Any],
        minimum_coverage_ratio: float,
    ) -> Iterator[PreparedSentimentInputs]:
        strategy_id = str(strategy_meta.get("id") or "").strip()
        # A dual run and either single-strategy run must never overlap; all of
        # them publish into the same latest-snapshot family.
        lock_name = f"{MATERIALIZATION_LOCK_PREFIX}:all"[:64]
        with self._connection_factory(dict_cursor=True) as conn:
            lock_acquired = False
            prepared: PreparedSentimentInputs | None = None
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock_name,))
                    lock_acquired = int((cursor.fetchone() or {}).get("acquired") or 0) == 1
                if not lock_acquired:
                    raise SentimentSnapshotConcurrentRunError(
                        f"materialization already running for {strategy_id}"
                    )

                # GET_LOCK is connection-scoped. End any implicit transaction,
                # then freeze all subsequent source reads at one InnoDB snapshot.
                conn.commit()
                with conn.cursor() as cursor:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                    cursor.execute("SELECT NOW(6) AS decision_as_of")
                    decision_as_of = _as_naive_datetime(
                        (cursor.fetchone() or {}).get("decision_as_of")
                    )
                    audit = self._build_audit(
                        cursor,
                        strategy_meta=strategy_meta,
                        decision_as_of=decision_as_of,
                        minimum_coverage_ratio=minimum_coverage_ratio,
                    )
                    for dataset in audit.datasets:
                        self._register_source_manifest(cursor, audit, dataset)

                @contextmanager
                def borrowed_connection(*, dict_cursor: bool = True):
                    if not dict_cursor:
                        raise ValueError("selection snapshot requires a dictionary cursor")
                    yield conn

                def register_read_view(
                    candidates: Sequence[Mapping[str, Any]],
                ) -> SourceDatasetBatch:
                    return self._register_selection_read_view(conn, audit, candidates)

                prepared = PreparedSentimentInputs(
                    audit=audit,
                    selection_repository=SelectionRepository(
                        connection_factory=borrowed_connection
                    ),
                    _register_read_view=register_read_view,
                    _commit=conn.commit,
                    read_connection_factory=borrowed_connection,
                )
                yield prepared
                prepared.commit()
            except BaseException:
                if prepared is None or not prepared.committed:
                    conn.rollback()
                raise
            finally:
                if lock_acquired:
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "SELECT RELEASE_LOCK(%s) AS released",
                                (lock_name,),
                            )
                            released = int(
                                (cursor.fetchone() or {}).get("released") or 0
                            )
                            if released != 1:
                                raise RuntimeError(
                                    f"failed to release MySQL materialization lock: {lock_name}"
                                )
                        conn.commit()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        _discard_connection(conn)
                        raise

    def _build_audit(
        self,
        cursor: Any,
        *,
        strategy_meta: Mapping[str, Any],
        decision_as_of: datetime,
        minimum_coverage_ratio: float,
    ) -> MaterializationInputAudit:
        strategy_id = str(strategy_meta.get("id") or "")
        strategy_version = str(strategy_meta.get("version") or "")
        capability = strategy_meta.get("capability") or {}
        declared = [
            str(value).strip()
            for value in capability.get("required_datasets", [])
            if str(value).strip()
        ]
        required_datasets = list(dict.fromkeys([*declared, "stock_technical_feature_daily"]))
        clock_mode = StockSelector._selection_clock_mode(decision_as_of)
        if clock_mode == "intraday":
            required_datasets.append("stock_realtime_snapshot")
        eligible_code_prefixes = required_dataset_code_prefixes(required_datasets)
        universe_filter_sql = sql_code_prefix_filter("sb.code", eligible_code_prefixes)

        unknown = set(required_datasets) - SUPPORTED_STOCK_DATASETS - {"sector_opinion_daily"}
        if unknown:
            raise SentimentSnapshotMaterializationError(
                f"unsupported required datasets: {', '.join(sorted(unknown))}"
            )

        date_operator = "<=" if decision_as_of.time() >= time(15, 5) else "<"
        cursor.execute(
            f"SELECT MAX(trade_date) AS trade_date FROM daily_kline "
            f"WHERE trade_date {date_operator} %s",
            (decision_as_of.date(),),
        )
        reference_trade_date = (cursor.fetchone() or {}).get("trade_date")
        if isinstance(reference_trade_date, datetime):
            reference_trade_date = reference_trade_date.date()

        cursor.execute(
            """
            SELECT COUNT(*) AS expected_entities, MAX(updated_at) AS received_at
            FROM stock_basic
            WHERE instrument_type='stock' AND COALESCE(is_delisted, 0)=0
            """
        )
        all_active_universe = cursor.fetchone() or {}
        if eligible_code_prefixes:
            cursor.execute(
                f"""
                SELECT COUNT(*) AS expected_entities, MAX(updated_at) AS received_at
                FROM stock_basic sb
                WHERE sb.instrument_type='stock' AND COALESCE(sb.is_delisted, 0)=0
                  {universe_filter_sql}
                """
            )
            universe = cursor.fetchone() or {}
        else:
            universe = all_active_universe
        expected = int(universe.get("expected_entities") or 0)
        all_active_expected = int(all_active_universe.get("expected_entities") or 0)
        stock_basic_received = universe.get("received_at")
        datasets: list[SourceDatasetBatch] = [
            self._batch(
                "stock_basic",
                decision_as_of=decision_as_of,
                source_time=stock_basic_received,
                received_at=stock_basic_received,
                actual_rows=expected,
                expected_entities=expected,
                actual_entities=expected,
                required=True,
                metadata={
                    "universe": (
                        "required_dataset_supported_active_stock"
                        if eligible_code_prefixes
                        else "active_stock"
                    ),
                    "is_delisted": 0,
                    "eligible_code_prefixes": list(eligible_code_prefixes),
                    "all_active_entity_count": all_active_expected,
                    "excluded_entity_count": max(0, all_active_expected - expected),
                    "scope_reason": (
                        "hard-required dataset provider support"
                        if eligible_code_prefixes
                        else None
                    ),
                },
            )
        ]

        for dataset_name in required_datasets:
            if dataset_name == "sector_opinion_daily":
                datasets.append(
                    self._probe_sector_opinion(
                        cursor,
                        decision_as_of=decision_as_of,
                        reference_trade_date=reference_trade_date,
                    )
                )
            elif dataset_name == "stock_realtime_snapshot":
                datasets.append(
                    self._probe_realtime(
                        cursor,
                        decision_as_of=decision_as_of,
                        expected_entities=expected,
                        universe_filter_sql=universe_filter_sql,
                    )
                )
            else:
                datasets.append(
                    self._probe_dated_dataset(
                        cursor,
                        dataset_name=dataset_name,
                        decision_as_of=decision_as_of,
                        reference_trade_date=reference_trade_date,
                        expected_entities=expected,
                        universe_filter_sql=universe_filter_sql,
                    )
                )

        # These tables are not hard gates, but they are read by the selector's
        # scoring/risk view.  Freeze and register their exact point-in-time
        # slices so a published score never cites only the declared core tables.
        datasets.extend(
            self._probe_optional_scoring_datasets(
                cursor,
                decision_as_of=decision_as_of,
                reference_trade_date=reference_trade_date,
                expected_entities=all_active_expected,
            )
        )

        covered = self._covered_entity_intersection(
            cursor,
            expected_entities=expected,
            required_datasets=required_datasets,
            reference_trade_date=reference_trade_date,
            decision_as_of=decision_as_of,
            universe_filter_sql=universe_filter_sql,
        )
        errors: list[str] = []
        if expected <= 0:
            errors.append("active stock universe is empty")
        if reference_trade_date is None:
            errors.append("daily_kline has no eligible reference trade date")
        coverage_ratio = 0.0 if expected <= 0 else covered / expected
        if coverage_ratio < minimum_coverage_ratio:
            errors.append(
                f"exact required-dataset intersection coverage {coverage_ratio:.8f} "
                f"is below {minimum_coverage_ratio:.8f}"
            )
        errors.extend(self._required_dataset_quality_errors(datasets, decision_as_of))

        return MaterializationInputAudit(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            decision_as_of=decision_as_of,
            reference_trade_date=reference_trade_date,
            clock_mode=clock_mode,
            expected_entity_count=expected,
            covered_entity_count=covered,
            minimum_coverage_ratio=minimum_coverage_ratio,
            datasets=datasets,
            quality_errors=list(dict.fromkeys(errors)),
        )

    @staticmethod
    def _required_dataset_quality_errors(
        datasets: Sequence[SourceDatasetBatch],
        decision_as_of: datetime,
    ) -> list[str]:
        errors: list[str] = []
        for dataset in datasets:
            if not dataset.required:
                continue
            if dataset.actual_entities <= 0:
                errors.append(f"required dataset {dataset.dataset_name} is empty")
            if dataset.source_time is None:
                errors.append(
                    f"required dataset {dataset.dataset_name} source_time is missing"
                )
            elif dataset.source_time > decision_as_of:
                errors.append(
                    f"required dataset {dataset.dataset_name} source_time is after decision_as_of"
                )
            if dataset.received_at is None:
                errors.append(
                    f"required dataset {dataset.dataset_name} received_at is missing"
                )
            elif dataset.received_at > decision_as_of:
                errors.append(
                    f"required dataset {dataset.dataset_name} received_at is after decision_as_of"
                )
        return errors

    def _probe_dated_dataset(
        self,
        cursor: Any,
        *,
        dataset_name: str,
        decision_as_of: datetime,
        reference_trade_date: date | None,
        expected_entities: int,
        universe_filter_sql: str = "",
    ) -> SourceDatasetBatch:
        received_column, source_column = self._DATED_DATASETS[dataset_name]
        if reference_trade_date is None:
            return self._batch(
                dataset_name,
                decision_as_of=decision_as_of,
                source_time=None,
                received_at=None,
                actual_rows=0,
                expected_entities=expected_entities,
                actual_entities=0,
                required=True,
            )
        source_projection = (
            f", GROUP_CONCAT(DISTINCT source.{source_column} ORDER BY source.{source_column}) AS providers"
            if source_column
            else ", NULL AS providers"
        )
        extra_filter = (
            " AND source.source_trade_date=%s"
            if dataset_name == "stock_technical_feature_daily"
            else ""
        )
        params: list[Any] = [reference_trade_date]
        if extra_filter:
            params.append(reference_trade_date)
        cursor.execute(
            f"""
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT source.code) AS actual_entities,
                   MAX(source.{received_column}) AS received_at
                   {source_projection}
            FROM {dataset_name} source
            INNER JOIN stock_basic sb ON sb.code=source.code
            WHERE source.trade_date=%s
              {extra_filter}
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
              {universe_filter_sql}
            """,
            params,
        )
        row = cursor.fetchone() or {}
        source_time = datetime.combine(reference_trade_date, time(15, 0))
        return self._batch(
            dataset_name,
            decision_as_of=decision_as_of,
            source_time=source_time,
            received_at=row.get("received_at"),
            actual_rows=int(row.get("actual_rows") or 0),
            expected_entities=expected_entities,
            actual_entities=int(row.get("actual_entities") or 0),
            required=True,
            metadata={
                "trade_date": reference_trade_date,
                "providers": row.get("providers"),
            },
        )

    def _probe_sector_opinion(
        self,
        cursor: Any,
        *,
        decision_as_of: datetime,
        reference_trade_date: date | None,
    ) -> SourceDatasetBatch:
        cursor.execute(
            "SELECT MAX(as_of_datetime) AS source_time FROM sector_opinion_daily "
            "WHERE as_of_datetime<=%s",
            (decision_as_of,),
        )
        source_time = (cursor.fetchone() or {}).get("source_time")
        if source_time is None:
            row: Mapping[str, Any] = {}
        else:
            cursor.execute(
                """
                SELECT COUNT(*) AS actual_rows, MAX(updated_at) AS received_at,
                       SUM(news_count) AS news_count, SUM(source_count) AS source_count
                FROM sector_opinion_daily
                WHERE as_of_datetime=%s
                """,
                (source_time,),
            )
            row = cursor.fetchone() or {}
        actual_rows = int(row.get("actual_rows") or 0)
        return self._batch(
            "sector_opinion_daily",
            decision_as_of=decision_as_of,
            source_time=source_time,
            received_at=row.get("received_at"),
            actual_rows=actual_rows,
            expected_entities=1,
            actual_entities=1 if actual_rows > 0 else 0,
            required=True,
            metadata={
                "trade_date": reference_trade_date,
                "news_count": int(row.get("news_count") or 0),
                "source_count": int(row.get("source_count") or 0),
            },
        )

    def _probe_realtime(
        self,
        cursor: Any,
        *,
        decision_as_of: datetime,
        expected_entities: int,
        universe_filter_sql: str = "",
    ) -> SourceDatasetBatch:
        cursor.execute(
            f"""
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT realtime.code) AS actual_entities,
                   MAX(realtime.quote_time) AS source_time,
                   MAX(realtime.received_at) AS received_at,
                   GROUP_CONCAT(DISTINCT realtime.source ORDER BY realtime.source) AS providers,
                   GROUP_CONCAT(DISTINCT realtime.batch_id ORDER BY realtime.batch_id) AS provider_batch_ids
            FROM stock_realtime_snapshot realtime
            INNER JOIN stock_basic sb ON sb.code=realtime.code
            WHERE realtime.trade_date=%s
              AND realtime.quote_time<=%s
              AND COALESCE(realtime.is_stale, 0)=0
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
              {universe_filter_sql}
            """,
            (decision_as_of.date(), decision_as_of),
        )
        row = cursor.fetchone() or {}
        return self._batch(
            "stock_realtime_snapshot",
            decision_as_of=decision_as_of,
            source_time=row.get("source_time"),
            received_at=row.get("received_at"),
            actual_rows=int(row.get("actual_rows") or 0),
            expected_entities=expected_entities,
            actual_entities=int(row.get("actual_entities") or 0),
            required=True,
            metadata={
                "trade_date": decision_as_of.date(),
                "providers": row.get("providers"),
                "provider_batch_ids": row.get("provider_batch_ids"),
                "stale_rows_excluded": True,
            },
        )

    def _probe_optional_scoring_datasets(
        self,
        cursor: Any,
        *,
        decision_as_of: datetime,
        reference_trade_date: date | None,
        expected_entities: int,
    ) -> list[SourceDatasetBatch]:
        """Audit the optional tables that the selector actually reads.

        Optional coverage never changes the formal 98% core intersection.
        Every query is nevertheless point-in-time bounded so a non-empty batch
        can be cited in candidate lineage without inventing timestamps.
        """

        batches: list[SourceDatasetBatch] = []

        def probe(
            dataset_name: str,
            sql: str,
            params: Sequence[Any],
            *,
            expected: int,
            coverage_basis: str,
            batch_identity: str,
        ) -> None:
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone() or {}
            metadata = {
                "decision_as_of": decision_as_of,
                "reference_trade_date": reference_trade_date,
                "providers": row.get("providers"),
                "provider_batch_ids": row.get("provider_batch_ids"),
                "source_time_min": row.get("source_time_min"),
                "source_trade_date_min": row.get("source_trade_date_min"),
                "source_trade_date_max": row.get("source_trade_date_max"),
                "coverage_basis": coverage_basis,
                "batch_identity": batch_identity,
                "point_in_time_bounded": True,
            }
            batches.append(
                self._batch(
                    dataset_name,
                    decision_as_of=decision_as_of,
                    source_time=row.get("source_time"),
                    received_at=row.get("received_at"),
                    actual_rows=int(row.get("actual_rows") or 0),
                    expected_entities=expected,
                    actual_entities=int(row.get("actual_entities") or 0),
                    required=False,
                    metadata=metadata,
                )
            )

        probe(
            "stock_realtime_moneyflow_snapshot",
            """
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT realtime.code) AS actual_entities,
                   MIN(realtime.quote_time) AS source_time_min,
                   MAX(realtime.quote_time) AS source_time,
                   MAX(realtime.updated_at) AS received_at,
                   MIN(realtime.trade_date) AS source_trade_date_min,
                   MAX(realtime.trade_date) AS source_trade_date_max,
                   GROUP_CONCAT(DISTINCT realtime.source ORDER BY realtime.source) AS providers,
                   GROUP_CONCAT(
                       DISTINCT CONCAT(realtime.source, '@', CAST(realtime.quote_time AS CHAR))
                       ORDER BY realtime.source, realtime.quote_time
                   ) AS provider_batch_ids
            FROM stock_realtime_moneyflow_snapshot realtime
            INNER JOIN stock_basic sb ON sb.code=realtime.code
            WHERE realtime.trade_date = (
                SELECT MAX(trade_date)
                FROM stock_realtime_moneyflow_snapshot
                WHERE quote_time<=%s AND created_at<=%s AND updated_at<=%s
            )
              AND realtime.quote_time<=%s
              AND realtime.created_at<=%s
              AND realtime.updated_at<=%s
              AND realtime.quote_time >= DATE_SUB((
                  SELECT MAX(quote_time)
                  FROM stock_realtime_moneyflow_snapshot
                  WHERE quote_time<=%s AND created_at<=%s AND updated_at<=%s
              ), INTERVAL 20 MINUTE)
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
            """,
            [decision_as_of] * 9,
            expected=expected_entities,
            coverage_basis="active_stock_universe",
            batch_identity="provider+quote_time",
        )
        probe(
            "stock_popularity_snapshot",
            """
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT pop.code) AS actual_entities,
                   MIN(pop.quote_time) AS source_time_min,
                   MAX(pop.quote_time) AS source_time,
                   MAX(pop.updated_at) AS received_at,
                   MIN(pop.trade_date) AS source_trade_date_min,
                   MAX(pop.trade_date) AS source_trade_date_max,
                   GROUP_CONCAT(DISTINCT pop.source ORDER BY pop.source) AS providers,
                   GROUP_CONCAT(
                       DISTINCT CONCAT(pop.source, '@', CAST(pop.quote_time AS CHAR))
                       ORDER BY pop.source, pop.quote_time
                   ) AS provider_batch_ids
            FROM stock_popularity_snapshot pop
            INNER JOIN stock_basic sb ON sb.code=pop.code
            WHERE pop.quote_time<=%s AND pop.created_at<=%s AND pop.updated_at<=%s
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
            """,
            (decision_as_of, decision_as_of, decision_as_of),
            expected=expected_entities,
            coverage_basis="active_stock_universe",
            batch_identity="provider+quote_time",
        )
        probe(
            "stock_sentiment_daily",
            """
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT sentiment.code) AS actual_entities,
                   MIN(TIMESTAMP(sentiment.trade_date, '15:00:00')) AS source_time_min,
                   MAX(TIMESTAMP(sentiment.trade_date, '15:00:00')) AS source_time,
                   MAX(sentiment.updated_at) AS received_at,
                   MIN(sentiment.trade_date) AS source_trade_date_min,
                   MAX(sentiment.trade_date) AS source_trade_date_max,
                   GROUP_CONCAT(DISTINCT sentiment.source ORDER BY sentiment.source) AS providers,
                   GROUP_CONCAT(
                       DISTINCT CONCAT(sentiment.source, '@', CAST(sentiment.trade_date AS CHAR))
                       ORDER BY sentiment.source, sentiment.trade_date
                   ) AS provider_batch_ids
            FROM stock_sentiment_daily sentiment
            INNER JOIN (
                SELECT code, MAX(trade_date) AS trade_date
                FROM stock_sentiment_daily
                WHERE trade_date<=%s AND updated_at<=%s
                GROUP BY code
            ) chosen
              ON chosen.code=sentiment.code AND chosen.trade_date=sentiment.trade_date
            INNER JOIN stock_basic sb ON sb.code=sentiment.code
            WHERE sentiment.updated_at<=%s
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
            """,
            (reference_trade_date, decision_as_of, decision_as_of),
            expected=expected_entities,
            coverage_basis="active_stock_universe",
            batch_identity="provider+trade_date",
        )
        probe(
            "market_context_daily",
            """
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT context.index_code) AS actual_entities,
                   MIN(TIMESTAMP(context.trade_date, '15:00:00')) AS source_time_min,
                   MAX(TIMESTAMP(context.trade_date, '15:00:00')) AS source_time,
                   MAX(context.updated_at) AS received_at,
                   MIN(context.trade_date) AS source_trade_date_min,
                   MAX(context.trade_date) AS source_trade_date_max,
                   GROUP_CONCAT(DISTINCT context.source ORDER BY context.source) AS providers,
                   GROUP_CONCAT(
                       DISTINCT CONCAT(context.source, '@', CAST(context.trade_date AS CHAR))
                       ORDER BY context.source, context.trade_date
                   ) AS provider_batch_ids
            FROM market_context_daily context
            WHERE context.trade_date=%s AND context.updated_at<=%s
              AND context.index_code IN ('000300.SH', '000905.SH', '000852.SH')
            """,
            (reference_trade_date, decision_as_of),
            expected=3,
            coverage_basis="configured_market_indices",
            batch_identity="provider+trade_date",
        )
        probe(
            "market_sector_fund_flow_snapshot",
            """
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT CONCAT(flow.sector_type, ':', flow.sector_name)) AS actual_entities,
                   MIN(flow.quote_time) AS source_time_min,
                   MAX(flow.quote_time) AS source_time,
                   MAX(flow.updated_at) AS received_at,
                   MIN(flow.trade_date) AS source_trade_date_min,
                   MAX(flow.trade_date) AS source_trade_date_max,
                   GROUP_CONCAT(DISTINCT flow.source ORDER BY flow.source) AS providers,
                   GROUP_CONCAT(
                       DISTINCT CONCAT(flow.source, '@', CAST(flow.quote_time AS CHAR))
                       ORDER BY flow.source, flow.quote_time
                   ) AS provider_batch_ids
            FROM market_sector_fund_flow_snapshot flow
            WHERE flow.trade_date = (
                SELECT MAX(trade_date)
                FROM market_sector_fund_flow_snapshot
                WHERE quote_time<=%s AND created_at<=%s AND updated_at<=%s
            )
              AND flow.quote_time<=%s
              AND flow.created_at<=%s
              AND flow.updated_at<=%s
              AND flow.quote_time >= DATE_SUB((
                  SELECT MAX(quote_time)
                  FROM market_sector_fund_flow_snapshot
                  WHERE quote_time<=%s AND created_at<=%s AND updated_at<=%s
              ), INTERVAL 20 MINUTE)
            """,
            [decision_as_of] * 9,
            expected=0,
            coverage_basis="observed_sector_set_no_fixed_denominator",
            batch_identity="provider+quote_time",
        )
        probe(
            "stock_instrument_lifecycle",
            """
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT lifecycle.code) AS actual_entities,
                   MIN(lifecycle.source_updated_at) AS source_time_min,
                   MAX(lifecycle.source_updated_at) AS source_time,
                   MAX(lifecycle.updated_at) AS received_at,
                   NULL AS source_trade_date_min,
                   NULL AS source_trade_date_max,
                   GROUP_CONCAT(DISTINCT lifecycle.source ORDER BY lifecycle.source) AS providers,
                   GROUP_CONCAT(DISTINCT lifecycle.source_sync_id ORDER BY lifecycle.source_sync_id) AS provider_batch_ids
            FROM stock_instrument_lifecycle lifecycle
            INNER JOIN stock_basic sb ON sb.code=lifecycle.code
            WHERE lifecycle.source_updated_at<=%s
              AND lifecycle.created_at<=%s
              AND lifecycle.updated_at<=%s
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
            """,
            (decision_as_of, decision_as_of, decision_as_of),
            expected=expected_entities,
            coverage_basis="active_stock_universe",
            batch_identity="native_source_sync_id",
        )
        probe(
            "stock_name_history",
            """
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT names.code) AS actual_entities,
                   MIN(names.source_updated_at) AS source_time_min,
                   MAX(names.source_updated_at) AS source_time,
                   MAX(names.updated_at) AS received_at,
                   MIN(names.start_date) AS source_trade_date_min,
                   MAX(COALESCE(names.end_date, names.start_date)) AS source_trade_date_max,
                   GROUP_CONCAT(DISTINCT names.source ORDER BY names.source) AS providers,
                   GROUP_CONCAT(DISTINCT names.source_sync_id ORDER BY names.source_sync_id) AS provider_batch_ids
            FROM stock_name_history names
            INNER JOIN stock_basic sb ON sb.code=names.code
            WHERE names.start_date<=%s
              AND (names.end_date IS NULL OR names.end_date>=%s)
              AND names.source_updated_at<=%s
              AND names.created_at<=%s
              AND names.updated_at<=%s
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
            """,
            (
                reference_trade_date,
                reference_trade_date,
                decision_as_of,
                decision_as_of,
                decision_as_of,
            ),
            expected=0,
            coverage_basis="effective_name_intervals_no_fixed_denominator",
            batch_identity="native_source_sync_id",
        )
        probe(
            "stock_status_snapshot",
            """
            SELECT COUNT(*) AS actual_rows,
                   COUNT(DISTINCT status_row.code) AS actual_entities,
                   MIN(status_row.updated_at) AS source_time_min,
                   MAX(status_row.updated_at) AS source_time,
                   MAX(status_row.updated_at) AS received_at,
                   MIN(status_row.trade_date) AS source_trade_date_min,
                   MAX(status_row.trade_date) AS source_trade_date_max,
                   GROUP_CONCAT(DISTINCT status_row.source ORDER BY status_row.source) AS providers,
                   GROUP_CONCAT(
                       DISTINCT CONCAT(status_row.source, '@', CAST(status_row.trade_date AS CHAR))
                       ORDER BY status_row.source, status_row.trade_date
                   ) AS provider_batch_ids
            FROM stock_status_snapshot status_row
            INNER JOIN (
                SELECT code, MAX(trade_date) AS trade_date
                FROM stock_status_snapshot
                WHERE trade_date<=%s AND created_at<=%s AND updated_at<=%s
                GROUP BY code
            ) chosen ON chosen.code=status_row.code AND chosen.trade_date=status_row.trade_date
            INNER JOIN stock_basic sb ON sb.code=status_row.code
            WHERE status_row.created_at<=%s AND status_row.updated_at<=%s
              AND sb.instrument_type='stock'
              AND COALESCE(sb.is_delisted, 0)=0
            """,
            (
                decision_as_of.date(),
                decision_as_of,
                decision_as_of,
                decision_as_of,
                decision_as_of,
            ),
            expected=expected_entities,
            coverage_basis="active_stock_universe",
            batch_identity="provider+trade_date",
        )

        if tuple(item.dataset_name for item in batches) != OPTIONAL_SCORING_DATASETS:
            raise SentimentSnapshotMaterializationError(
                "optional scoring lineage registry is incomplete"
            )
        return batches

    def _covered_entity_intersection(
        self,
        cursor: Any,
        *,
        expected_entities: int,
        required_datasets: Sequence[str],
        reference_trade_date: date | None,
        decision_as_of: datetime,
        universe_filter_sql: str = "",
    ) -> int:
        if expected_entities <= 0 or reference_trade_date is None:
            return 0
        if "sector_opinion_daily" in required_datasets:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM sector_opinion_daily "
                "WHERE as_of_datetime=(SELECT MAX(as_of_datetime) FROM sector_opinion_daily "
                "WHERE as_of_datetime<=%s)",
                (decision_as_of,),
            )
            if int((cursor.fetchone() or {}).get("count") or 0) <= 0:
                return 0

        joins: list[str] = []
        required_aliases: list[str] = []
        params: list[Any] = []
        alias_index = 0
        for dataset_name in required_datasets:
            if dataset_name == "sector_opinion_daily":
                continue
            alias_index += 1
            alias = f"d{alias_index}"
            required_aliases.append(alias)
            if dataset_name == "stock_realtime_snapshot":
                joins.append(
                    f"LEFT JOIN (SELECT DISTINCT code FROM stock_realtime_snapshot "
                    f"WHERE trade_date=%s AND quote_time<=%s AND COALESCE(is_stale, 0)=0) "
                    f"{alias} ON {alias}.code=sb.code"
                )
                params.extend([decision_as_of.date(), decision_as_of])
            elif dataset_name == "stock_technical_feature_daily":
                joins.append(
                    f"LEFT JOIN (SELECT DISTINCT code FROM {dataset_name} "
                    f"WHERE trade_date=%s AND source_trade_date=%s) {alias} "
                    f"ON {alias}.code=sb.code"
                )
                params.extend([reference_trade_date, reference_trade_date])
            else:
                joins.append(
                    f"LEFT JOIN (SELECT DISTINCT code FROM {dataset_name} WHERE trade_date=%s) "
                    f"{alias} ON {alias}.code=sb.code"
                )
                params.append(reference_trade_date)
        condition = " AND ".join(f"{alias}.code IS NOT NULL" for alias in required_aliases)
        cursor.execute(
            f"""
            SELECT SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) AS covered_entities
            FROM stock_basic sb
            {' '.join(joins)}
            WHERE sb.instrument_type='stock' AND COALESCE(sb.is_delisted, 0)=0
              {universe_filter_sql}
            """,
            params,
        )
        return int((cursor.fetchone() or {}).get("covered_entities") or 0)

    def _batch(
        self,
        dataset_name: str,
        *,
        decision_as_of: datetime,
        source_time: Any,
        received_at: Any,
        actual_rows: int,
        expected_entities: int,
        actual_entities: int,
        required: bool,
        metadata: Mapping[str, Any] | None = None,
    ) -> SourceDatasetBatch:
        normalized_source_time = _as_naive_datetime(source_time) if source_time else None
        normalized_received_at = _as_naive_datetime(received_at) if received_at else None
        coverage = (
            0.0
            if expected_entities <= 0
            else round(min(1.0, actual_entities / expected_entities), 8)
        )
        descriptor = {
            "dataset_name": dataset_name,
            "decision_as_of": decision_as_of,
            "source_time": normalized_source_time,
            "received_at": normalized_received_at,
            "actual_rows": actual_rows,
            "expected_entities": expected_entities,
            "actual_entities": actual_entities,
            "coverage_ratio": coverage,
            "metadata": dict(metadata or {}),
        }
        payload_hash = _sha256(descriptor)
        batch_id = f"mysql-{dataset_name}-{payload_hash[:24]}"[:96]
        return SourceDatasetBatch(
            dataset_name=dataset_name,
            batch_id=batch_id,
            source_time=normalized_source_time,
            received_at=normalized_received_at,
            actual_rows=actual_rows,
            expected_entities=expected_entities,
            actual_entities=actual_entities,
            coverage_ratio=coverage,
            required=required,
            payload_hash=payload_hash,
            metadata=dict(metadata or {}),
        )

    def _register_source_manifest(
        self,
        cursor: Any,
        audit: MaterializationInputAudit,
        dataset: SourceDatasetBatch,
    ) -> None:
        lineage_complete = bool(
            dataset.actual_entities > 0
            and dataset.source_time is not None
            and dataset.received_at is not None
        )
        coverage_has_denominator = dataset.expected_entities > 0
        coverage_passed = bool(
            dataset.dataset_name == "sector_opinion_daily"
            or (
                coverage_has_denominator
                and dataset.coverage_ratio >= audit.minimum_coverage_ratio
            )
        )
        dataset_passed = bool(lineage_complete and coverage_passed)
        if dataset.required:
            manifest_status = "published" if dataset_passed else "rejected"
            quality_status = "passed" if dataset_passed else "failed"
            quality_reason = (
                None
                if dataset_passed
                else "required dataset is empty, lacks lineage timestamps, or is below coverage gate"
            )
        elif not lineage_complete:
            manifest_status = "rejected"
            quality_status = "missing"
            quality_reason = "optional scoring dataset is empty or lacks lineage timestamps"
        elif dataset_passed:
            manifest_status = "published"
            quality_status = "passed"
            quality_reason = None
        else:
            manifest_status = "published"
            quality_status = "partial"
            quality_reason = (
                "optional scoring dataset observed; partial coverage does not enter the core gate"
                if coverage_has_denominator
                else "optional scoring dataset observed without a fixed coverage denominator"
            )
        manifest_received_at = dataset.received_at or audit.decision_as_of
        logical_trade_date = (
            dataset.metadata.get("source_trade_date_max")
            or dataset.metadata.get("trade_date")
            or audit.reference_trade_date
        )
        source_event_time_min = (
            dataset.metadata.get("source_time_min") or dataset.source_time
        )
        manifest_metadata = {
            **dataset.metadata,
            "provider": dataset.provider,
            "required": dataset.required,
            "lineage_source_time": dataset.source_time,
            "lineage_received_at": dataset.received_at,
            "manifest_received_at_semantics": (
                "source_received_at"
                if dataset.received_at is not None
                else "audit_observed_at_for_missing_optional_batch"
            ),
        }
        cursor.execute(
            """
            INSERT INTO source_batch_manifest (
                batch_id, source_name, dataset_name, logical_trade_date,
                source_event_time_min, source_event_time_max, received_at, published_at,
                status, quality_status, quality_reason,
                expected_rows, actual_rows, expected_entities, actual_entities,
                stale_rows, rejected_rows, coverage_ratio, schema_version,
                payload_hash, parent_batch_ids_json, metadata_json
            ) VALUES (
                %s, 'mysql_read_model', %s, %s,
                %s, %s, %s, NOW(6),
                %s, %s, %s,
                NULL, %s, %s, %s,
                0, 0, %s, %s,
                %s, NULL, %s
            )
            ON DUPLICATE KEY UPDATE
                id=LAST_INSERT_ID(id),
                quality_status=VALUES(quality_status),
                quality_reason=VALUES(quality_reason),
                actual_rows=VALUES(actual_rows),
                actual_entities=VALUES(actual_entities),
                coverage_ratio=VALUES(coverage_ratio),
                metadata_json=VALUES(metadata_json),
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                dataset.batch_id,
                dataset.dataset_name,
                logical_trade_date,
                source_event_time_min,
                dataset.source_time,
                manifest_received_at,
                manifest_status,
                quality_status,
                quality_reason,
                dataset.actual_rows,
                dataset.expected_entities,
                dataset.actual_entities,
                Decimal(str(dataset.coverage_ratio)),
                MATERIALIZER_VERSION,
                dataset.payload_hash,
                _canonical_json(manifest_metadata),
            ),
        )
        dataset.manifest_id = int(cursor.lastrowid or 0)

    def _register_selection_read_view(
        self,
        conn: Any,
        audit: MaterializationInputAudit,
        candidates: Sequence[Mapping[str, Any]],
    ) -> SourceDatasetBatch:
        normalized_rows = [dict(item) for item in candidates]
        payload_hash = _sha256(normalized_rows)
        source_times = [item.source_time for item in audit.datasets if item.source_time]
        received_times = [item.received_at for item in audit.datasets if item.received_at]
        batch = SourceDatasetBatch(
            dataset_name="sentiment_selection_input",
            batch_id=f"mysql-sentiment-selection-input-{payload_hash[:24]}"[:96],
            source_time=max(source_times) if source_times else None,
            received_at=max(received_times) if received_times else None,
            actual_rows=len(normalized_rows),
            expected_entities=audit.expected_entity_count,
            actual_entities=audit.covered_entity_count,
            coverage_ratio=audit.coverage_ratio,
            required=True,
            payload_hash=payload_hash,
            metadata={
                "parent_manifest_ids": [
                    int(item.manifest_id)
                    for item in audit.datasets
                    if item.manifest_id
                    and item.dataset_name != "sentiment_selection_input"
                ],
                "candidate_row_count": len(normalized_rows),
                "consistent_read": "repeatable_read",
            },
        )
        with conn.cursor() as cursor:
            self._register_source_manifest(cursor, audit, batch)
        return batch


class SentimentSnapshotMaterializationService:
    """Build and atomically publish a deterministic sentiment read model."""

    def __init__(
        self,
        *,
        input_repository: MySQLSentimentSnapshotInputRepository | None = None,
        snapshot_repository: SentimentCandidateSnapshotRepository | None = None,
        strategy_service: StrategyService | None = None,
        selector_factory: SelectorFactory | None = None,
        local_clock_mode: Callable[[], str] | None = None,
        factor_evaluation_repository: StrategyFactorEvaluationRepository | None = None,
    ) -> None:
        self.input_repository = input_repository or MySQLSentimentSnapshotInputRepository()
        self.snapshot_repository = snapshot_repository or SentimentCandidateSnapshotRepository()
        self.strategy_service = strategy_service or StrategyService()
        self.selector_factory = selector_factory or _selector_factory
        self.local_clock_mode = local_clock_mode or StockSelector._selection_clock_mode
        self.factor_evaluation_repository = (
            factor_evaluation_repository or StrategyFactorEvaluationRepository()
        )

    @staticmethod
    def _dynamic_source_batches(
        datasets: Sequence[SourceDatasetBatch],
    ) -> list[SourceDatasetBatch]:
        return [
            item
            for item in datasets
            if item.required
            and item.dataset_name not in STATIC_OR_DERIVED_DATASETS
            and item.source_time is not None
        ]

    @classmethod
    def _freshness_seconds(
        cls,
        audit: MaterializationInputAudit,
        datasets: Sequence[SourceDatasetBatch],
    ) -> int | None:
        dynamic = cls._dynamic_source_batches(datasets)
        if not dynamic:
            return None
        # Freshness is constrained by the oldest required dynamic source.  A
        # newer quote must not hide a stale factor/news/technical input.
        oldest_source_time = min(item.source_time for item in dynamic if item.source_time)
        return max(0, int((audit.decision_as_of - oldest_source_time).total_seconds()))

    @classmethod
    def _input_audit_metadata(
        cls,
        *,
        audit: MaterializationInputAudit,
        strategy_meta: Mapping[str, Any],
        datasets: Sequence[SourceDatasetBatch],
    ) -> dict[str, Any]:
        freshness_seconds = cls._freshness_seconds(audit, datasets)
        capability = strategy_meta.get("capability") or {}
        try:
            maximum_age_days = float(
                capability.get("maximum_data_age_days", DEFAULT_MAXIMUM_DATA_AGE_DAYS)
            )
        except (TypeError, ValueError):
            maximum_age_days = DEFAULT_MAXIMUM_DATA_AGE_DAYS
        if maximum_age_days <= 0:
            maximum_age_days = DEFAULT_MAXIMUM_DATA_AGE_DAYS
        maximum_age_seconds = int(maximum_age_days * 86_400)
        if freshness_seconds is None:
            freshness_status = "unknown"
        elif freshness_seconds <= maximum_age_seconds:
            freshness_status = "fresh"
        else:
            freshness_status = "stale"

        decision_data_version = "sentiment-input-" + _sha256(
            {
                "decision_as_of": audit.decision_as_of,
                "reference_trade_date": audit.reference_trade_date,
                "batch_ids": sorted(
                    item.batch_id
                    for item in datasets
                    if item.required and item.dataset_name not in STATIC_OR_DERIVED_DATASETS
                ),
            }
        )[:24]
        return {
            "freshness_status": freshness_status,
            "freshness_seconds": freshness_seconds,
            "market_coverage_ratio": audit.coverage_ratio,
            "decision_data_version": decision_data_version,
            "decision_clock_mode": audit.clock_mode,
        }

    @classmethod
    def _inject_input_audit_metadata(
        cls,
        candidates: Sequence[Mapping[str, Any]],
        *,
        audit: MaterializationInputAudit,
        strategy_meta: Mapping[str, Any],
        datasets: Sequence[SourceDatasetBatch],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        metadata = cls._input_audit_metadata(
            audit=audit,
            strategy_meta=strategy_meta,
            datasets=datasets,
        )
        candidate_metadata = {
            key: metadata[key]
            for key in (
                "freshness_status",
                "market_coverage_ratio",
                "decision_data_version",
                "decision_clock_mode",
            )
        }
        return (
            [{**dict(item), **candidate_metadata} for item in candidates],
            metadata,
        )

    @staticmethod
    def _decision_realtime_batch_ids(
        datasets: Sequence[SourceDatasetBatch],
    ) -> list[str]:
        """Return the exact realtime provider batches audited in this snapshot."""

        values: set[str] = set()
        for dataset in datasets:
            if dataset.dataset_name != "stock_realtime_snapshot":
                continue
            raw = dataset.metadata.get("provider_batch_ids")
            items = raw if isinstance(raw, (list, tuple, set)) else str(raw or "").split(",")
            values.update(str(item).strip() for item in items if str(item).strip())
        return sorted(values)

    @staticmethod
    def _audit_eligible_code_prefixes(
        audit: MaterializationInputAudit,
    ) -> tuple[str, ...]:
        stock_basic = next(
            (item for item in audit.datasets if item.dataset_name == "stock_basic"),
            None,
        )
        raw = stock_basic.metadata.get("eligible_code_prefixes") if stock_basic else []
        values = raw if isinstance(raw, (list, tuple, set)) else []
        return tuple(str(value).strip().lower() for value in values if str(value).strip())

    @classmethod
    def _filter_candidates_to_audit_universe(
        cls,
        candidates: Sequence[Mapping[str, Any]],
        audit: MaterializationInputAudit,
    ) -> list[dict[str, Any]]:
        return filter_rows_to_code_prefixes(
            candidates,
            cls._audit_eligible_code_prefixes(audit),
        )

    def materialize(
        self,
        *,
        strategy_id: str = "a_share_sentiment",
        allow_shadow: bool = False,
        max_picks: int | None = None,
    ) -> dict[str, Any]:
        strategy_id = str(strategy_id or "").strip()
        if strategy_id not in SENTIMENT_STRATEGY_IDS:
            raise SentimentSnapshotMaterializationError(
                f"only sentiment strategies can be materialized: {strategy_id}"
            )
        if not self.strategy_service.is_registered_strategy(strategy_id):
            raise SentimentSnapshotMaterializationError(
                f"strategy is not registered: {strategy_id}"
            )
        strategy_meta = self.strategy_service.get_strategy_meta(strategy_id)
        capability = strategy_meta.get("capability") or {}
        runtime_status = str(capability.get("runtime_status") or "disabled")
        if runtime_status not in {"enabled", "legacy_enabled"}:
            if not (allow_shadow and str(strategy_meta.get("mode")) == "shadow_only"):
                raise SentimentSnapshotMaterializationError(
                    f"strategy {strategy_id} is {runtime_status}; use allow_shadow only for shadow_only materialization"
                )

        minimum_coverage = max(
            MIN_COVERAGE_RATIO,
            float(capability.get("minimum_coverage") or MIN_COVERAGE_RATIO),
        )
        with self.input_repository.open_consistent_inputs(
            strategy_meta=strategy_meta,
            minimum_coverage_ratio=minimum_coverage,
        ) as prepared:
            audit = prepared.audit
            base_batches = tuple(audit.datasets)
            if not audit.passed:
                # Persist failed source manifests for diagnosis, but never stage
                # or publish a candidate snapshot that could replace the last good one.
                prepared.commit()
                raise SentimentSnapshotInputQualityError(audit)
            if self.local_clock_mode() != audit.clock_mode:
                raise SentimentSnapshotMaterializationError(
                    "database and application selection clocks disagree; check server timezone and retry"
                )

            overrides: dict[str, Any] = {}
            if max_picks is not None:
                if int(max_picks) <= 0:
                    raise ValueError("max_picks must be greater than zero")
                overrides["max_picks"] = int(max_picks)
            overrides["decision_as_of"] = audit.decision_as_of
            realtime_batch_ids = self._decision_realtime_batch_ids(base_batches)
            if realtime_batch_ids:
                overrides["decision_realtime_batch_ids"] = realtime_batch_ids
            overrides["capture_factor_evaluation_trace"] = True
            selector = self.selector_factory(
                strategy_id,
                overrides,
                prepared.selection_repository,
            )
            bundle = selector.load_candidates_from_mysql(
                candidate_limit=None,
                instrument_type="stock",
                market_board="all",
                decision_as_of=audit.decision_as_of,
            )
            scoped_candidates = self._filter_candidates_to_audit_universe(
                list(bundle.get("candidates") or []),
                audit,
            )
            input_candidates, input_metadata = self._inject_input_audit_metadata(
                scoped_candidates,
                audit=audit,
                strategy_meta=strategy_meta,
                datasets=base_batches,
            )
            bundle = {**bundle, "candidates": input_candidates}
            unique_codes = {
                str(item.get("code") or "").strip()
                for item in input_candidates
                if str(item.get("code") or "").strip()
            }
            if len(input_candidates) != audit.expected_entity_count or len(unique_codes) != len(
                input_candidates
            ):
                raise SentimentSnapshotMaterializationError(
                    "selector input view does not match the audited active-stock universe"
                )
            read_view = prepared.register_read_view(input_candidates)
            results = selector.run(bundle)
            if self.local_clock_mode() != audit.clock_mode:
                raise SentimentSnapshotMaterializationError(
                    "selection crossed a market clock boundary; retry to avoid a mixed-mode snapshot"
                )
            candidate_rows = self._snapshot_candidates(
                results,
                audit.source_lineage,
                strategy_id=strategy_id,
                history_connection_factory=prepared.read_connection_factory,
            )
            prepared.commit()

            config = self.strategy_service.loader.load_config(strategy_id)
            strategy_config_hash = _sha256(config)
            implementation_hash = hashlib.sha256(
                inspect.getsource(selector.strategy.__class__).encode("utf-8")
            ).hexdigest()
            output_hash = _sha256(
                [
                    {
                        "code": item.get("code"),
                        "rank_no": item.get("rank_no"),
                        "score": item.get("score"),
                        "trade_grade_state": item.get("trade_grade_state"),
                    }
                    for item in candidate_rows
                ]
            )
            snapshot_digest = _sha256(
                {
                    "strategy_id": strategy_id,
                    "strategy_version": strategy_meta.get("version"),
                    "decision_as_of": audit.decision_as_of,
                    "read_view_batch_id": read_view.batch_id,
                    "strategy_config_hash": strategy_config_hash,
                    "implementation_hash": implementation_hash,
                    "output_hash": output_hash,
                }
            )
            snapshot_id = (
                f"sentiment-{strategy_id}-{audit.decision_as_of:%Y%m%dT%H%M%S%f}-"
                f"{snapshot_digest[:12]}"
            )[:96]
            freshness_seconds = self._freshness_seconds(audit, base_batches)
            published = self.snapshot_repository.stage_and_publish(
                snapshot_id=snapshot_id,
                strategy_id=strategy_id,
                strategy_version=str(strategy_meta.get("version") or ""),
                trade_date=audit.reference_trade_date,
                decision_as_of=audit.decision_as_of,
                candidates=candidate_rows,
                source_manifest_ids=audit.source_manifest_ids,
                expected_entity_count=audit.expected_entity_count,
                covered_entity_count=audit.covered_entity_count,
                strategy_config_hash=strategy_config_hash,
                source_batch_set_hash=_sha256(
                    sorted(item.batch_id for item in audit.datasets)
                ),
                implementation_hash=implementation_hash,
                news_event_set_hash=next(
                    (
                        item.payload_hash
                        for item in audit.datasets
                        if item.dataset_name == "sector_opinion_daily"
                    ),
                    None,
                ),
                generated_at=audit.decision_as_of,
                freshness_seconds=freshness_seconds,
                ai_mode="local_core",
                metadata={
                    "materializer_version": MATERIALIZER_VERSION,
                    "external_provider_calls": False,
                    "selection_core": "StockSelector.run",
                    "input_read_view_batch_id": read_view.batch_id,
                    "input_read_view_hash": read_view.payload_hash,
                    "output_hash": output_hash,
                    "input_audit": audit.as_dict(),
                    **input_metadata,
                },
            )

        payload = self._result_payload(published, audit, len(candidate_rows))
        if published.status == "ready":
            payload["factor_research_snapshot"] = self._persist_factor_trace(
                published_snapshot_id=published.snapshot_id,
                selector=selector,
                audit=audit,
                strategy_meta=strategy_meta,
                strategy_config_hash=strategy_config_hash,
                source_lineage=audit.source_lineage,
                metadata={"paired_run": False},
            )
        return payload

    def materialize_dual(self, *, max_picks: int | None = None) -> dict[str, Any]:
        """Compatibility wrapper for the current stable/v0.5 shadow pair."""

        return self.materialize_pair(
            baseline_strategy_id="a_share_sentiment",
            candidate_strategy_id="a_share_sentiment_v05",
            max_picks=max_picks,
        )

    def materialize_pair(
        self,
        *,
        baseline_strategy_id: str,
        candidate_strategy_id: str,
        max_picks: int | None = None,
    ) -> dict[str, Any]:
        """Run one enabled baseline and one shadow candidate on the same inputs.

        Both outputs remain immutable candidate snapshots. This method never
        writes ``selection_result`` and never promotes the shadow strategy.
        """

        strategy_ids = (
            str(baseline_strategy_id).strip(),
            str(candidate_strategy_id).strip(),
        )
        if not all(strategy_ids) or strategy_ids[0] == strategy_ids[1]:
            raise ValueError("baseline and candidate strategy ids must be distinct")
        strategy_metas = [self.strategy_service.get_strategy_meta(value) for value in strategy_ids]
        stable_capability = strategy_metas[0].get("capability") or {}
        if str(stable_capability.get("runtime_status") or "disabled") not in {
            "enabled",
            "legacy_enabled",
        }:
            raise SentimentSnapshotMaterializationError(
                "stable sentiment strategy is not runtime enabled"
            )
        if str(strategy_metas[1].get("mode") or "") != "shadow_only":
            raise SentimentSnapshotMaterializationError(
                f"candidate strategy {strategy_ids[1]} must remain shadow_only"
            )

        required_datasets: list[str] = []
        minimum_coverage = MIN_COVERAGE_RATIO
        for meta in strategy_metas:
            capability = meta.get("capability") or {}
            required_datasets.extend(capability.get("required_datasets") or [])
            minimum_coverage = max(
                minimum_coverage,
                float(capability.get("minimum_coverage") or MIN_COVERAGE_RATIO),
            )
        dual_meta = {
            "id": f"{strategy_ids[0]}__{strategy_ids[1]}__paired_shadow",
            "version": "+".join(str(meta.get("version") or "") for meta in strategy_metas),
            "capability": {"required_datasets": list(dict.fromkeys(required_datasets))},
        }

        factor_trace_queue: list[dict[str, Any]] = []
        with self.input_repository.open_consistent_inputs(
            strategy_meta=dual_meta,
            minimum_coverage_ratio=minimum_coverage,
        ) as prepared:
            audit = prepared.audit
            if not audit.passed:
                prepared.commit()
                raise SentimentSnapshotInputQualityError(audit)
            if self.local_clock_mode() != audit.clock_mode:
                raise SentimentSnapshotMaterializationError(
                    "database and application selection clocks disagree; check server timezone and retry"
                )

            base_batches = list(audit.datasets)
            outputs: list[dict[str, Any]] = []
            for meta in strategy_metas:
                outputs.append(
                    self._run_strategy_core(
                        prepared=prepared,
                        audit=audit,
                        strategy_meta=meta,
                        base_batches=base_batches,
                        max_picks=max_picks,
                    )
                )
            if self.local_clock_mode() != audit.clock_mode:
                raise SentimentSnapshotMaterializationError(
                    "dual selection crossed a market clock boundary; retry both strategies together"
                )
            prepared.commit()

            common_input_hash = _sha256(
                {
                    "decision_as_of": audit.decision_as_of,
                    "reference_trade_date": audit.reference_trade_date,
                    "base_batch_ids": [item.batch_id for item in base_batches],
                    "read_view_batch_ids": [item["read_view"].batch_id for item in outputs],
                }
            )
            run_payloads: dict[str, Any] = {}
            for output in outputs:
                strategy_audit = self._audit_for_strategy(
                    audit,
                    output["strategy_meta"],
                    [*base_batches, output["read_view"]],
                )
                published = self._publish_strategy_output(
                    output=output,
                    audit=strategy_audit,
                    extra_metadata={
                        "dual_run": True,
                        "paired_run": True,
                        "dual_input_hash": common_input_hash,
                        "paired_input_hash": common_input_hash,
                        "dual_strategy_ids": list(strategy_ids),
                        "paired_strategy_ids": list(strategy_ids),
                    },
                )
                run_payloads[str(output["strategy_meta"]["id"])] = self._result_payload(
                    published,
                    strategy_audit,
                    len(output["candidate_rows"]),
                )
                if published.status == "ready":
                    factor_trace_queue.append(
                        {
                            "published_snapshot_id": published.snapshot_id,
                            "selector": output["selector"],
                            "audit": strategy_audit,
                            "strategy_meta": output["strategy_meta"],
                            "strategy_config_hash": output["strategy_config_hash"],
                            "source_lineage": strategy_audit.source_lineage,
                            "metadata": {
                                "paired_run": True,
                                "paired_input_hash": common_input_hash,
                            },
                        }
                    )

        for item in factor_trace_queue:
            strategy_key = str(item["strategy_meta"]["id"])
            run_payloads[strategy_key][
                "factor_research_snapshot"
            ] = self._persist_factor_trace(**item)

        return {
            "status": (
                "success"
                if all(item.get("snapshot_status") == "ready" for item in run_payloads.values())
                else "rejected"
            ),
            "mode": "stable_plus_shadow",
            "baseline_strategy_id": strategy_ids[0],
            "candidate_strategy_id": strategy_ids[1],
            "decision_as_of": audit.decision_as_of,
            "trade_date": audit.reference_trade_date,
            "coverage_ratio": audit.coverage_ratio,
            "dual_input_hash": common_input_hash,
            "external_provider_calls": False,
            "selection_result_written": False,
            "runs": run_payloads,
        }

    def _run_strategy_core(
        self,
        *,
        prepared: PreparedSentimentInputs,
        audit: MaterializationInputAudit,
        strategy_meta: Mapping[str, Any],
        base_batches: Sequence[SourceDatasetBatch],
        max_picks: int | None,
    ) -> dict[str, Any]:
        strategy_id = str(strategy_meta.get("id") or "")
        overrides: dict[str, Any] = {}
        if max_picks is not None:
            if int(max_picks) <= 0:
                raise ValueError("max_picks must be greater than zero")
            overrides["max_picks"] = int(max_picks)
        overrides["decision_as_of"] = audit.decision_as_of
        realtime_batch_ids = self._decision_realtime_batch_ids(base_batches)
        if realtime_batch_ids:
            overrides["decision_realtime_batch_ids"] = realtime_batch_ids
        overrides["capture_factor_evaluation_trace"] = True
        selector = self.selector_factory(
            strategy_id,
            overrides,
            prepared.selection_repository,
        )
        bundle = selector.load_candidates_from_mysql(
            candidate_limit=None,
            instrument_type="stock",
            market_board="all",
            decision_as_of=audit.decision_as_of,
        )
        scoped_candidates = self._filter_candidates_to_audit_universe(
            list(bundle.get("candidates") or []),
            audit,
        )
        input_candidates, input_metadata = self._inject_input_audit_metadata(
            scoped_candidates,
            audit=audit,
            strategy_meta=strategy_meta,
            datasets=base_batches,
        )
        bundle = {**bundle, "candidates": input_candidates}
        unique_codes = {
            str(item.get("code") or "").strip()
            for item in input_candidates
            if str(item.get("code") or "").strip()
        }
        if len(input_candidates) != audit.expected_entity_count or len(unique_codes) != len(
            input_candidates
        ):
            raise SentimentSnapshotMaterializationError(
                f"{strategy_id} input view does not match the audited active-stock universe"
            )
        read_view = prepared.register_read_view(input_candidates)
        results = selector.run(bundle)
        source_lineage = [
            item.lineage()
            for item in [*base_batches, read_view]
            if item.lineage_ready
        ]
        candidate_rows = self._snapshot_candidates(
            results,
            source_lineage,
            strategy_id=strategy_id,
            history_connection_factory=prepared.read_connection_factory,
        )
        config = self.strategy_service.loader.load_config(strategy_id)
        strategy_config_hash = _sha256(config)
        implementation_hash = hashlib.sha256(
            inspect.getsource(selector.strategy.__class__).encode("utf-8")
        ).hexdigest()
        output_hash = _sha256(
            [
                {
                    "code": item.get("code"),
                    "rank_no": item.get("rank_no"),
                    "score": item.get("score"),
                    "trade_grade_state": item.get("trade_grade_state"),
                }
                for item in candidate_rows
            ]
        )
        return {
            "strategy_meta": dict(strategy_meta),
            "selector": selector,
            "read_view": read_view,
            "candidate_rows": candidate_rows,
            "strategy_config_hash": strategy_config_hash,
            "implementation_hash": implementation_hash,
            "output_hash": output_hash,
            "input_metadata": input_metadata,
        }

    def _persist_factor_trace(
        self,
        *,
        published_snapshot_id: str,
        selector: StockSelector,
        audit: MaterializationInputAudit,
        strategy_meta: Mapping[str, Any],
        strategy_config_hash: str,
        source_lineage: Sequence[Mapping[str, Any]],
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        trace_rows = list(
            getattr(selector, "last_factor_evaluation_trace", None) or []
        )
        if not trace_rows:
            return {
                "status": "skipped",
                "reason": "selector_factor_trace_empty",
                "snapshot_id": published_snapshot_id,
            }
        return self.factor_evaluation_repository.persist_snapshot(
            snapshot_id=published_snapshot_id,
            source_snapshot_id=published_snapshot_id,
            strategy_id=str(strategy_meta.get("id") or ""),
            strategy_version=str(strategy_meta.get("version") or ""),
            strategy_config_hash=strategy_config_hash,
            trade_date=audit.reference_trade_date,
            decision_as_of=audit.decision_as_of,
            expected_entity_count=audit.expected_entity_count,
            trace_rows=trace_rows,
            source_lineage=source_lineage,
            trace_mode="full_forward_trace",
            metadata={
                "selection_result_written": False,
                "selection_core": "StockSelector.run",
                **dict(metadata or {}),
            },
        )

    def _publish_strategy_output(
        self,
        *,
        output: Mapping[str, Any],
        audit: MaterializationInputAudit,
        extra_metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotStageResult:
        strategy_meta = output["strategy_meta"]
        strategy_id = str(strategy_meta.get("id") or "")
        strategy_config_hash = str(output["strategy_config_hash"])
        implementation_hash = str(output["implementation_hash"])
        output_hash = str(output["output_hash"])
        read_view: SourceDatasetBatch = output["read_view"]
        snapshot_digest = _sha256(
            {
                "strategy_id": strategy_id,
                "strategy_version": strategy_meta.get("version"),
                "decision_as_of": audit.decision_as_of,
                "read_view_batch_id": read_view.batch_id,
                "strategy_config_hash": strategy_config_hash,
                "implementation_hash": implementation_hash,
                "output_hash": output_hash,
            }
        )
        snapshot_id = (
            f"sentiment-{strategy_id}-{audit.decision_as_of:%Y%m%dT%H%M%S%f}-"
            f"{snapshot_digest[:12]}"
        )[:96]
        freshness_seconds = self._freshness_seconds(audit, audit.datasets)
        metadata = {
            "materializer_version": MATERIALIZER_VERSION,
            "external_provider_calls": False,
            "selection_core": "StockSelector.run",
            "input_read_view_batch_id": read_view.batch_id,
            "input_read_view_hash": read_view.payload_hash,
            "output_hash": output_hash,
            "input_audit": audit.as_dict(),
            **dict(output.get("input_metadata") or {}),
            **dict(extra_metadata or {}),
        }
        return self.snapshot_repository.stage_and_publish(
            snapshot_id=snapshot_id,
            strategy_id=strategy_id,
            strategy_version=str(strategy_meta.get("version") or ""),
            trade_date=audit.reference_trade_date,
            decision_as_of=audit.decision_as_of,
            candidates=output["candidate_rows"],
            source_manifest_ids=audit.source_manifest_ids,
            expected_entity_count=audit.expected_entity_count,
            covered_entity_count=audit.covered_entity_count,
            strategy_config_hash=strategy_config_hash,
            source_batch_set_hash=_sha256(sorted(item.batch_id for item in audit.datasets)),
            implementation_hash=implementation_hash,
            news_event_set_hash=next(
                (
                    item.payload_hash
                    for item in audit.datasets
                    if item.dataset_name == "sector_opinion_daily"
                ),
                None,
            ),
            generated_at=audit.decision_as_of,
            freshness_seconds=freshness_seconds,
            ai_mode="local_core",
            metadata=metadata,
        )

    @staticmethod
    def _audit_for_strategy(
        base: MaterializationInputAudit,
        strategy_meta: Mapping[str, Any],
        datasets: Sequence[SourceDatasetBatch],
    ) -> MaterializationInputAudit:
        return MaterializationInputAudit(
            strategy_id=str(strategy_meta.get("id") or ""),
            strategy_version=str(strategy_meta.get("version") or ""),
            decision_as_of=base.decision_as_of,
            reference_trade_date=base.reference_trade_date,
            clock_mode=base.clock_mode,
            expected_entity_count=base.expected_entity_count,
            covered_entity_count=base.covered_entity_count,
            minimum_coverage_ratio=base.minimum_coverage_ratio,
            datasets=list(datasets),
            quality_errors=list(base.quality_errors),
        )

    @staticmethod
    def _snapshot_candidates(
        results: Sequence[Mapping[str, Any]],
        source_lineage: Sequence[Mapping[str, Any]],
        *,
        strategy_id: str,
        history_connection_factory: ConnectionFactory | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for index, raw in enumerate(results, start=1):
            item = dict(raw)
            explain = (
                item.get("explain")
                if isinstance(item.get("explain"), Mapping)
                else {}
            )
            explain_raw_metrics = (
                explain.get("raw_metrics")
                if isinstance(explain.get("raw_metrics"), Mapping)
                else {}
            )
            strategy_raw_metrics = (
                item.get("strategy_raw_metrics")
                if isinstance(item.get("strategy_raw_metrics"), Mapping)
                else {}
            )
            grade = str(
                item.get("trade_grade_state")
                or item.get("signal_grade")
                or item.get("grade_state")
                or "watch"
            ).strip().lower()
            grade_reason = item.get("trade_grade_reason") or item.get("grade_reason")
            trade_plan = attach_turtle_research_shadow(
                item,
                strategy_id=strategy_id,
                connection_factory=history_connection_factory,
            )
            rows.append(
                {
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "industry": (
                        item.get("industry")
                        or strategy_raw_metrics.get("industry")
                        or explain_raw_metrics.get("industry")
                    ),
                    "candidate_state": "eligible",
                    "eligibility_reason": grade_reason,
                    "is_selected": True,
                    "is_tradable": grade == "tradable",
                    "rank_no": item.get("rank_no") or index,
                    "score": item.get("score"),
                    "trade_grade_state": grade,
                    "opinion_sector_type": item.get("opinion_sector_type"),
                    "opinion_sector_name": item.get("opinion_sector_name"),
                    "opinion_match_type": item.get("opinion_match_type"),
                    "market_opinion_snapshot_id": item.get("market_opinion_snapshot_id"),
                    "selected_price": item.get("selected_price") or item.get("close"),
                    "selected_price_source": item.get("selected_price_source")
                    or "daily_kline",
                    "selected_price_quote_time": item.get("selected_price_quote_time"),
                    "factor_json": item.get("factors") or {},
                    "explain_json": item.get("explain") or {},
                    "trade_plan_json": trade_plan,
                    "source_lineage": [dict(value) for value in source_lineage],
                    "signal_grade": grade,
                    "validation_status": item.get("validation_status"),
                    "score_breakdown": item.get("score_breakdown")
                    or item.get("factor_contributions")
                    or {},
                    "gate_results": item.get("gate_results") or {},
                    "evidence_ids": list(item.get("evidence_ids") or []),
                    "ai_status": item.get("ai_status")
                    or item.get("ai_overlay_state")
                    or "not_available",
                }
            )
        return rows

    @staticmethod
    def _result_payload(
        published: SnapshotStageResult,
        audit: MaterializationInputAudit,
        candidate_count: int,
    ) -> dict[str, Any]:
        return {
            "status": "success" if published.status == "ready" else "rejected",
            "snapshot_id": published.snapshot_id,
            "snapshot_status": published.status,
            "quality_status": published.quality_status,
            "candidate_count": candidate_count,
            "strategy_id": audit.strategy_id,
            "strategy_version": audit.strategy_version,
            "trade_date": audit.reference_trade_date,
            "decision_as_of": audit.decision_as_of,
            "coverage_ratio": audit.coverage_ratio,
            "expected_entity_count": audit.expected_entity_count,
            "covered_entity_count": audit.covered_entity_count,
            "clock_mode": audit.clock_mode,
            "source_manifest_ids": audit.source_manifest_ids,
            "validation": published.validation.as_dict(),
        }
