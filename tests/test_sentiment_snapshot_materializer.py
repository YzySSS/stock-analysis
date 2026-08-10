from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from app.stock_selection.sentiment_snapshot import SnapshotStageResult, SnapshotValidation
from app.stock_selection.dataset_scope import (
    filter_rows_to_code_prefixes,
    required_dataset_code_prefixes,
    sql_code_prefix_filter,
)
from app.stock_selection.sentiment_snapshot_materializer import (
    MATERIALIZER_VERSION,
    MaterializationInputAudit,
    MySQLSentimentSnapshotInputRepository,
    OPTIONAL_SCORING_DATASETS,
    PreparedSentimentInputs,
    SentimentSnapshotInputQualityError,
    SentimentSnapshotMaterializationError,
    SentimentSnapshotMaterializationService,
    SourceDatasetBatch,
)
from app.strategies.service import StrategyService


DECISION = datetime(2026, 7, 21, 16, 0, 0)


class MaterializerVersionContractTests(unittest.TestCase):
    def test_materializer_version_fits_source_manifest_schema_version_column(self):
        self.assertLessEqual(len(MATERIALIZER_VERSION), 32)


class DatasetScopeContractTests(unittest.TestCase):
    def test_daily_moneyflow_scope_is_explicitly_shanghai_and_shenzhen(self):
        prefixes = required_dataset_code_prefixes(
            ("daily_kline", "stock_moneyflow_daily")
        )

        self.assertEqual(prefixes, ("sh.", "sz."))
        self.assertEqual(
            sql_code_prefix_filter("sb.code", prefixes),
            " AND LEFT(sb.code, 3) IN ('sh.', 'sz.')",
        )

    def test_scope_filter_keeps_supported_rows_and_fails_closed_for_bse(self):
        rows = [
            {"code": "sh.600000"},
            {"code": "sz.000001"},
            {"code": "bj.920001"},
        ]

        self.assertEqual(
            [item["code"] for item in filter_rows_to_code_prefixes(rows, ("sh.", "sz."))],
            ["sh.600000", "sz.000001"],
        )


def source_batch(
    dataset_name: str,
    manifest_id: int,
    *,
    coverage_ratio: float = 0.99,
    source_time: datetime | None = None,
    received_at: datetime | None = None,
    required: bool = True,
    metadata: dict[str, Any] | None = None,
) -> SourceDatasetBatch:
    return SourceDatasetBatch(
        dataset_name=dataset_name,
        batch_id=f"mysql-{dataset_name}-batch",
        source_time=source_time or datetime(2026, 7, 21, 15, 0, 0),
        received_at=received_at or datetime(2026, 7, 21, 15, 5, 0),
        actual_rows=99,
        expected_entities=100,
        actual_entities=99,
        coverage_ratio=coverage_ratio,
        required=required,
        manifest_id=manifest_id,
        payload_hash=str(manifest_id) * 64,
        metadata=dict(metadata or {}),
    )


def input_audit(*, covered: int = 99, errors: list[str] | None = None):
    return MaterializationInputAudit(
        strategy_id="a_share_sentiment",
        strategy_version="0.4.4",
        decision_as_of=DECISION,
        reference_trade_date=date(2026, 7, 21),
        clock_mode="postclose",
        expected_entity_count=100,
        covered_entity_count=covered,
        minimum_coverage_ratio=0.98,
        datasets=[
            source_batch("stock_basic", 1, coverage_ratio=1.0),
            source_batch("daily_kline", 2),
            source_batch("factor_input_daily", 3),
            source_batch("sector_opinion_daily", 4, coverage_ratio=1.0),
            source_batch("stock_technical_feature_daily", 5),
        ],
        quality_errors=list(errors or []),
    )


class FakeInputRepository:
    def __init__(self, audit: MaterializationInputAudit, candidate_rows: list[dict[str, Any]]):
        self.audit = audit
        self.candidate_rows = candidate_rows
        self.open_calls: list[dict[str, Any]] = []
        self.commit_count = 0

    @contextmanager
    def open_consistent_inputs(self, **kwargs):
        self.open_calls.append(kwargs)

        @contextmanager
        def borrowed_connection(*, dict_cursor=True):
            self.borrowed_dict_cursor = dict_cursor
            yield object()

        self.borrowed_connection_factory = borrowed_connection

        def register_read_view(rows):
            self.asserted_rows = list(rows)
            return SourceDatasetBatch(
                dataset_name="sentiment_selection_input",
                batch_id="mysql-sentiment-selection-input-batch",
                source_time=datetime(2026, 7, 21, 15, 0, 0),
                received_at=datetime(2026, 7, 21, 15, 5, 0),
                actual_rows=len(rows),
                expected_entities=self.audit.expected_entity_count,
                actual_entities=self.audit.covered_entity_count,
                coverage_ratio=self.audit.coverage_ratio,
                manifest_id=99,
                payload_hash="f" * 64,
            )

        prepared = PreparedSentimentInputs(
            audit=self.audit,
            selection_repository=object(),  # type: ignore[arg-type]
            _register_read_view=register_read_view,
            _commit=self._commit,
            read_connection_factory=borrowed_connection,
        )
        yield prepared

    def _commit(self):
        self.commit_count += 1


class FakeStrategy:
    pass


class FakeSelector:
    def __init__(self, candidate_rows, results):
        self.candidate_rows = candidate_rows
        self.results = results
        self.strategy = FakeStrategy()
        self.load_calls = []

    def load_candidates_from_mysql(self, **kwargs):
        self.load_calls.append(kwargs)
        return {"candidates": list(self.candidate_rows)}

    def run(self, bundle):
        self.bundle = bundle
        return list(self.results)


class FakeSnapshotRepository:
    def __init__(self):
        self.calls = []

    def stage_and_publish(self, **kwargs):
        self.calls.append(kwargs)
        validation = SnapshotValidation(
            coverage_ratio=kwargs["covered_entity_count"] / kwargs["expected_entity_count"],
            critical_completeness_ratio=1.0,
            expected_entity_count=kwargs["expected_entity_count"],
            covered_entity_count=kwargs["covered_entity_count"],
            candidate_count=len(kwargs["candidates"]),
            errors=(),
        )
        return SnapshotStageResult(
            snapshot_id=kwargs["snapshot_id"],
            status="ready",
            quality_status="passed",
            validation=validation,
        )


class IntersectionCursor:
    def __init__(self, covered_entities: int):
        self.covered_entities = covered_entities
        self.execute_calls = []
        self._next = None

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))
        if "FROM sector_opinion_daily" in sql:
            self._next = {"count": 1}
        else:
            self._next = {"covered_entities": self.covered_entities}

    def fetchone(self):
        return self._next


class SequenceCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.execute_calls = []
        self._next = None

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))
        self._next = self.rows.pop(0) if self.rows else {}

    def fetchone(self):
        return self._next


class ManifestCursor:
    def __init__(self):
        self.execute_calls = []
        self.lastrowid = 0

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))
        self.lastrowid += 1


class ReleaseFailingCursor:
    def __init__(self):
        self._next = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        if "GET_LOCK" in sql:
            self._next = {"acquired": 1}
        elif "SELECT NOW" in sql:
            self._next = {"decision_as_of": DECISION}
        elif "RELEASE_LOCK" in sql:
            raise ConnectionError("release failed")

    def fetchone(self):
        return self._next


class ReleaseFailingConnection:
    def __init__(self, *, pooled: bool):
        self._cursor = ReleaseFailingCursor()
        self.pooled = pooled
        self.commit_count = 0
        self.rollback_count = 0
        self.invalidate_count = 0
        self.close_count = 0
        if not pooled:
            self.invalidate = None

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def invalidate(self):
        self.invalidate_count += 1

    def close(self):
        self.close_count += 1


class FixedAuditInputRepository(MySQLSentimentSnapshotInputRepository):
    def __init__(self, audit, connection):
        @contextmanager
        def connection_factory(*, dict_cursor=True):
            self.dict_cursor = dict_cursor
            yield connection

        super().__init__(connection_factory=connection_factory)
        self.audit = audit

    def _build_audit(self, *_args, **_kwargs):
        return self.audit

    def _register_source_manifest(self, *_args, **_kwargs):
        return None


class SentimentSnapshotMaterializationTests(unittest.TestCase):
    def test_decision_realtime_batches_are_normalized_from_audited_metadata(self):
        batch = source_batch(
            "stock_realtime_snapshot",
            6,
            metadata={"provider_batch_ids": "batch-b,batch-a,batch-a"},
        )

        self.assertEqual(
            SentimentSnapshotMaterializationService._decision_realtime_batch_ids(
                [batch]
            ),
            ["batch-a", "batch-b"],
        )

    @staticmethod
    def candidate_rows(count: int = 100) -> list[dict[str, Any]]:
        return [
            {
                "code": f"sh.{600000 + index}",
                "name": f"stock-{index}",
                "trade_date": date(2026, 7, 21),
            }
            for index in range(count)
        ]

    def test_below_98_percent_never_runs_selector_or_publishes(self):
        audit = input_audit(
            covered=97,
            errors=["exact required-dataset intersection coverage 0.97 is below 0.98"],
        )
        inputs = FakeInputRepository(audit, self.candidate_rows())
        snapshots = FakeSnapshotRepository()
        selector_calls = []
        service = SentimentSnapshotMaterializationService(
            input_repository=inputs,  # type: ignore[arg-type]
            snapshot_repository=snapshots,  # type: ignore[arg-type]
            selector_factory=lambda *_args: selector_calls.append(_args),  # type: ignore[arg-type]
            local_clock_mode=lambda: "postclose",
        )

        with self.assertRaises(SentimentSnapshotInputQualityError) as raised:
            service.materialize(strategy_id="a_share_sentiment")

        self.assertEqual(raised.exception.audit.coverage_ratio, 0.97)
        self.assertEqual(inputs.commit_count, 1)
        self.assertEqual(selector_calls, [])
        self.assertEqual(snapshots.calls, [])

    @patch(
        "app.stock_selection.sentiment_snapshot_materializer."
        "attach_turtle_research_shadow"
    )
    def test_materializes_exact_audited_universe_without_external_provider(
        self,
        attach_shadow,
    ):
        rows = self.candidate_rows()
        audit = input_audit()
        inputs = FakeInputRepository(audit, rows)
        snapshots = FakeSnapshotRepository()
        selector = FakeSelector(
            rows,
            [
                {
                    "code": "sh.600000",
                    "name": "stock-0",
                    "rank_no": 1,
                    "score": 72.5,
                    "trade_grade_state": "tradable",
                    "trade_grade_reason": "all gates passed",
                    "factors": {"sector_heat": 80},
                    "explain": {
                        "reason": "fixture",
                        "raw_metrics": {"industry": "银行"},
                    },
                    "trade_plan": {
                        "version": "selection_trade_plan_v3_risk_control",
                        "invalidates_on": "fixture",
                    },
                    "close": 10.5,
                }
            ],
        )
        attach_shadow.return_value = {
            "version": "selection_trade_plan_v3_risk_control",
            "invalidates_on": "fixture",
            "research_shadow": {
                "version": "selection_trade_plan_v4_turtle_risk",
            },
        }
        service = SentimentSnapshotMaterializationService(
            input_repository=inputs,  # type: ignore[arg-type]
            snapshot_repository=snapshots,  # type: ignore[arg-type]
            selector_factory=lambda *_args: selector,
            local_clock_mode=lambda: "postclose",
        )

        result = service.materialize(strategy_id="a_share_sentiment")

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["coverage_ratio"], 0.99)
        self.assertEqual(inputs.commit_count, 1)
        self.assertEqual(selector.load_calls[0]["candidate_limit"], None)
        self.assertEqual(selector.load_calls[0]["market_board"], "all")
        self.assertEqual(selector.load_calls[0]["decision_as_of"], DECISION)
        self.assertEqual(len(snapshots.calls), 1)
        published = snapshots.calls[0]
        self.assertEqual(published["expected_entity_count"], 100)
        self.assertEqual(published["covered_entity_count"], 99)
        self.assertEqual(published["ai_mode"], "local_core")
        self.assertEqual(published["freshness_seconds"], 3600)
        self.assertEqual(published["metadata"]["freshness_status"], "fresh")
        self.assertFalse(published["metadata"]["external_provider_calls"])
        self.assertEqual(published["source_manifest_ids"], [1, 2, 3, 4, 5, 99])
        candidate = published["candidates"][0]
        self.assertTrue(candidate["is_selected"])
        self.assertTrue(candidate["is_tradable"])
        self.assertEqual(candidate["industry"], "银行")
        self.assertEqual(candidate["factor_json"], {"sector_heat": 80})
        self.assertEqual(
            candidate["trade_plan_json"]["research_shadow"]["version"],
            "selection_trade_plan_v4_turtle_risk",
        )
        attach_shadow.assert_called_once()
        self.assertEqual(
            attach_shadow.call_args.kwargs["strategy_id"],
            "a_share_sentiment",
        )
        self.assertIs(
            attach_shadow.call_args.kwargs["connection_factory"],
            inputs.borrowed_connection_factory,
        )
        injected = selector.bundle["candidates"][0]
        self.assertEqual(injected["freshness_status"], "fresh")
        self.assertEqual(injected["market_coverage_ratio"], 0.99)
        self.assertEqual(injected["decision_clock_mode"], "postclose")
        self.assertTrue(injected["decision_data_version"].startswith("sentiment-input-"))
        self.assertEqual(
            {item["dataset_name"] for item in candidate["source_lineage"]},
            {
                "stock_basic",
                "daily_kline",
                "factor_input_daily",
                "sector_opinion_daily",
                "stock_technical_feature_daily",
                "sentiment_selection_input",
            },
        )

    def test_manual_experimental_strategy_materializes_without_shadow_override(self):
        rows = self.candidate_rows()
        audit = input_audit()
        audit.strategy_id = "a_share_sentiment_v05"
        audit.strategy_version = "0.5.1"
        inputs = FakeInputRepository(audit, rows)
        snapshots = FakeSnapshotRepository()
        selector = FakeSelector(
            rows,
            [
                {
                    "code": "sh.600000",
                    "rank_no": 1,
                    "score": 72,
                    "trade_grade_state": "watch",
                }
            ],
        )
        service = SentimentSnapshotMaterializationService(
            input_repository=inputs,  # type: ignore[arg-type]
            snapshot_repository=snapshots,  # type: ignore[arg-type]
            selector_factory=lambda *_args: selector,
            local_clock_mode=lambda: "postclose",
        )

        result = service.materialize(strategy_id="a_share_sentiment_v05")

        self.assertEqual(result["strategy_id"], "a_share_sentiment_v05")
        self.assertEqual(len(inputs.open_calls), 1)
        self.assertEqual(len(snapshots.calls), 1)

    def test_explicit_shadow_materialization_writes_v05_snapshot_only(self):
        rows = self.candidate_rows()
        audit = input_audit()
        audit.strategy_id = "a_share_sentiment_v05"
        audit.strategy_version = "0.5.1"
        inputs = FakeInputRepository(audit, rows)
        snapshots = FakeSnapshotRepository()
        selector = FakeSelector(
            rows,
            [
                {
                    "code": "sh.600000",
                    "rank_no": 1,
                    "score": 72,
                    "signal_grade": "tradable",
                    "grade_reason": "all deterministic gates passed",
                    "validation_status": "shadow_only",
                    "score_breakdown": {"catalyst_quality": 18.5},
                    "gate_results": {"hard_gate_pass": True},
                    "evidence_ids": ["news-1", "news-2"],
                    "ai_status": "advisory_only",
                }
            ],
        )
        service = SentimentSnapshotMaterializationService(
            input_repository=inputs,  # type: ignore[arg-type]
            snapshot_repository=snapshots,  # type: ignore[arg-type]
            selector_factory=lambda *_args: selector,
            local_clock_mode=lambda: "postclose",
        )

        result = service.materialize(
            strategy_id="a_share_sentiment_v05",
            allow_shadow=True,
        )

        self.assertEqual(result["strategy_id"], "a_share_sentiment_v05")
        self.assertEqual(result["strategy_version"], "0.5.1")
        self.assertEqual(len(snapshots.calls), 1)
        self.assertEqual(snapshots.calls[0]["strategy_id"], "a_share_sentiment_v05")
        self.assertEqual(snapshots.calls[0]["ai_mode"], "local_core")
        candidate = snapshots.calls[0]["candidates"][0]
        self.assertEqual(candidate["trade_grade_state"], "tradable")
        self.assertTrue(candidate["is_tradable"])
        self.assertEqual(candidate["eligibility_reason"], "all deterministic gates passed")
        self.assertEqual(candidate["signal_grade"], "tradable")
        self.assertEqual(candidate["validation_status"], "shadow_only")
        self.assertEqual(candidate["score_breakdown"], {"catalyst_quality": 18.5})
        self.assertEqual(candidate["gate_results"], {"hard_gate_pass": True})
        self.assertEqual(candidate["evidence_ids"], ["news-1", "news-2"])
        self.assertEqual(candidate["ai_status"], "advisory_only")

    def test_oldest_dynamic_source_controls_manifest_freshness_and_static_is_excluded(self):
        rows = self.candidate_rows()
        audit = input_audit()
        audit.datasets = [
            source_batch("stock_basic", 1, source_time=datetime(2020, 1, 1, 0, 0)),
            source_batch("daily_kline", 2, source_time=datetime(2026, 7, 21, 14, 0)),
            source_batch("factor_input_daily", 3, source_time=datetime(2026, 7, 21, 15, 0)),
            source_batch("sector_opinion_daily", 4, source_time=datetime(2026, 7, 21, 15, 30)),
            source_batch("stock_technical_feature_daily", 5, source_time=datetime(2026, 7, 21, 15, 45)),
        ]
        inputs = FakeInputRepository(audit, rows)
        snapshots = FakeSnapshotRepository()
        selector = FakeSelector(
            rows,
            [{"code": "sh.600000", "score": 70, "trade_grade_state": "watch"}],
        )
        service = SentimentSnapshotMaterializationService(
            input_repository=inputs,  # type: ignore[arg-type]
            snapshot_repository=snapshots,  # type: ignore[arg-type]
            selector_factory=lambda *_args: selector,
            local_clock_mode=lambda: "postclose",
        )

        service.materialize(strategy_id="a_share_sentiment")

        published = snapshots.calls[0]
        self.assertEqual(published["freshness_seconds"], 7200)
        self.assertEqual(published["metadata"]["freshness_status"], "fresh")
        self.assertEqual(selector.bundle["candidates"][0]["freshness_status"], "fresh")

    def test_optional_scoring_lineage_is_published_but_does_not_age_formal_freshness(self):
        rows = self.candidate_rows()
        audit = input_audit()
        audit.datasets.append(
            source_batch(
                "stock_popularity_snapshot",
                6,
                coverage_ratio=0.25,
                source_time=datetime(2020, 1, 1, 0, 0),
                received_at=datetime(2026, 7, 21, 15, 10),
                required=False,
                metadata={
                    "providers": "eastmoney_hot_rank",
                    "provider_batch_ids": "eastmoney_hot_rank@2020-01-01 00:00:00",
                },
            )
        )
        inputs = FakeInputRepository(audit, rows)
        snapshots = FakeSnapshotRepository()
        selector = FakeSelector(
            rows,
            [{"code": "sh.600000", "score": 70, "trade_grade_state": "watch"}],
        )
        service = SentimentSnapshotMaterializationService(
            input_repository=inputs,  # type: ignore[arg-type]
            snapshot_repository=snapshots,  # type: ignore[arg-type]
            selector_factory=lambda *_args: selector,
            local_clock_mode=lambda: "postclose",
        )

        service.materialize(strategy_id="a_share_sentiment")

        published = snapshots.calls[0]
        self.assertEqual(published["freshness_seconds"], 3600)
        self.assertEqual(published["metadata"]["freshness_status"], "fresh")
        self.assertEqual(published["source_manifest_ids"], [1, 2, 3, 4, 5, 6, 99])
        optional_lineage = next(
            item
            for item in published["candidates"][0]["source_lineage"]
            if item["dataset_name"] == "stock_popularity_snapshot"
        )
        self.assertEqual(optional_lineage["provider"], "eastmoney_hot_rank")
        self.assertEqual(
            optional_lineage["provider_batch_ids"],
            "eastmoney_hot_rank@2020-01-01 00:00:00",
        )
        self.assertFalse(optional_lineage["required"])

    def test_optional_scoring_probe_freezes_all_actual_selector_tables_as_of_decision(self):
        observed = {
            "actual_rows": 25,
            "actual_entities": 20,
            "source_time_min": datetime(2026, 7, 21, 14, 40),
            "source_time": datetime(2026, 7, 21, 15, 0),
            "received_at": datetime(2026, 7, 21, 15, 5),
            "source_trade_date_min": date(2026, 7, 21),
            "source_trade_date_max": date(2026, 7, 21),
            "providers": "fixture_provider",
            "provider_batch_ids": "fixture-batch",
        }
        cursor = SequenceCursor(
            [
                dict(observed),
                dict(observed),
                dict(observed),
                dict(observed, actual_entities=3),
                dict(observed, actual_entities=12),
                dict(observed),
                dict(observed, actual_entities=5),
                dict(observed),
            ]
        )
        repository = MySQLSentimentSnapshotInputRepository()

        batches = repository._probe_optional_scoring_datasets(
            cursor,
            decision_as_of=DECISION,
            reference_trade_date=date(2026, 7, 21),
            expected_entities=100,
        )

        self.assertEqual(tuple(item.dataset_name for item in batches), OPTIONAL_SCORING_DATASETS)
        self.assertTrue(all(not item.required for item in batches))
        self.assertTrue(all(item.source_time <= DECISION for item in batches if item.source_time))
        self.assertTrue(all(item.received_at <= DECISION for item in batches if item.received_at))
        self.assertTrue(all(item.provider == "fixture_provider" for item in batches))
        self.assertTrue(all(item.metadata["point_in_time_bounded"] for item in batches))
        sql_by_table = "\n".join(sql for sql, _params in cursor.execute_calls)
        for table in OPTIONAL_SCORING_DATASETS:
            self.assertIn(table, sql_by_table)
        self.assertTrue(
            all(
                DECISION in tuple(params or ())
                for _sql, params in cursor.execute_calls
            )
        )

    def test_optional_partial_or_empty_manifest_is_never_reported_as_passed(self):
        repository = MySQLSentimentSnapshotInputRepository()
        audit = input_audit()
        cursor = ManifestCursor()
        partial = source_batch(
            "stock_realtime_moneyflow_snapshot",
            0,
            coverage_ratio=0.25,
            required=False,
            metadata={"providers": "akshare_ths_moneyflow"},
        )
        partial.manifest_id = None
        repository._register_source_manifest(cursor, audit, partial)

        partial_params = cursor.execute_calls[-1][1]
        self.assertEqual(partial_params[6], "published")
        self.assertEqual(partial_params[7], "partial")
        self.assertNotEqual(partial_params[7], "passed")
        self.assertEqual(partial.manifest_id, 1)

        missing = SourceDatasetBatch(
            dataset_name="stock_popularity_snapshot",
            batch_id="mysql-stock-popularity-empty",
            source_time=None,
            received_at=None,
            actual_rows=0,
            expected_entities=100,
            actual_entities=0,
            coverage_ratio=0.0,
            required=False,
            payload_hash="e" * 64,
            metadata={"providers": None},
        )
        repository._register_source_manifest(cursor, audit, missing)

        missing_params = cursor.execute_calls[-1][1]
        self.assertEqual(missing_params[6], "rejected")
        self.assertEqual(missing_params[7], "missing")
        self.assertNotEqual(missing_params[7], "passed")
        self.assertIsNone(missing.source_time)
        self.assertIsNone(missing.received_at)

    def test_required_dataset_without_lineage_timestamps_fails_audit_but_optional_does_not(self):
        required_missing = SourceDatasetBatch(
            dataset_name="daily_kline",
            batch_id="missing-required",
            source_time=None,
            received_at=None,
            actual_rows=100,
            expected_entities=100,
            actual_entities=100,
            coverage_ratio=1.0,
            required=True,
        )
        optional_missing = SourceDatasetBatch(
            dataset_name="stock_popularity_snapshot",
            batch_id="missing-optional",
            source_time=None,
            received_at=None,
            actual_rows=0,
            expected_entities=100,
            actual_entities=0,
            coverage_ratio=0.0,
            required=False,
        )

        errors = MySQLSentimentSnapshotInputRepository._required_dataset_quality_errors(
            [required_missing, optional_missing],
            DECISION,
        )

        self.assertEqual(
            errors,
            [
                "required dataset daily_kline source_time is missing",
                "required dataset daily_kline received_at is missing",
            ],
        )
        audit = input_audit(errors=errors)
        self.assertFalse(audit.passed)

    def test_stale_oldest_dynamic_source_is_injected_as_watch_gate_metadata(self):
        rows = self.candidate_rows()
        audit = input_audit()
        audit.datasets[1] = source_batch(
            "daily_kline",
            2,
            source_time=datetime(2026, 7, 19, 15, 0),
        )
        inputs = FakeInputRepository(audit, rows)
        snapshots = FakeSnapshotRepository()
        selector = FakeSelector(
            rows,
            [{"code": "sh.600000", "score": 70, "trade_grade_state": "watch"}],
        )
        service = SentimentSnapshotMaterializationService(
            input_repository=inputs,  # type: ignore[arg-type]
            snapshot_repository=snapshots,  # type: ignore[arg-type]
            selector_factory=lambda *_args: selector,
            local_clock_mode=lambda: "postclose",
        )

        service.materialize(strategy_id="a_share_sentiment")

        published = snapshots.calls[0]
        self.assertEqual(published["freshness_seconds"], 176400)
        self.assertEqual(published["metadata"]["freshness_status"], "stale")
        injected = selector.bundle["candidates"][0]
        self.assertEqual(injected["freshness_status"], "stale")
        self.assertEqual(injected["decision_clock_mode"], "postclose")

    def test_dual_run_uses_one_consistent_input_and_writes_only_snapshots(self):
        rows = self.candidate_rows()
        inputs = FakeInputRepository(input_audit(), rows)
        snapshots = FakeSnapshotRepository()
        selectors = {
            "a_share_sentiment": FakeSelector(
                rows,
                [
                    {
                        "code": "sh.600000",
                        "rank_no": 1,
                        "score": 70,
                        "trade_grade_state": "watch",
                    }
                ],
            ),
            "a_share_sentiment_v05": FakeSelector(
                rows,
                [
                    {
                        "code": "sh.600001",
                        "rank_no": 1,
                        "score": 73,
                        "trade_grade_state": "tradable",
                    }
                ],
            ),
        }
        factory_calls = []

        def selector_factory(strategy_id, _overrides, _repository):
            factory_calls.append(strategy_id)
            return selectors[strategy_id]

        service = SentimentSnapshotMaterializationService(
            input_repository=inputs,  # type: ignore[arg-type]
            snapshot_repository=snapshots,  # type: ignore[arg-type]
            selector_factory=selector_factory,
            local_clock_mode=lambda: "postclose",
        )

        result = service.materialize_dual()

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["mode"], "stable_plus_shadow")
        self.assertFalse(result["external_provider_calls"])
        self.assertFalse(result["selection_result_written"])
        self.assertEqual(len(inputs.open_calls), 1)
        self.assertEqual(inputs.commit_count, 1)
        self.assertEqual(
            factory_calls,
            ["a_share_sentiment", "a_share_sentiment_v05"],
        )
        self.assertTrue(
            all(
                selector.load_calls[0]["decision_as_of"] == DECISION
                for selector in selectors.values()
            )
        )
        self.assertEqual(len(snapshots.calls), 2)
        self.assertEqual(
            {call["strategy_id"] for call in snapshots.calls},
            {"a_share_sentiment", "a_share_sentiment_v05"},
        )
        self.assertEqual(
            {call["decision_as_of"] for call in snapshots.calls},
            {DECISION},
        )
        self.assertEqual(
            {
                call["metadata"]["dual_input_hash"]
                for call in snapshots.calls
            },
            {result["dual_input_hash"]},
        )
        self.assertTrue(all(call["metadata"]["dual_run"] for call in snapshots.calls))

    def test_actual_strategy_registry_remains_source_of_metadata(self):
        service = StrategyService()
        meta = service.get_strategy_meta("a_share_sentiment")
        self.assertEqual(meta["version"], "0.4.4")
        self.assertEqual(meta["mode"], "frozen_baseline")

    def test_materializer_has_no_external_provider_execution_path(self):
        source = Path(
            "app/stock_selection/sentiment_snapshot_materializer.py"
        ).read_text(encoding="utf-8")
        for forbidden in (
            "NewsAggregator",
            "DeepSeekSentimentReranker",
            "refresh_sentiment_candidates",
            "import requests",
            "import akshare",
            "import tushare",
            ".run_strategy(",
        ):
            self.assertNotIn(forbidden, source)

    def test_mysql_audit_uses_exact_required_dataset_code_intersection(self):
        cursor = IntersectionCursor(97)
        repository = MySQLSentimentSnapshotInputRepository()

        covered = repository._covered_entity_intersection(
            cursor,
            expected_entities=100,
            required_datasets=(
                "daily_kline",
                "factor_input_daily",
                "sector_opinion_daily",
                "stock_technical_feature_daily",
            ),
            reference_trade_date=date(2026, 7, 21),
            decision_as_of=DECISION,
        )

        self.assertEqual(covered, 97)
        intersection_sql = cursor.execute_calls[-1][0]
        self.assertIn("LEFT JOIN (SELECT DISTINCT code FROM daily_kline", intersection_sql)
        self.assertIn("LEFT JOIN (SELECT DISTINCT code FROM factor_input_daily", intersection_sql)
        self.assertIn(
            "LEFT JOIN (SELECT DISTINCT code FROM stock_technical_feature_daily",
            intersection_sql,
        )
        self.assertIn("source_trade_date=%s", intersection_sql)
        self.assertIn("SUM(CASE WHEN", intersection_sql)

    def test_mysql_audit_applies_required_dataset_provider_scope(self):
        cursor = IntersectionCursor(99)
        repository = MySQLSentimentSnapshotInputRepository()
        prefixes = required_dataset_code_prefixes(("stock_moneyflow_daily",))

        covered = repository._covered_entity_intersection(
            cursor,
            expected_entities=100,
            required_datasets=("daily_kline", "stock_moneyflow_daily"),
            reference_trade_date=date(2026, 7, 21),
            decision_as_of=DECISION,
            universe_filter_sql=sql_code_prefix_filter("sb.code", prefixes),
        )

        self.assertEqual(covered, 99)
        self.assertIn(
            "LEFT(sb.code, 3) IN ('sh.', 'sz.')",
            cursor.execute_calls[-1][0],
        )

    def test_release_lock_failure_invalidates_pooled_connection(self):
        connection = ReleaseFailingConnection(pooled=True)
        repository = FixedAuditInputRepository(input_audit(), connection)

        with self.assertRaisesRegex(ConnectionError, "release failed"):
            with repository.open_consistent_inputs(
                strategy_meta={"id": "a_share_sentiment"},
                minimum_coverage_ratio=0.98,
            ):
                pass

        self.assertEqual(connection.invalidate_count, 1)
        self.assertEqual(connection.close_count, 0)
        self.assertGreaterEqual(connection.rollback_count, 1)

    def test_release_lock_failure_physically_closes_direct_connection(self):
        connection = ReleaseFailingConnection(pooled=False)
        repository = FixedAuditInputRepository(input_audit(), connection)

        with self.assertRaisesRegex(ConnectionError, "release failed"):
            with repository.open_consistent_inputs(
                strategy_meta={"id": "a_share_sentiment"},
                minimum_coverage_ratio=0.98,
            ):
                pass

        self.assertEqual(connection.invalidate_count, 0)
        self.assertEqual(connection.close_count, 1)


if __name__ == "__main__":
    unittest.main()
