from __future__ import annotations

import json
import unittest
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from app.stock_selection.sentiment_snapshot import (
    PublishedSentimentSnapshot,
    SELECTION_CONTRACT_ENVELOPE_KEY,
    SentimentCandidateSnapshotRepository,
    SnapshotIntegrityError,
    validate_sentiment_snapshot,
)
from app.strategies.service import StrategyService


def candidate(
    code: str = "sh.600000",
    *,
    source_time: str = "2026-07-21 09:20:00",
    received_at: str = "2026-07-21 09:21:00",
) -> dict[str, Any]:
    return {
        "code": code,
        "name": "浦发银行",
        "candidate_state": "eligible",
        "is_selected": True,
        "is_tradable": True,
        "rank_no": 1,
        "score": 72.5,
        "source_lineage": [
            {
                "provider": "tushare",
                "batch_id": "quote-20260721-0920",
                "source_time": source_time,
                "received_at": received_at,
            }
        ],
    }


class RecordingCursor:
    def __init__(
        self,
        *,
        fetchone_results: list[Any] | None = None,
        fetchall_results: list[Any] | None = None,
    ) -> None:
        self.execute_calls: list[tuple[str, Any]] = []
        self.executemany_calls: list[tuple[str, Any]] = []
        self._fetchone_results = list(fetchone_results or [])
        self._fetchall_results = list(fetchall_results or [])
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement: str, params: Any = None) -> None:
        self.execute_calls.append((statement, params))

    def executemany(self, statement: str, params: Any) -> None:
        self.executemany_calls.append((statement, list(params)))

    def fetchone(self):
        return self._fetchone_results.pop(0)

    def fetchall(self):
        return self._fetchall_results.pop(0)


class RecordingConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> RecordingCursor:
        return self._cursor


class RecordingFactory:
    def __init__(self, cursor: RecordingCursor) -> None:
        self.cursor = cursor
        self.entries = 0
        self.dict_cursor_values: list[bool] = []

    @contextmanager
    def __call__(self, *, dict_cursor: bool = True):
        self.entries += 1
        self.dict_cursor_values.append(dict_cursor)
        yield RecordingConnection(self.cursor)


class RecordingCache:
    def __init__(self, *, fail_writes: bool = False) -> None:
        self.fail_writes = fail_writes
        self.values: dict[str, Any] = {}
        self.set_calls: list[tuple[str, Any, float | None]] = []

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> bool:
        self.set_calls.append((key, value, ttl_seconds))
        if self.fail_writes:
            raise ConnectionError("cache unavailable")
        self.values[key] = value
        return True


def building_manifest(*, candidate_count: int = 1) -> dict[str, Any]:
    return {
        "id": 22,
        "snapshot_id": "sentiment-snapshot-2",
        "strategy_id": "a_share_sentiment_v05",
        "strategy_version": "0.5.0",
        "trade_date": "2026-07-21",
        "decision_as_of": datetime(2026, 7, 21, 9, 30),
        "status": "building",
        "quality_status": "pending",
        "source_manifest_ids_json": json.dumps([101, 102]),
        "candidate_count": candidate_count,
        "coverage_ratio": 0.99,
        "metadata_json": json.dumps(
            {
                "expected_entity_count": 5000,
                "covered_entity_count": 4950,
                "critical_fields": ["code", "candidate_state", "source_lineage"],
            }
        ),
    }


def stored_candidate(
    *,
    snapshot_id: str = "sentiment-snapshot-2",
    source_time: str = "2026-07-21 09:20:00",
) -> dict[str, Any]:
    row = candidate(source_time=source_time)
    row["snapshot_id"] = snapshot_id
    row["source_lineage_json"] = json.dumps(row.pop("source_lineage"), ensure_ascii=False)
    row["factor_json"] = "{}"
    row["explain_json"] = "{}"
    row["trade_plan_json"] = "{}"
    return row


class SentimentSnapshotValidationTests(unittest.TestCase):
    def test_rejects_coverage_below_98_percent(self):
        result = validate_sentiment_snapshot(
            [candidate()],
            expected_entity_count=5000,
            covered_entity_count=4899,
            decision_as_of="2026-07-21 09:30:00",
            source_manifest_ids=[101],
        )

        self.assertFalse(result.passed)
        self.assertAlmostEqual(result.coverage_ratio, 0.9798)
        self.assertTrue(any("coverage_ratio" in error for error in result.errors))

    def test_rejects_missing_lineage_and_future_source_time(self):
        missing = candidate("sz.000001")
        missing["source_lineage"] = [{"provider": "akshare"}]
        result = validate_sentiment_snapshot(
            [candidate(source_time="2026-07-21 09:31:00"), missing],
            expected_entity_count=100,
            covered_entity_count=100,
            decision_as_of="2026-07-21 09:30:00",
            source_manifest_ids=[101],
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.critical_completeness_ratio, 0.0)
        self.assertTrue(any("after decision_as_of" in error for error in result.errors))
        self.assertTrue(any("batch_id is required" in error for error in result.errors))


class SentimentSnapshotRepositoryTests(unittest.TestCase):
    def test_stage_writes_manifest_and_lineage_rows_without_publishing(self):
        cursor = RecordingCursor()
        factory = RecordingFactory(cursor)
        repository = SentimentCandidateSnapshotRepository(factory)

        result = repository.stage_snapshot(
            snapshot_id="sentiment-snapshot-1",
            strategy_id="a_share_sentiment_v05",
            strategy_version="0.5.0",
            trade_date="2026-07-21",
            decision_as_of="2026-07-21 09:30:00",
            candidates=[candidate()],
            source_manifest_ids=[101, 102],
            expected_entity_count=5000,
            covered_entity_count=4950,
            strategy_config_hash="a" * 64,
        )

        self.assertTrue(result.publishable)
        self.assertEqual((result.status, result.quality_status), ("building", "pending"))
        self.assertEqual(factory.dict_cursor_values, [False])
        self.assertEqual(len(cursor.execute_calls), 1)
        self.assertEqual(len(cursor.executemany_calls), 1)
        manifest_params = cursor.execute_calls[0][1]
        self.assertEqual(manifest_params[0], "sentiment-snapshot-1")
        self.assertEqual(manifest_params[5:7], ("building", "pending"))
        row_params = cursor.executemany_calls[0][1][0]
        lineage = json.loads(row_params[-2])
        self.assertEqual(lineage[0]["provider"], "tushare")
        self.assertEqual(lineage[0]["batch_id"], "quote-20260721-0920")

    def test_stage_and_decode_preserve_v05_selection_contract(self):
        cursor = RecordingCursor()
        repository = SentimentCandidateSnapshotRepository(RecordingFactory(cursor))
        row = {
            **candidate(),
            "trade_grade_state": "tradable",
            "signal_grade": "tradable",
            "validation_status": "shadow_only",
            "score_breakdown": {"catalyst_quality": 18.5},
            "gate_results": {"hard_gate_pass": True},
            "evidence_ids": ["news-1", "news-2"],
            "ai_status": "advisory_only",
            "explain_json": {"summary": "fixture"},
        }

        repository.stage_snapshot(
            snapshot_id="sentiment-v05-contract",
            strategy_id="a_share_sentiment_v05",
            strategy_version="0.5.0",
            trade_date="2026-07-21",
            decision_as_of="2026-07-21 09:30:00",
            candidates=[row],
            source_manifest_ids=[101],
            expected_entity_count=100,
            covered_entity_count=99,
            strategy_config_hash="a" * 64,
        )

        stored_params = cursor.executemany_calls[0][1][0]
        stored_explain = json.loads(stored_params[18])
        self.assertEqual(stored_explain["summary"], "fixture")
        self.assertEqual(
            stored_explain[SELECTION_CONTRACT_ENVELOPE_KEY]["signal_grade"],
            "tradable",
        )
        decoded = repository._decode_candidate(
            {
                **stored_candidate(snapshot_id="sentiment-v05-contract"),
                "trade_grade_state": "tradable",
                "explain_json": json.dumps(stored_explain, ensure_ascii=False),
            }
        )
        self.assertEqual(decoded["signal_grade"], "tradable")
        self.assertEqual(decoded["validation_status"], "shadow_only")
        self.assertEqual(decoded["score_breakdown"], {"catalyst_quality": 18.5})
        self.assertEqual(decoded["gate_results"], {"hard_gate_pass": True})
        self.assertEqual(decoded["evidence_ids"], ["news-1", "news-2"])
        self.assertEqual(decoded["ai_status"], "advisory_only")
        self.assertEqual(decoded["explain_json"], {"summary": "fixture"})

    def test_invalid_stage_is_persisted_as_rejected_not_ready(self):
        cursor = RecordingCursor()
        repository = SentimentCandidateSnapshotRepository(RecordingFactory(cursor))

        result = repository.stage_snapshot(
            snapshot_id="sentiment-snapshot-bad",
            strategy_id="a_share_sentiment_v05",
            strategy_version="0.5.0",
            trade_date="2026-07-21",
            decision_as_of="2026-07-21 09:30:00",
            candidates=[candidate(source_time="2026-07-21 09:31:00")],
            source_manifest_ids=[101],
            expected_entity_count=5000,
            covered_entity_count=4800,
            strategy_config_hash="a" * 64,
        )

        self.assertFalse(result.publishable)
        self.assertEqual((result.status, result.quality_status), ("rejected", "failed"))
        manifest_params = cursor.execute_calls[0][1]
        self.assertEqual(manifest_params[5:7], ("rejected", "failed"))
        self.assertNotIn("ready", manifest_params)

    def test_publish_revalidates_and_atomically_supersedes_previous_ready(self):
        previous = {"snapshot_id": "sentiment-snapshot-1"}
        cursor = RecordingCursor(
            fetchone_results=[building_manifest(), previous],
            fetchall_results=[[stored_candidate()]],
        )
        factory = RecordingFactory(cursor)
        cache = RecordingCache()
        repository = SentimentCandidateSnapshotRepository(factory, cache)

        result = repository.publish_snapshot("sentiment-snapshot-2")

        self.assertEqual((result.status, result.quality_status), ("ready", "passed"))
        self.assertEqual(factory.entries, 1)
        publish_calls = [
            (sql, params)
            for sql, params in cursor.execute_calls
            if "SET status='ready'" in sql
        ]
        self.assertEqual(len(publish_calls), 1)
        self.assertEqual(publish_calls[0][1][-2:], ("sentiment-snapshot-1", "sentiment-snapshot-2"))
        self.assertEqual(len(cache.set_calls), 1)
        cache_key, cache_value, cache_ttl = cache.set_calls[0]
        self.assertEqual(cache_key, "sentiment:snapshot:latest:a_share_sentiment_v05")
        self.assertEqual(cache_value["snapshot_id"], "sentiment-snapshot-2")
        self.assertIn("published_at", cache_value)
        self.assertEqual(cache_ttl, 86_400)
        self.assertEqual(
            repository.latest_snapshot_pointer("a_share_sentiment_v05")["snapshot_id"],
            "sentiment-snapshot-2",
        )

    def test_cache_failure_does_not_fail_database_publication(self):
        cursor = RecordingCursor(
            fetchone_results=[building_manifest(), {}],
            fetchall_results=[[stored_candidate()]],
        )
        repository = SentimentCandidateSnapshotRepository(
            RecordingFactory(cursor),
            RecordingCache(fail_writes=True),
        )

        result = repository.publish_snapshot("sentiment-snapshot-2")

        self.assertEqual((result.status, result.quality_status), ("ready", "passed"))

    def test_failed_publication_does_not_look_up_or_replace_previous_ready(self):
        cursor = RecordingCursor(
            fetchone_results=[building_manifest()],
            fetchall_results=[
                [stored_candidate(source_time="2026-07-21 09:31:00")]
            ],
        )
        repository = SentimentCandidateSnapshotRepository(RecordingFactory(cursor))

        result = repository.publish_snapshot("sentiment-snapshot-2")

        self.assertEqual((result.status, result.quality_status), ("rejected", "failed"))
        statements = "\n".join(sql for sql, _ in cursor.execute_calls)
        self.assertIn("SET status='rejected'", statements)
        self.assertNotIn("SET status='ready'", statements)
        self.assertNotIn("snapshot_id<>%s", statements)

    def test_latest_read_uses_exact_manifest_id_and_decodes_json(self):
        manifest = building_manifest()
        manifest.update(
            {
                "status": "ready",
                "quality_status": "passed",
                "published_at": datetime(2026, 7, 21, 9, 31),
            }
        )
        row = stored_candidate()
        cursor = RecordingCursor(
            fetchone_results=[manifest],
            fetchall_results=[[row]],
        )
        repository = SentimentCandidateSnapshotRepository(RecordingFactory(cursor))

        snapshot = repository.latest_complete_snapshot(
            snapshot_id="sentiment-snapshot-2",
            strategy_id="a_share_sentiment_v05",
            strategy_version="0.5.0",
            decision_as_of="2026-07-21 09:35:00",
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.snapshot_id, "sentiment-snapshot-2")
        self.assertEqual(cursor.execute_calls[0][1][0], "sentiment-snapshot-2")
        self.assertEqual(snapshot.candidates[0]["source_lineage"][0]["provider"], "tushare")
        row_selects = [
            params
            for sql, params in cursor.execute_calls
            if "FROM sentiment_candidate_snapshot\n" in sql
            and "manifest" not in sql
        ]
        self.assertEqual(row_selects, [("sentiment-snapshot-2",)])

    def test_latest_complete_manifest_is_lightweight_and_does_not_load_rows(self):
        manifest = building_manifest()
        manifest.update(
            {
                "status": "ready",
                "quality_status": "passed",
                "published_at": datetime(2026, 7, 21, 9, 31),
            }
        )
        cursor = RecordingCursor(fetchone_results=[manifest])
        repository = SentimentCandidateSnapshotRepository(RecordingFactory(cursor))

        result = repository.latest_complete_manifest(
            strategy_id="a_share_sentiment_v05",
            strategy_version="0.5.0",
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["snapshot_id"], "sentiment-snapshot-2")
        self.assertEqual(len(cursor.execute_calls), 1)
        self.assertIn("sentiment_candidate_snapshot_manifest", cursor.execute_calls[0][0])
        self.assertNotIn("FROM sentiment_candidate_snapshot\n", cursor.execute_calls[0][0])
        self.assertEqual(cursor._fetchall_results, [])

    def test_latest_read_refuses_manifest_row_count_mismatch(self):
        manifest = building_manifest(candidate_count=2)
        manifest.update(
            {
                "status": "ready",
                "quality_status": "passed",
                "published_at": datetime(2026, 7, 21, 9, 31),
            }
        )
        cursor = RecordingCursor(
            fetchone_results=[manifest],
            fetchall_results=[[stored_candidate()]],
        )
        repository = SentimentCandidateSnapshotRepository(RecordingFactory(cursor))

        with self.assertRaises(SnapshotIntegrityError):
            repository.latest_complete_snapshot()


class SentimentSnapshotSelectionReadTests(unittest.TestCase):
    def test_strategy_service_reads_only_selected_rows_from_one_snapshot(self):
        snapshot = PublishedSentimentSnapshot(
            manifest={
                "snapshot_id": "snapshot-ready-1",
                "decision_as_of": "2026-07-21 10:00:00",
                "published_at": "2026-07-21 10:00:01",
                "coverage_ratio": 0.99,
            },
            candidates=(
                {"code": "sh.600000", "is_selected": 1, "score": 72, "factor_json": {}},
                {"code": "sz.000001", "is_selected": 0, "score": 80, "factor_json": {}},
            ),
        )

        class FakeSnapshots:
            def latest_complete_snapshot(self, **kwargs):
                self.kwargs = kwargs
                return snapshot

        service = StrategyService(sentiment_snapshot_repository=FakeSnapshots())
        result = service._published_sentiment_result(
            strategy_meta={
                "id": "a_share_sentiment",
                "version": "0.4.4",
                "display_name": "A股舆情选股",
                "status": "stable",
            },
            serialized_meta={"runtime_ready": True, "validation_status": "unvalidated"},
            limit=3,
            score_threshold=68,
            run_id="run-snapshot-1",
            input_snapshot_id="snapshot-ready-1",
        )

        self.assertEqual(result["input_snapshot_id"], "snapshot-ready-1")
        self.assertEqual([item["code"] for item in result["results"]], ["sh.600000"])
        self.assertEqual(result["diagnostics"]["read_model"], "sentiment_candidate_snapshot")
        self.assertEqual(service.sentiment_snapshots.kwargs["snapshot_id"], "snapshot-ready-1")


if __name__ == "__main__":
    unittest.main()
