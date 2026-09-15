from __future__ import annotations

import unittest
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

from app.data_ingestion.market_opinion_event_repository import (
    persist_market_opinion_event,
)


class RecordingCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=None) -> None:
        self.last_sql = " ".join(sql.split())
        self.executed.append((self.last_sql, params))

    def fetchone(self):
        if "SELECT current_revision" in self.last_sql:
            return None
        if "SELECT event_revision" in self.last_sql:
            return None
        if "SELECT COUNT(*)" in self.last_sql:
            return (0,)
        if "MAX(CASE" in self.last_sql:
            return (1, 1, 1)
        return None


class RecordingConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class MarketOpinionEventRepositoryV06Tests(unittest.TestCase):
    def test_evidence_insert_is_placeholder_complete_and_append_only(self):
        cursor = RecordingCursor()

        @contextmanager
        def fake_connection(**_kwargs):
            yield RecordingConnection(cursor)

        with patch(
            "app.data_ingestion.market_opinion_event_repository.mysql_conn",
            fake_connection,
        ):
            result = persist_market_opinion_event(
                raw_id=17,
                evidence={
                    "source_id": "company_announcement",
                    "source_name": "公司公告",
                    "source_type": "announcement",
                    "title": "甲公司公告签订10亿元订单",
                    "summary": "甲公司公告签订10亿元订单，未来两年分期交付",
                    "published_at": "2026-09-15 10:00:00",
                    "source_time": "2026-09-15 10:00:00",
                    "received_at": "2026-09-15 10:00:05",
                    "impact_score": 90,
                    "event_type": "major_order",
                    "direction": "positive",
                },
                stock_matches=[
                    {
                        "code": "sh.600001",
                        "name": "甲公司",
                        "match_reason": "公告事实命中",
                        "relation_context": "甲公司公告签订10亿元订单",
                    }
                ],
                sector_matches=[],
            )

        self.assertEqual(result["event_revision"], 1)
        self.assertTrue(result["event_confirmed"])
        evidence_inserts = [
            (sql, params)
            for sql, params in cursor.executed
            if "INSERT INTO market_opinion_event_evidence" in sql
        ]
        self.assertEqual(len(evidence_inserts), 1)
        evidence_sql, evidence_params = evidence_inserts[0]
        self.assertEqual(evidence_sql.count("%s"), len(evidence_params))
        self.assertIn("ON DUPLICATE KEY UPDATE evidence_id=evidence_id", evidence_sql)
        self.assertFalse(
            any(
                sql.startswith("UPDATE market_opinion_event_evidence")
                for sql, _params in cursor.executed
            )
        )
        relation_inserts = [
            (sql, params)
            for sql, params in cursor.executed
            if "INSERT INTO market_opinion_stock_relation" in sql
        ]
        self.assertEqual(len(relation_inserts), 1)
        relation_sql, relation_params = relation_inserts[0]
        self.assertEqual(relation_sql.count("%s"), len(relation_params))
        self.assertIn(
            "ON DUPLICATE KEY UPDATE relation_id=relation_id", relation_sql
        )
        self.assertNotIn("relation_score=VALUES", relation_sql)


if __name__ == "__main__":
    unittest.main()
