from __future__ import annotations

import unittest
import json
from datetime import date
from decimal import Decimal

from scripts.invalidate_a_share_sentiment_legacy_results import (
    CURRENT_STRATEGY_VERSION,
    INVALIDATION_REASON,
    audit_legacy_results,
    invalidate_legacy_results,
)


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 0
        self._fetchone = []
        self._fetchall = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._fetchone.pop(0)

    def fetchall(self):
        return self._fetchall.pop(0)


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class SentimentLegacyInvalidationTests(unittest.TestCase):
    def test_audit_groups_all_versions_except_current(self):
        cursor = FakeCursor()
        cursor._fetchone = [
            {
                "result_count": 136,
                "included_count": Decimal("24"),
                "first_trade_date": date(2026, 5, 15),
                "last_trade_date": date(2026, 7, 18),
            }
        ]
        cursor._fetchall = [[{"strategy_version": "0.3.1", "result_count": 136, "included_count": Decimal("24")}]]

        result = audit_legacy_results(FakeConnection(cursor))

        self.assertEqual(result["result_count"], 136)
        self.assertEqual(result["included_count"], 24)
        self.assertEqual(result["first_trade_date"], "2026-05-15")
        json.dumps(result)
        self.assertEqual(result["current_strategy_version"], CURRENT_STRATEGY_VERSION)
        self.assertEqual(len(cursor.executed), 2)
        for _, params in cursor.executed:
            self.assertEqual(params, ("a_share_sentiment", CURRENT_STRATEGY_VERSION))

    def test_invalidation_labels_evidence_without_excluding_statistics(self):
        cursor = FakeCursor()
        cursor.rowcount = 136

        updated = invalidate_legacy_results(FakeConnection(cursor), "2026-07-21 21:00:00")

        self.assertEqual(updated, 136)
        sql, params = cursor.executed[0]
        self.assertIn("JSON_SET", sql)
        self.assertIn("legacy_include_in_stats_before_invalidation", sql)
        self.assertNotIn("include_in_stats = 0", sql)
        self.assertNotIn("DELETE", sql.upper())
        self.assertEqual(
            params,
            (
                INVALIDATION_REASON,
                "2026-07-21 21:00:00",
                "a_share_sentiment",
                CURRENT_STRATEGY_VERSION,
            ),
        )


if __name__ == "__main__":
    unittest.main()
