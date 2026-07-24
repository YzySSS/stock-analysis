from __future__ import annotations

import unittest
from contextlib import contextmanager

from app.stock_selection.trade_plan_events import (
    TradePlanEventRepository,
    immutable_trade_plan_id,
)


class FakeCursor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple]]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def executemany(self, sql, params):
        rows = list(params)
        self.executemany_calls.append((sql, rows))
        return len(rows)


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def connection_factory(cursor: FakeCursor):
    @contextmanager
    def factory(**_kwargs):
        yield FakeConnection(cursor)

    return factory


class TradePlanEventTests(unittest.TestCase):
    def test_plan_id_is_stable_and_source_scoped(self):
        first = immutable_trade_plan_id(
            source_kind="forward",
            source_id="obs-1",
            code="sh.600000",
            spec_hash="a" * 64,
        )
        second = immutable_trade_plan_id(
            source_kind="forward",
            source_id="obs-1",
            code="sh.600000",
            spec_hash="a" * 64,
        )
        other = immutable_trade_plan_id(
            source_kind="forward",
            source_id="obs-2",
            code="sh.600000",
            spec_hash="a" * 64,
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertLessEqual(len(first), 128)

    def test_append_is_idempotent_at_database_boundary(self):
        cursor = FakeCursor()
        repository = TradePlanEventRepository(
            connection_factory=connection_factory(cursor)
        )
        event = {
            "plan_id": "forward:abc",
            "selection_result_id": None,
            "snapshot_id": "snapshot-1",
            "code": "sh.600000",
            "trade_plan_version": "selection_trade_plan_v4_turtle_risk",
            "spec_hash": "a" * 64,
            "event_time": "2026-07-24 15:00:00",
            "event_type": "plan_created",
            "planned_price": 10.0,
            "observed_price": 9.9,
            "executable": False,
            "block_reason": None,
            "metadata": {"source": "test"},
        }

        affected = repository.append([event])

        self.assertEqual(affected, 1)
        sql, rows = cursor.executemany_calls[0]
        self.assertIn("INSERT IGNORE", sql)
        self.assertEqual(rows[0][0], "forward:abc")
        self.assertEqual(rows[0][7], "plan_created")


if __name__ == "__main__":
    unittest.main()
