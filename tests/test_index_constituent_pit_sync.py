from __future__ import annotations

import unittest

from app.data_ingestion.index_constituent_pit_sync import (
    IndexConstituentPitSync,
    month_bounds,
    month_periods,
)


class FakeFrame:
    def __init__(self, rows):
        self.rows = list(rows)
        self.empty = not self.rows

    def to_dict(self, orient):
        if orient != "records":
            raise AssertionError(orient)
        return list(self.rows)


class FakePro:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.calls = []

    def index_weight(self, **kwargs):
        self.calls.append(kwargs)
        return FakeFrame(self.rows)


class IndexConstituentPitSyncTests(unittest.TestCase):
    def test_month_helpers_are_inclusive_and_calendar_aware(self):
        self.assertEqual(
            month_periods("202312", "202403"),
            ["202312", "202401", "202402", "202403"],
        )
        self.assertEqual(month_bounds("202402"), ("20240201", "20240229"))
        with self.assertRaisesRegex(ValueError, "must not be after"):
            month_periods("202402", "202401")

    def test_fetch_uses_one_bounded_month_and_requested_fields(self):
        pro = FakePro([{"index_code": "000300.SH"}])
        sync = IndexConstituentPitSync(pro=pro)

        rows = sync._fetch_month("000300.SH", "202401")

        self.assertEqual(len(rows), 1)
        self.assertEqual(pro.calls[0]["start_date"], "20240101")
        self.assertEqual(pro.calls[0]["end_date"], "20240131")
        self.assertIn("con_code", pro.calls[0]["fields"])

    def test_normalization_maps_local_codes_and_preserves_effective_weight(self):
        rows, stats = IndexConstituentPitSync._normalize_rows(
            [
                {
                    "index_code": "000300.SH",
                    "con_code": "600000.SH",
                    "trade_date": "20240131",
                    "weight": "0.52",
                },
                {
                    "index_code": "000300.SH",
                    "con_code": "999999.SH",
                    "trade_date": "20240131",
                    "weight": "0.48",
                },
            ],
            requested_index_code="000300.SH",
            requested_month="202401",
            lifecycle_map={"600000.SH": "sh.600000"},
            run_id="pit-index-test",
            synced_at="2026-07-17 18:30:00",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][:5], ("000300.SH", "sh.600000", "600000.SH", "2024-01-31", 0.52))
        self.assertEqual(stats["snapshot_dates"], ["2024-01-31"])
        self.assertEqual(stats["snapshot_metrics"][0]["source_codes"], 2)
        self.assertEqual(stats["snapshot_metrics"][0]["matched_codes"], 1)
        self.assertEqual(stats["unmatched_codes"], ["999999.SH"])

    def test_guard_rejects_short_membership_or_invalid_weight_sum(self):
        errors = IndexConstituentPitSync._validation_errors(
            [
                {
                    "effective_date": "2024-01-31",
                    "source_codes": 200,
                    "matched_codes": 200,
                    "weight_sum": 80,
                }
            ],
            expected=300,
        )

        self.assertEqual(len(errors), 3)
        self.assertTrue(any("source members" in item for item in errors))
        self.assertTrue(any("matched members" in item for item in errors))
        self.assertTrue(any("weight sum" in item for item in errors))

        rounded_floor_errors = IndexConstituentPitSync._validation_errors(
            [
                {
                    "effective_date": "2024-01-31",
                    "source_codes": 47,
                    "matched_codes": 47,
                    "weight_sum": 100,
                }
            ],
            expected=50,
        )
        self.assertTrue(any("outside 48-52" in item for item in rounded_floor_errors))

    def test_all_a_is_not_accepted_as_an_index_partition(self):
        sync = IndexConstituentPitSync(pro=FakePro())
        with self.assertRaisesRegex(ValueError, "configured index universes"):
            sync.sync_month("ALL_A", "202401", "run", {})


if __name__ == "__main__":
    unittest.main()
