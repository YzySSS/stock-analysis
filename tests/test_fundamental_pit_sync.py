from __future__ import annotations

import unittest

from app.data_ingestion.fundamental_pit_sync import (
    FundamentalPitSync,
    quarter_end_periods,
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
    def __init__(self, pages=None):
        self.pages = pages or {}
        self.calls = []

    def fina_indicator_vip(self, **kwargs):
        self.calls.append(kwargs)
        return FakeFrame(self.pages.get(kwargs["offset"], []))


class GuardedSync(FundamentalPitSync):
    def __init__(self, records, existing_codes):
        super().__init__(pro=FakePro())
        self.records = records
        self.existing_codes = existing_codes
        self.manifests = []

    def _manifest(self, period, status, run_id, **kwargs):
        self.manifests.append((period, status, run_id, kwargs))

    def _fetch_period(self, period, page_size):
        return list(self.records), 1

    def _existing_period_codes(self, period):
        return self.existing_codes


class FundamentalPitSyncTests(unittest.TestCase):
    def test_quarter_end_periods_are_inclusive_and_validated(self):
        self.assertEqual(
            quarter_end_periods("20231231", "20240930"),
            ["20231231", "20240331", "20240630", "20240930"],
        )
        with self.assertRaisesRegex(ValueError, "calendar quarter ends"):
            quarter_end_periods("20240131", "20240331")
        with self.assertRaisesRegex(ValueError, "must not be after"):
            quarter_end_periods("20240630", "20240331")

    def test_period_fetch_paginates_until_a_short_page(self):
        pro = FakePro(
            {
                0: [{"ts_code": "600000.SH"}, {"ts_code": "000001.SZ"}],
                2: [{"ts_code": "000002.SZ"}],
            }
        )
        sync = FundamentalPitSync(pro=pro)

        rows, pages = sync._fetch_period("20241231", page_size=2)

        self.assertEqual(len(rows), 3)
        self.assertEqual(pages, 2)
        self.assertEqual([call["offset"] for call in pro.calls], [0, 2])
        self.assertTrue(all(call["period"] == "20241231" for call in pro.calls))

    def test_normalization_keeps_announcement_versions_and_profit_fallback(self):
        records = [
            {
                "ts_code": "600000.SH",
                "ann_date": "20250329",
                "end_date": "20241231",
                "update_flag": "1",
                "roe": "9.25",
                "roa": "0.75",
                "grossprofit_margin": None,
                "profit_to_gr": "31.5",
                "or_yoy": "4.2",
                "profit_yoy": None,
                "netprofit_yoy": "8.6",
                "eps": "1.11",
            },
            {
                "ts_code": "NOT-IN-UNIVERSE",
                "ann_date": "20250329",
                "end_date": "20241231",
            },
        ]

        rows, stats = FundamentalPitSync._normalize_rows(
            records,
            {"600000.SH": "sh.600000"},
            "pit-test",
            "2026-07-17 18:00:00",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row[:5], ("sh.600000", "600000.SH", "2025-03-29", "2024-12-31", "1"))
        self.assertEqual(row[8], 31.5)
        self.assertEqual(row[9], 4.2)
        self.assertEqual(row[10], 8.6)
        self.assertEqual(row[11], 1.11)
        self.assertEqual(stats["skipped_outside_universe"], 1)
        self.assertEqual(stats["valid_field_rows"], 1)

    def test_refresh_rejects_source_coverage_below_existing_floor(self):
        sync = GuardedSync(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20250329",
                    "end_date": "20241231",
                    "update_flag": "1",
                    "roe": 9.25,
                }
            ],
            existing_codes=100,
        )

        result = sync.sync_period(
            "20241231",
            "pit-test",
            {"600000.SH": "sh.600000"},
        )

        self.assertEqual(result["status"], "partial_success")
        self.assertEqual(result["coverage_floor"], 80)
        self.assertEqual(result["distinct_codes"], 1)
        self.assertEqual([item[1] for item in sync.manifests], ["running", "partial_success"])

    def test_first_refresh_of_a_mature_period_requires_half_the_lifecycle_universe(self):
        sync = GuardedSync(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20230329",
                    "end_date": "20221231",
                    "update_flag": "1",
                    "roe": 9.25,
                }
            ],
            existing_codes=0,
        )
        lifecycle = {"600000.SH": "sh.600000"}
        lifecycle.update({f"{index:06d}.SZ": f"sz.{index:06d}" for index in range(1, 1000)})

        result = sync.sync_period("20221231", "pit-test", lifecycle)

        self.assertEqual(result["status"], "partial_success")
        self.assertTrue(result["mature_period"])
        self.assertEqual(result["coverage_floor"], 500)


if __name__ == "__main__":
    unittest.main()
