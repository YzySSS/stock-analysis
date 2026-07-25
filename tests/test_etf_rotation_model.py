from __future__ import annotations

import copy
import unittest
from datetime import date, timedelta

from app.etf_rotation.model import build_rotation_candidates
from app.etf_rotation.spec import etf_rotation_spec_hash, load_etf_rotation_spec


class EtfRotationSpecTests(unittest.TestCase):
    def test_spec_is_explicit_research_only_universe(self) -> None:
        spec = load_etf_rotation_spec()

        self.assertEqual("research_only_shadow", spec["status"])
        self.assertEqual("etf", spec["instrument_type"])
        self.assertTrue(spec["allow_cash"])
        self.assertEqual(14, len(spec["sectors"]))
        self.assertEqual(
            len(spec["sectors"]),
            len({item["etf"]["ts_code"] for item in spec["sectors"]}),
        )
        self.assertEqual(
            "baab514e7d96736fac1bfc5383c62ef37aefa8f2c2bc194c28327c493a435a16",
            etf_rotation_spec_hash(),
        )


class EtfRotationModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = copy.deepcopy(load_etf_rotation_spec())
        self.fund_dates = [
            date(2026, 4, 30) + timedelta(days=index)
            for index in range(61)
        ]
        self.trade_date = self.fund_dates[-1].isoformat()
        sector_dates = self.fund_dates[-20:]

        self.sector_rows: list[dict] = []
        self.fund_rows_by_code: dict[str, list[dict]] = {}
        self.opinion_scores: dict[str, dict] = {}
        for sector_index, sector in enumerate(self.spec["sectors"]):
            is_leader = sector["sector_id"] == "bank"
            for current_date in sector_dates:
                for alias in sector["fund_flow_industries"]:
                    self.sector_rows.append(
                        {
                            "trade_date": current_date.isoformat(),
                            "industry_name": alias,
                            "net_amount": 100.0 if is_leader else -10.0 - sector_index,
                            "pct_change": 1.0 if is_leader else -0.2,
                        }
                    )

            code = sector["etf"]["ts_code"]
            self.fund_rows_by_code[code] = [
                {
                    "trade_date": current_date.isoformat(),
                    "close": 1 + row_index * (0.01 if is_leader else 0.0001),
                    "amount_yuan": (
                        1_500_000_000
                        if is_leader
                        else 100_000_000 + sector_index * 1_000_000
                    ),
                    "fund_share_10k": 1_000_000 + row_index * (1000 if is_leader else 1),
                    "nav_date": current_date.isoformat(),
                    "premium_discount_pct": 0.1,
                }
                for row_index, current_date in enumerate(self.fund_dates)
            ]
            self.opinion_scores[sector["sector_id"]] = {
                "trade_date": self.trade_date,
                "as_of_datetime": f"{self.trade_date} 15:45:00",
                "score": 90.0 if is_leader else 30.0,
                "aliases_present": sector["opinion_industries"],
                "alias_coverage": 1.0,
            }

    def build(self, timing_state: str = "risk_on") -> dict:
        return build_rotation_candidates(
            spec=self.spec,
            trade_date=self.trade_date,
            sector_rows=self.sector_rows,
            fund_rows_by_code=self.fund_rows_by_code,
            opinion_scores=self.opinion_scores,
            timing_signal={
                "trade_date": self.trade_date,
                "state": timing_state,
                "timing_score": 65,
            },
        )

    def test_selects_leader_and_allows_empty_days(self) -> None:
        result = self.build()

        self.assertGreaterEqual(result["selected_count"], 1)
        self.assertEqual("bank", result["candidates"][0]["sector_id"])
        self.assertTrue(result["candidates"][0]["is_selected"])

        missing_timing = build_rotation_candidates(
            spec=self.spec,
            trade_date=self.trade_date,
            sector_rows=self.sector_rows,
            fund_rows_by_code=self.fund_rows_by_code,
            opinion_scores=self.opinion_scores,
            timing_signal=None,
        )
        self.assertEqual(0, missing_timing["selection_cap"])
        self.assertEqual(0, missing_timing["selected_count"])

    def test_defensive_timing_caps_selection_at_one(self) -> None:
        self.spec["scoring"]["minimum_sector_score"] = 0
        self.spec["scoring"]["minimum_combined_score"] = 0

        result = self.build("defensive")

        self.assertEqual(1, result["selection_cap"])
        self.assertEqual(1, result["selected_count"])

    def test_exact_alias_contract_rejects_fuzzy_industry_name(self) -> None:
        for row in self.sector_rows:
            if row["industry_name"] == "银行":
                row["industry_name"] = "银行概念"

        result = self.build()
        bank = next(
            item for item in result["candidates"] if item["sector_id"] == "bank"
        )

        self.assertFalse(bank["gates"]["sector_alias_coverage_complete"])
        self.assertFalse(bank["is_eligible"])
        self.assertFalse(result["universe_ready"])

    def test_investability_failure_excludes_one_etf_without_blocking_universe(
        self,
    ) -> None:
        infrastructure_code = next(
            item["etf"]["ts_code"]
            for item in self.spec["sectors"]
            if item["sector_id"] == "infrastructure"
        )
        for row in self.fund_rows_by_code[infrastructure_code]:
            row["amount_yuan"] = 1_000_000

        result = self.build("defensive")
        infrastructure = next(
            item
            for item in result["candidates"]
            if item["sector_id"] == "infrastructure"
        )

        self.assertTrue(result["universe_ready"])
        self.assertTrue(infrastructure["data_complete"])
        self.assertFalse(infrastructure["gates"]["liquidity_sufficient"])
        self.assertFalse(infrastructure["is_eligible"])
        self.assertEqual(1, result["selected_count"])
        self.assertEqual("bank", result["candidates"][0]["sector_id"])


if __name__ == "__main__":
    unittest.main()
