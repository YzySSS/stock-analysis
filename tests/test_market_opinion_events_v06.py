from __future__ import annotations

import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from app.data_ingestion.market_opinion_events import (
    aggregate_event_evidence,
    build_event_relation_bundle,
    normalize_event_evidence,
)
from app.stock_selection.selector import StockSelector
from scripts.run_market_opinion_update import persist_v06_event_evidence


DECISION = datetime(2026, 9, 11, 11, 0, 0)


def evidence(
    title: str,
    *,
    source: str = "company_announcement",
    published_at: str = "2026-09-11 10:00:00",
    received_at: str = "2026-09-11 10:01:00",
    primary: bool = True,
    impact: float = 82.0,
) -> dict:
    return {
        "title": title,
        "summary": title,
        "source_id": source,
        "source_name": source,
        "published_at": published_at,
        "source_time": published_at,
        "received_at": received_at,
        "first_seen_at": received_at,
        "is_primary_source": primary,
        "credibility_score": 0.85,
        "impact_score": impact,
    }


class CanonicalEventContractTests(unittest.TestCase):
    def test_aware_source_clock_is_normalized_to_shanghai_database_clock(self):
        row = normalize_event_evidence(
            evidence(
                "甲公司公告签订10亿元订单",
                published_at="2026-09-11T02:59:00Z",
                received_at="2026-09-11T03:00:00Z",
            ),
            stock_codes=["sh.600001"],
            decision_as_of="2026-09-11 11:00:00",
        )

        self.assertEqual(row["source_time"], "2026-09-11 10:59:00")
        self.assertEqual(row["received_at"], "2026-09-11 11:00:00")
        self.assertEqual(row["availability_status"], "available")

    def test_t01_retitle_and_repeated_collection_remain_one_catalyst(self):
        first = normalize_event_evidence(
            evidence("甲公司公告签订10亿元订单，项目分期交付", source="wire-a", primary=False),
            stock_codes=["sh.600001"],
            sectors=["高端制造"],
            decision_as_of=DECISION,
        )
        retitled = normalize_event_evidence(
            evidence("快讯：甲公司签署10亿元合同，项目分期交付", source="wire-b", primary=False),
            stock_codes=["sh.600001"],
            sectors=["高端制造"],
            decision_as_of=DECISION,
        )

        events = aggregate_event_evidence([first, dict(first), retitled])

        self.assertEqual(first["canonical_event_id"], retitled["canonical_event_id"])
        self.assertEqual(first["revision_hash"], retitled["revision_hash"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_revision"], 1)
        self.assertGreaterEqual(events[0]["duplicate_evidence_count"], 1)
        self.assertEqual(events[0]["confirmation_status"], "confirmed_multi_source")

    def test_reposts_from_new_channels_do_not_become_independent_confirmation(self):
        original = normalize_event_evidence(
            evidence("甲公司公告签订10亿元订单", source="wire-a", primary=False),
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )
        repost = normalize_event_evidence(
            {
                **evidence(
                    "甲公司公告签署10亿元合同",
                    source="wire-b",
                    primary=False,
                ),
                "repost_of_evidence_id": original["evidence_id"],
            },
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )

        event = aggregate_event_evidence([original, repost])[0]

        self.assertEqual(event["reported_source_count"], 2)
        self.assertEqual(event["independent_source_count"], 1)
        self.assertEqual(event["confirmation_status"], "pending")

    def test_market_reaction_cannot_confirm_a_weak_business_source(self):
        catalyst = normalize_event_evidence(
            {
                **evidence(
                    "甲公司公告签订10亿元订单",
                    source="wire-a",
                    primary=False,
                ),
                "event_identity_hint": "甲公司-订单A",
            },
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )
        reaction = normalize_event_evidence(
            {
                **evidence(
                    "甲公司股价涨停，资金大幅流入",
                    source="exchange",
                    primary=True,
                ),
                "event_identity_hint": "甲公司-订单A",
            },
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )

        event = aggregate_event_evidence([catalyst, reaction])[0]

        self.assertEqual(reaction["content_role"], "market_reaction")
        self.assertEqual(event["reported_source_count"], 2)
        self.assertEqual(event["independent_source_count"], 1)
        self.assertEqual(event["confirmation_status"], "pending")

    def test_t02_market_reaction_cannot_manufacture_direct_catalyst(self):
        reaction = build_event_relation_bundle(
            [evidence("甲公司股价午后拉升，主力资金净流入", primary=True)],
            code="sh.600001",
            stock_name="甲公司",
            sector_name="高端制造",
            sector_type="theme",
            match_type="direct_news_match",
            match_reason="标题命中甲公司",
            decision_as_of=DECISION,
            snapshot_as_of=DECISION,
        )
        order = build_event_relation_bundle(
            [evidence("甲公司公告签订10亿元订单，项目分期交付", primary=True)],
            code="sh.600001",
            stock_name="甲公司",
            sector_name="高端制造",
            sector_type="theme",
            match_type="direct_news_match",
            match_reason="公告事实命中甲公司",
            decision_as_of=DECISION,
            snapshot_as_of=DECISION,
        )

        self.assertEqual(reaction["events"][0]["content_role"], "market_reaction")
        self.assertEqual(reaction["direct_event_ids"], [])
        self.assertEqual(order["events"][0]["content_role"], "original_catalyst")
        self.assertEqual(len(order["direct_event_ids"]), 1)

    def test_distinct_numberless_policies_do_not_collapse_into_one_event(self):
        robot = normalize_event_evidence(
            evidence("工信部印发机器人产业实施方案", source="government"),
            sectors=["高端制造"],
            decision_as_of=DECISION,
        )
        subsidy = normalize_event_evidence(
            evidence("发改委印发新能源补贴实施方案", source="government"),
            sectors=["高端制造"],
            decision_as_of=DECISION,
        )

        self.assertNotEqual(robot["canonical_event_id"], subsidy["canonical_event_id"])

    def test_material_update_with_explicit_identity_becomes_new_revision(self):
        first = normalize_event_evidence(
            {
                **evidence("甲公司公告中标10亿元项目"),
                "event_identity_hint": "甲公司-项目A",
            },
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )
        update = normalize_event_evidence(
            {
                **evidence("甲公司公告项目A已签订合同并开始分期交付"),
                "event_identity_hint": "甲公司-项目A",
                "content_role": "catalyst_update",
                "received_at": "2026-09-11 10:30:00",
            },
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )

        event = aggregate_event_evidence([first, update])[0]

        self.assertEqual(first["canonical_event_id"], update["canonical_event_id"])
        self.assertEqual(event["event_revision"], 2)
        self.assertEqual(event["delta_from_previous"], "material_update")
        self.assertEqual(event["content_role"], "catalyst_update")

    def test_t03_direct_theme_and_adverse_relations_all_survive_merge(self):
        direct = build_event_relation_bundle(
            [evidence("甲公司公告中标10亿元项目，后续分期交付")],
            code="sh.600001",
            stock_name="甲公司",
            sector_name="高端制造",
            sector_type="theme",
            match_type="direct_news_match",
            match_reason="公告事实命中",
            decision_as_of=DECISION,
            snapshot_as_of=DECISION,
        )
        theme = build_event_relation_bundle(
            [evidence("工信部印发高端制造实施方案", source="government")],
            code="sh.600001",
            stock_name="甲公司",
            sector_name="高端制造",
            sector_type="theme",
            match_type="sector_candidate",
            match_reason="主题候选池",
            decision_as_of=DECISION,
            snapshot_as_of=DECISION,
        )
        adverse = build_event_relation_bundle(
            [evidence("甲公司公告因重大事故停产并下修业绩", impact=95.0)],
            code="sh.600001",
            stock_name="甲公司",
            sector_name="高端制造",
            sector_type="theme",
            match_type="direct_news_match",
            match_reason="风险公告命中",
            decision_as_of=DECISION,
            snapshot_as_of=DECISION,
        )

        merged = StockSelector._merge_v06_bundles(
            [direct, theme, adverse], has_theme_lane=True
        )

        self.assertEqual(set(merged["candidate_lanes"]), {"direct_catalyst", "theme_leader"})
        self.assertTrue(merged["direct_event_ids"])
        self.assertTrue(merged["adverse_event_ids"])
        self.assertEqual(merged["evidence_quality"]["status"], "invalid")
        self.assertGreaterEqual(len(merged["relations"]), 3)

    def test_t05_persistence_comes_from_driver_not_article_freshness(self):
        one_off = normalize_event_evidence(
            evidence("甲公司公告2026年半年报利润预增50%"),
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )
        staged = normalize_event_evidence(
            evidence("甲公司公告签订10亿元订单，未来两年分期交付"),
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )

        self.assertEqual(one_off["received_at"], staged["received_at"])
        self.assertTrue(one_off["is_one_off"])
        self.assertFalse(staged["is_one_off"])
        self.assertGreater(staged["persistence_score"], one_off["persistence_score"])

    def test_snapshot_bridge_and_ledger_copy_of_same_raw_row_are_not_double_counted(self):
        bridge_evidence = normalize_event_evidence(
            {
                **evidence("甲公司公告签订10亿元订单"),
                "raw_id": 77,
                "canonical_event_id": "b" * 64,
                "evidence_id": "1" * 64,
            },
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )
        ledger_evidence = {
            **bridge_evidence,
            "canonical_event_id": "a" * 64,
            "evidence_id": "2" * 64,
        }
        bridge = {
            "events": aggregate_event_evidence([bridge_evidence]),
            "relations": [
                {
                    "relation_id": "3" * 64,
                    "canonical_event_id": "b" * 64,
                    "relation_type": "direct_business",
                    "relation_status": "confirmed_primary",
                }
            ],
        }
        ledger = {
            "events": aggregate_event_evidence([ledger_evidence]),
            "relations": [
                {
                    "relation_id": "4" * 64,
                    "canonical_event_id": "a" * 64,
                    "relation_type": "direct_business",
                    "relation_status": "confirmed_primary",
                }
            ],
        }

        merged = StockSelector._merge_v06_bundles(
            [bridge, ledger], has_theme_lane=False
        )

        self.assertEqual(len(merged["events"]), 1)
        self.assertEqual(merged["events"][0]["canonical_event_id"], "a" * 64)
        self.assertEqual(merged["evidence_ids"], ["2" * 64])
        self.assertEqual(
            [row["relation_id"] for row in merged["relations"]], ["4" * 64]
        )

    def test_positive_wording_without_baseline_does_not_invent_surprise(self):
        row = normalize_event_evidence(
            evidence("甲公司公告利润大幅增长"),
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )

        self.assertEqual(row["expectation_status"], "unknown")
        self.assertIsNone(row["surprise_pct"])

    def test_v06_ingestion_uses_fact_importance_not_legacy_amplified_score(self):
        item = SimpleNamespace(
            item_id="news-1",
            source_id="wire-a",
            source_name="wire-a",
            source_type="realtime",
            title="甲公司公告签订10亿元订单",
            summary=None,
            url="https://example.invalid/news-1",
            published_at=DECISION,
            effective_time=DECISION,
            crawl_time=DECISION,
        )
        with patch(
            "scripts.run_market_opinion_update.persist_market_opinion_event",
            return_value={"relation_count": 0},
        ) as persist:
            persist_v06_event_evidence(
                raw_id=1,
                item=item,
                stock_matches=[],
                sector_matches=[],
                direction="positive",
                event_type="major_order",
                event_importance_score=73.0,
                effective_until=DECISION,
            )

        self.assertEqual(persist.call_args.kwargs["evidence"]["impact_score"], 73.0)
        self.assertNotIn("amplification_score", persist.call_args.kwargs["evidence"])

    def test_t09_late_received_old_timestamp_is_excluded(self):
        row = normalize_event_evidence(
            evidence(
                "甲公司公告签订10亿元订单",
                published_at="2026-09-11 10:00:00",
                received_at="2026-09-11 11:00:01",
            ),
            stock_codes=["sh.600001"],
            decision_as_of=DECISION,
        )
        event = aggregate_event_evidence([row])[0]

        self.assertFalse(row["eligible_at_decision"])
        self.assertEqual(row["after_decision_fields"], ["received_at"])
        self.assertEqual(event["available_evidence_count"], 0)
        self.assertEqual(event["confirmation_status"], "pending")
        self.assertEqual(event["evidence"], [])
        self.assertEqual(event["content_role"], "opinion_repost")

    def test_future_fact_cannot_upgrade_historical_stock_relation(self):
        bundle = build_event_relation_bundle(
            [
                {
                    **evidence("有关部门印发产业实施方案"),
                    "event_identity_hint": "政策事件A",
                },
                {
                    **evidence(
                        "甲公司公告业务中标10亿元订单",
                        published_at="2026-09-11 10:30:00",
                        received_at="2026-09-11 11:00:01",
                    ),
                    "event_identity_hint": "政策事件A",
                },
            ],
            code="sh.600001",
            stock_name="甲公司",
            sector_name="高端制造",
            sector_type="theme",
            match_type="direct_news_match",
            match_reason="历史回放",
            decision_as_of=DECISION,
            snapshot_as_of=DECISION,
        )

        self.assertEqual(len(bundle["events"]), 1)
        self.assertEqual(bundle["events"][0]["excluded_evidence_count"], 1)
        self.assertEqual(bundle["relations"][0]["relation_status"], "unconfirmed")
        self.assertEqual(bundle["direct_event_ids"], [])


if __name__ == "__main__":
    unittest.main()
