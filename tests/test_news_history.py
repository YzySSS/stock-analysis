from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from app.data_ingestion.news_history import (
    MajorNewsBackfill, NewsHistoryArchive, TZ, capture_news, normalize_news, strict_asof,
)


class FakeFrame:
    columns = ("title", "pub_time", "src", "url")

    def __init__(self, rows):
        self.rows = rows

    def to_dict(self, orient):
        return self.rows


class NewsHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.archive = NewsHistoryArchive(self.temp.name)

    def row(self, **kwargs):
        return normalize_news({"title": "公司公告", "url": "https://example.com/1",
            "content": kwargs.pop("content", "原文"), "datetime": kwargs.pop("published", "2026-11-02 08:00:00")},
            provider="test", mode=kwargs.pop("mode", "live_observation"),
            observed_at=kwargs.pop("observed", "2026-11-02T09:00:00+08:00"), **kwargs)

    def test_versions_preserved_and_asof_never_uses_later_revision(self):
        first = self.row()
        revised = self.row(content="已修订", observed="2026-11-02T10:00:00+08:00")
        self.assertEqual(first["article_id"], revised["article_id"])
        self.assertNotEqual(first["version_sha256"], revised["version_sha256"])
        self.assertEqual(strict_asof([first,revised], "2026-11-02 09:30:00"), [first])
        self.assertEqual(strict_asof([first,revised], "2026-11-02 08:30:00"), [])
        self.assertEqual(strict_asof([first,revised], "2026-11-02 10:30:00"), [revised])

    def test_provider_date_does_not_prove_morning_visibility(self):
        row = self.row(published="2026-11-02")
        self.assertEqual(row["publication_precision"], "date")
        self.assertEqual(strict_asof([row], "2026-11-02 09:30:00"), [])
        self.assertEqual(len(strict_asof([row], "2026-11-03 00:00:00")), 1)

    def test_same_clock_conflicting_versions_fail_closed(self):
        rows = [self.row(content="初版"), self.row(content="新版")]
        self.assertEqual(strict_asof(rows, "2026-11-02 09:30:00"), [])
        newer = self.row(content="后续已确认",observed="2026-11-02 10:00:00")
        self.assertEqual(strict_asof(rows+[newer], "2026-11-02 10:30:00"), [newer])

    def test_newsnow_retains_raw_date_precision_and_does_not_claim_synthetic_provider_id(self):
        row = normalize_news({"title": "只有标题", "item_id": "只有标题",
            "published_at": datetime(2026, 11, 2), "raw": {"title": "只有标题", "pubDate": "2026-11-02"}},
            provider="newsnow.cls", mode="live_observation", observed_at="2026-11-02 09:00:00")
        self.assertEqual(row["identity_kind"], "synthetic_title")
        self.assertIsNone(row["provider_original_id"])
        self.assertEqual(row["provider_published_raw"], "2026-11-02")
        self.assertEqual(row["publication_precision"], "date")
        self.assertEqual(strict_asof([row], "2026-11-02 09:30:00"), [])
        self.assertEqual(len(strict_asof([row], "2026-11-03 00:00:00")), 1)

    def test_newsnow_upstream_epoch_timezone_is_not_lost_to_adapter_normalization(self):
        row = normalize_news({"title": "有来源ID", "item_id": "标题替代ID",
            "published_at": datetime(2026, 11, 2, 1),
            "raw": {"id": "real-123", "pubDate": "2026-11-02T01:00:00Z"}},
            provider="newsnow.cls", mode="live_observation", observed_at="2026-11-02 08:00:00")
        self.assertEqual(row["provider_original_id"], "real-123")
        self.assertEqual(row["provider_published_at"], "2026-11-02T09:00:00+08:00")
        self.assertEqual(strict_asof([row], "2026-11-02 08:30:00"), [])
        self.assertEqual(len(strict_asof([row], "2026-11-02 09:00:00")), 1)
        epoch = datetime(2026, 11, 2, 9, tzinfo=TZ).timestamp() * 1000
        numeric = normalize_news({"title": "数值时间", "raw": {"id": "real-124", "pubDate": epoch}},
            provider="newsnow.cls", mode="live_observation", observed_at="2026-11-02 08:00:00")
        self.assertEqual(numeric["provider_published_at"], "2026-11-02T09:00:00+08:00")
        self.assertEqual(numeric["publication_precision"], "timestamp")

    def test_rss_publication_claim_preserves_rfc_timezone(self):
        row = self.row(published="Mon, 02 Nov 2026 01:00:00 GMT")
        self.assertEqual(row["provider_published_at"], "2026-11-02T09:00:00+08:00")
        self.assertEqual(row["publication_precision"], "timestamp")

    def test_backfill_and_legacy_excluded_even_after_import(self):
        rows = [self.row(mode=mode) for mode in ("historical_backfill", "legacy_inventory")]
        self.assertEqual(strict_asof(rows, "2026-11-17 23:00:00"), [])
        self.assertTrue(all(not r["historical_first_seen_proven"] for r in rows))

    def test_payload_and_observation_tampering_detected(self):
        ref = capture_news([{"title":"新闻","content":"全文"}], provider="test", archive=self.archive)
        path = self.archive._path("news",ref["sha256"])
        bundle = self.archive.load("news",ref["sha256"])
        bundle["rows"][0]["observed_at"] = "2020-01-01"
        path.write_bytes(gzip.compress(json.dumps(bundle).encode()))
        with self.assertRaisesRegex(ValueError,"hash mismatch"):
            self.archive.load("news",ref["sha256"])

    def test_quota_failure_visible_and_does_not_delete_evidence(self):
        ref = capture_news([{"title":"新闻"}], provider="test", archive=self.archive)
        full = NewsHistoryArchive(self.temp.name,max_bytes=0)
        result = capture_news([{"title":"另一条"}], provider="test", archive=full)
        self.assertEqual(result["status"], "warning")
        self.assertTrue(self.archive.load("news",ref["sha256"]))

    def test_provider_cap_splits_and_boundary_duplicates_are_deduplicated(self):
        def fetch(**kwargs):
            lo, hi = kwargs["start_date"], kwargs["end_date"]
            if lo.endswith("00:00:00") and hi.endswith("02:00:00"):
                return FakeFrame([{},{}])
            return FakeFrame([{"title":"边界新闻", "pub_time":"2026-11-02 01:00:00", "src":"财联社", "url":"https://example.com/2"}])
        pro = Mock()
        pro.major_news.side_effect = fetch
        result = MajorNewsBackfill(pro, archive=self.archive,response_cap=2,min_interval=0).run(
            datetime(2026,11,2),datetime(2026,11,2,2))
        self.assertEqual(result["status"],"retrieved")
        self.assertEqual(result["calls"],3)
        self.assertEqual(result["unique_rows"],1)
        self.assertEqual([w["unique_rows"] for w in result["windows"]],[1,0])
        self.assertFalse(result["provider_coverage_complete_proven"])

    def test_unresolved_single_second_cap_never_claims_success(self):
        pro = Mock()
        pro.major_news.return_value = FakeFrame([{},{}])
        result = MajorNewsBackfill(pro, archive=self.archive,response_cap=2,min_interval=0).run(
            datetime(2026,11,2),datetime(2026,11,2,0,0,1))
        self.assertEqual(result["status"],"partial")
        self.assertEqual(result["windows"][0]["status"],"unresolved_provider_cap")

    def test_call_budget_and_out_of_window_provider_rows_explicit(self):
        pro = Mock()
        pro.major_news.return_value = FakeFrame([{},{}])
        result = MajorNewsBackfill(pro, archive=self.archive,response_cap=2,max_calls=1,min_interval=0).run(
            datetime(2026,11,2),datetime(2026,11,2,2))
        self.assertEqual(result["status"],"partial")
        self.assertTrue(any(w["status"] == "call_budget_exhausted" for w in result["windows"]))
        pro.major_news.return_value = FakeFrame([{"title":"旧新闻", "pub_time":"2026-10-01"}])
        result = MajorNewsBackfill(pro, archive=self.archive,min_interval=0).run(datetime(2026,11,2),datetime(2026,11,2,2))
        self.assertEqual(result["status"],"partial")
        self.assertEqual(result["windows"][0]["status"],"provider_error")

    def test_provider_empty_window_retained_not_invented(self):
        pro = Mock()
        pro.major_news.return_value = FakeFrame([])
        result = MajorNewsBackfill(pro, archive=self.archive,min_interval=0).run(datetime(2026,11,2),datetime(2026,11,2,2))
        self.assertEqual(result["status"],"retrieved")
        self.assertEqual(result["unique_rows"],0)
        self.assertEqual(result["windows"][0]["provider_rows"],0)

    def test_provider_hourly_throttle_stops_calls_and_preserves_unfetched_windows(self):
        pro = Mock()
        pro.major_news.side_effect = RuntimeError("频率超限(30次/小时)")
        result = MajorNewsBackfill(pro,archive=self.archive,min_interval=0).run(
            datetime(2026,11,2),datetime(2026,11,2,18))
        self.assertEqual(pro.major_news.call_count,1)
        self.assertEqual(result["status"],"partial")
        self.assertEqual(len(result["windows"]),3)
        self.assertEqual(result["windows"][-1]["status"],"provider_throttle_deferred")
        self.assertIsNotNone(result["provider_throttle"]["retry_not_before"])

    def test_resume_reads_only_failed_windows(self):
        pro = Mock()
        pro.major_news.side_effect = [FakeFrame([]),RuntimeError("limited")]
        job = MajorNewsBackfill(pro,archive=self.archive,min_interval=0)
        result = job.run(datetime(2026,11,2),datetime(2026,11,2,12))
        previous = self.archive.load("news_manifest",result["manifest"]["sha256"])
        pro.major_news.side_effect = None
        pro.major_news.return_value = FakeFrame([])
        pro.major_news.reset_mock()
        resumed = MajorNewsBackfill(pro,archive=self.archive,min_interval=0).run(
            datetime(2026,11,2),datetime(2026,11,2,12),previous=previous)
        self.assertEqual(resumed["status"],"retrieved")
        self.assertEqual(pro.major_news.call_count,1)
        self.assertEqual(pro.major_news.call_args.kwargs["start_date"],"2026-11-02 06:00:00")
        self.assertEqual(resumed["resumed_from_manifest_sha256"],result["manifest"]["sha256"])

    def test_resume_before_provider_cooldown_never_calls_network(self):
        pro = Mock()
        pro.major_news.side_effect = RuntimeError("频率超限(30次/小时)")
        initial = MajorNewsBackfill(pro, archive=self.archive, min_interval=0).run(
            datetime(2026,11,2),datetime(2026,11,2,12))
        previous = self.archive.load("news_manifest", initial["manifest"]["sha256"])
        pro.major_news.reset_mock()
        resumed = MajorNewsBackfill(pro, archive=self.archive, min_interval=0).run(
            datetime(2026,11,2),datetime(2026,11,2,12),previous=previous)
        pro.major_news.assert_not_called()
        self.assertEqual(resumed["calls"], 0)
        self.assertEqual(resumed["status"], "partial")
        self.assertEqual(resumed["provider_throttle"], initial["provider_throttle"])
        previous["provider_throttle"]["retry_not_before"] = (datetime.now(TZ)-timedelta(seconds=1)).isoformat()
        pro.major_news.side_effect = None
        pro.major_news.return_value = FakeFrame([])
        after_cooldown = MajorNewsBackfill(pro, archive=self.archive, min_interval=0).run(
            datetime(2026,11,2),datetime(2026,11,2,12),previous=previous)
        self.assertEqual(after_cooldown["calls"], 2)
        self.assertEqual(after_cooldown["status"], "retrieved")

    def test_exclusive_final_end_does_not_import_next_day(self):
        pro = Mock()
        pro.major_news.return_value = FakeFrame([{"title":"下一日", "pub_time":"2026-11-03 00:00:00", "src":"媒体", "url":"https://example.com/3"}])
        result = MajorNewsBackfill(pro,archive=self.archive,min_interval=0).run(
            datetime(2026,11,2,23),datetime(2026,11,3))
        self.assertEqual(result["unique_rows"],0)

    def test_live_aggregator_capture_precedes_dedup_and_filter(self):
        from app.data_ingestion.news_provider import NewsAggregator
        aggregator = NewsAggregator()
        aggregator.akshare.get_stock_news = Mock(return_value=[{"title":"重复","content":"初版"},{"title":"重复","content":"新版"}])
        with patch("app.data_ingestion.news_history.capture_news", return_value={"status":"sealed","rows":2}) as capture:
            result = aggregator.get_stock_news("000001","平安银行",sources=["akshare"])
        self.assertEqual(len(result),1)
        self.assertEqual(len(capture.call_args.args[0]),2)
        self.assertEqual(aggregator.archive_summary()["rows"],2)


if __name__ == "__main__":
    unittest.main()
