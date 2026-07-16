from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch
from unittest.mock import MagicMock

from scripts.run_stock_popularity_update import (
    FALLBACK_SOURCE,
    PRIMARY_SOURCE,
    PopularityRow,
    PopularitySourceUnavailable,
    extract_baidu_items,
    fetch_popularity_rows,
    save_rows,
)


def popularity_row(source: str) -> PopularityRow:
    return PopularityRow(
        code="sh.600000",
        name="浦发银行",
        trade_date="2026-07-15",
        quote_time="2026-07-15 15:00:00",
        quote_minute="2026-07-15 15:00:00",
        source=source,
        source_rank=1,
        source_score=None,
        pct_chg=1.0,
        popularity_score=100.0,
        raw_json={},
    )


class BaiduPayloadTests(unittest.TestCase):
    def test_extracts_original_nested_body_shape(self):
        payload = {
            "ResultCode": "0",
            "Result": {"list": {"body": [{"name": "浦发银行", "heat": "123"}]}},
        }

        self.assertEqual(extract_baidu_items(payload), [{"name": "浦发银行", "heat": "123"}])

    def test_403_empty_result_is_explicit_source_error(self):
        with self.assertRaises(PopularitySourceUnavailable) as raised:
            extract_baidu_items({"ResultCode": "403", "Result": []})

        self.assertIn("ResultCode=403", raised.exception.source_errors[PRIMARY_SOURCE])


class PopularityFallbackTests(unittest.TestCase):
    def test_primary_failure_uses_eastmoney_and_reports_partial_source_error(self):
        now = datetime(2026, 7, 15, 15, 55)
        fallback_rows = [popularity_row(FALLBACK_SOURCE)]
        primary_error = PopularitySourceUnavailable({PRIMARY_SOURCE: "ResultCode=403"})

        with (
            patch("scripts.run_stock_popularity_update.fetch_baidu_rows", side_effect=primary_error),
            patch("scripts.run_stock_popularity_update.fetch_eastmoney_rows", return_value=fallback_rows),
        ):
            result = fetch_popularity_rows(now)

        self.assertEqual(result.source_used, FALLBACK_SOURCE)
        self.assertEqual(result.rows, fallback_rows)
        self.assertEqual(result.source_errors, {PRIMARY_SOURCE: "ResultCode=403"})

    def test_primary_success_does_not_call_fallback(self):
        now = datetime(2026, 7, 15, 15, 30)
        primary_rows = [popularity_row(PRIMARY_SOURCE)]

        with (
            patch("scripts.run_stock_popularity_update.fetch_baidu_rows", return_value=primary_rows),
            patch("scripts.run_stock_popularity_update.fetch_eastmoney_rows") as fallback,
        ):
            result = fetch_popularity_rows(now)

        self.assertEqual(result.source_used, PRIMARY_SOURCE)
        self.assertEqual(result.source_errors, {})
        fallback.assert_not_called()


class PopularitySnapshotTests(unittest.TestCase):
    def test_full_snapshot_batch_prunes_codes_missing_from_latest_batch(self):
        conn_context = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        conn_context.__enter__.return_value = conn
        conn.cursor.return_value.__enter__.return_value = cursor
        cursor.rowcount = 1

        with patch("scripts.run_stock_popularity_update.mysql_conn", return_value=conn_context):
            save_rows([popularity_row(FALLBACK_SOURCE)], retention_days=3)

        statements = [str(call.args[0]) for call in cursor.execute.call_args_list]
        self.assertTrue(any("DELETE FROM stock_popularity_snapshot" in statement for statement in statements))


if __name__ == "__main__":
    unittest.main()
