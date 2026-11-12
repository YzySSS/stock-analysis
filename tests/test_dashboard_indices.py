from __future__ import annotations

import unittest
from unittest.mock import patch

import requests

from app.dashboard import index_market as market
from app.shared.cache import InMemoryCacheBackend


class DashboardIndexTests(unittest.TestCase):
    def test_quote_uses_provider_clock_and_symbol(self):
        row = [""] * 34
        row[2], row[3], row[4] = "000001", "3655.83", "3680.64"
        row[30], row[32] = "20261112161427", "-0.67"
        quotes = market.parse_quotes('v_sh000001="' + "~".join(row) + '";')
        self.assertEqual(quotes["sh000001"]["quote_time"], "2026-11-12 16:14:27")
        self.assertEqual(quotes["sh000001"]["change_pct"], -.67)
        row[2] = "399001"
        self.assertEqual(market.parse_quotes('v_sh000001="' + "~".join(row) + '";'), {})

    def test_no_invented_quotes_from_invalid_prices_or_clocks(self):
        for price, clock in [("nan", "20261112161427"), ("-1", "20261112161427"), ("3655", "bad")]:
            row = [""] * 34
            row[2], row[3], row[30] = "000001", price, clock
            self.assertEqual(market.parse_quotes('v_sh000001="' + "~".join(row) + '";'), {})

    def test_minute_preserves_date_and_converts_cumulative_volume(self):
        data = {"code": 0, "data": {"sh000001": {"data": {"date": "20261112", "data": [
            "0930 3673 100 1000", "0931 3676 145 1400", "0931 9999 200 0",
            "1200 9999 300 0", "1300 3660 205 0", "1500 nan 220 0",
        ]}}}}
        result = market.parse_series(data, "sh000001", "minute")
        self.assertEqual(result["trade_date"], "2026-11-12")
        self.assertEqual([p["price"] for p in result["points"]], [3673, 3676, 3660])
        self.assertEqual([p["volume"] for p in result["points"]], [100, 45, 60])

    def test_daily_uses_close_not_open_and_keeps_sixty_points(self):
        data = {"code": 0, "data": {"sz399001": {"day": [
            ["2026-11-12", "200", "100", "205", "99", "10"],
            ["2026-11-11", "190", "95", "200", "90", "9"],
        ]}}}
        result = market.parse_series(data, "sz399001", "daily")
        self.assertEqual([p["price"] for p in result["points"]], [95, 100])
        self.assertEqual(result["trade_date"], "2026-11-12")

    def test_empty_or_wrong_symbol_series_is_unavailable_not_filled(self):
        for payload in [{"code": 1}, {"code": 0, "data": {}}, {"code": 0, "data": {"sz399001": {"day": []}}}]:
            with self.assertRaises(ValueError):
                market.parse_series(payload, "sh000001", "daily")

    def test_malformed_provider_shapes_fail_closed(self):
        for payload in [[], {"code": 0, "data": [1]}, {"code": 0, "data": {"sh000001": [1]}}, {"code": 0, "data": {"sh000001": {"data": [1]}}}]:
            with self.assertRaises(ValueError):
                market.parse_series(payload, "sh000001", "minute")

    def test_allowlist_rejects_arbitrary_symbol_and_period(self):
        with patch.object(market.requests, "get") as get:
            for symbol, period in [("http://evil", "minute"), ("sh000001", "week")]:
                with self.assertRaises(ValueError):
                    market.index_series(symbol, period)
            get.assert_not_called()

    def test_cache_reuses_success_and_identifies_stale_fallback(self):
        cache = InMemoryCacheBackend()
        with patch.object(market, "get_cache_backend", return_value=cache), patch.object(market.time, "time", return_value=1000):
            data = market._cached("test", lambda: {"status": "available", "trade_date": "2026-11-12"})
            self.assertFalse(data["cache_stale"])
            def unavailable():
                raise requests.Timeout()
            self.assertFalse(market._cached("test", unavailable)["cache_stale"])
        with patch.object(market, "get_cache_backend", return_value=cache), patch.object(market.time, "time", return_value=1040):
            data = market._cached("test", unavailable)
            self.assertTrue(data["cache_stale"])
            self.assertEqual(data["trade_date"], "2026-11-12")

    def test_cold_provider_failure_is_explicit(self):
        with patch.object(market, "get_cache_backend", return_value=InMemoryCacheBackend()), patch.object(market.requests, "get", side_effect=requests.Timeout()):
            result = market.index_overview()
        self.assertEqual(len(result["indices"]), 3)
        self.assertTrue(all(row["quote"] is None and row["series"]["status"] == "unavailable" for row in result["indices"]))


if __name__ == "__main__":
    unittest.main()
