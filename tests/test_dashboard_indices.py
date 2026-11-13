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
        with patch.object(market, "_http_session") as session:
            for symbol, period in [("http://evil", "minute"), ("sh000001", "week")]:
                with self.assertRaises(ValueError):
                    market.index_series(symbol, period)
            session.assert_not_called()

    def test_expired_cache_returns_immediately_and_schedules_only_one_refresh(self):
        cache = InMemoryCacheBackend()
        key = "dashboard:index-display:v1:test"
        try:
            with patch.object(market, "get_cache_backend", return_value=cache), patch.object(market.time, "time", return_value=1000):
                data = market._cached("test", lambda: {"status": "available", "trade_date": "2026-11-12"})
                self.assertFalse(data["cache_stale"])
            with patch.object(market, "get_cache_backend", return_value=cache), patch.object(market.time, "time", return_value=1040), patch.object(market._REFRESH_EXECUTOR, "submit") as submit:
                def blocked_provider():
                    self.fail("expired display cache must not wait for provider")
                for _ in range(3):
                    data = market._cached("test", blocked_provider)
                    self.assertTrue(data["cache_stale"])
                    self.assertTrue(data["cache_refreshing"])
                    self.assertEqual(data["trade_date"], "2026-11-12")
                submit.assert_called_once()
        finally:
            market._REFRESHING.discard(key)

    def test_failed_background_refresh_backs_off_without_extending_evidence_age(self):
        clock = [1000]
        cache = InMemoryCacheBackend(clock=lambda: clock[0])
        key = "dashboard:index-display:v1:test"
        with patch.object(market, "get_cache_backend", return_value=cache), patch.object(market.time, "time", side_effect=lambda: clock[0]):
            market._cached("test", lambda: {"status": "available", "trade_date": "2026-11-12"})
            clock[0] = 1040
            def unavailable():
                raise requests.Timeout()
            market._refresh_cached(cache, key, unavailable)
            with patch.object(market._REFRESH_EXECUTOR, "submit") as submit:
                result = market._cached("test", unavailable)
                self.assertTrue(result["cache_stale"])
                self.assertFalse(result["cache_refreshing"])
                self.assertEqual(result["trade_date"], "2026-11-12")
                submit.assert_not_called()
            self.assertEqual(cache.get(key)["cached_at"], 1000)
            clock[0] = 1901
            self.assertIsNone(cache.get(key))

    def test_successful_background_refresh_keeps_provider_clock_and_replaces_points(self):
        cache = InMemoryCacheBackend()
        key = "dashboard:index-display:v1:test"
        with patch.object(market, "get_cache_backend", return_value=cache), patch.object(market.time, "time", return_value=1000):
            market._cached("test", lambda: {"status": "available", "trade_date": "2026-11-12", "points": [1]})
        with patch.object(market, "get_cache_backend", return_value=cache), patch.object(market.time, "time", return_value=1040):
            market._refresh_cached(cache, key, lambda: {"status": "available", "trade_date": "2026-11-13", "points": [2]})
            result = market._cached("test", lambda: self.fail("fresh result must be reused"))
            self.assertEqual(result["trade_date"], "2026-11-13")
            self.assertEqual(result["points"], [2])
            self.assertFalse(result["cache_stale"])

    def test_cold_provider_failure_is_explicit(self):
        with patch.object(market, "get_cache_backend", return_value=InMemoryCacheBackend()), patch.object(market, "_http_session") as session:
            session.return_value.get.side_effect = requests.Timeout()
            result = market.index_overview()
            second = market.index_overview()
            self.assertEqual(session.return_value.get.call_count, 4)
            self.assertEqual(result, second)
        self.assertEqual(len(result["indices"]), 3)
        self.assertTrue(all(row["quote"] is None and row["series"]["status"] == "unavailable" for row in result["indices"]))


if __name__ == "__main__":
    unittest.main()
