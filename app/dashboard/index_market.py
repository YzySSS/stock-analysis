"""Read-only index display data; never used as a strategy input.

Provider clocks are preserved. Missing/malformed responses stay unavailable;
the UI must not synthesize paths from a quote or reuse another index's series.
"""
from __future__ import annotations

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import math
import re
import threading
import time
from typing import Any, Callable

import requests

from app.shared.cache import get_cache_backend

INDICES = {"sh000001": "上证指数", "sz399001": "深证成指", "sz399006": "创业板指"}
_LOCK = threading.Lock()
_REFRESH_LOCK = threading.Lock()
_REFRESHING: set[str] = set()
# At most seven allowlisted display keys; one worker refreshes them serially.
_REFRESH_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="index-display")
_HTTP_LOCAL = threading.local()
_FRESH_SECONDS = 30
_STALE_SECONDS = 900


def _http_session() -> requests.Session:
    # A single overview reads three series from the same provider. Reuse TLS
    # connections without sharing a non-thread-safe Session across API threads.
    if not hasattr(_HTTP_LOCAL, "session"):
        _HTTP_LOCAL.session = requests.Session()
    return _HTTP_LOCAL.session


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def _date(value: str) -> str:
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def parse_quotes(raw: str) -> dict[str, dict[str, Any]]:
    result = {}
    for symbol, content in re.findall(r'v_(sh000001|sz399001|sz399006)="([^"]*)"', raw):
        fields = content.split("~")
        if len(fields) < 34 or fields[2] != symbol[2:]:
            continue
        price = _number(fields[3])
        if price is None or price <= 0:
            continue
        try:
            clock = datetime.strptime(fields[30], "%Y%m%d%H%M%S")
        except ValueError:
            continue
        result[symbol] = {
            "symbol": symbol, "name": INDICES[symbol], "price": price,
            "pre_close": _number(fields[4]), "change_pct": _number(fields[32]),
            "trade_date": clock.date().isoformat(), "quote_time": clock.isoformat(sep=" "),
            "source": "tencent_quote", "timezone": "Asia/Shanghai",
        }
    return result


def parse_series(payload: dict, symbol: str, period: str) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise ValueError("provider error")
    by_symbol = payload.get("data") or {}
    if not isinstance(by_symbol, dict):
        raise ValueError("invalid provider data")
    data = by_symbol.get(symbol) or {}
    if not isinstance(data, dict):
        raise ValueError("invalid index data")
    points = []
    if period == "minute":
        nested = data.get("data") or {}
        if not isinstance(nested, dict):
            raise ValueError("invalid minute data")
        trade_date = _date(str(nested.get("date") or ""))
        previous_volume = 0.0
        previous_clock = ""
        for raw in nested.get("data") or []:
            fields = str(raw).split()
            if len(fields) < 3:
                continue
            clock, price, cumulative = fields[0], _number(fields[1]), _number(fields[2])
            if not re.fullmatch(r"\d{4}", clock) or not ("0930" <= clock <= "1130" or "1300" <= clock <= "1500"):
                continue
            try:
                datetime.strptime(clock, "%H%M")
            except ValueError:
                continue
            if clock <= previous_clock or price is None or price <= 0:
                continue
            volume = max(0, cumulative - previous_volume) if cumulative is not None and cumulative >= previous_volume else None
            if cumulative is not None:
                previous_volume = cumulative
            previous_clock = clock
            points.append({"time": f"{clock[:2]}:{clock[2:]}", "price": price, "volume": volume})
        source = "tencent_index_minute"
    else:
        for row in data.get("day") or []:
            if len(row) < 6:
                continue
            day = datetime.strptime(str(row[0]), "%Y-%m-%d").date().isoformat()
            price = _number(row[2])
            if price is not None and price > 0:
                points.append({"time": day, "price": price, "volume": _number(row[5])})
        points = sorted({p["time"]: p for p in points}.values(), key=lambda p: p["time"])[-60:]
        trade_date = points[-1]["time"] if points else None
        source = "tencent_index_daily"
    if not points:
        raise ValueError("empty index series")
    return {"symbol": symbol, "period": period, "trade_date": trade_date,
            "points": points, "source": source, "timezone": "Asia/Shanghai",
            "volume_unit": "provider_native", "status": "available"}


def _load_cached(cache, key: str, load: Callable[[], dict]) -> dict:
    with _LOCK:
        cached = cache.get(key)
        now = time.time()
        if isinstance(cached, dict) and now < cached.get("refresh_after", cached.get("cached_at", 0) + _FRESH_SECONDS):
            return cached
        try:
            payload = load()
            now = time.time()
            result = {"payload": payload, "cached_at": now, "refresh_after": now + _FRESH_SECONDS}
            cache.set(key, result, ttl_seconds=_STALE_SECONDS)
            return result
        except (requests.RequestException, ValueError, TypeError, KeyError, IndexError):
            now = time.time()
            if isinstance(cached, dict) and now - cached.get("cached_at", 0) < _STALE_SECONDS:
                # Back off after a failed refresh, without extending the life of
                # old provider evidence beyond its original 15-minute bound.
                result = dict(cached, refresh_after=now + _FRESH_SECONDS, refresh_failed=True)
                cache.set(key, result, ttl_seconds=_STALE_SECONDS - (now - cached["cached_at"]))
                return result
            result = {"payload": {"status": "unavailable"}, "cached_at": now,
                      "refresh_after": now + _FRESH_SECONDS, "refresh_failed": True}
            cache.set(key, result, ttl_seconds=_FRESH_SECONDS)
            return result


def _refresh_cached(cache, key: str, load: Callable[[], dict]) -> None:
    try:
        _load_cached(cache, key, load)
    finally:
        with _REFRESH_LOCK:
            _REFRESHING.discard(key)


def _cached(key: str, load: Callable[[], dict]) -> dict:
    cache = get_cache_backend()
    key = "dashboard:index-display:v1:" + key
    cached = cache.get(key)
    now = time.time()
    if isinstance(cached, dict) and now - cached.get("cached_at", 0) < _STALE_SECONDS:
        stale = now - cached["cached_at"] >= _FRESH_SECONDS
        refreshing = False
        if now >= cached.get("refresh_after", cached["cached_at"] + _FRESH_SECONDS):
            with _REFRESH_LOCK:
                if key not in _REFRESHING:
                    _REFRESHING.add(key)
                    try:
                        _REFRESH_EXECUTOR.submit(_refresh_cached, cache, key, load)
                    except RuntimeError:
                        _REFRESHING.discard(key)
                refreshing = key in _REFRESHING
        return dict(cached["payload"], cache_stale=stale, cache_refreshing=refreshing)
    # Only a genuinely cold key waits for the provider. Expired-but-retained
    # display data returns immediately, with its original provider date intact.
    result = _load_cached(cache, key, load)
    return dict(result["payload"], cache_stale=False, cache_refreshing=False)


def index_series(symbol: str, period: str) -> dict:
    if symbol not in INDICES or period not in ("minute", "daily"):
        raise ValueError("unsupported index or period")
    def load():
        url = ("https://web.ifzq.gtimg.cn/appstock/app/minute/query" if period == "minute"
               else "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get")
        params = {"code": symbol} if period == "minute" else {"param": f"{symbol},day,,,60,qfq"}
        response = _http_session().get(url, params=params, timeout=(2, 4))
        response.raise_for_status()
        return parse_series(response.json(), symbol, period)
    return dict(_cached(f"{symbol}:{period}", load), symbol=symbol, name=INDICES[symbol], period=period)


def index_overview() -> dict:
    def load():
        response = _http_session().get("https://qt.gtimg.cn/q=" + ",".join(INDICES), timeout=(2, 4))
        response.raise_for_status()
        quotes = parse_quotes(response.text)
        if not quotes:
            raise ValueError("empty index quotes")
        return {"quotes": quotes, "status": "available"}
    quotes = _cached("quotes", load)
    items = []
    for symbol, name in INDICES.items():
        quote = (quotes.get("quotes") or {}).get(symbol)
        items.append({"symbol": symbol, "name": name, "quote": quote,
                      "quote_cache_stale": quotes.get("cache_stale", False),
                      "quote_cache_refreshing": quotes.get("cache_refreshing", False),
                      "series": index_series(symbol, "minute")})
    return {"indices": items, "display_only": True}
