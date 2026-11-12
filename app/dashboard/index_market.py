"""Read-only index display data; never used as a strategy input.

Provider clocks are preserved. Missing/malformed responses stay unavailable;
the UI must not synthesize paths from a quote or reuse another index's series.
"""
from __future__ import annotations

from datetime import datetime
import math
import re
import threading
import time
from typing import Any, Callable

import requests

from app.shared.cache import get_cache_backend

INDICES = {"sh000001": "上证指数", "sz399001": "深证成指", "sz399006": "创业板指"}
_LOCK = threading.Lock()
_FRESH_SECONDS = 30
_STALE_SECONDS = 900


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


def _cached(key: str, load: Callable[[], dict]) -> dict:
    cache = get_cache_backend()
    key = "dashboard:index-display:v1:" + key
    cached = cache.get(key)
    now = time.time()
    if isinstance(cached, dict) and now - cached.get("cached_at", 0) < _FRESH_SECONDS:
        return dict(cached["payload"], cache_stale=False)
    with _LOCK:
        cached = cache.get(key)
        if isinstance(cached, dict) and now - cached.get("cached_at", 0) < _FRESH_SECONDS:
            return dict(cached["payload"], cache_stale=False)
        try:
            payload = load()
            cache.set(key, {"payload": payload, "cached_at": now}, ttl_seconds=_STALE_SECONDS)
            return dict(payload, cache_stale=False)
        except (requests.RequestException, ValueError, TypeError, KeyError, IndexError):
            if isinstance(cached, dict) and now - cached.get("cached_at", 0) < _STALE_SECONDS:
                return dict(cached["payload"], cache_stale=True)
            return {"status": "unavailable", "cache_stale": False}


def index_series(symbol: str, period: str) -> dict:
    if symbol not in INDICES or period not in ("minute", "daily"):
        raise ValueError("unsupported index or period")
    def load():
        url = ("https://web.ifzq.gtimg.cn/appstock/app/minute/query" if period == "minute"
               else "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get")
        params = {"code": symbol} if period == "minute" else {"param": f"{symbol},day,,,60,qfq"}
        response = requests.get(url, params=params, timeout=(2, 4))
        response.raise_for_status()
        return parse_series(response.json(), symbol, period)
    return dict(_cached(f"{symbol}:{period}", load), symbol=symbol, name=INDICES[symbol], period=period)


def index_overview() -> dict:
    def load():
        response = requests.get("https://qt.gtimg.cn/q=" + ",".join(INDICES), timeout=(2, 4))
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
                      "series": index_series(symbol, "minute")})
    return {"indices": items, "display_only": True}
