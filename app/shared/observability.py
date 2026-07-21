from __future__ import annotations

import math
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return round(ordered[index], 3)


@dataclass
class _EndpointWindow:
    durations_ms: Deque[float] = field(default_factory=lambda: deque(maxlen=256))
    count: int = 0
    error_count: int = 0
    last_status: int | None = None
    last_seen_at: str | None = None


class RequestMetrics:
    """Small in-process latency window suitable for a 2C4G deployment.

    Production-wide metrics should still be collected from access logs or a
    monitoring system. This bounded window makes regressions and server smoke
    tests observable without adding a mandatory metrics service.
    """

    def __init__(self, max_endpoints: int = 100) -> None:
        self._max_endpoints = max(1, int(max_endpoints))
        self._windows: dict[str, _EndpointWindow] = defaultdict(_EndpointWindow)
        self._lock = threading.Lock()

    def observe(self, method: str, route: str, status_code: int, duration_ms: float) -> None:
        key = f"{str(method).upper()} {route or 'unmatched'}"
        with self._lock:
            if key not in self._windows and len(self._windows) >= self._max_endpoints:
                key = "OTHER"
            window = self._windows[key]
            window.durations_ms.append(max(0.0, float(duration_ms)))
            window.count += 1
            if int(status_code) >= 500:
                window.error_count += 1
            window.last_status = int(status_code)
            window.last_seen_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def snapshot(self) -> dict:
        with self._lock:
            items = []
            total_count = 0
            total_errors = 0
            for key, window in sorted(self._windows.items()):
                values = list(window.durations_ms)
                total_count += window.count
                total_errors += window.error_count
                items.append(
                    {
                        "endpoint": key,
                        "count": window.count,
                        "error_count": window.error_count,
                        "error_rate": round(window.error_count / window.count, 6) if window.count else 0.0,
                        "sample_count": len(values),
                        "p50_ms": _percentile(values, 0.50),
                        "p95_ms": _percentile(values, 0.95),
                        "p99_ms": _percentile(values, 0.99),
                        "last_status": window.last_status,
                        "last_seen_at": window.last_seen_at,
                    }
                )
            return {
                "scope": "current_process",
                "window_size_per_endpoint": 256,
                "request_count": total_count,
                "error_count": total_errors,
                "error_rate": round(total_errors / total_count, 6) if total_count else 0.0,
                "items": items,
            }

    def reset(self) -> None:
        with self._lock:
            self._windows.clear()


request_metrics = RequestMetrics()


class DatabaseMetrics:
    """Bounded per-process timings for pool checkout and DB transactions."""

    def __init__(self, window_size: int = 256) -> None:
        self._checkout_ms: Deque[float] = deque(maxlen=max(1, int(window_size)))
        self._transaction_ms: Deque[float] = deque(maxlen=max(1, int(window_size)))
        self._count = 0
        self._error_count = 0
        self._lock = threading.Lock()

    def observe(self, *, checkout_ms: float, transaction_ms: float, success: bool) -> None:
        with self._lock:
            self._checkout_ms.append(max(0.0, float(checkout_ms)))
            self._transaction_ms.append(max(0.0, float(transaction_ms)))
            self._count += 1
            if not success:
                self._error_count += 1

    def snapshot(self) -> dict:
        with self._lock:
            checkout = list(self._checkout_ms)
            transaction = list(self._transaction_ms)
            return {
                "scope": "current_process",
                "transaction_count": self._count,
                "error_count": self._error_count,
                "error_rate": round(self._error_count / self._count, 6) if self._count else 0.0,
                "pool_checkout_p50_ms": _percentile(checkout, 0.50),
                "pool_checkout_p95_ms": _percentile(checkout, 0.95),
                "transaction_p50_ms": _percentile(transaction, 0.50),
                "transaction_p95_ms": _percentile(transaction, 0.95),
                "sample_count": len(transaction),
            }

    def reset(self) -> None:
        with self._lock:
            self._checkout_ms.clear()
            self._transaction_ms.clear()
            self._count = 0
            self._error_count = 0


database_metrics = DatabaseMetrics()
