from __future__ import annotations

import importlib
import json
import math
import time
from abc import ABC, abstractmethod
from threading import Lock, RLock
from typing import Any, Callable

from app.shared.settings import CacheSettings, cache_settings


def _encode(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _decode(value: bytes | str) -> Any:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value)


def _normalized_key(key: str) -> str:
    normalized = str(key).strip()
    if not normalized:
        raise ValueError("cache key cannot be empty")
    return normalized


class CacheBackend(ABC):
    @abstractmethod
    def get(self, key: str) -> Any | None:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> bool:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def diagnostics(self) -> dict[str, Any]:
        raise NotImplementedError


class DisabledCacheBackend(CacheBackend):
    def get(self, key: str) -> Any | None:
        _normalized_key(key)
        return None

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> bool:
        _normalized_key(key)
        return False

    def delete(self, key: str) -> bool:
        _normalized_key(key)
        return False

    def clear(self) -> int:
        return 0

    def diagnostics(self) -> dict[str, Any]:
        return {
            "enabled": False,
            "backend": "disabled",
            "status": "disabled",
            "hits": 0,
            "misses": 0,
            "errors": 0,
        }


class InMemoryCacheBackend(CacheBackend):
    def __init__(
        self,
        *,
        default_ttl_seconds: float = 60,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if default_ttl_seconds <= 0:
            raise ValueError("default_ttl_seconds must be positive")
        self.default_ttl_seconds = float(default_ttl_seconds)
        self._clock = clock
        self._items: dict[str, tuple[float, bytes]] = {}
        self._lock = RLock()
        self._hits = 0
        self._misses = 0
        self._errors = 0

    def _prune_expired_locked(self) -> None:
        now = self._clock()
        expired = [key for key, (expires_at, _) in self._items.items() if expires_at <= now]
        for key in expired:
            self._items.pop(key, None)

    def get(self, key: str) -> Any | None:
        normalized = _normalized_key(key)
        with self._lock:
            item = self._items.get(normalized)
            if item is None or item[0] <= self._clock():
                self._items.pop(normalized, None)
                self._misses += 1
                return None
            try:
                value = _decode(item[1])
            except Exception:
                self._items.pop(normalized, None)
                self._errors += 1
                self._misses += 1
                return None
            self._hits += 1
            return value

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> bool:
        normalized = _normalized_key(key)
        ttl = self.default_ttl_seconds if ttl_seconds is None else float(ttl_seconds)
        if ttl <= 0:
            self.delete(normalized)
            return False
        try:
            encoded = _encode(value)
        except Exception:
            with self._lock:
                self._errors += 1
            return False
        with self._lock:
            self._items[normalized] = (self._clock() + ttl, encoded)
        return True

    def delete(self, key: str) -> bool:
        normalized = _normalized_key(key)
        with self._lock:
            return self._items.pop(normalized, None) is not None

    def clear(self) -> int:
        with self._lock:
            count = len(self._items)
            self._items.clear()
            return count

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            self._prune_expired_locked()
            return {
                "enabled": True,
                "backend": "memory",
                "status": "ready",
                "entries": len(self._items),
                "hits": self._hits,
                "misses": self._misses,
                "errors": self._errors,
                "default_ttl_seconds": self.default_ttl_seconds,
            }


class RedisCacheBackend(CacheBackend):
    """Optional Redis cache with a write-through in-memory fallback.

    The ``redis`` package is imported only on first use. ``client`` is injectable
    so tests can use fakeredis without a running Redis server.
    """

    def __init__(
        self,
        *,
        redis_url: str | None,
        key_prefix: str = "stock-analysis",
        default_ttl_seconds: float = 60,
        socket_connect_timeout_seconds: float = 0.5,
        socket_timeout_seconds: float = 0.5,
        fallback: CacheBackend | None = None,
        client: Any | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.redis_url = redis_url
        self.key_prefix = str(key_prefix).strip().rstrip(":") or "stock-analysis"
        self.default_ttl_seconds = float(default_ttl_seconds)
        self.socket_connect_timeout_seconds = float(socket_connect_timeout_seconds)
        self.socket_timeout_seconds = float(socket_timeout_seconds)
        self.fallback = fallback
        self._clock = clock
        self._client = client
        self._client_initialized = client is not None
        self._client_lock = Lock()
        self._stats_lock = Lock()
        self._hits = 0
        self._misses = 0
        self._errors = 0
        self._last_error: str | None = None
        self._fallback_active = False
        self._consecutive_errors = 0
        self._retry_at = 0.0

    def _key(self, key: str) -> str:
        return f"{self.key_prefix}:{_normalized_key(key)}"

    def _record_error(self, exc: BaseException | str) -> None:
        message = str(exc)
        with self._stats_lock:
            self._errors += 1
            self._last_error = message[:200]
            self._fallback_active = self.fallback is not None
            self._consecutive_errors += 1
            retry_seconds = min(30.0, 1.0 * (2 ** min(self._consecutive_errors - 1, 5)))
            self._retry_at = self._clock() + retry_seconds

    def _record_success(self) -> None:
        with self._stats_lock:
            self._fallback_active = False
            self._consecutive_errors = 0
            self._retry_at = 0.0
            self._last_error = None

    def _redis_client(self) -> Any | None:
        if self._clock() < self._retry_at:
            return None
        with self._client_lock:
            if self._clock() < self._retry_at:
                return None
            self._client_initialized = True
            if self._client is None and not self.redis_url:
                self._record_error("CACHE_REDIS_URL is not configured")
                return None
            try:
                if self._client is None:
                    redis_module = importlib.import_module("redis")
                    self._client = redis_module.Redis.from_url(
                        self.redis_url,
                        decode_responses=False,
                        socket_connect_timeout=self.socket_connect_timeout_seconds,
                        socket_timeout=self.socket_timeout_seconds,
                    )
                self._client.ping()
                self._record_success()
            except Exception as exc:
                self._record_error(exc)
                # A redis-py client can reconnect after the circuit interval;
                # retain it instead of rebuilding a socket pool per request.
                return None
            return self._client

    def get(self, key: str) -> Any | None:
        client = self._redis_client()
        if client is not None:
            try:
                raw = client.get(self._key(key))
                if raw is None:
                    with self._stats_lock:
                        self._misses += 1
                    self._fallback_active = False
                    return self.fallback.get(key) if self.fallback is not None else None
                value = _decode(raw)
                with self._stats_lock:
                    self._hits += 1
                self._record_success()
                return value
            except Exception as exc:
                self._record_error(exc)
        if self.fallback is None:
            with self._stats_lock:
                self._misses += 1
            return None
        return self.fallback.get(key)

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> bool:
        ttl = self.default_ttl_seconds if ttl_seconds is None else float(ttl_seconds)
        fallback_saved = self.fallback.set(key, value, ttl) if self.fallback is not None else False
        if ttl <= 0:
            self.delete(key)
            return False
        client = self._redis_client()
        if client is None:
            return fallback_saved
        try:
            client.set(self._key(key), _encode(value), ex=max(1, int(math.ceil(ttl))))
            self._record_success()
            return True
        except Exception as exc:
            self._record_error(exc)
            return fallback_saved

    def delete(self, key: str) -> bool:
        fallback_deleted = self.fallback.delete(key) if self.fallback is not None else False
        client = self._redis_client()
        if client is None:
            return fallback_deleted
        try:
            deleted = bool(client.delete(self._key(key)))
            return deleted or fallback_deleted
        except Exception as exc:
            self._record_error(exc)
            return fallback_deleted

    def clear(self) -> int:
        fallback_count = self.fallback.clear() if self.fallback is not None else 0
        client = self._redis_client()
        if client is None:
            return fallback_count
        try:
            keys = list(client.scan_iter(match=f"{self.key_prefix}:*"))
            redis_count = int(client.delete(*keys)) if keys else 0
            return max(redis_count, fallback_count)
        except Exception as exc:
            self._record_error(exc)
            return fallback_count

    def diagnostics(self) -> dict[str, Any]:
        with self._stats_lock:
            result = {
                "enabled": True,
                "backend": "redis",
                "status": (
                    "fallback"
                    if self._fallback_active
                    else "ready"
                    if self._client is not None
                    else "uninitialized"
                ),
                "client_initialized": self._client_initialized,
                "fallback_active": self._fallback_active,
                "hits": self._hits,
                "misses": self._misses,
                "errors": self._errors,
                "last_error": self._last_error,
                "consecutive_errors": self._consecutive_errors,
                "retry_in_seconds": max(0.0, self._retry_at - self._clock()),
                "key_prefix": self.key_prefix,
                "default_ttl_seconds": self.default_ttl_seconds,
            }
        if self.fallback is not None:
            result["fallback"] = self.fallback.diagnostics()
        return result


def create_cache_backend(
    settings: CacheSettings | None = None,
    *,
    redis_client: Any | None = None,
) -> CacheBackend:
    final_settings = settings or cache_settings
    if not final_settings.enabled:
        return DisabledCacheBackend()
    memory = InMemoryCacheBackend(default_ttl_seconds=final_settings.default_ttl_seconds)
    if final_settings.backend == "memory":
        return memory
    fallback = memory if final_settings.redis_fallback_to_memory else None
    return RedisCacheBackend(
        redis_url=final_settings.redis_url,
        key_prefix=final_settings.key_prefix,
        default_ttl_seconds=final_settings.default_ttl_seconds,
        socket_connect_timeout_seconds=final_settings.redis_socket_connect_timeout_seconds,
        socket_timeout_seconds=final_settings.redis_socket_timeout_seconds,
        fallback=fallback,
        client=redis_client,
    )


_CACHE_BACKEND: CacheBackend | None = None
_CACHE_BACKEND_LOCK = Lock()


def get_cache_backend() -> CacheBackend:
    global _CACHE_BACKEND
    with _CACHE_BACKEND_LOCK:
        if _CACHE_BACKEND is None:
            _CACHE_BACKEND = create_cache_backend()
        return _CACHE_BACKEND


def reset_cache_backend() -> None:
    global _CACHE_BACKEND
    with _CACHE_BACKEND_LOCK:
        _CACHE_BACKEND = None


def cache_diagnostics() -> dict[str, Any]:
    return get_cache_backend().diagnostics()
