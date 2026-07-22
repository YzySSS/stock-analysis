from __future__ import annotations

import unittest

from app.shared.cache import (
    DisabledCacheBackend,
    InMemoryCacheBackend,
    RedisCacheBackend,
    create_cache_backend,
)
from app.shared.settings import CacheSettings

try:
    import fakeredis
except ImportError:  # pragma: no cover - optional test dependency
    fakeredis = None


class FakeRedisClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.data: dict[str, bytes] = {}
        self.ping_count = 0

    def _check(self) -> None:
        if self.fail:
            raise ConnectionError("redis unavailable")

    def ping(self) -> bool:
        self.ping_count += 1
        self._check()
        return True

    def get(self, key: str) -> bytes | None:
        self._check()
        return self.data.get(key)

    def set(self, key: str, value: bytes, ex: int | None = None) -> bool:
        self._check()
        self.data[key] = value
        return True

    def delete(self, *keys: str) -> int:
        self._check()
        deleted = 0
        for key in keys:
            deleted += int(self.data.pop(key, None) is not None)
        return deleted

    def scan_iter(self, match: str):
        self._check()
        prefix = match[:-1] if match.endswith("*") else match
        return iter([key for key in self.data if key.startswith(prefix)])


class SharedCacheTests(unittest.TestCase):
    def test_memory_backend_honors_ttl_and_returns_json_copy(self):
        now = [100.0]
        cache = InMemoryCacheBackend(default_ttl_seconds=10, clock=lambda: now[0])
        source = {"items": [1, 2]}

        self.assertTrue(cache.set("key", source))
        source["items"].append(3)
        self.assertEqual(cache.get("key"), {"items": [1, 2]})

        now[0] = 111.0
        self.assertIsNone(cache.get("key"))
        diagnostics = cache.diagnostics()
        self.assertEqual(diagnostics["hits"], 1)
        self.assertEqual(diagnostics["misses"], 1)
        self.assertEqual(diagnostics["entries"], 0)

    def test_factory_can_be_explicitly_disabled(self):
        backend = create_cache_backend(CacheSettings(enabled=False))
        self.assertIsInstance(backend, DisabledCacheBackend)
        self.assertFalse(backend.set("key", {"value": 1}))
        self.assertIsNone(backend.get("key"))
        self.assertEqual(backend.diagnostics()["status"], "disabled")

    def test_redis_backend_namespaces_and_round_trips_json(self):
        client = FakeRedisClient()
        backend = RedisCacheBackend(
            redis_url=None,
            key_prefix="stock:test",
            default_ttl_seconds=30,
            client=client,
        )

        self.assertTrue(backend.set("dashboard", {"ok": True}))
        self.assertIn("stock:test:dashboard", client.data)
        self.assertEqual(backend.get("dashboard"), {"ok": True})
        self.assertTrue(backend.delete("dashboard"))
        self.assertIsNone(backend.get("dashboard"))
        self.assertEqual(client.ping_count, 1)
        self.assertTrue(backend.diagnostics()["connection_verified"])

    def test_redis_failure_automatically_uses_write_through_memory_fallback(self):
        fallback = InMemoryCacheBackend(default_ttl_seconds=30)
        backend = RedisCacheBackend(
            redis_url="redis://unused",
            fallback=fallback,
            client=FakeRedisClient(fail=True),
        )

        self.assertTrue(backend.set("selection", {"count": 3}))
        self.assertEqual(backend.get("selection"), {"count": 3})
        diagnostics = backend.diagnostics()
        self.assertEqual(diagnostics["status"], "fallback")
        self.assertTrue(diagnostics["fallback_active"])
        self.assertGreaterEqual(diagnostics["errors"], 1)

    def test_redis_circuit_retries_and_recovers_after_backoff(self):
        now = [100.0]
        client = FakeRedisClient(fail=True)
        fallback = InMemoryCacheBackend(default_ttl_seconds=30, clock=lambda: now[0])
        backend = RedisCacheBackend(
            redis_url="redis://unused",
            fallback=fallback,
            client=client,
            clock=lambda: now[0],
        )

        self.assertTrue(backend.set("selection", {"count": 3}))
        self.assertEqual(backend.get("selection"), {"count": 3})
        self.assertEqual(backend.diagnostics()["status"], "fallback")

        client.fail = False
        now[0] = 101.1
        self.assertEqual(backend.get("selection"), {"count": 3})
        self.assertEqual(backend.diagnostics()["status"], "ready")

    def test_redis_factory_without_url_falls_back_lazily(self):
        backend = create_cache_backend(
            CacheSettings(enabled=True, backend="redis", redis_url=None)
        )

        self.assertEqual(backend.diagnostics()["status"], "uninitialized")
        self.assertTrue(backend.set("key", [1, 2, 3]))
        self.assertEqual(backend.get("key"), [1, 2, 3])
        self.assertEqual(backend.diagnostics()["status"], "fallback")

    @unittest.skipIf(fakeredis is None, "fakeredis is not installed")
    def test_backend_accepts_fakeredis_client(self):
        backend = RedisCacheBackend(
            redis_url=None,
            key_prefix="fakeredis-compatible",
            client=fakeredis.FakeRedis(),
        )

        self.assertTrue(backend.set("key", {"value": 7}, ttl_seconds=5))
        self.assertEqual(backend.get("key"), {"value": 7})
        self.assertEqual(backend.clear(), 1)


if __name__ == "__main__":
    unittest.main()
