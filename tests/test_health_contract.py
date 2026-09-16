from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi import Response

from app.api.routes import health as health_route


class FakeCache:
    def get(self, key: str):
        if key.endswith(":a_share_sentiment"):
            return {
                "snapshot_id": "snapshot-stable-1",
                "coverage_ratio": 0.99,
            }
        return None


class HealthContractTests(unittest.TestCase):
    def test_memory_health_exposes_cache_and_snapshot_contract(self):
        with patch.object(
            health_route,
            "cache_diagnostics",
            return_value={"backend": "memory", "status": "ready"},
        ), patch.object(health_route, "get_cache_backend", return_value=FakeCache()):
            payload = health_route.health()

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["cache_mode"], "memory")
        self.assertEqual(payload["redis_status"], "disabled")
        self.assertEqual(payload["data_snapshot_id"], "snapshot-stable-1")

    def test_uninitialized_redis_health_does_not_connect(self):
        class FailOnGet:
            def get(self, _key: str):
                raise AssertionError("health must not initialize Redis")

        with patch.object(
            health_route,
            "cache_diagnostics",
            return_value={"backend": "redis", "status": "uninitialized"},
        ), patch.object(health_route, "get_cache_backend", return_value=FailOnGet()):
            payload = health_route.health()

        self.assertEqual(payload["redis_status"], "degraded")
        self.assertIsNone(payload["data_snapshot_id"])

    def test_not_ready_payload_sets_http_503(self):
        response = Response()
        with patch.object(
            health_route,
            "build_operational_readiness",
            return_value={
                "status": "not_ready",
                "accepting_jobs": False,
                "reasons": ["日线落后独立交易日历 1 个交易日"],
            },
        ):
            payload = health_route.readiness(response)

        self.assertEqual(response.status_code, 503)
        self.assertFalse(payload["accepting_jobs"])


if __name__ == "__main__":
    unittest.main()
