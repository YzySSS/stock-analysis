from __future__ import annotations

import argparse
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts import probe_tushare_realtime_permissions as probe


def args(**overrides) -> argparse.Namespace:
    values = {
        "token_env": None,
        "endpoint": "both",
        "ts_code": "600000.SH",
        "freq": "1MIN",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class FakeFrame:
    columns = ("ts_code", "time", "close")

    def __len__(self) -> int:
        return 2


class FakeClient:
    def rt_min(self, **_kwargs):
        return FakeFrame()

    def rt_min_daily(self, **_kwargs):
        return FakeFrame()


class TushareRealtimePermissionProbeTests(unittest.TestCase):
    def test_missing_explicit_token_env_skips_without_building_client(self):
        called = False

        def factory(_token: str):
            nonlocal called
            called = True
            return FakeClient()

        result = probe.run_probe(args(), environ={}, client_factory=factory)
        self.assertEqual(result["status"], "skipped")
        self.assertFalse(result["network_attempted"])
        self.assertFalse(called)

    def test_configured_env_probes_both_endpoints_without_exposing_token(self):
        secret = "super-secret-token-value"
        captured_token: list[str] = []

        def factory(token: str):
            captured_token.append(token)
            return FakeClient()

        result = probe.run_probe(
            args(token_env="TUSHARE_RT_TOKEN"),
            environ={"TUSHARE_RT_TOKEN": secret},
            client_factory=factory,
        )
        serialized = json.dumps(result, ensure_ascii=False)
        self.assertEqual(result["status"], "success")
        self.assertEqual(captured_token, [secret])
        self.assertNotIn(secret, serialized)
        self.assertEqual([item["endpoint"] for item in result["results"]], list(probe.ENDPOINTS))
        self.assertEqual([item["row_count"] for item in result["results"]], [2, 2])

    def test_provider_exception_is_classified_without_returning_raw_message(self):
        secret = "token-that-must-not-leak"

        class DeniedClient(FakeClient):
            def rt_min(self, **_kwargs):
                raise RuntimeError(f"没有权限 token={secret}")

        result = probe.run_probe(
            args(token_env="TUSHARE_RT_TOKEN", endpoint="rt_min"),
            environ={"TUSHARE_RT_TOKEN": secret},
            client_factory=lambda _token: DeniedClient(),
        )
        serialized = json.dumps(result, ensure_ascii=False)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["results"][0]["reason"], "permission_denied")
        self.assertNotIn(secret, serialized)
        self.assertNotIn("token=", serialized)

    def test_main_without_token_env_is_successful_noop(self):
        output = io.StringIO()
        with patch.object(probe, "_build_client") as build_client, redirect_stdout(output):
            exit_code = probe.main([])
        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "skipped")
        build_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
