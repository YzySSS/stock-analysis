from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any


ENDPOINTS = ("rt_min", "rt_min_daily")
FREQUENCIES = ("1MIN", "5MIN", "15MIN", "30MIN", "60MIN")
ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Probe Tushare rt_min/rt_min_daily permissions without writing data. "
            "The token value is accepted only through an explicitly named environment variable."
        )
    )
    parser.add_argument(
        "--token-env",
        help="Environment variable containing the token. Omit to skip without importing Tushare or making a request.",
    )
    parser.add_argument("--endpoint", choices=("both", *ENDPOINTS), default="both")
    parser.add_argument("--ts-code", default="600000.SH")
    parser.add_argument("--freq", choices=FREQUENCIES, default="1MIN")
    return parser


def _build_client(token: str):
    import tushare as ts

    return ts.pro_api(token)


def _result_shape(value: Any) -> tuple[int, list[str]]:
    if value is None:
        return 0, []
    try:
        count = int(len(value))
    except (TypeError, ValueError):
        count = 0
    columns = getattr(value, "columns", None)
    if columns is None:
        return count, []
    try:
        return count, [str(column) for column in columns]
    except TypeError:
        return count, []


def _safe_failure_reason(exc: Exception) -> str:
    message = str(exc).casefold()
    if any(
        marker in message
        for marker in ("\u6ca1\u6709\u6743\u9650", "\u65e0\u6743\u9650", "permission", "forbidden", "unauthorized")
    ):
        return "permission_denied"
    if any(
        marker in message
        for marker in ("rate limit", "\u9891\u7387", "\u6bcf\u5206\u949f", "too many requests")
    ):
        return "rate_limited"
    if isinstance(exc, (ImportError, ModuleNotFoundError)):
        return "dependency_unavailable"
    return "request_failed"


def _probe_endpoint(client: Any, endpoint: str, *, ts_code: str, freq: str) -> dict[str, Any]:
    try:
        method = getattr(client, endpoint)
        value = method(ts_code=ts_code, freq=freq)
        row_count, columns = _result_shape(value)
        return {
            "endpoint": endpoint,
            "status": "available",
            "row_count": row_count,
            "columns": columns,
        }
    except Exception as exc:  # The raw provider message may contain credentials; never return or log it.
        return {
            "endpoint": endpoint,
            "status": "unavailable",
            "reason": _safe_failure_reason(exc),
            "error_type": type(exc).__name__,
        }


def run_probe(
    args: argparse.Namespace,
    *,
    environ: Mapping[str, str] | None = None,
    client_factory: Callable[[str], Any] = _build_client,
) -> dict[str, Any]:
    if not args.token_env:
        return {
            "status": "skipped",
            "reason": "token_env_not_provided",
            "network_attempted": False,
        }
    if not ENV_NAME_PATTERN.fullmatch(args.token_env):
        return {
            "status": "skipped",
            "reason": "invalid_token_env_name",
            "network_attempted": False,
        }

    environment = os.environ if environ is None else environ
    token = str(environment.get(args.token_env) or "").strip()
    if not token:
        return {
            "status": "skipped",
            "reason": "token_not_configured",
            "network_attempted": False,
        }

    endpoints: Sequence[str] = ENDPOINTS if args.endpoint == "both" else (args.endpoint,)
    try:
        client = client_factory(token)
    except Exception as exc:
        return {
            "status": "failed",
            "reason": _safe_failure_reason(exc),
            "error_type": type(exc).__name__,
            "network_attempted": True,
            "results": [],
        }

    results = [
        _probe_endpoint(client, endpoint, ts_code=args.ts_code, freq=args.freq)
        for endpoint in endpoints
    ]
    available = len([item for item in results if item["status"] == "available"])
    overall_status = "success" if available == len(results) else "partial" if available else "failed"
    return {
        "status": overall_status,
        "network_attempted": True,
        "ts_code": args.ts_code,
        "freq": args.freq,
        "results": results,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_probe(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] in {"success", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
