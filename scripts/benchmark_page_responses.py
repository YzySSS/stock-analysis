from __future__ import annotations

import argparse
import json
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Any, Iterable
from urllib.parse import urlencode, urlsplit

import requests


@dataclass(frozen=True)
class Target:
    name: str
    path: str
    kind: str


PROFILE_TARGETS: dict[str, tuple[Target, ...]] = {
    "dashboard": (
        Target("dashboard_html", "/", "page"),
        Target("dashboard_js", "/static/js/home.js", "asset"),
        Target("dashboard_compact", "/api/dashboard/summary?limit=8&compact=true", "api"),
    ),
    "portfolio": (
        Target("portfolio_html", "/portfolio", "page"),
        Target("portfolio_js", "/static/js/portfolio.js", "asset"),
        Target("portfolio_positions", "/api/portfolio", "api"),
    ),
    "selection": (
        Target("selection_html", "/selection", "page"),
        Target("selection_js", "/static/js/selection.js", "asset"),
        Target("selection_strategies", "/api/strategies?instrument_type=stock", "api"),
        Target(
            "selection_results",
            "/api/selection/results?instrument_type=stock&limit=3",
            "api",
        ),
    ),
    "backtest": (
        Target("backtest_html", "/backtest", "page"),
        Target("backtest_js", "/static/js/backtest.js", "asset"),
        Target("backtest_strategies", "/api/strategies?instrument_type=stock", "api"),
        Target("backtest_factor_status", "/api/factor-input/status", "api"),
        Target(
            "backtest_validations",
            "/api/backtest/validations?limit=10&compact=true",
            "api",
        ),
        Target("backtest_runs", "/api/backtest/runs?limit=20&compact=true", "api"),
    ),
}


def validate_base_url(value: str) -> str:
    parsed = urlsplit(str(value or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("base URL must be an absolute http(s) URL")
    if parsed.query or parsed.fragment:
        raise ValueError("base URL cannot contain query parameters or fragments")
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"


def targets_for_profiles(profiles: Iterable[str]) -> list[Target]:
    result: list[Target] = []
    seen: set[str] = set()
    for profile in profiles:
        for target in PROFILE_TARGETS[profile]:
            if target.path not in seen:
                result.append(target)
                seen.add(target.path)
    return result


def pick_default_backtest_run(items: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    eligible = [item for item in items if item.get("run_id")]

    def progress(item: dict[str, Any]) -> float:
        try:
            return float(item.get("progress_pct") or 0)
        except (TypeError, ValueError):
            return 0.0

    running = sorted(
        (item for item in eligible if item.get("status") == "running"),
        key=progress,
        reverse=True,
    )
    if running:
        return running[0]
    for status in ("queued", "success"):
        selected = next((item for item in eligible if item.get("status") == status), None)
        if selected:
            return selected
    return None


def summarize_samples(
    target: Target,
    samples_ms: list[float],
    *,
    status_code: int,
    response_bytes: int,
) -> dict[str, Any]:
    if not samples_ms:
        raise ValueError("at least one timing sample is required")
    first_budget_ms = 100.0 if target.kind in {"page", "asset"} else 800.0
    warm_budget_ms = 100.0 if target.kind in {"page", "asset"} else 200.0
    warm_samples = samples_ms[1:] or samples_ms
    first_ms = samples_ms[0]
    warm_median_ms = statistics.median(warm_samples)
    return {
        **asdict(target),
        "status_code": int(status_code),
        "response_bytes": int(response_bytes),
        "first_observed_ms": round(first_ms, 2),
        "warm_median_ms": round(warm_median_ms, 2),
        "max_observed_ms": round(max(samples_ms), 2),
        "samples_ms": [round(value, 2) for value in samples_ms],
        "first_budget_ms": first_budget_ms,
        "warm_budget_ms": warm_budget_ms,
        "first_budget_pass": status_code == 200 and first_ms < first_budget_ms,
        "warm_budget_pass": status_code == 200 and warm_median_ms < warm_budget_ms,
    }


def measure_target(
    session: requests.Session,
    base_url: str,
    target: Target,
    *,
    repetitions: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    samples_ms: list[float] = []
    status_code = 0
    response_bytes = 0
    for _ in range(repetitions):
        started = time.perf_counter()
        response = session.get(f"{base_url}{target.path}", timeout=timeout_seconds)
        samples_ms.append((time.perf_counter() - started) * 1000)
        status_code = response.status_code
        response_bytes = len(response.content)
    return summarize_samples(
        target,
        samples_ms,
        status_code=status_code,
        response_bytes=response_bytes,
    )


def measure_backtest_first_screen(
    session: requests.Session,
    base_url: str,
    *,
    repetitions: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    fixed_paths = (
        "/api/strategies?instrument_type=stock",
        "/api/factor-input/status",
        "/api/backtest/validations?limit=10&compact=true",
        "/api/backtest/runs?limit=20&compact=true",
    )
    samples_ms: list[float] = []
    response_bytes = 0
    request_count = 0
    selected_run_id: str | None = None

    for _ in range(repetitions):
        started = time.perf_counter()
        sample_bytes = 0
        responses: dict[str, requests.Response] = {}
        for path in fixed_paths:
            response = session.get(f"{base_url}{path}", timeout=timeout_seconds)
            response.raise_for_status()
            sample_bytes += len(response.content)
            responses[path] = response

        runs = responses[fixed_paths[-1]].json().get("items", [])
        default_run = pick_default_backtest_run(runs)
        sample_request_count = len(fixed_paths)
        if default_run:
            selected_run_id = str(default_run["run_id"])
            results_path = "/api/backtest/results?" + urlencode({"run_id": selected_run_id})
            result_response = session.get(f"{base_url}{results_path}", timeout=timeout_seconds)
            result_response.raise_for_status()
            sample_bytes += len(result_response.content)
            result = result_response.json()
            return_mode = (
                result.get("return_mode")
                or (result.get("request") or {}).get("return_mode")
                or default_run.get("return_mode")
                or "1d"
            )
            trades_path = "/api/backtest/trades?" + urlencode(
                {
                    "run_id": selected_run_id,
                    "limit": 10,
                    "page": 1,
                    "return_mode": return_mode,
                }
            )
            trades_response = session.get(f"{base_url}{trades_path}", timeout=timeout_seconds)
            trades_response.raise_for_status()
            sample_bytes += len(trades_response.content)
            sample_request_count += 2

        samples_ms.append((time.perf_counter() - started) * 1000)
        response_bytes = sample_bytes
        request_count = sample_request_count

    result = summarize_samples(
        Target("backtest_first_screen_data_chain", "sequence:backtest-first-screen", "api"),
        samples_ms,
        status_code=200,
        response_bytes=response_bytes,
    )
    result.update(
        {
            "request_count": request_count,
            "selected_run_id": selected_run_id,
            "measurement_scope": "serial DOMContentLoaded data chain, including latest results and first trade page",
        }
    )
    return result


def measure_dashboard_local_cache_miss(repetitions: int) -> dict[str, Any]:
    from app.api.routes import dashboard

    samples_ms: list[float] = []
    response_bytes = 0
    for _ in range(repetitions):
        dashboard._DASHBOARD_CACHE.clear()
        started = time.perf_counter()
        payload = dashboard.dashboard_summary(limit=8, compact=True)
        samples_ms.append((time.perf_counter() - started) * 1000)
        response_bytes = len(json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"))
    target = Target(
        "dashboard_compact_local_cache_miss",
        "direct:dashboard_summary(limit=8,compact=true)",
        "api",
    )
    result = summarize_samples(
        target,
        samples_ms,
        status_code=200,
        response_bytes=response_bytes,
    )
    result["first_budget_pass"] = max(samples_ms) < result["first_budget_ms"]
    result["warm_budget_ms"] = None
    result["warm_budget_pass"] = None
    result["cache_miss_median_ms"] = round(statistics.median(samples_ms), 2)
    result["measurement_scope"] = "fresh Python process cache miss; includes DB/read-model work, excludes ASGI/network"
    return result


def run_benchmark(
    *,
    base_url: str,
    profiles: list[str],
    repetitions: int,
    timeout_seconds: float,
    include_dashboard_local_cache_miss: bool,
) -> dict[str, Any]:
    base_url = validate_base_url(base_url)
    with requests.Session() as session:
        results = []
        if "backtest" in profiles:
            results.append(
                measure_backtest_first_screen(
                    session,
                    base_url,
                    repetitions=repetitions,
                    timeout_seconds=timeout_seconds,
                )
            )
        results.extend(
            measure_target(
                session,
                base_url,
                target,
                repetitions=repetitions,
                timeout_seconds=timeout_seconds,
            )
            for target in targets_for_profiles(profiles)
        )
    if include_dashboard_local_cache_miss and "dashboard" in profiles:
        results.append(measure_dashboard_local_cache_miss(repetitions))
    failed = [
        item["name"]
        for item in results
        if item["first_budget_pass"] is False or item["warm_budget_pass"] is False
    ]
    return {
        "status": "pass" if not failed else "budget_failed",
        "base_url": base_url,
        "profiles": profiles,
        "repetitions": repetitions,
        "budget_contract": {
            "page_or_asset_ms": 100,
            "api_first_observed_ms": 800,
            "api_warm_median_ms": 200,
            "note": "first_observed is a live upper-bound sample; use local cache-miss probe for deterministic Dashboard cache misses",
        },
        "failed_targets": failed,
        "results": results,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serially benchmark stock-analysis first-screen page/API responses")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--profile",
        action="append",
        choices=tuple(PROFILE_TARGETS),
        help="repeat to benchmark multiple profiles; defaults to all",
    )
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--dashboard-local-cache-miss", action="store_true")
    args = parser.parse_args(argv)
    if args.repetitions < 2 or args.repetitions > 20:
        parser.error("--repetitions must be between 2 and 20")
    profiles = args.profile or list(PROFILE_TARGETS)
    try:
        report = run_benchmark(
            base_url=args.base_url,
            profiles=profiles,
            repetitions=args.repetitions,
            timeout_seconds=args.timeout_seconds,
            include_dashboard_local_cache_miss=args.dashboard_local_cache_miss,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 1
    print(json.dumps(report, ensure_ascii=False, default=str))
    return 0 if report["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
