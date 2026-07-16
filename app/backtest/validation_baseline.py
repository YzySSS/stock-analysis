from __future__ import annotations

import json
import re
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Sequence

from app.backtest.policy import BACKTEST_METHODOLOGY_VERSION, LEGACY_BACKTEST_METHODOLOGY_VERSION
from app.backtest.repository import BacktestRepository
from app.backtest.service import BacktestRequest, BacktestService
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock


ALLOWED_BASELINE_STRATEGIES = ("lowvol_reversal", "v13_three_factor")
TERMINAL_STATUSES = {"success", "failed", "cancelled"}
DEFAULT_THRESHOLDS = {
    "lowvol_reversal": 60.0,
    "v13_three_factor": 65.0,
}
METRIC_FIELDS = (
    "sample_days",
    "total_picks",
    "total_trades",
    "total_return_pct",
    "avg_return_pct",
    "max_drawdown_pct",
    "win_rate_pct",
)
REQUEST_DEFAULTS = {
    "max_picks": 3,
    "score_threshold": None,
    "use_adjusted_price": False,
    "commission_bps": 0.0,
    "stamp_tax_bps": 0.0,
    "slippage_bps": 0.0,
    "apply_execution_constraints": False,
}
BASELINE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        loaded = json.loads(value)
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _request_value(request: Dict[str, Any], field: str) -> Any:
    default = REQUEST_DEFAULTS[field]
    value = request.get(field, default)
    if value is None and default is not None:
        return default
    return value


def validate_baseline_id(baseline_id: str) -> str:
    value = str(baseline_id or "").strip()
    if not BASELINE_ID_PATTERN.fullmatch(value):
        raise ValueError("baseline_id 仅允许 1~80 位字母、数字、点、下划线、冒号或连字符")
    return value


def system_memory_snapshot() -> Dict[str, int]:
    values: Dict[str, int] = {}
    with open("/proc/meminfo", "r", encoding="utf-8") as handle:
        for line in handle:
            key, raw = line.split(":", 1)
            token = raw.strip().split()[0]
            values[key] = int(token)
    return {
        "available_mb": values.get("MemAvailable", 0) // 1024,
        "swap_used_mb": max(values.get("SwapTotal", 0) - values.get("SwapFree", 0), 0) // 1024,
    }


def build_run_comparison(current: Dict[str, Any], legacy: Dict[str, Any] | None) -> Dict[str, Any]:
    current_request = _json_dict(current.get("request_json"))
    if not legacy:
        return {
            "strategy_id": current.get("strategy_id"),
            "current_run_id": current.get("run_id"),
            "legacy_run_id": None,
            "comparison_level": "no_legacy_comparator",
            "metrics": {field: {"current": _number(current.get(field)), "legacy": None, "delta": None} for field in METRIC_FIELDS},
            "caveats": ["没有相同策略、日期区间和收益模式的旧口径成功任务"],
        }

    legacy_request = _json_dict(legacy.get("request_json"))
    request_matches = {
        field: _request_value(current_request, field) == _request_value(legacy_request, field)
        for field in REQUEST_DEFAULTS
    }
    window_match = all(
        str(current.get(field)) == str(legacy.get(field))
        for field in ("strategy_id", "start_date", "end_date", "return_mode")
    )
    strategy_version_match = str(current.get("strategy_version") or "") == str(legacy.get("strategy_version") or "")
    current_hash = current.get("strategy_config_hash")
    legacy_hash = legacy.get("strategy_config_hash")
    config_hash_verifiable = bool(current_hash and legacy_hash)
    config_hash_match = bool(config_hash_verifiable and current_hash == legacy_hash)
    request_match = all(request_matches.values())

    if window_match and request_match and strategy_version_match and config_hash_match:
        comparison_level = "controlled_methodology_comparison"
    elif window_match and request_match and strategy_version_match and not config_hash_verifiable:
        comparison_level = "directional_same_version_unverifiable_config"
    else:
        comparison_level = "directional_only"

    metrics: Dict[str, Dict[str, float | int | None]] = {}
    for field in METRIC_FIELDS:
        current_value = _number(current.get(field))
        legacy_value = _number(legacy.get(field))
        delta = None
        if current_value is not None and legacy_value is not None:
            delta = round(float(current_value) - float(legacy_value), 4)
        metrics[field] = {"current": current_value, "legacy": legacy_value, "delta": delta}

    caveats = ["新旧任务的方法论版本不同，收益差异不能视为策略改进证据"]
    if not strategy_version_match:
        caveats.append("策略版本不同，无法把差异单独归因于交易时点修复")
    if not config_hash_verifiable:
        caveats.append("旧任务缺少 strategy_config_hash，配置一致性无法验证")
    elif not config_hash_match:
        caveats.append("策略配置 hash 不同，比较仅能作为方向性参考")
    if not request_match:
        caveats.append("部分请求参数不同，比较仅能作为方向性参考")

    return {
        "strategy_id": current.get("strategy_id"),
        "current_run_id": current.get("run_id"),
        "legacy_run_id": legacy.get("run_id"),
        "current_methodology": current.get("methodology_version"),
        "legacy_methodology": legacy.get("methodology_version") or LEGACY_BACKTEST_METHODOLOGY_VERSION,
        "comparison_level": comparison_level,
        "checks": {
            "window_match": window_match,
            "request_match": request_match,
            "request_fields": request_matches,
            "strategy_version_match": strategy_version_match,
            "config_hash_verifiable": config_hash_verifiable,
            "config_hash_match": config_hash_match,
        },
        "metrics": metrics,
        "caveats": caveats,
    }


class BacktestValidationBaseline:
    """Runs a small, serial, system-test-only backtest validation baseline."""

    def __init__(
        self,
        service: BacktestService | None = None,
        repository: BacktestRepository | None = None,
    ) -> None:
        self.service = service or BacktestService()
        self.repository = repository or getattr(self.service, "repository", None) or BacktestRepository()

    def _active_runs(self) -> list[Dict[str, Any]]:
        return self.repository.list_active_runs()

    @staticmethod
    def _assert_resources(min_available_mb: int, max_swap_used_mb: int) -> Dict[str, int]:
        snapshot = system_memory_snapshot()
        if snapshot["available_mb"] < min_available_mb:
            raise RuntimeError(
                f"可用内存仅 {snapshot['available_mb']} MiB，低于基线任务下限 {min_available_mb} MiB"
            )
        if snapshot["swap_used_mb"] > max_swap_used_mb:
            raise RuntimeError(
                f"Swap 已使用 {snapshot['swap_used_mb']} MiB，超过基线任务上限 {max_swap_used_mb} MiB"
            )
        return snapshot

    def _assert_queue_idle(self) -> None:
        active = self._active_runs()
        if active:
            raise RuntimeError(f"回测队列当前非空，拒绝叠加基线任务：{active}")

    def plan(
        self,
        *,
        baseline_id: str,
        strategies: Sequence[str],
        start_date: str,
        end_date: str,
        return_mode: str = "1d",
        max_trade_days: int = 10,
        max_picks: int = 3,
        min_available_mb: int = 1024,
        max_swap_used_mb: int = 512,
    ) -> Dict[str, Any]:
        baseline_id = validate_baseline_id(baseline_id)
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
        if start_date > end_date:
            raise ValueError("start_date 不能晚于 end_date")
        unique_strategies = list(dict.fromkeys(strategies))
        unsupported = [item for item in unique_strategies if item not in ALLOWED_BASELINE_STRATEGIES]
        if unsupported:
            raise ValueError(f"B3 基线仅支持：{', '.join(ALLOWED_BASELINE_STRATEGIES)}；收到：{unsupported}")
        if not unique_strategies:
            raise ValueError("至少指定一个策略")
        if return_mode not in {"1d", "3d"}:
            raise ValueError("B3 基线仅允许 1d 或 3d，避免混入复杂交易规则")
        if max_trade_days < 1 or max_trade_days > 20:
            raise ValueError("max_trade_days 需在 1~20 之间")
        if max_picks < 1 or max_picks > 10:
            raise ValueError("max_picks 需在 1~10 之间")

        trade_dates = self.service._fetch_trade_dates(start_date, end_date)
        if not trade_dates:
            raise ValueError("指定区间没有可用的因子和日线交集")
        if len(trade_dates) > max_trade_days:
            raise ValueError(f"区间包含 {len(trade_dates)} 个交易日，超过受控上限 {max_trade_days}")

        return {
            "baseline_id": baseline_id,
            "status": "planned",
            "methodology_version": BACKTEST_METHODOLOGY_VERSION,
            "strategies": unique_strategies,
            "start_date": start_date,
            "end_date": end_date,
            "return_mode": return_mode,
            "trade_dates": trade_dates,
            "sample_days": len(trade_dates),
            "max_picks": max_picks,
            "thresholds": {item: DEFAULT_THRESHOLDS[item] for item in unique_strategies},
            "resource_snapshot": system_memory_snapshot(),
            "active_queue": self._active_runs(),
            "safeguards": {
                "system_test_only": True,
                "serial_execution": True,
                "max_trade_days": max_trade_days,
                "min_available_mb": min_available_mb,
                "max_swap_used_mb": max_swap_used_mb,
            },
            "interpretation": "engineering_baseline_only",
        }

    def _wait_for_run(self, run_id: str, timeout_seconds: int, poll_seconds: float) -> Dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            run = self.service.get_run(run_id)
            if run.get("status") in TERMINAL_STATUSES:
                return run
            time.sleep(poll_seconds)
        self.service.request_cancel(run_id)
        raise TimeoutError(f"基线任务 {run_id} 超过 {timeout_seconds} 秒，已请求取消")

    def execute(
        self,
        *,
        baseline_id: str,
        strategies: Sequence[str],
        start_date: str,
        end_date: str,
        return_mode: str = "1d",
        max_trade_days: int = 10,
        max_picks: int = 3,
        min_available_mb: int = 1024,
        max_swap_used_mb: int = 512,
        timeout_seconds: int = 900,
        poll_seconds: float = 2.0,
    ) -> Dict[str, Any]:
        plan = self.plan(
            baseline_id=baseline_id,
            strategies=strategies,
            start_date=start_date,
            end_date=end_date,
            return_mode=return_mode,
            max_trade_days=max_trade_days,
            max_picks=max_picks,
            min_available_mb=min_available_mb,
            max_swap_used_mb=max_swap_used_mb,
        )
        lock_handle = acquire_mysql_advisory_lock("stock_analysis_backtest_validation_baseline")
        if lock_handle is None:
            raise RuntimeError("另一个回测验证基线正在执行")
        submitted: list[str] = []
        report: Dict[str, Any] | None = None
        try:
            existing = self._load_baseline_runs(plan["baseline_id"])
            if existing:
                raise RuntimeError(
                    f"基线 {plan['baseline_id']} 已存在 {len(existing)} 条任务；请使用 report-only 或更换 baseline_id"
                )
            self._assert_queue_idle()
            for strategy_id in plan["strategies"]:
                self._assert_queue_idle()
                self._assert_resources(min_available_mb, max_swap_used_mb)
                request = BacktestRequest(
                    strategy_id=strategy_id,
                    start_date=start_date,
                    end_date=end_date,
                    return_mode=return_mode,
                    instrument_type="stock",
                    max_picks=max_picks,
                    score_threshold=DEFAULT_THRESHOLDS[strategy_id],
                    is_system_test=True,
                    validation_baseline_id=plan["baseline_id"],
                )
                run = self.service.submit(request)
                run_id = str(run["run_id"])
                submitted.append(run_id)
                finished = self._wait_for_run(run_id, timeout_seconds, poll_seconds)
                if finished.get("status") != "success":
                    raise RuntimeError(
                        f"基线任务 {run_id} 状态为 {finished.get('status')}：{finished.get('error_message')}"
                    )
            report = self.report(plan["baseline_id"])
        finally:
            release_error = release_mysql_advisory_lock(lock_handle)
            if release_error and report is not None:
                report["lock_release_warning"] = release_error
        if report is None:
            raise RuntimeError("基线执行未生成报告")
        report["submitted_run_ids"] = submitted
        report["execution_resource_snapshot"] = system_memory_snapshot()
        return report

    def _load_baseline_runs(self, baseline_id: str) -> list[Dict[str, Any]]:
        return self.repository.list_baseline_runs(baseline_id)

    def _legacy_candidates(self, current: Dict[str, Any]) -> list[Dict[str, Any]]:
        return self.repository.list_legacy_candidates(
            current,
            LEGACY_BACKTEST_METHODOLOGY_VERSION,
        )

    @staticmethod
    def _legacy_match_score(current: Dict[str, Any], legacy: Dict[str, Any]) -> tuple[int, int]:
        current_request = _json_dict(current.get("request_json"))
        legacy_request = _json_dict(legacy.get("request_json"))
        score = sum(
            _request_value(current_request, field) == _request_value(legacy_request, field)
            for field in REQUEST_DEFAULTS
        )
        if str(current.get("strategy_version") or "") == str(legacy.get("strategy_version") or ""):
            score += 2
        return score, int(legacy.get("id") or 0)

    def report(self, baseline_id: str) -> Dict[str, Any]:
        baseline_id = validate_baseline_id(baseline_id)
        runs = self._load_baseline_runs(baseline_id)
        if not runs:
            raise ValueError(f"未找到基线 {baseline_id}")

        comparisons = []
        for current in runs:
            legacy_candidates = self._legacy_candidates(current)
            legacy = max(
                legacy_candidates,
                key=lambda row: self._legacy_match_score(current, row),
                default=None,
            )
            comparisons.append(build_run_comparison(current, legacy))

        sample_days = max(int(row.get("sample_days") or 0) for row in runs)
        statuses = {str(row.get("status")) for row in runs}
        return {
            "baseline_id": baseline_id,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "methodology_version": BACKTEST_METHODOLOGY_VERSION,
            "status": "success" if statuses == {"success"} else "incomplete",
            "run_count": len(runs),
            "sample_days": sample_days,
            "validation_scope": "engineering_baseline_only" if sample_days < 20 else "research_baseline_unvalidated",
            "statistical_validation": False,
            "comparisons": comparisons,
            "known_limitations": [
                "样本少于 20 个交易日时只验证工程链路，不评价策略有效性",
                "历史 ST、退市和成分变更数据仍不完整",
                "旧任务缺少配置 hash 时只能做方向性比较",
            ],
        }
