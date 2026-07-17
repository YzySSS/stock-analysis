from __future__ import annotations

import hashlib
import inspect
import json
import math
import random
import re
import time
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from app.backtest.policy import BACKTEST_METHODOLOGY_VERSION
from app.backtest.repository import BacktestRepository
from app.backtest.service import BacktestRequest, BacktestService
from app.backtest.validation_baseline import system_memory_snapshot
from app.backtest.validation_repository import StrategyValidationRepository
from app.shared.index_universe import ALL_A_UNIVERSE_CODE, normalize_backtest_universe, universe_label
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.stock_selection.selector import StockSelector


PROTOCOL_VERSION = "frozen_oos_protocol_v2"
REPORT_ENGINE_VERSION = "oos_report_v1"
HISTORICAL_HOLDOUT = "historical_locked_holdout"
PROSPECTIVE_OOS = "prospective_oos"
ALLOWED_VALIDATION_MODES = {HISTORICAL_HOLDOUT, PROSPECTIVE_OOS}
ALLOWED_VALIDATION_STRATEGIES = {"lowvol_reversal", "v13_three_factor"}
ALLOWED_BENCHMARK_INDEXES = {"000300.SH"}
TERMINAL_RUN_STATUSES = {"success", "failed", "cancelled"}
PROTOCOL_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")
DEFAULT_CRITERIA = {
    "minimum_trade_days": 120,
    "minimum_trades": 120,
    "minimum_benchmark_coverage_pct": 98.0,
    "minimum_total_return_pct": 0.0,
    "minimum_excess_return_pct": 0.0,
    "minimum_sharpe_ratio": 0.5,
    "maximum_drawdown_floor_pct": -20.0,
    "minimum_positive_excess_period_ratio": 0.5,
    "minimum_bootstrap_mean_excess_ci_low_pct": 0.0,
}
REPORT_LIMITATIONS = (
    "历史冻结诊断用于排雷，不具备真正未见样本的资格，不能升级策略 validation_status",
    "真正样本外协议必须在冻结数据截止日之后开始，且执行时配置、实现源码 hash 和方法论必须完全一致",
    "基准使用沪深300指数下一开盘至再下一开盘收益；策略为空仓的信号日按 0% 收益计",
    "bootstrap 为固定随机种子的日收益重采样，只提供不确定性提示，未完整建模序列相关性",
    "历史复权因子未覆盖整个验证区间，因此当前协议使用未复权的一日收益并显式保留该限制",
    "指数历史成分仍是月度权重快照精度，完整性以 DQ5 审计为准",
)


@dataclass(frozen=True)
class StrategyValidationRequest:
    protocol_id: str
    strategy_id: str
    validation_mode: str
    start_date: str
    end_date: str
    batch_id: str | None = None
    universe_code: str = ALL_A_UNIVERSE_CODE
    return_mode: str = "1d"
    benchmark_index_code: str = "000300.SH"
    max_picks: int = 3
    score_threshold: float = 60.0
    use_adjusted_price: bool = False
    commission_bps: float = 3.0
    stamp_tax_bps: float = 5.0
    slippage_bps: float = 5.0
    apply_execution_constraints: bool = True
    minimum_trade_days: int = 120
    minimum_trades: int = 120


def validate_protocol_id(protocol_id: str) -> str:
    value = str(protocol_id or "").strip()
    if not PROTOCOL_ID_PATTERN.fullmatch(value):
        raise ValueError("protocol_id 仅允许 1~80 位字母、数字、点、下划线、冒号或连字符")
    return value


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        loaded = json.loads(value)
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _compound(returns_pct: Sequence[float]) -> float:
    equity = 1.0
    for value in returns_pct:
        equity *= 1 + float(value) / 100
    return round((equity - 1) * 100, 4)


def _max_drawdown(returns_pct: Sequence[float]) -> float:
    equity = 1.0
    peak = 1.0
    drawdown = 0.0
    for value in returns_pct:
        equity *= 1 + float(value) / 100
        peak = max(peak, equity)
        drawdown = min(drawdown, ((equity - peak) / peak) * 100 if peak else 0.0)
    return round(drawdown, 4)


def _annualized_return(total_return_pct: float, days: int) -> float | None:
    if days <= 0 or total_return_pct <= -100:
        return None
    equity = 1 + total_return_pct / 100
    return round((equity ** (252 / days) - 1) * 100, 4)


def _annualized_ratio(returns_pct: Sequence[float]) -> float | None:
    if len(returns_pct) < 2:
        return None
    values = [float(value) / 100 for value in returns_pct]
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    if variance <= 0:
        return None
    return round((mean / math.sqrt(variance)) * math.sqrt(252), 4)


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(item) for item in values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _bootstrap_mean_ci(
    values: Sequence[float],
    *,
    seed_key: str,
    samples: int = 1000,
) -> dict[str, float | int | None]:
    if len(values) < 2:
        return {"samples": 0, "mean_pct": None, "ci_low_pct": None, "ci_high_pct": None}
    seed = int(hashlib.sha256(seed_key.encode("utf-8")).hexdigest()[:16], 16)
    generator = random.Random(seed)
    numeric = [float(item) for item in values]
    means = [
        sum(generator.choice(numeric) for _ in numeric) / len(numeric)
        for _ in range(samples)
    ]
    return {
        "samples": samples,
        "mean_pct": round(sum(numeric) / len(numeric), 6),
        "ci_low_pct": round(_percentile(means, 0.025) or 0.0, 6),
        "ci_high_pct": round(_percentile(means, 0.975) or 0.0, 6),
    }


def normalize_protocol_row(row: dict[str, Any], *, include_details: bool = True) -> dict[str, Any]:
    result = dict(row)
    for field in ("strategy_snapshot_json", "request_json", "criteria_json", "report_json"):
        result[field] = _json_dict(result.get(field)) if result.get(field) else None
    if not include_details:
        report = result.get("report_json") or {}
        result["report_json"] = {
            "generated_at": report.get("generated_at"),
            "verdict": report.get("verdict"),
            "validation_status": report.get("validation_status"),
            "metrics": report.get("metrics") or {},
            "interpretation": report.get("interpretation"),
        } if report else None
        result.pop("strategy_snapshot_json", None)
        result.pop("request_json", None)
        result.pop("criteria_json", None)
    for field in ("frozen_at", "executed_at", "finished_at", "created_at", "updated_at"):
        if result.get(field):
            result[field] = str(result[field])
    for field in ("freeze_data_cutoff_date", "start_date", "end_date"):
        if result.get(field):
            result[field] = str(result[field])
    for field in ("score_threshold", "commission_bps", "stamp_tax_bps", "slippage_bps"):
        result[field] = _as_float(result.get(field))
    for field in ("eligible_for_validation", "use_adjusted_price", "execution_constraints_enabled"):
        result[field] = bool(result.get(field))
    return result


def _request_matches(protocol: dict[str, Any], run: dict[str, Any]) -> tuple[bool, dict[str, bool]]:
    expected = _json_dict(protocol.get("request_json"))
    actual = _json_dict(run.get("request_json"))
    fields = (
        "strategy_id",
        "start_date",
        "end_date",
        "return_mode",
        "instrument_type",
        "universe_code",
        "use_adjusted_price",
        "commission_bps",
        "stamp_tax_bps",
        "slippage_bps",
        "apply_execution_constraints",
        "max_picks",
        "score_threshold",
        "validation_implementation_hash",
    )
    checks = {field: str(actual.get(field)) == str(expected.get(field)) for field in fields}
    return all(checks.values()), checks


def _implementation_fingerprint(selector: StockSelector) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[2]
    targets: list[Any] = [
        selector.strategy.__class__,
        StockSelector,
        BacktestService,
        BacktestRepository,
        normalize_backtest_universe,
    ]
    targets.extend(selector.strategy.__class__.__mro__[1:])
    files: dict[str, Path] = {}
    for target in targets:
        try:
            source_path = inspect.getsourcefile(target)
        except TypeError:
            continue
        if not source_path:
            continue
        path = Path(source_path).resolve()
        try:
            relative = str(path.relative_to(project_root))
        except ValueError:
            continue
        files[relative] = path
    digest = hashlib.sha256()
    items = []
    for relative, path in sorted(files.items()):
        payload = path.read_bytes()
        file_hash = hashlib.sha256(payload).hexdigest()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
        items.append({"path": relative, "sha256": file_hash})
    if not items:
        raise RuntimeError("无法解析策略实现源码，拒绝冻结不可审计协议")
    return {"sha256": digest.hexdigest(), "files": items}


def _frozen_implementation_hash(protocol: dict[str, Any]) -> str | None:
    snapshot = _json_dict(protocol.get("strategy_snapshot_json"))
    implementation = _json_dict(snapshot.get("implementation_fingerprint"))
    value = implementation.get("sha256")
    return str(value) if value else None


def build_validation_report(
    *,
    protocol: dict[str, Any],
    run: dict[str, Any],
    daily_rows: Sequence[dict[str, Any]],
    benchmark_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    criteria = {**DEFAULT_CRITERIA, **_json_dict(protocol.get("criteria_json"))}
    benchmark_dates = [_as_date(row["trade_date"]) for row in benchmark_rows]
    benchmark_opens = [_as_float(row.get("open")) for row in benchmark_rows]
    strategy_returns: list[float] = []
    aligned_strategy: list[float] = []
    benchmark_returns: list[float] = []
    aligned_dates: list[date] = []
    no_trade_days = 0
    missing_return_days = 0

    for row in daily_rows:
        signal_date = _as_date(row["trade_date"])
        strategy_return = _as_float(row.get("avg_return_1d_pct"))
        if strategy_return is None:
            strategy_return = 0.0
            if int(row.get("pick_count") or 0) == 0:
                no_trade_days += 1
            else:
                missing_return_days += 1
        strategy_returns.append(strategy_return)
        position = bisect_right(benchmark_dates, signal_date)
        if position + 1 >= len(benchmark_dates):
            continue
        entry_open = benchmark_opens[position]
        exit_open = benchmark_opens[position + 1]
        if entry_open is None or exit_open is None or entry_open <= 0:
            continue
        aligned_dates.append(signal_date)
        aligned_strategy.append(strategy_return)
        benchmark_returns.append(((exit_open / entry_open) - 1) * 100)

    excess_daily = [strategy - benchmark for strategy, benchmark in zip(aligned_strategy, benchmark_returns)]
    sample_days = len(daily_rows)
    aligned_days = len(excess_daily)
    benchmark_coverage_pct = round((aligned_days / sample_days) * 100, 4) if sample_days else 0.0
    strategy_total_return_pct = _compound(strategy_returns)
    aligned_strategy_total_return_pct = _compound(aligned_strategy)
    benchmark_total_return_pct = _compound(benchmark_returns)
    aligned_strategy_equity = 1 + aligned_strategy_total_return_pct / 100
    benchmark_equity = 1 + benchmark_total_return_pct / 100
    excess_return_pct = (
        round(((aligned_strategy_equity / benchmark_equity) - 1) * 100, 4)
        if benchmark_equity > 0
        else None
    )

    periods: dict[str, dict[str, list[float]]] = {}
    for signal_date, strategy_return, benchmark_return in zip(aligned_dates, aligned_strategy, benchmark_returns):
        period = f"{signal_date.year}Q{((signal_date.month - 1) // 3) + 1}"
        bucket = periods.setdefault(period, {"strategy": [], "benchmark": []})
        bucket["strategy"].append(strategy_return)
        bucket["benchmark"].append(benchmark_return)
    period_rows = []
    positive_excess_periods = 0
    for period, values in sorted(periods.items()):
        strategy_total = _compound(values["strategy"])
        benchmark_total = _compound(values["benchmark"])
        benchmark_period_equity = 1 + benchmark_total / 100
        excess_total = (
            round((((1 + strategy_total / 100) / benchmark_period_equity) - 1) * 100, 4)
            if benchmark_period_equity > 0
            else None
        )
        if excess_total is not None and excess_total > 0:
            positive_excess_periods += 1
        period_rows.append(
            {
                "period": period,
                "trade_days": len(values["strategy"]),
                "strategy_return_pct": strategy_total,
                "benchmark_return_pct": benchmark_total,
                "excess_return_pct": excess_total,
            }
        )
    positive_period_ratio = round(positive_excess_periods / len(period_rows), 4) if period_rows else 0.0
    bootstrap = _bootstrap_mean_ci(excess_daily, seed_key=str(protocol.get("protocol_id")))
    request_match, request_field_checks = _request_matches(protocol, run)
    frozen_implementation_hash = _frozen_implementation_hash(protocol)
    run_request = _json_dict(run.get("request_json"))

    structural_checks = {
        "run_success": str(run.get("status")) == "success",
        "methodology_hash_frozen": str(run.get("methodology_version")) == str(protocol.get("methodology_version")),
        "strategy_config_hash_frozen": str(run.get("strategy_config_hash")) == str(protocol.get("strategy_config_hash")),
        "request_frozen": request_match,
        "implementation_code_frozen": bool(
            frozen_implementation_hash
            and run_request.get("validation_implementation_hash") == frozen_implementation_hash
        ),
        "minimum_trade_days": sample_days >= int(criteria["minimum_trade_days"]),
        "minimum_trades": int(run.get("total_trades") or 0) >= int(criteria["minimum_trades"]),
        "benchmark_coverage": benchmark_coverage_pct >= float(criteria["minimum_benchmark_coverage_pct"]),
        "complete_return_series": missing_return_days == 0,
    }
    sharpe_ratio = _annualized_ratio(strategy_returns)
    max_drawdown_pct = _max_drawdown(strategy_returns)
    ci_low = _as_float(bootstrap.get("ci_low_pct"))
    performance_checks = {
        "positive_net_return": strategy_total_return_pct > float(criteria["minimum_total_return_pct"]),
        "positive_excess_return": excess_return_pct is not None and excess_return_pct > float(criteria["minimum_excess_return_pct"]),
        "minimum_sharpe_ratio": sharpe_ratio is not None and sharpe_ratio >= float(criteria["minimum_sharpe_ratio"]),
        "maximum_drawdown": max_drawdown_pct >= float(criteria["maximum_drawdown_floor_pct"]),
        "period_consistency": positive_period_ratio >= float(criteria["minimum_positive_excess_period_ratio"]),
        "bootstrap_mean_excess_support": ci_low is not None and ci_low > float(criteria["minimum_bootstrap_mean_excess_ci_low_pct"]),
    }
    structural_ready = all(structural_checks.values())
    performance_pass = all(performance_checks.values())
    mode = str(protocol.get("validation_mode"))
    if not structural_ready:
        verdict = "inconclusive"
    elif mode == HISTORICAL_HOLDOUT:
        verdict = "historical_diagnostic_pass" if performance_pass else "historical_diagnostic_fail"
    else:
        verdict = "prospective_oos_pass" if performance_pass else "prospective_oos_fail"
    validation_status = "oos_pass_candidate" if verdict == "prospective_oos_pass" else "validation_pending"

    return {
        "protocol_id": protocol.get("protocol_id"),
        "protocol_version": protocol.get("protocol_version"),
        "report_engine_version": REPORT_ENGINE_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "validation_mode": mode,
        "eligible_for_validation": bool(protocol.get("eligible_for_validation")),
        "verdict": verdict,
        "validation_status": validation_status,
        "auto_promoted": False,
        "run_id": run.get("run_id"),
        "frozen_identity": {
            "strategy_id": protocol.get("strategy_id"),
            "strategy_version": protocol.get("strategy_version"),
            "strategy_config_hash": protocol.get("strategy_config_hash"),
            "strategy_implementation_hash": frozen_implementation_hash,
            "methodology_version": protocol.get("methodology_version"),
            "freeze_data_cutoff_date": str(protocol.get("freeze_data_cutoff_date")),
            "start_date": str(protocol.get("start_date")),
            "end_date": str(protocol.get("end_date")),
            "universe_code": protocol.get("universe_code"),
            "universe_label": universe_label(str(protocol.get("universe_code"))),
        },
        "metrics": {
            "sample_days": sample_days,
            "trade_days": sample_days - no_trade_days,
            "no_trade_days": no_trade_days,
            "missing_return_days": missing_return_days,
            "total_trades": int(run.get("total_trades") or 0),
            "strategy_total_return_pct": strategy_total_return_pct,
            "strategy_annualized_return_pct": _annualized_return(strategy_total_return_pct, sample_days),
            "strategy_max_drawdown_pct": max_drawdown_pct,
            "strategy_sharpe_ratio": sharpe_ratio,
            "benchmark_index_code": protocol.get("benchmark_index_code"),
            "benchmark_coverage_pct": benchmark_coverage_pct,
            "benchmark_total_return_pct": benchmark_total_return_pct,
            "aligned_strategy_total_return_pct": aligned_strategy_total_return_pct,
            "excess_return_pct": excess_return_pct,
            "information_ratio": _annualized_ratio(excess_daily),
            "positive_excess_period_ratio": positive_period_ratio,
            "bootstrap_mean_excess_daily": bootstrap,
        },
        "criteria": criteria,
        "checks": {
            "structural": structural_checks,
            "request_fields": request_field_checks,
            "performance": performance_checks,
        },
        "periods": period_rows,
        "limitations": list(REPORT_LIMITATIONS),
        "interpretation": (
            "前瞻样本外通过候选；仍需人工复核并保持 research_only"
            if verdict == "prospective_oos_pass"
            else "历史冻结诊断仅用于排雷，不改变策略未验证状态"
            if mode == HISTORICAL_HOLDOUT
            else "前瞻样本外未通过或证据不足，策略继续保持未验证"
        ),
    }


class StrategyValidationService:
    def __init__(
        self,
        *,
        backtest_service: BacktestService | None = None,
        backtest_repository: BacktestRepository | None = None,
        repository: StrategyValidationRepository | None = None,
    ) -> None:
        self.backtest_service = backtest_service or BacktestService()
        self.backtest_repository = backtest_repository or self.backtest_service.repository
        self.repository = repository or StrategyValidationRepository()

    @staticmethod
    def _selector(request: StrategyValidationRequest | dict[str, Any]) -> StockSelector:
        strategy_id = request.strategy_id if isinstance(request, StrategyValidationRequest) else str(request["strategy_id"])
        max_picks = request.max_picks if isinstance(request, StrategyValidationRequest) else int(request["max_picks"])
        score_threshold = request.score_threshold if isinstance(request, StrategyValidationRequest) else float(request["score_threshold"])
        return StockSelector(
            strategy_id=strategy_id,
            strategy_overrides={"max_picks": max_picks, "score_threshold": score_threshold},
        )

    @staticmethod
    def _snapshot(selector: StockSelector) -> dict[str, Any]:
        return {
            "strategy_meta": selector.strategy_meta,
            "strategy_config": getattr(selector.strategy, "config", {}),
            "implementation_fingerprint": _implementation_fingerprint(selector),
        }

    @staticmethod
    def _criteria(request: StrategyValidationRequest) -> dict[str, Any]:
        return {
            **DEFAULT_CRITERIA,
            "minimum_trade_days": request.minimum_trade_days,
            "minimum_trades": request.minimum_trades,
        }

    def plan(self, request: StrategyValidationRequest) -> dict[str, Any]:
        protocol_id = validate_protocol_id(request.protocol_id)
        start = _as_date(request.start_date)
        end = _as_date(request.end_date)
        if start > end:
            raise ValueError("start_date 不能晚于 end_date")
        if request.validation_mode not in ALLOWED_VALIDATION_MODES:
            raise ValueError(f"validation_mode 仅支持：{sorted(ALLOWED_VALIDATION_MODES)}")
        if request.strategy_id not in ALLOWED_VALIDATION_STRATEGIES:
            raise ValueError(f"冻结验证仅支持：{sorted(ALLOWED_VALIDATION_STRATEGIES)}")
        if request.return_mode != "1d":
            raise ValueError("冻结验证当前协议只允许 1d，确保基准与成交语义唯一")
        if request.benchmark_index_code not in ALLOWED_BENCHMARK_INDEXES:
            raise ValueError(f"benchmark_index_code 仅支持：{sorted(ALLOWED_BENCHMARK_INDEXES)}")
        if not 1 <= request.max_picks <= 10:
            raise ValueError("max_picks 需在 1~10 之间")
        if not 0 <= request.score_threshold <= 100:
            raise ValueError("score_threshold 需在 0~100 之间")
        if request.minimum_trade_days < 60 or request.minimum_trade_days > 260:
            raise ValueError("minimum_trade_days 需在 60~260 之间")
        if request.minimum_trades < 30:
            raise ValueError("minimum_trades 不能低于 30")
        for field in ("commission_bps", "stamp_tax_bps", "slippage_bps"):
            value = float(getattr(request, field))
            if value < 0 or value > 100:
                raise ValueError(f"{field} 需在 0~100 之间")

        universe_code = normalize_backtest_universe(request.universe_code)
        cutoff_value = self.repository.current_data_cutoff()
        if not cutoff_value:
            raise RuntimeError("无法确定当前日线/因子共同数据截止日")
        cutoff = _as_date(cutoff_value)
        eligible = request.validation_mode == PROSPECTIVE_OOS
        trade_dates: list[str] = []
        if request.validation_mode == HISTORICAL_HOLDOUT:
            if end > cutoff:
                raise ValueError("历史冻结诊断的 end_date 不能晚于冻结数据截止日")
            trade_dates = self.backtest_service._fetch_trade_dates(request.start_date, request.end_date)
            if len(trade_dates) < request.minimum_trade_days:
                raise ValueError(
                    f"历史窗口仅 {len(trade_dates)} 个交易日，低于预声明下限 {request.minimum_trade_days}"
                )
            if len(trade_dates) > self.backtest_service.MAX_BACKTEST_DAYS:
                raise ValueError(
                    f"历史窗口 {len(trade_dates)} 个交易日，超过单任务上限 {self.backtest_service.MAX_BACKTEST_DAYS}"
                )
        else:
            if start <= cutoff:
                raise ValueError("真正前瞻样本外的 start_date 必须晚于冻结数据截止日")
            minimum_calendar_days = math.ceil(request.minimum_trade_days * 7 / 5)
            if (end - start).days + 1 < minimum_calendar_days:
                raise ValueError(
                    f"前瞻窗口至少预留 {minimum_calendar_days} 个自然日，才能容纳约 {request.minimum_trade_days} 个交易日"
                )

        selector = self._selector(request)
        snapshot = self._snapshot(selector)
        config_hash = self.backtest_service._strategy_config_hash(selector)
        implementation_hash = str(snapshot["implementation_fingerprint"]["sha256"])
        request_json = {
            "strategy_id": request.strategy_id,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "return_mode": request.return_mode,
            "trade_strategy_id": "next_open_1d",
            "evaluation_mode": "realistic",
            "instrument_type": "stock",
            "universe_code": universe_code,
            "use_adjusted_price": request.use_adjusted_price,
            "commission_bps": request.commission_bps,
            "stamp_tax_bps": request.stamp_tax_bps,
            "slippage_bps": request.slippage_bps,
            "apply_execution_constraints": request.apply_execution_constraints,
            "max_picks": request.max_picks,
            "score_threshold": request.score_threshold,
            "is_system_test": True,
            "validation_baseline_id": protocol_id,
            "validation_implementation_hash": implementation_hash,
        }
        return {
            "protocol_id": protocol_id,
            "batch_id": request.batch_id,
            "strategy_id": request.strategy_id,
            "strategy_version": selector.strategy_meta.get("version"),
            "strategy_config_hash": config_hash,
            "methodology_version": BACKTEST_METHODOLOGY_VERSION,
            "protocol_version": PROTOCOL_VERSION,
            "validation_mode": request.validation_mode,
            "eligible_for_validation": eligible,
            "frozen_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "freeze_data_cutoff_date": str(cutoff),
            "start_date": request.start_date,
            "end_date": request.end_date,
            "universe_code": universe_code,
            "universe_label": universe_label(universe_code),
            "return_mode": request.return_mode,
            "benchmark_index_code": request.benchmark_index_code,
            "max_picks": request.max_picks,
            "score_threshold": request.score_threshold,
            "use_adjusted_price": request.use_adjusted_price,
            "commission_bps": request.commission_bps,
            "stamp_tax_bps": request.stamp_tax_bps,
            "slippage_bps": request.slippage_bps,
            "execution_constraints_enabled": request.apply_execution_constraints,
            "minimum_trade_days": request.minimum_trade_days,
            "minimum_trades": request.minimum_trades,
            "strategy_snapshot_json": snapshot,
            "request_json": request_json,
            "criteria_json": self._criteria(request),
            "status": "planned",
            "verdict": "pending",
            "validation_status": "validation_pending",
            "planned_trade_days": len(trade_dates) if trade_dates else None,
            "interpretation": (
                "historical_diagnostic_only"
                if request.validation_mode == HISTORICAL_HOLDOUT
                else "prospective_oos_eligible_after_window_closes"
            ),
            "limitations": list(REPORT_LIMITATIONS),
        }

    def freeze(self, request: StrategyValidationRequest) -> dict[str, Any]:
        plan = self.plan(request)
        if self.repository.get_protocol(plan["protocol_id"]):
            raise ValueError(f"protocol_id 已存在：{plan['protocol_id']}")
        values = {
            **plan,
            "status": "frozen",
            "eligible_for_validation": int(bool(plan["eligible_for_validation"])),
            "use_adjusted_price": int(bool(plan["use_adjusted_price"])),
            "execution_constraints_enabled": int(bool(plan["execution_constraints_enabled"])),
        }
        self.repository.create_protocol(values)
        return self.get(plan["protocol_id"])

    def get(self, protocol_id: str) -> dict[str, Any]:
        row = self.repository.get_protocol(validate_protocol_id(protocol_id))
        if not row:
            raise ValueError("validation protocol not found")
        return normalize_protocol_row(row)

    def list(
        self,
        *,
        limit: int = 20,
        strategy_id: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        return [
            normalize_protocol_row(row, include_details=not compact)
            for row in self.repository.list_protocols(limit=limit, strategy_id=strategy_id)
        ]

    def rebuild_report(self, protocol_id: str) -> dict[str, Any]:
        protocol = self.get(protocol_id)
        if protocol.get("status") != "success" or not protocol.get("run_id"):
            raise ValueError("仅允许为已有成功 run 的协议重建确定性报告")
        run = self.backtest_service.get_run(str(protocol["run_id"]))
        if run.get("status") != "success":
            raise ValueError("关联回测任务不是 success，不能重建报告")
        daily_rows = self.repository.load_run_daily(str(protocol["run_id"]))
        benchmark_rows = self.repository.load_benchmark_rows(
            index_code=str(protocol["benchmark_index_code"]),
            start_date=str(protocol["start_date"]),
            end_date=str(protocol["end_date"]),
        )
        report = build_validation_report(
            protocol=protocol,
            run=run,
            daily_rows=daily_rows,
            benchmark_rows=benchmark_rows,
        )
        self.repository.replace_report(
            protocol_id=str(protocol["protocol_id"]),
            verdict=str(report["verdict"]),
            validation_status=str(report["validation_status"]),
            report=report,
        )
        return self.get(str(protocol["protocol_id"]))

    def supersede(
        self,
        protocol_id: str,
        *,
        replacement_protocol_id: str,
        reason: str,
    ) -> dict[str, Any]:
        protocol_id = validate_protocol_id(protocol_id)
        replacement_protocol_id = validate_protocol_id(replacement_protocol_id)
        if not self.repository.get_protocol(replacement_protocol_id):
            raise ValueError("replacement protocol not found")
        if not self.repository.supersede_protocol(
            protocol_id=protocol_id,
            replacement_protocol_id=replacement_protocol_id,
            reason=reason,
        ):
            raise ValueError("仅 frozen 或 failed 协议可标记为 superseded")
        return self.get(protocol_id)

    @staticmethod
    def _assert_resources(min_available_mb: int, max_swap_used_mb: int) -> dict[str, int]:
        snapshot = system_memory_snapshot()
        if snapshot["available_mb"] < min_available_mb:
            raise RuntimeError(
                f"可用内存仅 {snapshot['available_mb']} MiB，低于验证任务下限 {min_available_mb} MiB"
            )
        if snapshot["swap_used_mb"] > max_swap_used_mb:
            raise RuntimeError(
                f"Swap 已使用 {snapshot['swap_used_mb']} MiB，超过验证任务上限 {max_swap_used_mb} MiB"
            )
        return snapshot

    def _wait_for_run(self, run_id: str, timeout_seconds: int, poll_seconds: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            run = self.backtest_service.get_run(run_id)
            if run.get("status") in TERMINAL_RUN_STATUSES:
                return run
            time.sleep(poll_seconds)
        self.backtest_service.request_cancel(run_id)
        raise TimeoutError(f"样本外验证任务 {run_id} 超过 {timeout_seconds} 秒，已请求取消")

    def _finalize_success(self, protocol: dict[str, Any], finished_run: dict[str, Any]) -> dict[str, Any]:
        protocol_id = str(protocol["protocol_id"])
        run_id = str(finished_run["run_id"])
        daily_rows = self.repository.load_run_daily(run_id)
        benchmark_rows = self.repository.load_benchmark_rows(
            index_code=str(protocol["benchmark_index_code"]),
            start_date=str(protocol["start_date"]),
            end_date=str(protocol["end_date"]),
        )
        report = build_validation_report(
            protocol=protocol,
            run=finished_run,
            daily_rows=daily_rows,
            benchmark_rows=benchmark_rows,
        )
        self.repository.finish_protocol(
            protocol_id=protocol_id,
            verdict=str(report["verdict"]),
            validation_status=str(report["validation_status"]),
            report=report,
            finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return self.get(protocol_id)

    def reconcile(self, protocol_id: str) -> dict[str, Any]:
        """Finalize a protocol after a coordinator interruption without rerunning the backtest."""
        protocol = self.get(protocol_id)
        if protocol.get("status") == "success":
            return protocol
        if protocol.get("status") != "running" or not protocol.get("run_id"):
            raise ValueError("仅允许协调器中断后、已有 run_id 的 running 协议执行 reconcile")
        run = self.backtest_service.get_run(str(protocol["run_id"]))
        if run.get("status") in {"queued", "running"}:
            return {**protocol, "linked_run_status": run.get("status")}
        if run.get("status") == "success":
            return self._finalize_success(protocol, run)
        message = f"关联回测任务 {run.get('run_id')} 状态为 {run.get('status')}：{run.get('error_message')}"
        self.repository.fail_protocol(
            str(protocol["protocol_id"]),
            message,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return self.get(str(protocol["protocol_id"]))

    def execute(
        self,
        protocol_id: str,
        *,
        min_available_mb: int = 1024,
        max_swap_used_mb: int = 512,
        timeout_seconds: int = 7200,
        poll_seconds: float = 3.0,
    ) -> dict[str, Any]:
        protocol_id = validate_protocol_id(protocol_id)
        protocol = self.get(protocol_id)
        if protocol.get("status") != "frozen":
            raise ValueError(f"协议当前状态为 {protocol.get('status')}，仅 frozen 可执行")
        lock_handle = acquire_mysql_advisory_lock("stock_analysis_strategy_validation")
        if lock_handle is None:
            raise RuntimeError("另一个冻结验证任务正在执行")
        marked_running = False
        try:
            active = self.backtest_repository.list_active_runs()
            if active:
                raise RuntimeError(f"回测队列当前非空，拒绝叠加冻结验证任务：{active}")
            self._assert_resources(min_available_mb, max_swap_used_mb)
            current_cutoff = self.repository.current_data_cutoff()
            if not current_cutoff or _as_date(current_cutoff) < _as_date(protocol["end_date"]):
                raise RuntimeError(
                    f"协议窗口尚未闭合：当前数据截止 {current_cutoff}，要求至少到 {protocol['end_date']}"
                )
            trade_dates = self.backtest_service._fetch_trade_dates(protocol["start_date"], protocol["end_date"])
            if len(trade_dates) < int(protocol["minimum_trade_days"]):
                raise RuntimeError(
                    f"窗口仅 {len(trade_dates)} 个交易日，低于冻结下限 {protocol['minimum_trade_days']}"
                )
            if len(trade_dates) > self.backtest_service.MAX_BACKTEST_DAYS:
                raise RuntimeError("窗口超过回测单任务交易日上限")
            current_selector = self._selector(protocol)
            current_hash = self.backtest_service._strategy_config_hash(current_selector)
            if current_hash != protocol["strategy_config_hash"]:
                raise RuntimeError(
                    "策略配置已漂移，拒绝使用新配置污染冻结协议；请保留旧实现或新建协议"
                )
            frozen_implementation_hash = _frozen_implementation_hash(protocol)
            current_implementation_hash = _implementation_fingerprint(current_selector).get("sha256")
            if not frozen_implementation_hash or current_implementation_hash != frozen_implementation_hash:
                raise RuntimeError(
                    "策略实现源码已漂移或旧协议缺少代码指纹，拒绝执行；请保留旧实现或新建协议"
                )
            if BACKTEST_METHODOLOGY_VERSION != protocol["methodology_version"]:
                raise RuntimeError("回测方法论版本已变化，拒绝跨方法论执行冻结协议")

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not self.repository.mark_running(protocol_id, now):
                raise RuntimeError("冻结协议状态已变化，未开始执行")
            marked_running = True
            request_payload = _json_dict(protocol["request_json"])
            request = BacktestRequest(**request_payload)
            run = self.backtest_service.submit(request)
            run_id = str(run["run_id"])
            self.repository.attach_run(protocol_id, run_id)
            finished_run = self._wait_for_run(run_id, timeout_seconds, poll_seconds)
            if finished_run.get("status") != "success":
                raise RuntimeError(
                    f"回测任务 {run_id} 状态为 {finished_run.get('status')}：{finished_run.get('error_message')}"
                )
            return self._finalize_success(protocol, finished_run)
        except Exception as exc:
            if marked_running:
                self.repository.fail_protocol(
                    protocol_id,
                    str(exc),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
            raise
        finally:
            release_mysql_advisory_lock(lock_handle)
