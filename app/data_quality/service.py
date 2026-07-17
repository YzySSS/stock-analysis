from __future__ import annotations

import math
from datetime import date
from typing import Any

from app.data_quality.repository import DataQualityRepository


STATUS_RANK = {"pass": 0, "warn": 1, "fail": 2}
PIT_MARKET_FIELD_MIN_COVERAGE = 0.95
PIT_FUNDAMENTAL_MIN_COVERAGE = 0.95


def _count(payload: dict[str, Any], key: str) -> int:
    return int(payload.get(key) or 0)


def _iso_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _gap_status(actionable: int, total: int) -> str:
    if actionable <= 0:
        return "pass"
    fail_threshold = max(20, math.ceil(max(total, 1) * 0.01))
    return "fail" if actionable > fail_threshold else "warn"


def _check(
    check_id: str,
    dataset: str,
    label: str,
    status: str,
    message: str,
    metrics: dict[str, Any],
    samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "dataset": dataset,
        "label": label,
        "status": status,
        "message": message,
        "metrics": metrics,
        "samples": samples or [],
    }


def _gap_tracking_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    actionable = [item for item in samples if item.get("classification") == "actionable_missing"]
    streaks = [
        int(item["consecutive_missing_trade_days"])
        for item in actionable
        if item.get("consecutive_missing_trade_days") is not None
    ]
    return {
        "tracked_actionable_samples": len(actionable),
        "single_day_samples": sum(value == 1 for value in streaks),
        "persistent_samples": sum(value >= 2 for value in streaks),
        "long_running_samples": sum(value >= 5 for value in streaks),
        "max_consecutive_missing_trade_days": max(streaks) if streaks else 0,
    }


def _gap_tracking_message(metrics: dict[str, Any]) -> str:
    tracked = int(metrics.get("tracked_actionable_samples") or 0)
    if tracked <= 0:
        return ""
    return (
        f" 待处理样本中持续缺失 {int(metrics.get('persistent_samples') or 0)} 只，"
        f"最长 {int(metrics.get('max_consecutive_missing_trade_days') or 0)} 个交易日。"
    )


def evaluate_data_quality(snapshot: dict[str, Any]) -> dict[str, Any]:
    dates = snapshot.get("dates") or {}
    stock = snapshot.get("stock_basic") or {}
    kline = snapshot.get("daily_kline") or {}
    factor = snapshot.get("factor_input_daily") or {}
    point_in_time = snapshot.get("point_in_time_status") or {}
    fundamental_pit = snapshot.get("point_in_time_fundamentals") or {}
    future = snapshot.get("future_rows") or {}
    upstream_attempts = snapshot.get("upstream_attempts") or {}
    active_stocks = _count(stock, "active_stock_rows")
    checks: list[dict[str, Any]] = []

    identity_keys = (
        "missing_instrument_type",
        "invalid_code",
        "missing_name",
        "missing_market",
        "market_code_mismatch",
    )
    identity_errors = sum(_count(stock, key) for key in identity_keys)
    checks.append(
        _check(
            "stock_basic_identity",
            "stock_basic",
            "股票主数据身份字段",
            "fail" if identity_errors else "pass",
            f"身份字段异常 {identity_errors} 条；当前有效股票 {active_stocks} 只。",
            {key: _count(stock, key) for key in identity_keys} | {"active_stock_rows": active_stocks},
        )
    )

    classification_gaps = (
        _count(stock, "missing_listing_date")
        + _count(stock, "missing_industry")
        + _count(stock, "suspected_delisted_active")
    )
    checks.append(
        _check(
            "stock_basic_classification",
            "stock_basic",
            "股票主数据分类字段",
            "warn" if classification_gaps else "pass",
            (
                f"上市日期缺 { _count(stock, 'missing_listing_date') } 条，"
                f"行业缺失或占位值 { _count(stock, 'missing_industry') } 条，"
                f"疑似已退市但仍在有效池 { _count(stock, 'suspected_delisted_active') } 条。"
            ),
            {
                "missing_listing_date": _count(stock, "missing_listing_date"),
                "missing_industry": _count(stock, "missing_industry"),
                "suspected_delisted_active": _count(stock, "suspected_delisted_active"),
            },
        )
    )

    kline_metrics = kline.get("metrics") or {}
    kline_integrity_keys = (
        "duplicate_rows",
        "orphan_rows",
        "null_ohlc",
        "nonpositive_ohlc",
        "invalid_ohlc_order",
        "invalid_volume",
        "invalid_amount",
        "missing_source",
    )
    kline_errors = sum(_count(kline_metrics, key) for key in kline_integrity_keys)
    has_kline = bool(dates.get("daily_kline_trade_date"))
    checks.append(
        _check(
            "daily_kline_integrity",
            "daily_kline",
            "最新日线值域与关联完整性",
            "fail" if not has_kline or kline_errors else "pass",
            (
                "没有可审计的日线数据。"
                if not has_kline
                else f"{dates.get('daily_kline_trade_date')} 共 {_count(kline_metrics, 'rows_count')} 条，硬异常 {kline_errors} 条。"
            ),
            {key: _count(kline_metrics, key) for key in kline_integrity_keys}
            | {"rows_count": _count(kline_metrics, "rows_count")},
        )
    )

    kline_gaps = kline.get("gaps") or {}
    kline_actionable = _count(kline_gaps, "actionable_missing")
    kline_gap_status = _gap_status(kline_actionable, active_stocks)
    kline_tracking = _gap_tracking_metrics(kline.get("samples") or [])
    checks.append(
        _check(
            "daily_kline_coverage",
            "daily_kline",
            "最新日线缺口分层",
            "fail" if not has_kline else kline_gap_status,
            (
                f"缺口 {_count(kline_gaps, 'missing_total')} 只："
                f"停牌/暂停上市 {_count(kline_gaps, 'expected_non_trading')}，"
                f"当日新股 {_count(kline_gaps, 'new_listing_pending')}，"
                f"待处理 {kline_actionable}。"
                f"{_gap_tracking_message(kline_tracking)}"
            ),
            {key: _count(kline_gaps, key) for key in (
                "missing_total", "expected_non_trading", "new_listing_pending", "actionable_missing"
            )}
            | kline_tracking
            | {"upstream_attempt": upstream_attempts.get("daily_kline") or {}},
            kline.get("samples") or [],
        )
    )

    kline_date = _iso_date(dates.get("daily_kline_trade_date"))
    status_date = _iso_date(dates.get("status_snapshot_trade_date"))
    status_lag_days = (kline_date - status_date).days if kline_date and status_date else None
    status_alignment = "pass"
    if status_lag_days is None or status_lag_days > 0:
        status_alignment = "fail" if status_lag_days is None or status_lag_days > 7 else "warn"
    checks.append(
        _check(
            "status_snapshot_alignment",
            "stock_status_snapshot",
            "停复牌状态与日线对齐",
            status_alignment,
            (
                "没有可用于解释行情缺口的状态快照。"
                if status_lag_days is None
                else f"状态快照 {dates.get('status_snapshot_trade_date')}，相对日线落后 {status_lag_days} 个自然日。"
            ),
            {
                "daily_kline_trade_date": dates.get("daily_kline_trade_date"),
                "status_snapshot_trade_date": dates.get("status_snapshot_trade_date"),
                "lag_calendar_days": status_lag_days,
            },
        )
    )

    lifecycle = point_in_time.get("lifecycle") or {}
    name_history = point_in_time.get("name_history") or {}
    suspension = point_in_time.get("suspension") or {}
    pit_manifest = point_in_time.get("manifest") or {}
    lifecycle_rows = _count(lifecycle, "lifecycle_rows")
    relevant_codes = _count(name_history, "relevant_codes")
    name_covered_codes = _count(name_history, "name_covered_codes")
    name_gap = max(relevant_codes - name_covered_codes, 0)
    expected_suspension_days = _count(suspension, "expected_trade_days")
    successful_suspension_days = _count(suspension, "successful_days")
    suspension_gap = max(expected_suspension_days - successful_suspension_days, 0)
    pit_hard_errors = (
        _count(lifecycle, "invalid_lifecycle_rows")
        + _count(name_history, "invalid_intervals")
    )
    if lifecycle_rows <= 0:
        pit_truth_status = "fail"
    elif (
        pit_hard_errors
        or pit_manifest.get("lifecycle_status") != "success"
        or pit_manifest.get("name_history_status") != "success"
        or name_gap
        or suspension_gap
        or _count(suspension, "failed_days")
    ):
        pit_truth_status = "warn"
    else:
        pit_truth_status = "pass"
    checks.append(
        _check(
            "point_in_time_status_truth",
            "point_in_time_status",
            "历史上市/ST/停复牌真相层",
            pit_truth_status,
            (
                f"生命周期 {lifecycle_rows} 只（上市 {_count(lifecycle, 'active_rows')} / "
                f"退市 {_count(lifecycle, 'delisted_rows')}）；"
                f"名称/ST 区间覆盖 {name_covered_codes}/{relevant_codes} 只；"
                f"停复牌分区覆盖 {successful_suspension_days}/{expected_suspension_days} 个交易日。"
            ),
            {
                "lifecycle_rows": lifecycle_rows,
                "active_rows": _count(lifecycle, "active_rows"),
                "delisted_rows": _count(lifecycle, "delisted_rows"),
                "invalid_lifecycle_rows": _count(lifecycle, "invalid_lifecycle_rows"),
                "name_history_rows": _count(name_history, "rows_count"),
                "name_covered_codes": name_covered_codes,
                "relevant_codes": relevant_codes,
                "missing_name_history_codes": name_gap,
                "invalid_name_intervals": _count(name_history, "invalid_intervals"),
                "st_intervals": _count(name_history, "st_intervals"),
                "delisting_intervals": _count(name_history, "delisting_intervals"),
                "successful_suspension_days": successful_suspension_days,
                "expected_suspension_days": expected_suspension_days,
                "failed_suspension_days": _count(suspension, "failed_days"),
                "lifecycle_manifest_status": pit_manifest.get("lifecycle_status"),
                "name_history_manifest_status": pit_manifest.get("name_history_status"),
                "upstream_attempt": upstream_attempts.get("stock_status_pit") or {},
            },
            point_in_time.get("name_samples") or [],
        )
    )

    historical_market = point_in_time.get("historical_market_data") or {}
    historical_delisted = _count(historical_market, "historical_delisted_codes")
    missing_historical_market = _count(historical_market, "missing_market_data_codes")
    historical_factor_rows = _count(historical_market, "historical_factor_rows")
    historical_market_field_rows = _count(historical_market, "historical_market_field_rows")
    market_field_coverage_ratio = float(
        historical_market.get("market_field_coverage_ratio") or 0
    )
    historical_market_status = (
        "pass"
        if lifecycle_rows > 0
        and missing_historical_market == 0
        and market_field_coverage_ratio >= PIT_MARKET_FIELD_MIN_COVERAGE
        else "warn"
    )
    if lifecycle_rows <= 0:
        historical_market_status = "fail"
    checks.append(
        _check(
            "point_in_time_historical_universe_data",
            "point_in_time_status",
            "历史退市股票行情/因子覆盖",
            historical_market_status,
            (
                f"回测区间重叠退市股票 {historical_delisted} 只："
                f"日线覆盖 {_count(historical_market, 'kline_covered_codes')} 只，"
                f"因子覆盖 {_count(historical_market, 'factor_covered_codes')} 只，"
                f"区间内无市场活动 {_count(historical_market, 'no_market_activity_codes')} 只，"
                f"仍缺 {missing_historical_market} 只；"
                f"关键市场字段 {historical_market_field_rows}/{historical_factor_rows} 行"
                f"（{market_field_coverage_ratio * 100:.2f}%）。"
                + (
                    " 历史退市行情/因子缺口已归零，关键字段达到门槛。"
                    if missing_historical_market == 0
                    and market_field_coverage_ratio >= PIT_MARKET_FIELD_MIN_COVERAGE
                    else " 缺口或关键字段覆盖未达门槛前，继续视为幸存者偏差风险。"
                )
            ),
            {
                "history_start_date": point_in_time.get("history_start_date"),
                "history_end_date": point_in_time.get("history_end_date"),
                "historical_delisted_codes": historical_delisted,
                "kline_covered_codes": _count(historical_market, "kline_covered_codes"),
                "factor_covered_codes": _count(historical_market, "factor_covered_codes"),
                "no_market_activity_codes": _count(historical_market, "no_market_activity_codes"),
                "missing_market_data_codes": missing_historical_market,
                "historical_factor_rows": historical_factor_rows,
                "historical_market_field_rows": historical_market_field_rows,
                "market_field_coverage_ratio": market_field_coverage_ratio,
                "market_field_min_coverage": PIT_MARKET_FIELD_MIN_COVERAGE,
                "backtest_universe_ready": (
                    lifecycle_rows > 0
                    and missing_historical_market == 0
                    and market_field_coverage_ratio >= PIT_MARKET_FIELD_MIN_COVERAGE
                ),
                "upstream_attempt": upstream_attempts.get("stock_status_pit") or {},
            },
            point_in_time.get("samples") or [],
        )
    )

    fundamental_table = fundamental_pit.get("table") or {}
    fundamental_manifest = fundamental_pit.get("manifest") or {}
    fundamental_coverage = fundamental_pit.get("coverage") or {}
    fundamental_rows = _count(fundamental_table, "rows_count")
    fundamental_codes = _count(fundamental_table, "distinct_codes")
    fundamental_expected = _count(fundamental_coverage, "expected_rows")
    fundamental_covered = _count(fundamental_coverage, "covered_rows")
    fundamental_coverage_ratio = float(fundamental_coverage.get("coverage_ratio") or 0)
    fundamental_hard_errors = sum(
        _count(fundamental_table, key)
        for key in (
            "invalid_reporting_order_rows",
            "future_announcement_rows",
            "future_period_rows",
        )
    )
    fundamental_ready = (
        fundamental_rows > 0
        and fundamental_expected > 0
        and fundamental_hard_errors == 0
        and fundamental_coverage_ratio >= PIT_FUNDAMENTAL_MIN_COVERAGE
    )
    if fundamental_rows <= 0 or fundamental_expected <= 0 or fundamental_hard_errors:
        fundamental_status = "fail"
    elif (
        not fundamental_ready
        or _count(fundamental_manifest, "successful_periods") <= 0
        or _count(fundamental_manifest, "partial_periods") > 0
        or _count(fundamental_manifest, "failed_periods") > 0
    ):
        fundamental_status = "warn"
    else:
        fundamental_status = "pass"
    checks.append(
        _check(
            "point_in_time_fundamental_truth",
            "point_in_time_fundamentals",
            "基本面公告日真相层",
            fundamental_status,
            (
                f"公告版本 {fundamental_rows} 条、{fundamental_codes} 只股票；"
                f"{_count(fundamental_coverage, 'sample_dates')} 个代表交易日 as-of 覆盖 "
                f"{fundamental_covered}/{fundamental_expected} 行"
                f"（{fundamental_coverage_ratio * 100:.2f}%），"
                f"最低覆盖日 {fundamental_coverage.get('worst_trade_date')}。"
            ),
            {
                "history_start_date": fundamental_pit.get("history_start_date"),
                "history_end_date": fundamental_pit.get("history_end_date"),
                "rows_count": fundamental_rows,
                "distinct_codes": fundamental_codes,
                "min_announcement_date": fundamental_table.get("min_announcement_date"),
                "max_announcement_date": fundamental_table.get("max_announcement_date"),
                "min_period_end_date": fundamental_table.get("min_period_end_date"),
                "max_period_end_date": fundamental_table.get("max_period_end_date"),
                "empty_indicator_rows": _count(fundamental_table, "empty_indicator_rows"),
                "invalid_reporting_order_rows": _count(
                    fundamental_table,
                    "invalid_reporting_order_rows",
                ),
                "future_announcement_rows": _count(
                    fundamental_table,
                    "future_announcement_rows",
                ),
                "future_period_rows": _count(fundamental_table, "future_period_rows"),
                "manifest_periods": _count(fundamental_manifest, "manifest_periods"),
                "successful_periods": _count(fundamental_manifest, "successful_periods"),
                "partial_periods": _count(fundamental_manifest, "partial_periods"),
                "failed_periods": _count(fundamental_manifest, "failed_periods"),
                "sample_dates": _count(fundamental_coverage, "sample_dates"),
                "expected_rows": fundamental_expected,
                "covered_rows": fundamental_covered,
                "missing_rows": _count(fundamental_coverage, "missing_rows"),
                "coverage_ratio": fundamental_coverage_ratio,
                "minimum_coverage_ratio": PIT_FUNDAMENTAL_MIN_COVERAGE,
                "worst_trade_date": fundamental_coverage.get("worst_trade_date"),
                "worst_coverage_ratio": fundamental_coverage.get("worst_coverage_ratio"),
                "backtest_fundamental_ready": fundamental_ready,
                "upstream_attempt": upstream_attempts.get("fundamental_pit") or {},
            },
            fundamental_pit.get("samples") or [],
        )
    )

    factor_date = _iso_date(dates.get("factor_input_trade_date"))
    factor_lag_days = (kline_date - factor_date).days if kline_date and factor_date else None
    if factor_lag_days is None:
        factor_alignment = "fail"
    elif factor_lag_days < 0:
        factor_alignment = "fail"
    elif factor_lag_days > 0:
        factor_alignment = "warn"
    else:
        factor_alignment = "pass"
    checks.append(
        _check(
            "factor_input_alignment",
            "factor_input_daily",
            "因子输入交易日对齐",
            factor_alignment,
            (
                "日线或因子输入没有可比较的交易日。"
                if factor_lag_days is None
                else f"因子输入 {dates.get('factor_input_trade_date')}，相对日线落后 {factor_lag_days} 个自然日。"
            ),
            {
                "daily_kline_trade_date": dates.get("daily_kline_trade_date"),
                "factor_input_trade_date": dates.get("factor_input_trade_date"),
                "lag_calendar_days": factor_lag_days,
            },
        )
    )

    factor_coverage = factor.get("coverage_gaps") or {}
    factor_actionable = _count(factor_coverage, "actionable_missing")
    factor_coverage_tracking = _gap_tracking_metrics(factor.get("coverage_samples") or [])
    checks.append(
        _check(
            "factor_input_coverage",
            "factor_input_daily",
            "因子输入缺口分层",
            "fail" if not factor_date else _gap_status(factor_actionable, active_stocks),
            (
                f"缺口 {_count(factor_coverage, 'missing_total')} 只："
                f"停牌/暂停上市 {_count(factor_coverage, 'expected_non_trading')}，"
                f"当日新股 {_count(factor_coverage, 'new_listing_pending')}，"
                f"待处理 {factor_actionable}。"
                f"{_gap_tracking_message(factor_coverage_tracking)}"
            ),
            {key: _count(factor_coverage, key) for key in (
                "missing_total", "expected_non_trading", "new_listing_pending", "actionable_missing"
            )}
            | factor_coverage_tracking
            | {"upstream_attempt": upstream_attempts.get("factor_input_daily") or {}},
            factor.get("coverage_samples") or [],
        )
    )

    market_gaps = factor.get("market_field_gaps") or {}
    market_actionable = _count(market_gaps, "actionable_missing")
    market_tracking = _gap_tracking_metrics(factor.get("samples") or [])
    checks.append(
        _check(
            "factor_input_market_fields",
            "factor_input_daily",
            "因子市场字段完整性",
            "fail" if not factor_date else _gap_status(market_actionable, active_stocks),
            (
                f"换手率、量比或市值字段缺失 {_count(market_gaps, 'missing_total')} 只："
                f"合法非交易 {_count(market_gaps, 'expected_non_trading')}，"
                f"待处理 {market_actionable}。"
                f"{_gap_tracking_message(market_tracking)}"
                "PE 不参与本检查，避免把亏损股误判为故障。"
            ),
            {key: _count(market_gaps, key) for key in (
                "missing_total", "expected_non_trading", "new_listing_pending", "actionable_missing"
            )}
            | market_tracking
            | {"upstream_attempt": upstream_attempts.get("factor_input_daily") or {}},
            factor.get("samples") or [],
        )
    )

    factor_metrics = factor.get("metrics") or {}
    missing_provenance = _count(factor_metrics, "missing_provenance_rows")
    factor_structural_errors = (
        missing_provenance
        + _count(factor_metrics, "duplicate_rows")
        + _count(factor_metrics, "orphan_rows")
        + _count(factor_metrics, "null_completeness")
    )
    checks.append(
        _check(
            "factor_input_integrity",
            "factor_input_daily",
            "因子输入来源与关联完整性",
            "fail" if not factor_date or factor_structural_errors else "pass",
            (
                f"来源/关联硬异常 {factor_structural_errors} 条；"
                f"低于 0.8 的旧完整度分 {_count(factor_metrics, 'low_completeness_rows')} 条，仅作观察。"
            ),
            {
                "duplicate_rows": _count(factor_metrics, "duplicate_rows"),
                "orphan_rows": _count(factor_metrics, "orphan_rows"),
                "null_completeness": _count(factor_metrics, "null_completeness"),
                "missing_provenance_rows": missing_provenance,
                "low_completeness_rows": _count(factor_metrics, "low_completeness_rows"),
                "missing_pe_rows": _count(factor_metrics, "missing_pe_rows"),
                "missing_pb_rows": _count(factor_metrics, "missing_pb_rows"),
                "avg_completeness": factor_metrics.get("avg_completeness"),
            },
        )
    )

    missing_all_fundamental = _count(factor_metrics, "missing_all_fundamental_rows")
    fundamental_ratio = missing_all_fundamental / active_stocks if active_stocks else 1.0
    fundamental_status = "pass"
    if missing_all_fundamental:
        fundamental_status = "fail" if fundamental_ratio > 0.05 else "warn"
    checks.append(
        _check(
            "factor_input_fundamental_fields",
            "factor_input_daily",
            "最新因子基本面快照可用性（非 PIT）",
            fundamental_status,
            (
                f"最新截面六项核心基本面字段全部为空 {missing_all_fundamental} 只，"
                f"占有效股票 {fundamental_ratio:.2%}；该项不代表历史 PIT 回测可用性。"
            ),
            {
                "missing_all_fundamental_rows": missing_all_fundamental,
                "active_stock_rows": active_stocks,
                "missing_ratio": round(fundamental_ratio, 6),
            },
        )
    )

    future_total = sum(_count(future, key) for key in ("daily_kline", "factor_input_daily", "stock_status_snapshot"))
    checks.append(
        _check(
            "future_trade_dates",
            "cross_dataset",
            "未来日期污染",
            "fail" if future_total else "pass",
            f"发现未来交易日记录 {future_total} 条。" if future_total else "未发现未来交易日记录。",
            {key: _count(future, key) for key in ("daily_kline", "factor_input_daily", "stock_status_snapshot")},
        )
    )

    counts = {
        "pass": sum(check["status"] == "pass" for check in checks),
        "warn": sum(check["status"] == "warn" for check in checks),
        "fail": sum(check["status"] == "fail" for check in checks),
    }
    worst = max((check["status"] for check in checks), key=lambda status: STATUS_RANK[status])
    health = {"pass": "healthy", "warn": "warning", "fail": "error"}[worst]
    return {
        "audit_version": snapshot.get("audit_version") or "dq1",
        "health": health,
        "status": worst,
        "generated_at": snapshot.get("generated_at"),
        "reference_trade_date": dates.get("daily_kline_trade_date"),
        "history_lookback_trade_days": snapshot.get("history_lookback_trade_days"),
        "upstream_attempts": upstream_attempts,
        "counts": counts,
        "checks": checks,
    }


class DataQualityAuditService:
    def __init__(self, repository: DataQualityRepository | None = None) -> None:
        self.repository = repository or DataQualityRepository()

    def run(self) -> dict[str, Any]:
        return evaluate_data_quality(self.repository.fetch_snapshot())
