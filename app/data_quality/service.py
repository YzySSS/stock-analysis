from __future__ import annotations

import math
from datetime import date
from typing import Any

from app.data_quality.repository import DataQualityRepository


STATUS_RANK = {"pass": 0, "warn": 1, "fail": 2}


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


def evaluate_data_quality(snapshot: dict[str, Any]) -> dict[str, Any]:
    dates = snapshot.get("dates") or {}
    stock = snapshot.get("stock_basic") or {}
    kline = snapshot.get("daily_kline") or {}
    factor = snapshot.get("factor_input_daily") or {}
    future = snapshot.get("future_rows") or {}
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
            ),
            {key: _count(kline_gaps, key) for key in (
                "missing_total", "expected_non_trading", "new_listing_pending", "actionable_missing"
            )},
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
            ),
            {key: _count(factor_coverage, key) for key in (
                "missing_total", "expected_non_trading", "new_listing_pending", "actionable_missing"
            )},
        )
    )

    market_gaps = factor.get("market_field_gaps") or {}
    market_actionable = _count(market_gaps, "actionable_missing")
    checks.append(
        _check(
            "factor_input_market_fields",
            "factor_input_daily",
            "因子市场字段完整性",
            "fail" if not factor_date else _gap_status(market_actionable, active_stocks),
            (
                f"换手率、量比或市值字段缺失 {_count(market_gaps, 'missing_total')} 只："
                f"合法非交易 {_count(market_gaps, 'expected_non_trading')}，"
                f"待处理 {market_actionable}。PE 不参与本检查，避免把亏损股误判为故障。"
            ),
            {key: _count(market_gaps, key) for key in (
                "missing_total", "expected_non_trading", "new_listing_pending", "actionable_missing"
            )},
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
            "因子基本面字段可用性",
            fundamental_status,
            f"六项核心基本面字段全部为空 {missing_all_fundamental} 只，占有效股票 {fundamental_ratio:.2%}。",
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
        "health": health,
        "status": worst,
        "generated_at": snapshot.get("generated_at"),
        "reference_trade_date": dates.get("daily_kline_trade_date"),
        "counts": counts,
        "checks": checks,
    }


class DataQualityAuditService:
    def __init__(self, repository: DataQualityRepository | None = None) -> None:
        self.repository = repository or DataQualityRepository()

    def run(self) -> dict[str, Any]:
        return evaluate_data_quality(self.repository.fetch_snapshot())
