from __future__ import annotations

import threading
import time
from datetime import date, datetime
from typing import Any, Dict, Iterable, Optional

from app.shared.db import mysql_conn
from app.shared.instrument_policy import (
    STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS,
    STOCK_DAILY_COMPLETENESS_RATIO,
    STOCK_INSTRUMENT_TYPE,
)
from app.shared.strategy_loader import StrategyLoader
from app.stock_selection.dataset_scope import (
    required_dataset_code_prefixes,
    sql_code_prefix_filter,
)


class StrategyCapabilityService:
    """Evaluate strategy capabilities from registry declarations and live data state."""

    DATASET_CACHE_SECONDS = 60
    LOADABILITY_CACHE_SECONDS = 300
    RUNTIME_ENABLED_STATUSES = {"enabled", "legacy_enabled"}
    BACKTEST_ENABLED_STATUSES = {"research_only"}
    VALIDATED_STATUSES = {"validated", "production_validated"}
    EVENT_DATASETS = {"sector_opinion_daily"}

    _cache_lock = threading.Lock()
    _dataset_cache: tuple[float, Dict[str, Any]] | None = None
    _loadability_cache: Dict[str, tuple[float, bool, Optional[str]]] = {}

    def __init__(
        self,
        loader: Optional[StrategyLoader] = None,
        dataset_snapshot: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.loader = loader or StrategyLoader()
        self._dataset_snapshot_override = dataset_snapshot

    def get_dataset_snapshot(self) -> Dict[str, Any]:
        if self._dataset_snapshot_override is not None:
            return self._dataset_snapshot_override

        now = time.monotonic()
        with self._cache_lock:
            cached = self._dataset_cache
            if cached and now - cached[0] < self.DATASET_CACHE_SECONDS:
                return cached[1]

        snapshot = self._query_dataset_snapshot()
        with self._cache_lock:
            type(self)._dataset_cache = (now, snapshot)
        return snapshot

    @staticmethod
    def _query_dataset_snapshot() -> Dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        (SELECT COUNT(*) FROM stock_basic WHERE instrument_type='stock' AND COALESCE(is_delisted, 0)=0) AS stock_count,
                        (
                            SELECT k.trade_date
                            FROM daily_kline k
                            INNER JOIN stock_basic daily_sb ON daily_sb.code=k.code
                            WHERE daily_sb.instrument_type='{STOCK_INSTRUMENT_TYPE}'
                              AND COALESCE(daily_sb.is_delisted, 0)=0
                              AND k.trade_date >= DATE_SUB(
                                  (SELECT MAX(trade_date) FROM daily_kline),
                                  INTERVAL {STOCK_DAILY_COMPLETENESS_LOOKBACK_DAYS} DAY
                              )
                            GROUP BY k.trade_date
                            HAVING COUNT(DISTINCT k.code) >= (
                                SELECT COUNT(*) * {STOCK_DAILY_COMPLETENESS_RATIO}
                                FROM stock_basic
                                WHERE instrument_type='{STOCK_INSTRUMENT_TYPE}'
                                  AND COALESCE(is_delisted, 0)=0
                            )
                            ORDER BY k.trade_date DESC
                            LIMIT 1
                        ) AS reference_trade_date,
                        (SELECT MAX(trade_date) FROM factor_input_daily) AS factor_input_date,
                        (SELECT MAX(trade_date) FROM stock_moneyflow_daily) AS moneyflow_date,
                        (SELECT MAX(trade_date) FROM stock_chip_daily) AS chip_date,
                        (SELECT MAX(as_of_datetime) FROM sector_opinion_daily) AS sector_opinion_at
                    """
                )
                row = cursor.fetchone() or {}
                counts: Dict[str, int] = {}
                dated_tables = {
                    "daily_kline_codes": ("daily_kline", row.get("reference_trade_date")),
                    "factor_input_codes": ("factor_input_daily", row.get("factor_input_date")),
                    "moneyflow_codes": ("stock_moneyflow_daily", row.get("moneyflow_date")),
                    "chip_codes": ("stock_chip_daily", row.get("chip_date")),
                }
                for key, (table_name, latest_date) in dated_tables.items():
                    eligible_code_prefixes = required_dataset_code_prefixes((table_name,))
                    universe_filter_sql = sql_code_prefix_filter(
                        "sb.code", eligible_code_prefixes
                    )
                    expected_key = f"{key}_expected"
                    if eligible_code_prefixes:
                        cursor.execute(
                            f"""
                            SELECT COUNT(*) AS count
                            FROM stock_basic sb
                            WHERE sb.instrument_type='stock'
                              AND COALESCE(sb.is_delisted, 0)=0
                              {universe_filter_sql}
                            """
                        )
                        counts[expected_key] = int(
                            (cursor.fetchone() or {}).get("count") or 0
                        )
                    else:
                        counts[expected_key] = int(row.get("stock_count") or 0)
                    if not latest_date:
                        counts[key] = 0
                        continue
                    cursor.execute(
                        f"""
                        SELECT COUNT(DISTINCT source.code) AS count
                        FROM {table_name} source
                        INNER JOIN stock_basic sb ON sb.code = source.code
                        WHERE source.trade_date = %s
                          AND sb.instrument_type = 'stock'
                          AND COALESCE(sb.is_delisted, 0) = 0
                          {universe_filter_sql}
                        """,
                        (latest_date,),
                    )
                    counts[key] = int((cursor.fetchone() or {}).get("count") or 0)
                if row.get("sector_opinion_at"):
                    cursor.execute(
                        "SELECT COUNT(*) AS count FROM sector_opinion_daily WHERE as_of_datetime=%s",
                        (row.get("sector_opinion_at"),),
                    )
                    counts["sector_opinion_rows"] = int((cursor.fetchone() or {}).get("count") or 0)
                else:
                    counts["sector_opinion_rows"] = 0

        reference_trade_date = row.get("reference_trade_date")
        return {
            "stock_count": int(row.get("stock_count") or 0),
            "reference_trade_date": reference_trade_date,
            "datasets": {
                "daily_kline": {
                    "latest_at": reference_trade_date,
                    "covered_codes": counts["daily_kline_codes"],
                    "expected_codes": counts["daily_kline_codes_expected"],
                },
                "factor_input_daily": {
                    "latest_at": row.get("factor_input_date"),
                    "covered_codes": counts["factor_input_codes"],
                    "expected_codes": counts["factor_input_codes_expected"],
                },
                "stock_moneyflow_daily": {
                    "latest_at": row.get("moneyflow_date"),
                    "covered_codes": counts["moneyflow_codes"],
                    "expected_codes": counts["moneyflow_codes_expected"],
                    "eligible_code_prefixes": list(
                        required_dataset_code_prefixes(("stock_moneyflow_daily",))
                    ),
                },
                "stock_chip_daily": {
                    "latest_at": row.get("chip_date"),
                    "covered_codes": counts["chip_codes"],
                    "expected_codes": counts["chip_codes_expected"],
                },
                "sector_opinion_daily": {
                    "latest_at": row.get("sector_opinion_at"),
                    "row_count": counts["sector_opinion_rows"],
                },
            },
        }

    @staticmethod
    def _as_date(value: Any) -> Optional[date]:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
        except (TypeError, ValueError):
            try:
                return date.fromisoformat(str(value)[:10])
            except (TypeError, ValueError):
                return None

    def _loadability(self, strategy_id: str, entrypoint: str) -> tuple[bool, Optional[str]]:
        registry_path = getattr(self.loader, "registry_path", None)
        cache_key = f"{registry_path}:{strategy_id}:{entrypoint}" if registry_path else ""
        now = time.monotonic()
        if cache_key:
            with self._cache_lock:
                cached = self._loadability_cache.get(cache_key)
                if cached and now - cached[0] < self.LOADABILITY_CACHE_SECONDS:
                    return cached[1], cached[2]

        try:
            self.loader.load_strategy(strategy_id)
            result = (True, None)
        except Exception as exc:
            result = (False, f"{type(exc).__name__}: {str(exc)[:240]}")

        if cache_key:
            with self._cache_lock:
                self._loadability_cache[cache_key] = (now, result[0], result[1])
        return result

    def _dataset_status(
        self,
        dataset_name: str,
        snapshot: Dict[str, Any],
        minimum_coverage: float,
        maximum_data_age_days: int,
    ) -> Dict[str, Any]:
        raw = (snapshot.get("datasets") or {}).get(dataset_name) or {}
        stock_count = int(snapshot.get("stock_count") or 0)
        expected_codes = (
            int(raw.get("expected_codes") or 0)
            if raw.get("expected_codes") is not None
            else stock_count
        )
        latest_at = raw.get("latest_at")
        reference_date = self._as_date(snapshot.get("reference_trade_date"))
        latest_date = self._as_date(latest_at)
        age_days = (reference_date - latest_date).days if reference_date and latest_date else None
        covered_codes = raw.get("covered_codes")
        coverage = None
        if covered_codes is not None and expected_codes > 0:
            coverage = round(int(covered_codes or 0) / expected_codes, 6)

        reasons = []
        if not latest_date:
            reasons.append("无可用快照")
        elif age_days is not None and age_days > maximum_data_age_days:
            reasons.append(f"落后基准交易日 {age_days} 天")

        if dataset_name in self.EVENT_DATASETS:
            if int(raw.get("row_count") or 0) <= 0:
                reasons.append("最新批次为空")
        elif coverage is None:
            reasons.append("无法计算覆盖率")
        elif coverage < minimum_coverage:
            reasons.append(f"覆盖率 {coverage:.2%} 低于 {minimum_coverage:.2%}")

        return {
            "name": dataset_name,
            "latest_at": str(latest_at) if latest_at else None,
            "covered_codes": int(covered_codes or 0) if covered_codes is not None else None,
            "expected_codes": expected_codes,
            "eligible_code_prefixes": list(raw.get("eligible_code_prefixes") or []),
            "row_count": int(raw.get("row_count") or 0) if raw.get("row_count") is not None else None,
            "coverage": coverage,
            "minimum_coverage": None if dataset_name in self.EVENT_DATASETS else minimum_coverage,
            "age_days": age_days,
            "maximum_data_age_days": maximum_data_age_days,
            "ready": not reasons,
            "reason": "；".join(reasons) if reasons else "就绪",
        }

    @staticmethod
    def _unique(values: Iterable[str]) -> list[str]:
        return list(dict.fromkeys(value for value in values if value))

    def evaluate(
        self,
        strategy_meta: Dict[str, Any],
        instrument_type: str = "stock",
        dataset_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        strategy_id = str(strategy_meta.get("id") or "")
        capability = strategy_meta.get("capability") or {}
        supported_instruments = [str(value).strip().lower() for value in capability.get("supported_instrument_types", [])]
        required_datasets = [str(value).strip() for value in capability.get("required_datasets", []) if str(value).strip()]
        minimum_coverage = float(capability.get("minimum_coverage", 0.95) or 0.95)
        maximum_data_age_days = int(capability.get("maximum_data_age_days", 1) or 1)
        runtime_status = str(capability.get("runtime_status") or "disabled")
        backtest_status = str(capability.get("backtest_status") or "disabled")
        validation_status = str(capability.get("validation_status") or "unvalidated")
        evidence_status = str(capability.get("evidence_status") or "none")
        normalized_instrument = str(instrument_type or "stock").strip().lower()
        executable = bool(strategy_meta.get("executable", True))

        loadable, load_error = self._loadability(strategy_id, str(strategy_meta.get("entrypoint") or ""))
        instrument_compatible = normalized_instrument in supported_instruments
        snapshot = dataset_snapshot or self.get_dataset_snapshot()
        dataset_statuses = [
            self._dataset_status(
                name,
                snapshot=snapshot,
                minimum_coverage=minimum_coverage,
                maximum_data_age_days=maximum_data_age_days,
            )
            for name in required_datasets
        ]
        data_ready = bool(required_datasets) and all(item.get("ready") for item in dataset_statuses)

        common_reasons = []
        if not executable:
            common_reasons.append("注册表已禁用执行")
        if not loadable:
            common_reasons.append(f"策略加载失败：{load_error or '未知错误'}")
        if not instrument_compatible:
            common_reasons.append(f"不支持 {normalized_instrument} 标的")
        if not required_datasets:
            common_reasons.append("注册表未声明必需数据集")
        common_reasons.extend(
            f"{item['name']}：{item['reason']}"
            for item in dataset_statuses
            if not item.get("ready")
        )

        runtime_reasons = list(common_reasons)
        if runtime_status not in self.RUNTIME_ENABLED_STATUSES:
            runtime_reasons.append(f"实时状态为 {runtime_status}")
        backtest_reasons = list(common_reasons)
        if backtest_status not in self.BACKTEST_ENABLED_STATUSES:
            backtest_reasons.append(f"回测状态为 {backtest_status}")

        runtime_ready = not runtime_reasons
        backtest_ready = not backtest_reasons
        validated = validation_status in self.VALIDATED_STATUSES

        if runtime_ready:
            availability = "runtime_ready"
            availability_label = "兼容可执行" if runtime_status == "legacy_enabled" else "可执行"
            availability_note = "策略可加载、标的兼容且必需数据已达到注册表门槛。"
        elif not loadable:
            availability = "load_failed"
            availability_label = "加载失败"
            availability_note = runtime_reasons[0] if runtime_reasons else "策略加载失败。"
        elif not data_ready:
            availability = "data_not_ready"
            availability_label = "数据未就绪"
            availability_note = next((reason for reason in runtime_reasons if "：" in reason), "必需数据未达到门槛。")
        elif runtime_status == "prototype":
            availability = "prototype"
            availability_label = "原型"
            availability_note = "代码可加载，但尚无足够真实运行证据，选股入口保持关闭。"
        elif backtest_ready:
            availability = "research"
            availability_label = "仅研究"
            availability_note = (
                str(capability.get("evidence_note"))
                if evidence_status == "historical_diagnostic_fail"
                else "实时选股入口关闭，仅保留研究回测。"
            )
        else:
            availability = "display_only"
            availability_label = "仅展示"
            availability_note = "当前未开放实时选股执行。"

        return {
            "loadable": loadable,
            "load_error": load_error,
            "supported_instrument_types": supported_instruments,
            "instrument_compatible": instrument_compatible,
            "required_datasets": required_datasets,
            "dataset_statuses": dataset_statuses,
            "data_ready": data_ready,
            "runtime_status": runtime_status,
            "runtime_ready": runtime_ready,
            "runtime_reasons": self._unique(runtime_reasons),
            "backtest_status": backtest_status,
            "backtest_ready": backtest_ready,
            "backtest_reasons": self._unique(backtest_reasons),
            "validation_status": validation_status,
            "validated": validated,
            "evidence_status": evidence_status,
            "evidence_note": capability.get("evidence_note"),
            "availability": availability,
            "availability_label": availability_label,
            "availability_note": availability_note,
            "backtest_note": (
                "研究回测已开放；冻结历史诊断未通过，不得作为交易有效性证据。"
                if backtest_ready and evidence_status == "historical_diagnostic_fail"
                else "研究回测已开放；当前仍未通过交易有效性验证。"
                if backtest_ready
                else (backtest_reasons[0] if backtest_reasons else "回测入口未开放。")
            ),
            "readiness_reasons": self._unique([*common_reasons, *runtime_reasons, *backtest_reasons]),
            "reference_trade_date": str(snapshot.get("reference_trade_date")) if snapshot.get("reference_trade_date") else None,
        }
