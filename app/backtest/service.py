from __future__ import annotations

import json
import hashlib
import math
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

from pymysql.err import IntegrityError

from app.backtest.policy import BACKTEST_METHODOLOGY_VERSION, research_disclosure
from app.backtest.repository import BacktestRepository
from app.jobs.errors import record_job_error
from app.jobs.mysql_state import MySQLJobStateRepository, MySQLJobTable, StaleRecoveryResult
from app.shared.instrument_policy import SUPPORTED_BACKTEST_INSTRUMENT_TYPES, require_supported_instrument
from app.stock_selection.selector import StockSelector
from app.strategies.service import StrategyService


@dataclass
class BacktestRequest:
    strategy_id: str
    start_date: str
    end_date: str
    return_mode: str = "1d"
    trade_strategy_id: Optional[str] = None
    evaluation_mode: str = "research"
    instrument_type: str = "stock"
    use_adjusted_price: bool = False
    commission_bps: float = 0.0
    stamp_tax_bps: float = 0.0
    slippage_bps: float = 0.0
    apply_execution_constraints: bool = False
    max_picks: Optional[int] = None
    score_threshold: Optional[float] = None
    is_system_test: bool = False
    validation_baseline_id: Optional[str] = None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


class BacktestService:
    """V2 回测服务。

    使用 factor_input_daily + daily_kline 的历史输入快照构造候选池。
    新任务统一采用收盘后才能确认的信号时点：
    - 信号：T 日收盘后形成
    - 1d：T+1 开盘买入，下一交易日开盘卖出
    - 3d：T+1 开盘买入，持有三个交易日后收盘卖出

    注意：多因子舆情选股依赖 Tavily 精排；为避免历史回测消耗大量 Tavily 次数，
    当前暂不开放严格复刻回测。
    """

    MAX_BACKTEST_DAYS = 260
    STALE_RUNNING_SECONDS = 30 * 60
    DEFAULT_MAX_ATTEMPTS = 2

    def __init__(
        self,
        job_states: MySQLJobStateRepository | None = None,
        repository: BacktestRepository | None = None,
    ) -> None:
        self.job_states = job_states or MySQLJobStateRepository(
            MySQLJobTable(table="backtest_run")
        )
        self.repository = repository or BacktestRepository()

    @staticmethod
    def _stable_hash(value: Any) -> str:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @classmethod
    def _strategy_config_hash(cls, selector: StockSelector) -> str:
        return cls._stable_hash(
            {
                "strategy_meta": selector.strategy_meta,
                "strategy_config": getattr(selector.strategy, "config", {}),
            }
        )

    @staticmethod
    def _methodology_metadata(request: BacktestRequest) -> Dict[str, Any]:
        return {
            "methodology_version": BACKTEST_METHODOLOGY_VERSION,
            "signal_timing": "T日收盘后形成信号",
            "entry_timing": "T+1交易日开盘",
            "exit_timing": {
                "1d": "入场后的下一交易日开盘",
                "3d": "含入场日在内持有三个交易日后收盘",
                "triple_barrier_5d": "入场后五个交易日内止盈/止损/到期退出",
                "observe_t3_daily": "入场后逐日观察至第三个交易日",
            }.get(request.return_mode),
            "fundamental_policy": "non_point_in_time_fundamentals_excluded",
            "universe_policy": "tushare_lifecycle_name_st_point_in_time_v3",
            "known_limitations": [
                "历史状态源与退市股票行情覆盖必须以 DQ3 审计结果为准",
                "DQ3 未覆盖的历史名称/ST 状态按未知并从候选中保守排除",
                "停牌日通过 Tushare 事件留痕，实际成交仍以行情和开盘价约束判定",
                "回测仍为 research-only 且未完成样本外验证",
            ],
        }

    @staticmethod
    def _exclude_non_point_in_time_fields(row: Dict[str, Any]) -> Dict[str, Any]:
        item = dict(row)
        for field in (
            "pe_tushare",
            "pb_tushare",
            "roe",
            "roa",
            "grossprofit_margin",
            "netprofit_margin",
            "revenue_yoy",
            "profit_yoy",
            "eps",
            "completeness_score",
        ):
            item[field] = None
        # 只有名称历史区间命中时，is_st 才是信号日可知状态；绝不回退当前 stock_basic.is_st。
        # 未知状态 fail-closed，经策略已有的 ST 硬过滤排除，同时保留显式未知标记供审计。
        pit_status_available = bool(item.get("pit_status_available"))
        item["pit_status_unknown"] = not pit_status_available
        item["is_st"] = bool(item.get("is_st")) if pit_status_available else True
        return item

    def _fetch_data_cutoff(self, end_date: str) -> str | None:
        value = self.repository.fetch_data_cutoff(end_date)
        return str(value) if value else None

    def submit(self, request: BacktestRequest) -> Dict[str, Any]:
        self._validate_request(request)
        selector = StockSelector(
            strategy_id=request.strategy_id,
            strategy_overrides={
                "max_picks": request.max_picks,
                "score_threshold": request.score_threshold,
            },
        )
        trade_dates = self._fetch_trade_dates(request.start_date, request.end_date)
        if not trade_dates:
            raise ValueError("该区间缺少历史输入数据，请先补 factor_input_daily 后再回测")
        if len(trade_dates) > self.MAX_BACKTEST_DAYS:
            raise ValueError(f"V2-P0 单次最多回测 {self.MAX_BACKTEST_DAYS} 个交易日，当前 {len(trade_dates)} 个")

        idempotency_key = self._backtest_idempotency_key(request, selector)
        existing = self._get_active_by_idempotency(idempotency_key)
        if existing:
            existing["deduplicated"] = True
            return existing

        run_id = f"backtest_{request.strategy_id}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        try:
            self._create_run(
                run_id,
                request,
                selector,
                datetime.now(),
                status="queued",
                progress_total_days=len(trade_dates),
                idempotency_key=idempotency_key,
            )
        except IntegrityError as exc:
            if exc.args and int(exc.args[0]) == 1062:
                existing = self._get_active_by_idempotency(idempotency_key)
                if existing:
                    existing["deduplicated"] = True
                    return existing
            raise
        return self.get_run(run_id)

    def _backtest_idempotency_key(self, request: BacktestRequest, selector: StockSelector) -> str:
        return self._stable_hash(
            {
                "job_type": "backtest",
                "request": request.__dict__,
                "strategy_config_hash": self._strategy_config_hash(selector),
            }
        )

    def _get_active_by_idempotency(self, idempotency_key: str) -> Dict[str, Any] | None:
        row = self.repository.get_active_run_by_idempotency(idempotency_key)
        if not row:
            return None
        return self.get_run(str(row["run_id"]))

    def run_background(self, run_id: str) -> None:
        try:
            run = self.get_run(run_id)
            request_json = run.get("request_json") or {}
            if isinstance(request_json, str):
                request_json = json.loads(request_json)
            request = BacktestRequest(**request_json)
            self._execute(run_id, request)
        except Exception as exc:
            self._finish_run(
                run_id,
                "failed",
                0,
                0,
                0,
                {},
                str(exc),
                self._backtest_error_code(exc),
            )

    def claim_next_queued_run(self, worker_id: str | None = None) -> str | None:
        """Atomically claim the oldest queued run for the worker."""
        return self.job_states.claim_next(
            worker_id or self._default_worker_id(),
            running_phase="回测执行中",
        )

    def recover_stale_running_runs(self, stale_seconds: int | None = None) -> StaleRecoveryResult:
        result = self.job_states.recover_stale(stale_seconds or self.STALE_RUNNING_SECONDS)
        if result.failed:
            record_job_error(
                "backtest",
                "backtest",
                "stale_retry_exhausted",
                "backtest worker heartbeat stale and max attempts exhausted",
                count=result.failed,
            )
        return result

    def request_cancel(self, run_id: str) -> Dict[str, Any]:
        status = self.job_states.request_cancel(run_id)
        if status is None:
            raise ValueError("backtest run not found")
        return self.get_run(run_id)

    def _is_cancel_requested(self, run_id: str) -> bool:
        return self.job_states.is_cancel_requested(run_id)

    @staticmethod
    def _backtest_error_code(exc: Exception) -> str:
        return "invalid_request" if isinstance(exc, (TypeError, ValueError)) else "backtest_failed"

    @staticmethod
    def _default_worker_id() -> str:
        return f"{socket.gethostname()}:{os.getpid()}"

    def get_run(self, run_id: str) -> Dict[str, Any]:
        row = self.repository.get_run(run_id)
        if not row:
            raise ValueError("backtest run not found")
        summary_json = row.get("summary_json")
        request_json = row.get("request_json")
        methodology_json = row.get("methodology_json")
        if isinstance(summary_json, str):
            row["summary_json"] = json.loads(summary_json)
        if isinstance(request_json, str):
            row["request_json"] = json.loads(request_json)
        if isinstance(methodology_json, str):
            row["methodology_json"] = json.loads(methodology_json)
        return row

    def _validate_request(self, request: BacktestRequest) -> None:
        request.instrument_type = require_supported_instrument(
            request.instrument_type,
            operation="backtest",
            supported=SUPPORTED_BACKTEST_INSTRUMENT_TYPES,
        )
        StrategyService().require_backtest_ready(
            request.strategy_id,
            instrument_type=request.instrument_type,
        )
        if request.trade_strategy_id:
            mapped_mode = {
                "next_open_1d": "1d",
                "hold_3d_close": "3d",
                "triple_barrier_5d": "triple_barrier_5d",
                "observe_t3_daily": "observe_t3_daily",
            }.get(request.trade_strategy_id)
            if not mapped_mode:
                raise ValueError("trade_strategy_id 当前仅支持 next_open_1d / hold_3d_close / triple_barrier_5d / observe_t3_daily")
            request.return_mode = mapped_mode
        else:
            request.trade_strategy_id = "next_open_1d" if request.return_mode == "1d" else "hold_3d_close"
        request.evaluation_mode = "realistic" if (request.commission_bps or request.stamp_tax_bps or request.slippage_bps or request.apply_execution_constraints) else "research"
        if request.return_mode not in {"1d", "3d", "triple_barrier_5d", "observe_t3_daily"}:
            raise ValueError("return_mode 仅支持 1d / 3d / triple_barrier_5d / observe_t3_daily")
        if request.commission_bps < 0 or request.commission_bps > 100:
            raise ValueError("commission_bps 需在 0~100 之间")
        if request.slippage_bps < 0 or request.slippage_bps > 100:
            raise ValueError("slippage_bps 需在 0~100 之间")
        if request.stamp_tax_bps < 0 or request.stamp_tax_bps > 100:
            raise ValueError("stamp_tax_bps 需在 0~100 之间")
        if request.validation_baseline_id and not request.is_system_test:
            raise ValueError("validation_baseline_id 仅允许用于系统测试任务")
        if request.validation_baseline_id and len(request.validation_baseline_id) > 80:
            raise ValueError("validation_baseline_id 最长 80 个字符")
        # V2.1 supports adjusted return calculation when adj_factor_daily has
        # coverage for the requested date range.

    def run(self, request: BacktestRequest, save: bool = True) -> Dict[str, Any]:
        self._validate_request(request)

        selector = StockSelector(
            strategy_id=request.strategy_id,
            strategy_overrides={
                "max_picks": request.max_picks,
                "score_threshold": request.score_threshold,
            },
        )
        run_id = f"backtest_{request.strategy_id}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        started_at = datetime.now()

        if save:
            self._create_run(run_id, request, selector, started_at)

        try:
            trade_dates = self._fetch_trade_dates(request.start_date, request.end_date)
            if not trade_dates:
                raise ValueError("该区间缺少历史输入数据，请先补 factor_input_daily 后再回测")
            if len(trade_dates) > self.MAX_BACKTEST_DAYS:
                raise ValueError(f"V2-P0 单次最多回测 {self.MAX_BACKTEST_DAYS} 个交易日，当前 {len(trade_dates)} 个")

            all_picks: List[Dict[str, Any]] = []
            all_trades: List[Dict[str, Any]] = []
            daily_summaries: List[Dict[str, Any]] = []
            rejection_counts: Dict[str, int] = {}

            for trade_date in trade_dates:
                candidates = self._load_candidates(selector, trade_date, request.instrument_type)
                selected = selector.run({"candidates": candidates})
                picks = [self._build_pick(row, trade_date) for row in selected]
                trades, daily_rejections = self._build_trades(run_id, request.strategy_id, trade_date, picks, request)
                self._merge_counts(rejection_counts, daily_rejections)
                summary = self._build_daily_summary(run_id, request.strategy_id, trade_date, trades)
                all_picks.extend(picks)
                all_trades.extend(trades)
                daily_summaries.append(summary)

            summary = self._build_run_summary(daily_summaries, all_trades, request.return_mode, rejection_counts, len(all_picks), request)
            if save:
                self._save_results(run_id, request.strategy_id, all_picks, all_trades, daily_summaries)
                self._finish_run(run_id, "success", len(trade_dates), len(all_picks), len(all_trades), summary)

            return {
                **research_disclosure(BACKTEST_METHODOLOGY_VERSION),
                "run_id": run_id,
                "status": "success",
                "strategy_id": request.strategy_id,
                "start_date": request.start_date,
                "end_date": request.end_date,
                "return_mode": request.return_mode,
                "sample_days": len(trade_dates),
                "total_picks": len(all_picks),
                "total_trades": len(all_trades),
                "summary": summary,
                "curve": daily_summaries,
            }
        except Exception as exc:
            if save:
                self._finish_run(
                    run_id,
                    "failed",
                    0,
                    0,
                    0,
                    {},
                    str(exc),
                    self._backtest_error_code(exc),
                )
            raise

    def _execute(self, run_id: str, request: BacktestRequest) -> None:
        self._validate_request(request)
        selector = StockSelector(
            strategy_id=request.strategy_id,
            strategy_overrides={
                "max_picks": request.max_picks,
                "score_threshold": request.score_threshold,
            },
        )
        trade_dates = self._fetch_trade_dates(request.start_date, request.end_date)
        if not trade_dates:
            raise ValueError("该区间缺少历史输入数据，请先补 factor_input_daily 后再回测")
        if len(trade_dates) > self.MAX_BACKTEST_DAYS:
            raise ValueError(f"V2-P0 单次最多回测 {self.MAX_BACKTEST_DAYS} 个交易日，当前 {len(trade_dates)} 个")

        self._clear_run_results(run_id)
        self._mark_running(run_id, len(trade_dates))
        started = time.monotonic()
        all_picks: List[Dict[str, Any]] = []
        all_trades: List[Dict[str, Any]] = []
        daily_summaries: List[Dict[str, Any]] = []
        rejection_counts: Dict[str, int] = {}

        for index, trade_date in enumerate(trade_dates, start=1):
            if self._is_cancel_requested(run_id):
                self._finish_run(
                    run_id,
                    "cancelled",
                    len(trade_dates),
                    len(all_picks),
                    len(all_trades),
                    self._build_run_summary(daily_summaries, all_trades, request.return_mode, rejection_counts, len(all_picks), request),
                    "cancel requested",
                    error_code="cancelled_by_user",
                )
                return
            candidates = self._load_candidates(selector, trade_date, request.instrument_type)
            selected = selector.run({"candidates": candidates})
            picks = [self._build_pick(row, trade_date) for row in selected]
            trades, daily_rejections = self._build_trades(run_id, request.strategy_id, trade_date, picks, request)
            self._merge_counts(rejection_counts, daily_rejections)
            summary = self._build_daily_summary(run_id, request.strategy_id, trade_date, trades)
            self._save_results(run_id, request.strategy_id, picks, trades, [summary])
            all_picks.extend(picks)
            all_trades.extend(trades)
            daily_summaries.append(summary)
            elapsed = max(time.monotonic() - started, 0.001)
            seconds_left = int((elapsed / index) * (len(trade_dates) - index)) if index else None
            self._update_progress(run_id, index, len(trade_dates), trade_date, seconds_left)

        summary = self._build_run_summary(daily_summaries, all_trades, request.return_mode, rejection_counts, len(all_picks), request)
        self._finish_run(run_id, "success", len(trade_dates), len(all_picks), len(all_trades), summary)

    def _fetch_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        return [
            str(row["trade_date"])
            for row in self.repository.fetch_trade_dates(start_date, end_date)
        ]

    def _has_lowvol_feature_cache(self, trade_date: str) -> bool:
        row = self.repository.lowvol_feature_cache_counts(trade_date)
        cache_count = int(row.get("cache_count") or 0)
        expected_count = int(row.get("expected_count") or 0)
        if expected_count <= 0:
            return cache_count > 0
        return cache_count >= max(1000, int(expected_count * 0.9))

    def _load_candidates_from_feature_cache(self, selector: StockSelector, trade_date: str, instrument_type: str) -> List[Dict[str, Any]]:
        rows = self.repository.load_feature_candidate_rows(
            trade_date,
            instrument_type,
        )
        candidates: List[Dict[str, Any]] = []
        for row in rows:
            item = selector._build_candidate(self._exclude_non_point_in_time_fields(row))
            item["open"] = _to_float(row.get("open"))
            item["close"] = _to_float(row.get("close"))
            candidates.append(item)
        return candidates

    def _load_candidates(self, selector: StockSelector, trade_date: str, instrument_type: str) -> List[Dict[str, Any]]:
        try:
            if self._has_lowvol_feature_cache(trade_date):
                return self._load_candidates_from_feature_cache(selector, trade_date, instrument_type)
        except Exception:
            # Cache table is optional; fall back to point-in-time window SQL.
            pass
        kline_window_start = self._fetch_window_start_date("daily_kline", trade_date, 90)
        factor_window_start = self._fetch_window_start_date("factor_input_daily", trade_date, 10)
        rows = self.repository.load_candidate_rows(
            trade_date=trade_date,
            instrument_type=instrument_type,
            kline_window_start=kline_window_start,
            factor_window_start=factor_window_start,
        )
        candidates: List[Dict[str, Any]] = []
        for row in rows:
            item = selector._build_candidate(self._exclude_non_point_in_time_fields(row))  # 复用 V1 候选口径，V2 后续再拆出公共 builder
            item["open"] = _to_float(row.get("open"))
            item["close"] = _to_float(row.get("close"))
            candidates.append(item)
        return candidates

    def _fetch_window_start_date(self, table: str, trade_date: str, limit: int) -> str:
        value = self.repository.fetch_window_start_date(table, trade_date, limit)
        return str(value or trade_date)

    def _build_pick(self, item: Dict[str, Any], trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "code": item.get("code"),
            "rank_no": item.get("rank_no"),
            "score": item.get("score"),
            "entry_price": None,
            "entry_price_type": "next_open",
            "factor_json": {
                "factors": item.get("factors", {}),
                "raw_metrics": item.get("explain", {}).get("raw_metrics", {}),
            },
            "explain_json": item.get("explain", {}),
        }

    def _build_trades(self, run_id: str, strategy_id: str, trade_date: str, picks: Sequence[Dict[str, Any]], request: BacktestRequest) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
        if not picks:
            return [], {}
        codes = [p["code"] for p in picks if p.get("code")]
        lookahead = 6 if request.return_mode == "triple_barrier_5d" else 3 if request.return_mode in {"3d", "observe_t3_daily"} else 2
        future = self._fetch_future_bars(codes, trade_date, lookahead=lookahead)
        trades: List[Dict[str, Any]] = []
        rejection_counts: Dict[str, int] = {}
        for pick in picks:
            code = pick["code"]
            bars = future.get(code, [])
            entry_bar = bars[0] if bars else None
            entry_price = _to_float(entry_bar.get("open")) if entry_bar else None
            if entry_price is None or entry_price <= 0:
                self._count_reason(rejection_counts, "missing_entry_price")
                continue
            pick["entry_price"] = entry_price
            pick["entry_price_type"] = "next_open"
            buy_block_reason = self._execution_block_reason(entry_bar, "buy") if request.apply_execution_constraints else None
            if buy_block_reason:
                self._count_reason(rejection_counts, buy_block_reason)
                continue
            one_day_bar = bars[1] if request.return_mode in {"1d", "observe_t3_daily"} and len(bars) > 1 else None
            three_day_bar = bars[2] if request.return_mode in {"3d", "observe_t3_daily"} and len(bars) > 2 else None
            triple_exit_bar = None
            triple_exit_price = None
            triple_exit_reason = None
            triple_exit_factor = None
            if request.return_mode == "triple_barrier_5d":
                triple_exit_bar, triple_exit_price, triple_exit_reason, triple_exit_factor = self._resolve_triple_barrier_exit(
                    bars,
                    entry_price,
                    request,
                    take_profit_pct=6.0,
                    stop_loss_pct=-3.0,
                    max_holding_days=5,
                )
            exit_price_1d = _to_float(one_day_bar.get("close" if request.return_mode == "observe_t3_daily" else "open")) if one_day_bar else None
            exit_price_3d = _to_float(three_day_bar.get("close")) if three_day_bar else None
            entry_factor = _to_float(entry_bar.get("adj_factor")) if entry_bar else None
            one_day_factor = _to_float(one_day_bar.get("adj_factor")) if one_day_bar else None
            three_day_factor = _to_float(three_day_bar.get("adj_factor")) if three_day_bar else None
            if request.return_mode == "triple_barrier_5d" and request.apply_execution_constraints and self._is_limit_blocked(triple_exit_bar, "sell"):
                triple_exit_price = None
                triple_exit_factor = None
                triple_exit_reason = "sell_blocked_limit_down"
                self._count_reason(rejection_counts, "sell_blocked_limit_down")
            one_day_block_reason = self._execution_block_reason(one_day_bar, "sell") if request.apply_execution_constraints and one_day_bar else None
            if one_day_block_reason:
                exit_price_1d = None
                one_day_factor = None
                self._count_reason(rejection_counts, one_day_block_reason)
            three_day_block_reason = self._execution_block_reason(three_day_bar, "sell") if request.apply_execution_constraints and three_day_bar else None
            if three_day_block_reason:
                exit_price_3d = None
                three_day_factor = None
                self._count_reason(rejection_counts, three_day_block_reason)
            price_path = bars[:lookahead]
            if request.use_adjusted_price and entry_factor:
                high_values = [self._adjusted_compare_price(_to_float(b.get("high")), _to_float(b.get("adj_factor")), entry_factor) for b in price_path]
                low_values = [self._adjusted_compare_price(_to_float(b.get("low")), _to_float(b.get("adj_factor")), entry_factor) for b in price_path]
            else:
                high_values = [_to_float(b.get("high")) for b in price_path]
                low_values = [_to_float(b.get("low")) for b in price_path]
            high_values = [v for v in high_values if v is not None]
            low_values = [v for v in low_values if v is not None]
            trades.append(
                {
                    "run_id": run_id,
                    "strategy_id": strategy_id,
                    "trade_date": trade_date,
                    "code": code,
                    "entry_date": str(entry_bar.get("trade_date")) if entry_bar else None,
                    "entry_price": entry_price,
                    "exit_date_1d": str(one_day_bar.get("trade_date")) if one_day_bar else None,
                    "exit_price_1d": exit_price_1d,
                    "return_1d_pct": self._pct_return(entry_price, exit_price_1d, entry_factor, one_day_factor, request) if request.return_mode in {"1d", "observe_t3_daily"} else None,
                    "exit_date_3d": str((triple_exit_bar or three_day_bar).get("trade_date")) if (triple_exit_bar or three_day_bar) else None,
                    "exit_price_3d": triple_exit_price if request.return_mode == "triple_barrier_5d" else exit_price_3d,
                    "return_3d_pct": self._pct_return(entry_price, triple_exit_price, entry_factor, triple_exit_factor, request) if request.return_mode == "triple_barrier_5d" else self._pct_return(entry_price, exit_price_3d, entry_factor, three_day_factor, request) if request.return_mode in {"3d", "observe_t3_daily"} else None,
                    "trade_strategy_id": request.trade_strategy_id,
                    "exit_reason": triple_exit_reason if request.return_mode == "triple_barrier_5d" else None,
                    "max_gain_pct": self._pct_return(entry_price, max(high_values) if high_values else None),
                    "max_drawdown_pct": self._pct_return(entry_price, min(low_values) if low_values else None),
                }
            )
        return trades, rejection_counts

    def _resolve_triple_barrier_exit(
        self,
        bars: Sequence[Dict[str, Any]],
        entry_price: float,
        request: BacktestRequest,
        take_profit_pct: float,
        stop_loss_pct: float,
        max_holding_days: int,
    ) -> tuple[Dict[str, Any] | None, float | None, str | None, float | None]:
        """Resolve the first hit among take-profit, stop-loss and time-exit.

        Backtest granularity is daily OHLC, so the trigger price is approximated
        by the configured barrier price when high/low crosses it; otherwise the
        final holding day exits at close.
        """
        if not bars or entry_price <= 0:
            return None, None, "missing_entry", None
        entry_factor = _to_float(bars[0].get("adj_factor")) if bars else None
        take_profit_price = entry_price * (1 + take_profit_pct / 100)
        stop_loss_price = entry_price * (1 + stop_loss_pct / 100)
        future_bars = list(bars[1 : max_holding_days + 1])
        for bar in future_bars:
            factor = _to_float(bar.get("adj_factor"))
            high = _to_float(bar.get("high"))
            low = _to_float(bar.get("low"))
            if request.use_adjusted_price and entry_factor:
                high = self._adjusted_compare_price(high, factor, entry_factor)
                low = self._adjusted_compare_price(low, factor, entry_factor)
            if low is not None and low <= stop_loss_price:
                return bar, stop_loss_price, "stop_loss", factor
            if high is not None and high >= take_profit_price:
                return bar, take_profit_price, "take_profit", factor
        final_bar = future_bars[-1] if future_bars else (bars[-1] if bars else None)
        if not final_bar:
            return None, None, "missing_exit", None
        return final_bar, _to_float(final_bar.get("close")), "time_exit", _to_float(final_bar.get("adj_factor"))

    def _fetch_future_bars(self, codes: Sequence[str], trade_date: str, lookahead: int) -> Dict[str, List[Dict[str, Any]]]:
        if not codes:
            return {}
        grouped: Dict[str, List[Dict[str, Any]]] = {code: [] for code in codes}
        for row in self.repository.fetch_future_bar_rows(codes, trade_date):
            code = row["code"]
            if len(grouped.setdefault(code, [])) < lookahead:
                grouped[code].append(row)
        return grouped

    @staticmethod
    def _adjusted_compare_price(price: float | None, factor: float | None, entry_factor: float | None) -> float | None:
        if price is None or factor is None or entry_factor is None or entry_factor <= 0:
            return None
        return price * factor / entry_factor

    @staticmethod
    def _is_limit_blocked(bar: Dict[str, Any] | None, side: str) -> bool:
        return BacktestService._execution_block_reason(bar, side) is not None

    @staticmethod
    def _execution_block_reason(bar: Dict[str, Any] | None, side: str) -> str | None:
        if not bar:
            return "missing_bar"
        open_price = _to_float(bar.get("open"))
        prev_close = _to_float(bar.get("prev_close"))
        if open_price is None or open_price <= 0:
            return "suspended_or_no_open"
        if prev_close is None or prev_close <= 0:
            return None
        pct = (open_price - prev_close) / prev_close * 100
        if side == "buy":
            return "buy_blocked_limit_up" if pct >= BacktestService._limit_rate_for_bar(bar) - 0.2 else None
        if side == "sell":
            return "sell_blocked_limit_down" if pct <= -BacktestService._limit_rate_for_bar(bar) + 0.2 else None
        return None

    @staticmethod
    def _limit_rate_for_bar(bar: Dict[str, Any] | None) -> float:
        code = str((bar or {}).get("code") or "")
        name = str((bar or {}).get("name") or "")
        if "ST" in name.upper():
            return 5.0
        if code.startswith(("sz.300", "sh.688")):
            return 20.0
        if code.startswith("bj."):
            return 30.0
        return 10.0

    @staticmethod
    def _count_reason(counts: Dict[str, int], reason: str | None) -> None:
        if reason:
            counts[reason] = counts.get(reason, 0) + 1

    @staticmethod
    def _merge_counts(target: Dict[str, int], incoming: Dict[str, int]) -> None:
        for key, value in incoming.items():
            target[key] = target.get(key, 0) + int(value or 0)

    @staticmethod
    def _pct_return(
        entry: float | None,
        exit_price: float | None,
        entry_factor: float | None = None,
        exit_factor: float | None = None,
        request: BacktestRequest | None = None,
    ) -> float | None:
        if entry is None or exit_price is None or entry <= 0:
            return None
        use_adjusted_price = bool(request.use_adjusted_price) if request else False
        commission_rate = max(_to_float(getattr(request, "commission_bps", 0)) or 0, 0) / 10000 if request else 0.0
        stamp_tax_rate = max(_to_float(getattr(request, "stamp_tax_bps", 0)) or 0, 0) / 10000 if request else 0.0
        slippage_rate = max(_to_float(getattr(request, "slippage_bps", 0)) or 0, 0) / 10000 if request else 0.0
        if use_adjusted_price:
            if entry_factor is None or exit_factor is None or entry_factor <= 0:
                return None
            entry_value = entry * entry_factor
            exit_value = exit_price * exit_factor
        else:
            entry_value = entry
            exit_value = exit_price
        effective_entry = entry_value * (1 + commission_rate + slippage_rate)
        effective_exit = exit_value * (1 - commission_rate - stamp_tax_rate - slippage_rate)
        if effective_entry <= 0:
            return None
        return round(((effective_exit - effective_entry) / effective_entry) * 100, 4)

    def _build_daily_summary(self, run_id: str, strategy_id: str, trade_date: str, trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        returns_1d = [t["return_1d_pct"] for t in trades if t.get("return_1d_pct") is not None]
        returns_3d = [t["return_3d_pct"] for t in trades if t.get("return_3d_pct") is not None]
        return {
            "run_id": run_id,
            "strategy_id": strategy_id,
            "trade_date": trade_date,
            "pick_count": len(trades),
            "avg_return_1d_pct": self._avg(returns_1d),
            "avg_return_3d_pct": self._avg(returns_3d),
            "win_rate_1d_pct": self._win_rate(returns_1d),
            "win_rate_3d_pct": self._win_rate(returns_3d),
            "benchmark_return_1d_pct": None,
            "benchmark_return_3d_pct": None,
        }

    @staticmethod
    def _avg(values: Sequence[float]) -> float | None:
        return round(sum(values) / len(values), 4) if values else None

    @staticmethod
    def _win_rate(values: Sequence[float]) -> float | None:
        return round((len([v for v in values if v > 0]) / len(values)) * 100, 4) if values else None

    def _build_run_summary(
        self,
        daily: Sequence[Dict[str, Any]],
        trades: Sequence[Dict[str, Any]],
        return_mode: str,
        rejection_counts: Dict[str, int] | None = None,
        total_picks: int | None = None,
        request: BacktestRequest | None = None,
    ) -> Dict[str, Any]:
        field = "return_1d_pct" if return_mode == "1d" else "return_3d_pct"
        daily_field = "avg_return_1d_pct" if return_mode == "1d" else "avg_return_3d_pct"
        values = [t[field] for t in trades if t.get(field) is not None]
        daily_values = [row[daily_field] for row in daily if row.get(daily_field) is not None]
        best = max(trades, key=lambda t: t.get(field) if t.get(field) is not None else -999999, default=None)
        worst = min(trades, key=lambda t: t.get(field) if t.get(field) is not None else 999999, default=None)
        equity = 1.0
        peak = 1.0
        max_drawdown_pct = 0.0
        equity_curve: List[Dict[str, Any]] = []
        for row in daily:
            daily_return = row.get(daily_field)
            if daily_return is not None:
                equity *= 1 + float(daily_return) / 100
            peak = max(peak, equity)
            drawdown = ((equity - peak) / peak) * 100 if peak else 0.0
            max_drawdown_pct = min(max_drawdown_pct, drawdown)
            equity_curve.append(
                {
                    "trade_date": row.get("trade_date"),
                    "daily_return_pct": daily_return,
                    "equity": round(equity, 6),
                    "total_return_pct": round((equity - 1) * 100, 4),
                    "drawdown_pct": round(drawdown, 4),
                }
            )
        return {
            "methodology": self._methodology_metadata(request) if request else None,
            "return_mode": return_mode,
            "trade_days": len(daily),
            "trade_count": len(values),
            "total_picks": total_picks if total_picks is not None else len(trades),
            "total_return_pct": round((equity - 1) * 100, 4),
            "avg_return_pct": self._avg(values),
            "avg_daily_return_pct": self._avg(daily_values),
            "max_drawdown_pct": round(max_drawdown_pct, 4),
            "win_rate_pct": self._win_rate(values),
            "sharpe_ratio": self._sharpe(daily_values),
            "sortino_ratio": self._sortino(daily_values),
            "calmar_ratio": self._calmar(equity, len(daily_values), max_drawdown_pct),
            "rejection_counts": rejection_counts or {},
            "rejected_trade_count": sum((rejection_counts or {}).values()),
            "execution_rule_summary": self._execution_rule_summary(request),
            "best_trade": {"code": best.get("code"), "return_pct": best.get(field)} if best else None,
            "worst_trade": {"code": worst.get("code"), "return_pct": worst.get(field)} if worst else None,
            "equity_curve": equity_curve,
        }

    @staticmethod
    def _sharpe(daily_returns_pct: Sequence[float]) -> float | None:
        if len(daily_returns_pct) < 2:
            return None
        values = [float(v) / 100 for v in daily_returns_pct]
        avg = sum(values) / len(values)
        variance = sum((v - avg) ** 2 for v in values) / (len(values) - 1)
        std = math.sqrt(variance)
        if std <= 0:
            return None
        return round((avg / std) * math.sqrt(252), 4)

    @staticmethod
    def _sortino(daily_returns_pct: Sequence[float]) -> float | None:
        if len(daily_returns_pct) < 2:
            return None
        values = [float(v) / 100 for v in daily_returns_pct]
        avg = sum(values) / len(values)
        downside = [min(v, 0.0) for v in values]
        downside_variance = sum(v * v for v in downside) / len(values)
        downside_std = math.sqrt(downside_variance)
        if downside_std <= 0:
            return None
        return round((avg / downside_std) * math.sqrt(252), 4)

    @staticmethod
    def _calmar(equity: float, trade_days: int, max_drawdown_pct: float) -> float | None:
        if trade_days <= 0 or max_drawdown_pct >= 0:
            return None
        annualized_return = equity ** (252 / trade_days) - 1
        drawdown = abs(max_drawdown_pct) / 100
        if drawdown <= 0:
            return None
        return round(annualized_return / drawdown, 4)

    @staticmethod
    def _execution_rule_summary(request: BacktestRequest | None) -> Dict[str, Any]:
        if not request:
            return {}
        return {
            "a_share_realistic": bool(request.apply_execution_constraints or request.commission_bps or request.stamp_tax_bps or request.slippage_bps),
            "execution_constraints": bool(request.apply_execution_constraints),
            "commission_bps": request.commission_bps,
            "stamp_tax_bps": request.stamp_tax_bps,
            "slippage_bps": request.slippage_bps,
            "lot_size_rule": "A股默认100股整手；科创板/北交所后续接仓位模型时按200股最小单位处理。",
            "lot_size_affects_return": False,
        }

    def _create_run(
        self,
        run_id: str,
        request: BacktestRequest,
        selector: StockSelector,
        started_at: datetime,
        status: str = "running",
        progress_total_days: int = 0,
        idempotency_key: str | None = None,
    ) -> None:
        methodology = self._methodology_metadata(request)
        self.repository.create_run(
            {
                "run_id": run_id,
                "strategy_id": request.strategy_id,
                "trade_strategy_id": request.trade_strategy_id,
                "strategy_version": selector.strategy_meta.get("version"),
                "instrument_type": request.instrument_type,
                "start_date": request.start_date,
                "end_date": request.end_date,
                "return_mode": request.return_mode,
                "evaluation_mode": request.evaluation_mode,
                "methodology_version": BACKTEST_METHODOLOGY_VERSION,
                "data_cutoff_date": self._fetch_data_cutoff(request.end_date),
                "strategy_config_hash": self._strategy_config_hash(selector),
                "methodology_json": _to_json(methodology),
                "use_adjusted_price": int(request.use_adjusted_price),
                "commission_bps": request.commission_bps,
                "stamp_tax_bps": request.stamp_tax_bps,
                "slippage_bps": request.slippage_bps,
                "execution_constraints_enabled": int(request.apply_execution_constraints),
                "is_system_test": int(request.is_system_test),
                "validation_baseline_id": request.validation_baseline_id,
                "status": status,
                "idempotency_key": idempotency_key,
                "active_idempotency_key": idempotency_key if status == "queued" else None,
                "attempt_count": 0,
                "max_attempts": self.DEFAULT_MAX_ATTEMPTS,
                "phase": "任务已提交" if status == "queued" else "回测执行中",
                "request_json": _to_json(request.__dict__),
                "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                "progress_total_days": progress_total_days,
                "progress_done_days": 0,
                "progress_pct": 0,
            }
        )

    def _mark_running(self, run_id: str, progress_total_days: int) -> None:
        self.repository.mark_running(
            run_id,
            progress_total_days,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

    def _clear_run_results(self, run_id: str) -> None:
        self.repository.clear_run_results(run_id)

    def _update_progress(self, run_id: str, done_days: int, total_days: int, current_trade_date: str, seconds_left: int | None) -> None:
        progress_pct = round((done_days / total_days) * 100, 4) if total_days else 0
        self.repository.update_progress(
            run_id=run_id,
            done_days=done_days,
            total_days=total_days,
            progress_pct=progress_pct,
            current_trade_date=current_trade_date,
            seconds_left=seconds_left,
            now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

    def _save_results(self, run_id: str, strategy_id: str, picks: Sequence[Dict[str, Any]], trades: Sequence[Dict[str, Any]], daily: Sequence[Dict[str, Any]]) -> None:
        self.repository.save_results(
            run_id=run_id,
            strategy_id=strategy_id,
            picks=picks,
            trades=trades,
            daily=daily,
        )

    def _finish_run(
        self,
        run_id: str,
        status: str,
        sample_days: int,
        total_picks: int,
        total_trades: int,
        summary: Dict[str, Any],
        error_message: str | None = None,
        error_code: str | None = None,
    ) -> None:
        phase = {
            "success": "运行完成",
            "failed": "运行失败",
            "cancelled": "已取消",
        }.get(status, status)
        updated = self.repository.finish_run(
            {
                "run_id": run_id,
                "status": status,
                "phase": phase,
                "sample_days": sample_days,
                "total_picks": total_picks,
                "total_trades": total_trades,
                "total_return_pct": summary.get("total_return_pct"),
                "avg_return_pct": summary.get("avg_return_pct"),
                "max_drawdown_pct": summary.get("max_drawdown_pct"),
                "win_rate_pct": summary.get("win_rate_pct"),
                "summary": summary,
                "error_code": error_code,
                "error_message": error_message,
                "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        if updated and status == "failed":
            record_job_error("backtest", "backtest", error_code, error_message)
