from __future__ import annotations

import json
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

from app.shared.db import mysql_conn
from app.stock_selection.selector import StockSelector


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

    使用 factor_input_daily + daily_kline 的历史输入快照构造候选池，
    收益口径沿用 P0 文档：
    - 1d：当日开盘买入，下一交易日开盘卖出
    - 3d：当日开盘买入，第三个后续交易日收盘卖出

    注意：V12 选股依赖 Tavily 舆情精排；为避免历史回测消耗大量 Tavily 次数，
    当前暂不开放 V12 回测。
    """

    SUPPORTED_STRATEGIES = {
        "lowvol_reversal",
        "v13_three_factor",
        "fund_chip_repair",
        "quality_lowvol",
        "leader_tactics",
    }
    DISABLED_STRATEGIES = {
        "a_share_sentiment": "A股舆情选股 V2 依赖实时 market_opinion_v2，当前回测未注入历史舆情聚合，P0 阶段暂不支持回测。",
    }
    MAX_BACKTEST_DAYS = 260
    STALE_RUNNING_SECONDS = 30 * 60

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

        run_id = f"backtest_{request.strategy_id}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        self._create_run(run_id, request, selector, datetime.now(), status="queued", progress_total_days=len(trade_dates))
        return self.get_run(run_id)

    def run_background(self, run_id: str) -> None:
        run = self.get_run(run_id)
        request_json = run.get("request_json") or {}
        if isinstance(request_json, str):
            request_json = json.loads(request_json)
        request = BacktestRequest(**request_json)
        try:
            self._execute(run_id, request)
        except Exception as exc:
            self._finish_run(run_id, "failed", 0, 0, 0, {}, str(exc))

    def claim_next_queued_run(self, worker_id: str | None = None) -> str | None:
        """Atomically claim the oldest queued run for the worker."""
        worker_id = worker_id or self._default_worker_id()
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_id
                    FROM backtest_run
                    WHERE status = 'queued'
                    ORDER BY id ASC
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()
                if not row:
                    return None
                run_id = row["run_id"]
                cursor.execute(
                    """
                    UPDATE backtest_run
                    SET status='running', worker_id=%s, locked_at=%s, worker_heartbeat_at=%s,
                        started_at=%s, error_message=NULL
                    WHERE run_id=%s AND status='queued'
                    """,
                    (
                        worker_id,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        run_id,
                    ),
                )
                if cursor.rowcount != 1:
                    return None
                return run_id

    def recover_stale_running_runs(self, stale_seconds: int | None = None) -> int:
        stale_seconds = stale_seconds or self.STALE_RUNNING_SECONDS
        sql = """
        UPDATE backtest_run
        SET status='queued', worker_id=NULL, locked_at=NULL, worker_heartbeat_at=NULL,
            error_message=CONCAT(COALESCE(error_message, ''), %s)
        WHERE status='running'
          AND cancel_requested=0
          AND COALESCE(worker_heartbeat_at, started_at) < DATE_SUB(NOW(), INTERVAL %s SECOND)
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, ("\nRecovered from stale worker heartbeat.", stale_seconds))
                return cursor.rowcount

    def request_cancel(self, run_id: str) -> Dict[str, Any]:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE backtest_run
                    SET cancel_requested=1, status='cancelled', finished_at=NOW(), estimated_seconds_left=0,
                        worker_heartbeat_at=NOW(), error_message='cancel requested before worker started'
                    WHERE run_id=%s AND status='queued'
                    """,
                    (run_id,),
                )
                cursor.execute(
                    """
                    UPDATE backtest_run
                    SET cancel_requested=1, error_message='cancel requested'
                    WHERE run_id=%s AND status='running'
                    """,
                    (run_id,),
                )
        return self.get_run(run_id)

    def _is_cancel_requested(self, run_id: str) -> bool:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT cancel_requested FROM backtest_run WHERE run_id=%s", (run_id,))
                row = cursor.fetchone() or {}
        return bool(row.get("cancel_requested"))

    @staticmethod
    def _default_worker_id() -> str:
        return f"{socket.gethostname()}:{os.getpid()}"

    def get_run(self, run_id: str) -> Dict[str, Any]:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM backtest_run WHERE run_id=%s", (run_id,))
                row = cursor.fetchone()
        if not row:
            raise ValueError("backtest run not found")
        summary_json = row.get("summary_json")
        request_json = row.get("request_json")
        if isinstance(summary_json, str):
            row["summary_json"] = json.loads(summary_json)
        if isinstance(request_json, str):
            row["request_json"] = json.loads(request_json)
        return row

    def _validate_request(self, request: BacktestRequest) -> None:
        if request.strategy_id in self.DISABLED_STRATEGIES:
            raise ValueError(self.DISABLED_STRATEGIES[request.strategy_id])
        if request.strategy_id not in self.SUPPORTED_STRATEGIES:
            supported = " / ".join(sorted(self.SUPPORTED_STRATEGIES))
            raise ValueError(f"回测中心当前支持 {supported}")
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

            for trade_date in trade_dates:
                candidates = self._load_candidates(selector, trade_date, request.instrument_type)
                selected = selector.run({"candidates": candidates})
                picks = [self._build_pick(row, trade_date) for row in selected]
                trades = self._build_trades(run_id, request.strategy_id, trade_date, picks, request)
                summary = self._build_daily_summary(run_id, request.strategy_id, trade_date, trades)
                all_picks.extend(picks)
                all_trades.extend(trades)
                daily_summaries.append(summary)

            summary = self._build_run_summary(daily_summaries, all_trades, request.return_mode)
            if save:
                self._save_results(run_id, request.strategy_id, all_picks, all_trades, daily_summaries)
                self._finish_run(run_id, "success", len(trade_dates), len(all_picks), len(all_trades), summary)

            return {
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
                self._finish_run(run_id, "failed", 0, 0, 0, {}, str(exc))
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

        for index, trade_date in enumerate(trade_dates, start=1):
            if self._is_cancel_requested(run_id):
                self._finish_run(run_id, "cancelled", len(trade_dates), len(all_picks), len(all_trades), self._build_run_summary(daily_summaries, all_trades, request.return_mode), "cancel requested")
                return
            candidates = self._load_candidates(selector, trade_date, request.instrument_type)
            selected = selector.run({"candidates": candidates})
            picks = [self._build_pick(row, trade_date) for row in selected]
            trades = self._build_trades(run_id, request.strategy_id, trade_date, picks, request)
            summary = self._build_daily_summary(run_id, request.strategy_id, trade_date, trades)
            self._save_results(run_id, request.strategy_id, picks, trades, [summary])
            all_picks.extend(picks)
            all_trades.extend(trades)
            daily_summaries.append(summary)
            elapsed = max(time.monotonic() - started, 0.001)
            seconds_left = int((elapsed / index) * (len(trade_dates) - index)) if index else None
            self._update_progress(run_id, index, len(trade_dates), trade_date, seconds_left)

        summary = self._build_run_summary(daily_summaries, all_trades, request.return_mode)
        self._finish_run(run_id, "success", len(trade_dates), len(all_picks), len(all_trades), summary)

    def _fetch_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        sql = """
        SELECT DISTINCT f.trade_date
        FROM factor_input_daily f
        INNER JOIN daily_kline dk ON dk.code = f.code AND dk.trade_date = f.trade_date
        WHERE f.trade_date BETWEEN %s AND %s
        ORDER BY f.trade_date
        """
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (start_date, end_date))
                return [str(row["trade_date"]) for row in cursor.fetchall()]

    def _has_lowvol_feature_cache(self, trade_date: str) -> bool:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        (
                            SELECT COUNT(*)
                            FROM lowvol_reversal_feature_daily
                            WHERE trade_date = %s
                        ) AS cache_count,
                        (
                            SELECT COUNT(*)
                            FROM factor_input_daily f
                            INNER JOIN daily_kline dk
                              ON dk.code = f.code AND dk.trade_date = f.trade_date
                            INNER JOIN stock_basic sb ON sb.code = f.code
                            WHERE f.trade_date = %s
                              AND sb.instrument_type = 'stock'
                              AND sb.is_delisted = 0
                              AND dk.open IS NOT NULL
                              AND dk.open > 0
                        ) AS expected_count
                    """,
                    (trade_date, trade_date),
                )
                row = cursor.fetchone() or {}
        cache_count = int(row.get("cache_count") or 0)
        expected_count = int(row.get("expected_count") or 0)
        if expected_count <= 0:
            return cache_count > 0
        return cache_count >= max(1000, int(expected_count * 0.9))

    def _load_candidates_from_feature_cache(self, selector: StockSelector, trade_date: str, instrument_type: str) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            sb.code,
            sb.name,
            sb.industry,
            sb.instrument_type,
            sb.is_st,
            f.pe_tushare,
            f.pb_tushare,
            f.roe,
            f.roa,
            f.grossprofit_margin,
            f.netprofit_margin,
            f.revenue_yoy,
            f.profit_yoy,
            sb.eps,
            f.turnover_rate,
            f.volume_ratio,
            f.total_mv,
            f.completeness_score,
            dk.open,
            dk.close,
            dk.amount,
            dk.trade_date,
            lf.ma20,
            lf.ma60,
            lf.close_5d,
            lf.close_20d,
            lf.prev_close_1d,
            lf.max_close_20,
            lf.min_close_20,
            lf.avg_amount_20,
            lf.kline_count_20,
            lf.kline_count_60,
            lf.std_return_20,
            lf.pct_chg_1d,
            lf.turnover_rate_5d_avg,
            mf.net_mf_amount,
            mf.net_mf_vol,
            mf.buy_lg_amount,
            mf.sell_lg_amount,
            mf.buy_elg_amount,
            mf.sell_elg_amount,
            chip.his_low AS chip_his_low,
            chip.his_high AS chip_his_high,
            chip.cost_5pct AS chip_cost_5pct,
            chip.cost_15pct AS chip_cost_15pct,
            chip.cost_50pct AS chip_cost_50pct,
            chip.cost_85pct AS chip_cost_85pct,
            chip.cost_95pct AS chip_cost_95pct,
            chip.weight_avg AS chip_weight_avg,
            chip.winner_rate AS chip_winner_rate,
            DATEDIFF(f.trade_date, sb.listing_date) AS listed_days,
            ssd.sentiment_score,
            ssd.news_count,
            mcd.market_strength,
            mcd.market_state
        FROM factor_input_daily f
        INNER JOIN stock_basic sb ON sb.code = f.code
        INNER JOIN daily_kline dk ON dk.code = f.code AND dk.trade_date = f.trade_date
        INNER JOIN lowvol_reversal_feature_daily lf ON lf.code = f.code AND lf.trade_date = f.trade_date
        LEFT JOIN stock_moneyflow_daily mf ON mf.code = f.code AND mf.trade_date = f.trade_date
        LEFT JOIN stock_chip_daily chip ON chip.code = f.code AND chip.trade_date = f.trade_date
        LEFT JOIN stock_sentiment_daily ssd ON ssd.code = f.code
          AND ssd.trade_date = (
              SELECT MAX(s2.trade_date)
              FROM stock_sentiment_daily s2
              WHERE s2.code = f.code AND s2.trade_date <= f.trade_date
          )
        LEFT JOIN market_context_daily mcd ON mcd.trade_date = f.trade_date AND mcd.index_code = '000300.SH'
        WHERE f.trade_date = %s
          AND sb.instrument_type = %s
          AND sb.is_delisted = 0
          AND dk.open IS NOT NULL
          AND dk.open > 0
        ORDER BY sb.code
        """
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (trade_date, instrument_type))
                rows = cursor.fetchall()
        candidates: List[Dict[str, Any]] = []
        for row in rows:
            item = selector._build_candidate(row)
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
        sql = """
        SELECT
            sb.code,
            sb.name,
            sb.industry,
            sb.instrument_type,
            sb.is_st,
            f.pe_tushare,
            f.pb_tushare,
            f.roe,
            f.roa,
            f.grossprofit_margin,
            f.netprofit_margin,
            f.revenue_yoy,
            f.profit_yoy,
            sb.eps,
            f.turnover_rate,
            f.volume_ratio,
            f.total_mv,
            f.completeness_score,
            dk.open,
            dk.close,
            dk.amount,
            dk.trade_date,
            ma.ma20,
            ma.ma60,
            ma.close_5d,
            ma.close_20d,
            ma.prev_close_1d,
            ma.max_close_20,
            ma.min_close_20,
            ma.avg_amount_20,
            ma.kline_count_20,
            ma.kline_count_60,
            ma.std_return_20,
            ma.pct_chg_1d,
            tma.turnover_rate_5d_avg,
            mf.net_mf_amount,
            mf.net_mf_vol,
            mf.buy_lg_amount,
            mf.sell_lg_amount,
            mf.buy_elg_amount,
            mf.sell_elg_amount,
            chip.his_low AS chip_his_low,
            chip.his_high AS chip_his_high,
            chip.cost_5pct AS chip_cost_5pct,
            chip.cost_15pct AS chip_cost_15pct,
            chip.cost_50pct AS chip_cost_50pct,
            chip.cost_85pct AS chip_cost_85pct,
            chip.cost_95pct AS chip_cost_95pct,
            chip.weight_avg AS chip_weight_avg,
            chip.winner_rate AS chip_winner_rate,
            DATEDIFF(f.trade_date, sb.listing_date) AS listed_days,
            ssd.sentiment_score,
            ssd.news_count,
            mcd.market_strength,
            mcd.market_state
        FROM factor_input_daily f
        INNER JOIN stock_basic sb ON sb.code = f.code
        INNER JOIN daily_kline dk ON dk.code = f.code AND dk.trade_date = f.trade_date
        LEFT JOIN (
            SELECT
                code,
                AVG(CASE WHEN rn <= 20 THEN close END) AS ma20,
                AVG(CASE WHEN rn <= 60 THEN close END) AS ma60,
                MAX(CASE WHEN rn = 6 THEN close END) AS close_5d,
                MAX(CASE WHEN rn = 20 THEN close END) AS close_20d,
                MAX(CASE WHEN rn = 2 THEN close END) AS prev_close_1d,
                MAX(CASE WHEN rn <= 20 THEN close END) AS max_close_20,
                MIN(CASE WHEN rn <= 20 THEN close END) AS min_close_20,
                AVG(CASE WHEN rn <= 20 THEN amount END) AS avg_amount_20,
                SUM(CASE WHEN rn <= 20 THEN 1 ELSE 0 END) AS kline_count_20,
                COUNT(*) AS kline_count_60,
                STDDEV_SAMP(CASE WHEN rn <= 20 AND prev_close IS NOT NULL AND prev_close > 0 THEN close / prev_close - 1 END) AS std_return_20,
                MAX(CASE WHEN rn = 1 AND prev_close IS NOT NULL AND prev_close > 0 THEN (close - prev_close) / prev_close * 100 END) AS pct_chg_1d
            FROM (
                SELECT
                    code,
                    trade_date,
                    close,
                    amount,
                    LAG(close) OVER (PARTITION BY code ORDER BY trade_date) AS prev_close,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
                FROM daily_kline
                WHERE trade_date BETWEEN %s AND %s
            ) ranked
            WHERE rn <= 60
            GROUP BY code
        ) ma ON sb.code = ma.code
        LEFT JOIN (
            SELECT code, AVG(turnover_rate) AS turnover_rate_5d_avg
            FROM (
                SELECT
                    code,
                    turnover_rate,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
                FROM factor_input_daily
                WHERE trade_date BETWEEN %s AND %s
                  AND turnover_rate IS NOT NULL
            ) ranked_turnover
            WHERE rn <= 5
            GROUP BY code
        ) tma ON sb.code = tma.code
        LEFT JOIN stock_moneyflow_daily mf ON mf.code = f.code AND mf.trade_date = f.trade_date
        LEFT JOIN stock_chip_daily chip ON chip.code = f.code AND chip.trade_date = f.trade_date
        LEFT JOIN stock_sentiment_daily ssd ON ssd.code = f.code
          AND ssd.trade_date = (
              SELECT MAX(s2.trade_date)
              FROM stock_sentiment_daily s2
              WHERE s2.code = f.code AND s2.trade_date <= f.trade_date
          )
        LEFT JOIN market_context_daily mcd ON mcd.trade_date = f.trade_date AND mcd.index_code = '000300.SH'
        WHERE f.trade_date = %s
          AND sb.instrument_type = %s
          AND sb.is_delisted = 0
          AND dk.open IS NOT NULL
          AND dk.open > 0
        ORDER BY sb.code
        """
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (kline_window_start, trade_date, factor_window_start, trade_date, trade_date, instrument_type))
                rows = cursor.fetchall()
        candidates: List[Dict[str, Any]] = []
        for row in rows:
            item = selector._build_candidate(row)  # 复用 V1 候选口径，V2 后续再拆出公共 builder
            item["open"] = _to_float(row.get("open"))
            item["close"] = _to_float(row.get("close"))
            candidates.append(item)
        return candidates

    @staticmethod
    def _fetch_window_start_date(table: str, trade_date: str, limit: int) -> str:
        if table not in {"daily_kline", "factor_input_daily"}:
            raise ValueError("unsupported history table")
        sql = f"""
        SELECT MIN(trade_date) AS start_date
        FROM (
            SELECT DISTINCT trade_date
            FROM {table}
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT %s
        ) recent_dates
        """
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (trade_date, int(limit)))
                row = cursor.fetchone() or {}
        return str(row.get("start_date") or trade_date)

    def _build_pick(self, item: Dict[str, Any], trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "code": item.get("code"),
            "rank_no": item.get("rank_no"),
            "score": item.get("score"),
            "entry_price": item.get("open"),
            "entry_price_type": "open",
            "factor_json": {
                "factors": item.get("factors", {}),
                "raw_metrics": item.get("explain", {}).get("raw_metrics", {}),
            },
            "explain_json": item.get("explain", {}),
        }

    def _build_trades(self, run_id: str, strategy_id: str, trade_date: str, picks: Sequence[Dict[str, Any]], request: BacktestRequest) -> List[Dict[str, Any]]:
        if not picks:
            return []
        codes = [p["code"] for p in picks if p.get("code")]
        lookahead = 6 if request.return_mode == "triple_barrier_5d" else 4 if request.return_mode in {"3d", "observe_t3_daily"} else 2
        future = self._fetch_future_bars(codes, trade_date, lookahead=lookahead)
        trades: List[Dict[str, Any]] = []
        for pick in picks:
            code = pick["code"]
            bars = future.get(code, [])
            entry_price = _to_float(pick.get("entry_price"))
            if entry_price is None or entry_price <= 0:
                continue
            entry_bar = bars[0] if bars else None
            if request.apply_execution_constraints and self._is_limit_blocked(entry_bar, "buy"):
                continue
            one_day_bar = bars[1] if request.return_mode in {"1d", "observe_t3_daily"} and len(bars) > 1 else None
            three_day_bar = bars[3] if request.return_mode in {"3d", "observe_t3_daily"} and len(bars) > 3 else None
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
            if request.apply_execution_constraints and self._is_limit_blocked(one_day_bar, "sell"):
                exit_price_1d = None
                one_day_factor = None
            if request.apply_execution_constraints and self._is_limit_blocked(three_day_bar, "sell"):
                exit_price_3d = None
                three_day_factor = None
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
                    "entry_date": trade_date,
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
        return trades

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
        placeholders = ",".join(["%s"] * len(codes))
        sql = f"""
        SELECT dk.code, dk.trade_date, dk.open, dk.high, dk.low, dk.close,
               prev.close AS prev_close,
               af.adj_factor
        FROM daily_kline dk
        LEFT JOIN adj_factor_daily af ON af.code = dk.code AND af.trade_date = dk.trade_date
        LEFT JOIN daily_kline prev ON prev.code = dk.code
          AND prev.trade_date = (
            SELECT MAX(p.trade_date)
            FROM daily_kline p
            WHERE p.code = dk.code AND p.trade_date < dk.trade_date
          )
        WHERE dk.code IN ({placeholders}) AND dk.trade_date >= %s
        ORDER BY dk.code, dk.trade_date
        """
        params = list(codes) + [trade_date]
        grouped: Dict[str, List[Dict[str, Any]]] = {code: [] for code in codes}
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                for row in cursor.fetchall():
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
        if not bar:
            return True
        open_price = _to_float(bar.get("open"))
        prev_close = _to_float(bar.get("prev_close"))
        if open_price is None or open_price <= 0:
            return True
        if prev_close is None or prev_close <= 0:
            return False
        pct = (open_price - prev_close) / prev_close * 100
        if side == "buy":
            return pct >= 9.8
        if side == "sell":
            return pct <= -9.8
        return False

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

    def _build_run_summary(self, daily: Sequence[Dict[str, Any]], trades: Sequence[Dict[str, Any]], return_mode: str) -> Dict[str, Any]:
        field = "return_1d_pct" if return_mode == "1d" else "return_3d_pct"
        daily_field = "avg_return_1d_pct" if return_mode == "1d" else "avg_return_3d_pct"
        values = [t[field] for t in trades if t.get(field) is not None]
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
            "return_mode": return_mode,
            "trade_days": len(daily),
            "trade_count": len(values),
            "total_return_pct": round((equity - 1) * 100, 4),
            "avg_return_pct": self._avg(values),
            "avg_daily_return_pct": self._avg([row[daily_field] for row in daily if row.get(daily_field) is not None]),
            "max_drawdown_pct": round(max_drawdown_pct, 4),
            "win_rate_pct": self._win_rate(values),
            "best_trade": {"code": best.get("code"), "return_pct": best.get(field)} if best else None,
            "worst_trade": {"code": worst.get("code"), "return_pct": worst.get(field)} if worst else None,
            "equity_curve": equity_curve,
        }

    def _create_run(
        self,
        run_id: str,
        request: BacktestRequest,
        selector: StockSelector,
        started_at: datetime,
        status: str = "running",
        progress_total_days: int = 0,
    ) -> None:
        sql = """
        INSERT INTO backtest_run (
            run_id, strategy_id, trade_strategy_id, strategy_version, instrument_type, start_date, end_date,
            return_mode, evaluation_mode, use_adjusted_price, commission_bps, stamp_tax_bps, slippage_bps,
            execution_constraints_enabled, status, request_json, started_at,
            progress_total_days, progress_done_days, progress_pct
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0)
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        run_id,
                        request.strategy_id,
                        request.trade_strategy_id,
                        selector.strategy_meta.get("version"),
                        request.instrument_type,
                        request.start_date,
                        request.end_date,
                        request.return_mode,
                        request.evaluation_mode,
                        int(request.use_adjusted_price),
                        request.commission_bps,
                        request.stamp_tax_bps,
                        request.slippage_bps,
                        int(request.apply_execution_constraints),
                        status,
                        _to_json(request.__dict__),
                        started_at.strftime("%Y-%m-%d %H:%M:%S"),
                        progress_total_days,
                    ),
                )

    def _mark_running(self, run_id: str, progress_total_days: int) -> None:
        sql = """
        UPDATE backtest_run
        SET status='running', progress_total_days=%s, progress_done_days=0,
            progress_pct=0, current_trade_date=NULL, estimated_seconds_left=NULL,
            worker_heartbeat_at=%s,
            error_message=NULL, started_at=%s, finished_at=NULL
        WHERE run_id=%s
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(sql, (progress_total_days, now, now, run_id))

    def _clear_run_results(self, run_id: str) -> None:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                for table in ["backtest_pick", "backtest_trade", "backtest_summary_daily"]:
                    cursor.execute(f"DELETE FROM {table} WHERE run_id=%s", (run_id,))

    def _update_progress(self, run_id: str, done_days: int, total_days: int, current_trade_date: str, seconds_left: int | None) -> None:
        progress_pct = round((done_days / total_days) * 100, 4) if total_days else 0
        sql = """
        UPDATE backtest_run
        SET progress_done_days=%s, progress_total_days=%s, progress_pct=%s,
            current_trade_date=%s, estimated_seconds_left=%s, worker_heartbeat_at=%s
        WHERE run_id=%s
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (done_days, total_days, progress_pct, current_trade_date, seconds_left, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), run_id))

    def _save_results(self, run_id: str, strategy_id: str, picks: Sequence[Dict[str, Any]], trades: Sequence[Dict[str, Any]], daily: Sequence[Dict[str, Any]]) -> None:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                if picks:
                    cursor.executemany(
                        """
                        INSERT INTO backtest_pick (run_id, strategy_id, trade_date, code, rank_no, score, entry_price, entry_price_type, factor_json, explain_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE score=VALUES(score), rank_no=VALUES(rank_no), entry_price=VALUES(entry_price), factor_json=VALUES(factor_json), explain_json=VALUES(explain_json)
                        """,
                        [
                            (run_id, strategy_id, p["trade_date"], p["code"], p.get("rank_no"), p.get("score"), p.get("entry_price"), p.get("entry_price_type"), _to_json(p.get("factor_json")), _to_json(p.get("explain_json")))
                            for p in picks
                        ],
                    )
                if trades:
                    cursor.executemany(
                        """
                        INSERT INTO backtest_trade (run_id, strategy_id, trade_date, code, entry_date, entry_price, exit_date_1d, exit_price_1d, return_1d_pct, exit_date_3d, exit_price_3d, return_3d_pct, max_gain_pct, max_drawdown_pct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE exit_date_1d=VALUES(exit_date_1d), exit_price_1d=VALUES(exit_price_1d), return_1d_pct=VALUES(return_1d_pct), exit_date_3d=VALUES(exit_date_3d), exit_price_3d=VALUES(exit_price_3d), return_3d_pct=VALUES(return_3d_pct), max_gain_pct=VALUES(max_gain_pct), max_drawdown_pct=VALUES(max_drawdown_pct)
                        """,
                        [
                            (t["run_id"], t["strategy_id"], t["trade_date"], t["code"], t["entry_date"], t["entry_price"], t.get("exit_date_1d"), t.get("exit_price_1d"), t.get("return_1d_pct"), t.get("exit_date_3d"), t.get("exit_price_3d"), t.get("return_3d_pct"), t.get("max_gain_pct"), t.get("max_drawdown_pct"))
                            for t in trades
                        ],
                    )
                if daily:
                    cursor.executemany(
                        """
                        INSERT INTO backtest_summary_daily (run_id, strategy_id, trade_date, pick_count, avg_return_1d_pct, avg_return_3d_pct, win_rate_1d_pct, win_rate_3d_pct, benchmark_return_1d_pct, benchmark_return_3d_pct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE pick_count=VALUES(pick_count), avg_return_1d_pct=VALUES(avg_return_1d_pct), avg_return_3d_pct=VALUES(avg_return_3d_pct), win_rate_1d_pct=VALUES(win_rate_1d_pct), win_rate_3d_pct=VALUES(win_rate_3d_pct)
                        """,
                        [
                            (d["run_id"], d["strategy_id"], d["trade_date"], d["pick_count"], d.get("avg_return_1d_pct"), d.get("avg_return_3d_pct"), d.get("win_rate_1d_pct"), d.get("win_rate_3d_pct"), d.get("benchmark_return_1d_pct"), d.get("benchmark_return_3d_pct"))
                            for d in daily
                        ],
                    )

    def _finish_run(self, run_id: str, status: str, sample_days: int, total_picks: int, total_trades: int, summary: Dict[str, Any], error_message: str | None = None) -> None:
        sql = """
        UPDATE backtest_run
        SET status=%s, sample_days=%s, total_picks=%s, total_trades=%s,
            progress_done_days=CASE WHEN %s='success' THEN progress_total_days ELSE progress_done_days END,
            progress_pct=CASE WHEN %s='success' THEN 100 ELSE progress_pct END,
            estimated_seconds_left=CASE WHEN %s='success' THEN 0 ELSE estimated_seconds_left END,
            total_return_pct=%s, avg_return_pct=%s, max_drawdown_pct=%s, win_rate_pct=%s,
            worker_heartbeat_at=%s, estimated_seconds_left=CASE WHEN %s IN ('success','cancelled') THEN 0 ELSE estimated_seconds_left END,
            summary_json=%s, error_message=%s, finished_at=%s
        WHERE run_id=%s
        """
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        status,
                        sample_days,
                        total_picks,
                        total_trades,
                        status,
                        status,
                        status,
                        summary.get("total_return_pct"),
                        summary.get("avg_return_pct"),
                        summary.get("max_drawdown_pct"),
                        summary.get("win_rate_pct"),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        status,
                        _to_json(summary),
                        error_message,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        run_id,
                    ),
                )
