"""Replay recorded decisions, never reconstruct old sentiment with today's news.

This deliberately does not implement the ordinary historical-selector backtest.
The selected/observe classification is preserved; returns are counterfactual
next-session-open diagnostics, not fills or funded portfolio performance.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, time, timedelta
from typing import Any, Mapping, Sequence

from app.backtest.service import BacktestRequest, BacktestService
from app.shared.db import mysql_read_conn
from app.shared.market_clock import to_shanghai_wall_clock


METHOD = "recorded_sentiment_decision_next_session_v1"
STRATEGIES = frozenset({"a_share_sentiment", "a_share_sentiment_v05", "a_share_sentiment_v06"})
MAX_CANDIDATES = 50
MAX_SOURCES = 64


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str, allow_nan=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def decoded(value: Any, expected: type) -> Any:
    result = json.loads(value) if isinstance(value, str) else value
    if not isinstance(result, expected):
        raise ValueError("invalid snapshot evidence JSON")
    return result


def clock(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return to_shanghai_wall_clock(parsed)


def number(value: Any) -> float | None:
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def validate_evidence(bundle: Mapping[str, Any]) -> tuple[datetime, list[dict]]:
    manifest = bundle["manifest"]
    rows = list(bundle["candidates"])
    sources = list(bundle["sources"])
    if manifest.get("strategy_id") not in STRATEGIES:
        raise ValueError("not a supported recorded sentiment strategy")
    if (manifest.get("status"), manifest.get("quality_status")) != ("ready", "passed"):
        raise ValueError("snapshot is not ready/passed")
    decision, publication = clock(manifest["decision_as_of"]), clock(manifest["published_at"])
    if publication < decision:
        raise ValueError("snapshot published before decision")
    if number(manifest.get("coverage_ratio")) is None or float(manifest["coverage_ratio"]) < .98:
        raise ValueError("snapshot coverage below 98%")
    if (not manifest.get("strategy_version") or not manifest.get("strategy_config_hash")
            or not manifest.get("implementation_hash")):
        raise ValueError("snapshot implementation/config identity missing")
    if len(rows) != int(manifest["candidate_count"]) or len(rows) > MAX_CANDIDATES:
        raise ValueError("candidate count mismatch or over budget")
    if len({r["code"] for r in rows}) != len(rows):
        raise ValueError("duplicate candidate codes")
    source_ids = decoded(manifest["source_manifest_ids_json"], list)
    if (not source_ids or len(source_ids) > MAX_SOURCES
            or any(type(i) is not int for i in source_ids)
            or len(set(source_ids)) != len(source_ids)):
        raise ValueError("invalid source manifest set")
    if set(source_ids) != {int(s["id"]) for s in sources}:
        raise ValueError("source manifests missing or mixed")
    source_map = {int(s["id"]): s for s in sources}
    for source in sources:
        metadata = decoded(source.get("metadata_json") or {}, dict)
        required = metadata.get("required", True)
        if type(required) is not bool:
            raise ValueError("invalid required source flag")
        missing_optional = not required and source.get("quality_status") == "missing"
        # Sources are received before the decision; derived manifests may be
        # published during its transaction. Never substitute publication time
        # for the original receive clock.
        for field in ("source_event_time_max", "received_at"):
            if missing_optional and field == "source_event_time_max":
                continue
            if source.get(field) is None or clock(source[field]) > decision:
                raise ValueError("source clock missing or after decision")
        if missing_optional:
            continue
        if source.get("status") != "published" or source.get("quality_status") not in (
            ("passed",) if required else ("passed", "partial")
        ):
            raise ValueError("source manifest not published/passed")
    for row in rows:
        if row.get("snapshot_id") != manifest["snapshot_id"] or not row.get("row_hash"):
            raise ValueError("mixed candidate snapshot or missing row hash")
        if not row.get("is_selected"):
            raise ValueError("unselected row in recorded decision")
        lineage = decoded(row["source_lineage_json"], list)
        if not lineage:
            raise ValueError("candidate lineage missing")
        for item in lineage:
            source = source_map.get(int(item.get("source_manifest_id") or 0))
            if source is None or source["batch_id"] != item.get("batch_id"):
                raise ValueError("candidate lineage does not match source manifest")
            for field in ("source_time", "received_at"):
                if not item.get(field) or clock(item[field]) > decision:
                    raise ValueError("candidate source clock after decision")
    return publication, rows


def replay_bundle(bundle: Mapping[str, Any], *, calendar: Sequence[Mapping],
                  bars: Sequence[Mapping], holding_days: int = 3,
                  observed_through: datetime | None = None,
                  commission_bps: float = 3, stamp_tax_bps: float = 5,
                  slippage_bps: float = 5) -> dict:
    """Pure evaluation with fixed independent sessions and no provider access."""
    if holding_days not in (1, 3, 5):
        raise ValueError("holding_days must be 1, 3 or 5")
    for fee in (commission_bps, stamp_tax_bps, slippage_bps):
        if number(fee) is None or not 0 <= fee <= 100:
            raise ValueError("fees must be finite and between 0 and 100 bps")
    available, candidates = validate_evidence(bundle)
    now = to_shanghai_wall_clock(observed_through or datetime.now())
    if available > now:
        raise ValueError("snapshot not published by observed_through")
    start = available.date() + timedelta(days=1)
    dated = {date.fromisoformat(str(r["cal_date"])[:10]): r for r in calendar}
    if len(dated) != len(calendar):
        raise ValueError("duplicate independent calendar date")
    sessions = []
    day = start
    required = 2 if holding_days == 1 else holding_days
    for _ in range(32):
        row = dated.get(day)
        if (not row or row.get("source") != "tushare.trade_cal"
                or row.get("is_open") not in (0, 1)):
            raise ValueError("independent calendar missing or invalid")
        if row["is_open"]:
            sessions.append(str(day))
        if len(sessions) == required:
            break
        day += timedelta(days=1)
    if len(sessions) != required:
        raise ValueError("calendar does not cover holding window")
    indexed: dict[tuple[str, str], dict] = {}
    for raw in bars:
        key = (str(raw["code"]), str(raw["trade_date"])[:10])
        if key in indexed:
            raise ValueError("duplicate price/path row")
        indexed[key] = dict(raw)
    request = BacktestRequest(strategy_id=bundle["manifest"]["strategy_id"],
        start_date=sessions[0], end_date=sessions[-1], use_adjusted_price=True,
        commission_bps=commission_bps, stamp_tax_bps=stamp_tax_bps,
        slippage_bps=slippage_bps, apply_execution_constraints=True)
    labels = []
    for candidate in candidates:
        code = candidate["code"]
        path = [indexed.get((code, day), {"trade_date": day, "missing_bar": True})
                for day in sessions]
        # Daily rows are not an intraday quote feed: wait for close-data cutoff.
        due = datetime.combine(date.fromisoformat(sessions[-1]), time(18, 45))
        label = {"code": code, "name": candidate.get("name"),
                 "rank_no": candidate.get("rank_no"),
                 "original_trade_grade": candidate.get("trade_grade_state"),
                 "entry_date": sessions[0], "exit_date": sessions[-1],
                 "status": "pending", "return_pct": None, "reason": "not_matured"}
        if now >= due:
            issue = BacktestService._path_issue(path, required, adjusted=True)
            if not issue:
                issue = BacktestService._execution_block_reason_v6(path[0], "buy")
            if not issue:
                # The existing service guard examines `open`. For a planned
                # close exit it must instead examine the actual close-price
                # endpoint; an opening limit-down that later recovers is not
                # the same as a closing limit-down.
                exit_bar = dict(path[-1])
                if holding_days != 1:
                    exit_bar["open"] = exit_bar["close"]
                issue = BacktestService._execution_block_reason_v6(exit_bar, "sell")
            if issue:
                label.update(status="excluded", reason=issue)
            else:
                price = "open" if holding_days == 1 else "close"
                value = BacktestService._pct_return(number(path[0]["open"]),
                    number(path[-1][price]), number(path[0]["adj_factor"]),
                    number(path[-1]["adj_factor"]), request)
                label.update(status="ready", reason=None, return_pct=value,
                             entry_price=number(path[0]["open"]),
                             exit_price=number(path[-1][price]))
        labels.append(label)
    ready = [x["return_pct"] for x in labels if x["status"] == "ready"]
    return {"methodology_version": METHOD, "snapshot_id": bundle["manifest"]["snapshot_id"],
            "strategy_id": bundle["manifest"]["strategy_id"],
            "strategy_version": bundle["manifest"]["strategy_version"],
            "strategy_config_hash": bundle["manifest"]["strategy_config_hash"],
            "implementation_hash": bundle["manifest"]["implementation_hash"],
            "evidence_sha256": digest(bundle), "path_sha256": digest(list(bars)),
            "calendar_sha256": digest(list(calendar)), "available_at": str(available),
            "observed_through": str(now), "holding_days": holding_days,
            "fees_bps": {"commission_each_side": commission_bps,
                         "stamp_tax_sell": stamp_tax_bps, "slippage_each_side": slippage_bps},
            "candidate_count": len(labels), "ready_count": len(ready),
            "pending_count": sum(x["status"] == "pending" for x in labels),
            "excluded_count": sum(x["status"] == "excluded" for x in labels),
            "empty_decision": not labels, "avg_return_pct": sum(ready)/len(ready) if ready else None,
            "research_only": True, "selector_recomputed": False,
            "full_history_backtest_available": False,
            "performance_basis": "recorded_selection_counterfactual_not_execution_or_portfolio",
            "entry_policy": "next_exchange_session_after_actual_publication",
            "exit_policy": "next_session_open_t_plus_one" if holding_days == 1 else "target_session_close",
            "execution_constraint_basis": "conservative_daily_endpoint_heuristic_not_fill",
            "limitations": ["仅重放已记录决策，不重算历史新闻与选择器",
                            "观察级仍为观察；收益为下一交易日开盘的假设诊断，不是策略成交收益",
                            "未建资金账户、容量或分钟撮合；缺失/不可成交样本不计收益",
                            "旧row_hash缺少可逆规范化合同；本次封存hash不追认原始输入完整性"],
            "labels": labels}


class SentimentReplayRepository:
    def __init__(self, connection_factory=None):
        self.connect = connection_factory or mysql_read_conn

    def load_bundle(self, snapshot_id: str) -> dict:
        if not isinstance(snapshot_id, str) or not snapshot_id or len(snapshot_id) > 96:
            raise ValueError("exact snapshot_id is required (max 96 characters)")
        with self.connect() as conn:
            with conn.cursor() as c:
                c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                c.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
                c.execute("SELECT * FROM sentiment_candidate_snapshot_manifest WHERE snapshot_id=%s", (snapshot_id,))
                manifest = c.fetchone()
                if not manifest:
                    raise LookupError("snapshot not found")
                if int(manifest["candidate_count"]) > MAX_CANDIDATES:
                    raise ValueError("snapshot candidate budget exceeded")
                ids = decoded(manifest["source_manifest_ids_json"], list)
                if not ids or len(ids) > MAX_SOURCES or any(type(i) is not int for i in ids):
                    raise ValueError("source manifest budget or identity invalid")
                c.execute("SELECT * FROM sentiment_candidate_snapshot WHERE snapshot_id=%s ORDER BY rank_no,code LIMIT %s", (snapshot_id, MAX_CANDIDATES+1))
                candidates = c.fetchall() or []
                placeholders = ",".join(["%s"] * len(ids))
                c.execute(f"SELECT * FROM source_batch_manifest WHERE id IN ({placeholders}) ORDER BY id", ids)
                sources = c.fetchall() or []
        return {"manifest": manifest, "candidates": candidates, "sources": sources}

    def replay(self, snapshot_id: str, *, capture_inputs: bool = False, **kwargs) -> dict:
        bundle = self.load_bundle(snapshot_id)
        available, candidates = validate_evidence(bundle)
        start = available.date()+timedelta(days=1)
        end = start+timedelta(days=31)
        with self.connect() as conn:
            with conn.cursor() as c:
                c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                c.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
                c.execute("SELECT cal_date,is_open,source FROM etf_rotation_trade_calendar WHERE exchange_code='SSE' AND cal_date BETWEEN %s AND %s ORDER BY cal_date", (start, end))
                calendar = c.fetchall() or []
                bars = []
                codes = [r["code"] for r in candidates]
                if codes:
                    placeholders = ",".join(["%s"] * len(codes))
                    c.execute(f"""SELECT k.code,k.trade_date,k.open,k.high,k.low,k.close,k.volume,
                        af.adj_factor,nh.name,(nh.code IS NOT NULL) AS pit_status_available,
                        (SELECT p.close FROM daily_kline p WHERE p.code=k.code AND p.trade_date<k.trade_date ORDER BY p.trade_date DESC LIMIT 1) AS prev_close
                        FROM daily_kline k LEFT JOIN adj_factor_daily af ON af.code=k.code AND af.trade_date=k.trade_date
                        LEFT JOIN stock_name_history nh ON nh.code=k.code AND nh.start_date<=k.trade_date AND (nh.end_date IS NULL OR nh.end_date>=k.trade_date)
                        WHERE k.code IN ({placeholders}) AND k.trade_date BETWEEN %s AND %s ORDER BY k.code,k.trade_date""", [*codes,start,end])
                    bars = c.fetchall() or []
        result = replay_bundle(bundle, calendar=calendar, bars=bars, **kwargs)
        if capture_inputs:
            result['_sealed_inputs'] = {'bundle': bundle, 'calendar': calendar, 'bars': bars}
        return result

    def list_snapshots(self, *, strategy_id: str, limit: int = 20) -> list[dict]:
        if strategy_id not in STRATEGIES or not 1 <= limit <= 50:
            raise ValueError('invalid strategy or snapshot list limit')
        with self.connect() as conn:
            with conn.cursor() as c:
                c.execute('''SELECT snapshot_id,strategy_id,strategy_version,decision_as_of,
                    published_at,candidate_count,coverage_ratio FROM sentiment_candidate_snapshot_manifest
                    WHERE strategy_id=%s AND status='ready' AND quality_status='passed'
                      AND published_at IS NOT NULL
                    ORDER BY decision_as_of DESC LIMIT %s''', (strategy_id,limit))
                return c.fetchall() or []
