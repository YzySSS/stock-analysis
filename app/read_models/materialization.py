from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from contextlib import AbstractContextManager
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Iterable, Mapping, Sequence

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]

SUPPORTED_MODELS = (
    "realtime-rank",
    "tracking-summary",
    "operational-status",
)
TRACKING_LOOKBACK_DAYS = 365
DEFAULT_RANK_LIMIT = 100
TASK_RUNNING_STALE_SECONDS = 60 * 60


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _as_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _average(values: Iterable[float]) -> float | None:
    materialized = list(values)
    if not materialized:
        return None
    return round(sum(materialized) / len(materialized), 4)


def _percentage(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator * 100.0, 4)


def _max_datetime(values: Iterable[Any]) -> datetime | None:
    normalized = [item for item in (_as_datetime(value) for value in values) if item]
    return max(normalized) if normalized else None


def _metadata_grade(metadata: Mapping[str, Any]) -> str:
    raw_metrics = metadata.get("raw_metrics")
    raw_metrics = raw_metrics if isinstance(raw_metrics, Mapping) else {}
    factor_scores = metadata.get("factor_scores")
    factor_scores = factor_scores if isinstance(factor_scores, Mapping) else {}
    value = (
        metadata.get("signal_grade")
        or metadata.get("trade_grade_state")
        or raw_metrics.get("signal_grade")
        or raw_metrics.get("trade_grade_state")
        or factor_scores.get("trade_grade_state")
        or "tradable"
    )
    return str(value).strip().lower()


def _is_tradable(metadata: Mapping[str, Any]) -> bool:
    return _metadata_grade(metadata) in {
        "trade",
        "tradable",
        "trade_grade",
        "交易级",
    }


def _rank_rows(
    source_rows: Sequence[Mapping[str, Any]],
    *,
    rank_type: str,
    score_field: str,
    limit: int,
    source_batch_id: str | None = None,
) -> list[dict[str, Any]]:
    candidates: list[tuple[float, Mapping[str, Any]]] = []
    for item in source_rows:
        score = _number(item.get(score_field))
        code = str(item.get("code") or "").strip()
        if score is None or not code:
            continue
        candidates.append((score, item))
    candidates.sort(key=lambda pair: (-pair[0], str(pair[1].get("code") or "")))

    ranked: list[dict[str, Any]] = []
    for rank_no, (score, item) in enumerate(candidates[: max(1, int(limit))], start=1):
        quote_time = _as_datetime(item.get("quote_time"))
        trade_date = _as_date(item.get("trade_date")) or (quote_time.date() if quote_time else None)
        if quote_time is None or trade_date is None:
            continue
        metrics = {
            "score_field": score_field,
            "source_rank": item.get("source_rank"),
            "source_score": item.get("source_score"),
            "source_quote_time": quote_time,
            "source_batch_id": source_batch_id or item.get("batch_id"),
        }
        ranked.append(
            {
                "snapshot_id": None,
                "source_batch_id": source_batch_id or item.get("batch_id"),
                "trade_date": trade_date,
                "quote_time": quote_time,
                "rank_type": rank_type,
                "rank_no": rank_no,
                "code": str(item.get("code") or "").strip(),
                "name": item.get("name"),
                "latest_price": item.get("latest_price"),
                "pct_chg": item.get("pct_chg"),
                "amount": item.get("amount"),
                "net_amount": item.get("net_amount"),
                "rank_score": score,
                "is_stale": 1 if item.get("is_stale") else 0,
                "source": str(item.get("source") or "local_mysql")[:64],
                "metrics_json": _canonical_json(metrics),
            }
        )
    return ranked


def build_realtime_rank_rows(
    *,
    realtime_rows: Sequence[Mapping[str, Any]],
    moneyflow_rows: Sequence[Mapping[str, Any]],
    popularity_rows: Sequence[Mapping[str, Any]],
    source_batch_id: str | None,
    limit: int,
) -> tuple[str | None, list[dict[str, Any]]]:
    """Build deterministic ranking rows from three already-persisted snapshots."""

    rows = [
        *_rank_rows(
            realtime_rows,
            rank_type="pct_chg_top",
            score_field="pct_chg",
            limit=limit,
            source_batch_id=source_batch_id,
        ),
        *_rank_rows(
            realtime_rows,
            rank_type="amount_top",
            score_field="amount",
            limit=limit,
            source_batch_id=source_batch_id,
        ),
        *_rank_rows(
            moneyflow_rows,
            rank_type="net_inflow_top",
            score_field="net_amount",
            limit=limit,
            source_batch_id=None,
        ),
        *_rank_rows(
            popularity_rows,
            rank_type="popularity_top",
            score_field="popularity_score",
            limit=limit,
            source_batch_id=None,
        ),
    ]
    if not rows:
        return None, []

    latest_quote_time = max(item["quote_time"] for item in rows)
    signature = [
        {
            "rank_type": item["rank_type"],
            "rank_no": item["rank_no"],
            "code": item["code"],
            "rank_score": item["rank_score"],
            "quote_time": item["quote_time"],
            "source_batch_id": item["source_batch_id"],
        }
        for item in rows
    ]
    snapshot_id = (
        f"rank-{latest_quote_time.strftime('%Y%m%d%H%M%S')}-"
        f"{_stable_hash(signature)[:20]}"
    )
    for item in rows:
        item["snapshot_id"] = snapshot_id
    return snapshot_id, rows


def build_tracking_summary_rows(
    source_rows: Sequence[Mapping[str, Any]],
    *,
    summary_date: date,
    calculated_at: datetime,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw in source_rows:
        metadata = _json_mapping(raw.get("metadata_json"))
        strategy_id = str(raw.get("strategy_id") or "").strip()
        if not strategy_id:
            continue
        version = str(metadata.get("strategy_version") or raw.get("registry_version") or "").strip()
        instrument_type = str(raw.get("instrument_type") or "stock").strip() or "stock"
        entry_price = _number(raw.get("entry_price"))
        item = dict(raw)
        item["metadata"] = metadata
        item["returns"] = {}
        for horizon in (1, 3, 5, 20):
            close = _number(raw.get(f"close_{horizon}d"))
            item["returns"][horizon] = (
                round((close / entry_price - 1.0) * 100.0, 4)
                if close is not None and entry_price is not None and entry_price > 0
                else None
            )
        grouped[(strategy_id, version, instrument_type)].append(item)

    output: list[dict[str, Any]] = []
    for (strategy_id, version, instrument_type), items in sorted(grouped.items()):
        horizon_metrics: dict[int, dict[str, Any]] = {}
        for horizon in (1, 3, 5, 20):
            returns = [
                float(item["returns"][horizon])
                for item in items
                if item["returns"][horizon] is not None
            ]
            horizon_metrics[horizon] = {
                "matured": len(returns),
                "win_rate": _percentage(sum(value > 0 for value in returns), len(returns)),
                "average_return": _average(returns),
            }

        source_cutoff_at = _max_datetime(
            [item.get("created_at") for item in items]
            + [item.get("latest_future_date") for item in items]
        )
        source_signature = [
            {
                "selection_result_id": item.get("selection_result_id"),
                "code": item.get("code"),
                "trade_date": item.get("trade_date"),
                "entry_price": item.get("entry_price"),
                "returns": item["returns"],
                "include_in_stats": item.get("include_in_stats"),
            }
            for item in items
        ]
        summary_json = {
            "source": "selection_result+daily_kline",
            "lookback_days": TRACKING_LOOKBACK_DAYS,
            "benchmark_excess_status": "unavailable_no_replayable_benchmark_series",
            "benchmark_excess_fields": None,
        }
        output.append(
            {
                "summary_date": summary_date,
                "strategy_id": strategy_id,
                "strategy_version": version,
                "instrument_type": instrument_type,
                "selection_count": len(items),
                "tradable_count": sum(_is_tradable(item["metadata"]) for item in items),
                **{
                    f"matured_{horizon}d_count": horizon_metrics[horizon]["matured"]
                    for horizon in (1, 3, 5, 20)
                },
                **{
                    f"win_rate_{horizon}d_pct": horizon_metrics[horizon]["win_rate"]
                    for horizon in (1, 3, 5, 20)
                },
                **{
                    f"avg_return_{horizon}d_pct": horizon_metrics[horizon]["average_return"]
                    for horizon in (1, 3, 5, 20)
                },
                **{f"avg_excess_{horizon}d_pct": None for horizon in (1, 3, 5, 20)},
                "source_cutoff_at": source_cutoff_at,
                "source_snapshot_hash": _stable_hash(source_signature),
                "quality_status": "ready",
                "summary_json": _canonical_json(summary_json),
                "calculated_at": calculated_at,
            }
        )
    return output


def _status_projection(status: Any, *, running_age_seconds: int | None = None) -> tuple[str, str]:
    normalized = str(status or "unknown").strip().lower()
    if normalized == "running" and (running_age_seconds or 0) > TASK_RUNNING_STALE_SECONDS:
        return "stale", "warning"
    if normalized in {"success", "passed", "ready", "published"}:
        return "ready", "info"
    if normalized in {"partial_success", "warning", "degraded"}:
        return "degraded", "warning"
    if normalized in {"failed", "killed", "rejected", "error"}:
        return "failed", "error"
    if normalized in {"running", "received", "pending", "building"}:
        return normalized, "info"
    return "unknown", "warning"


def build_operational_status_rows(
    *,
    task_rows: Sequence[Mapping[str, Any]],
    manifest_rows: Sequence[Mapping[str, Any]],
    captured_at: datetime,
) -> tuple[str, list[dict[str, Any]]]:
    """Project persisted task/batch state without probing external systems."""

    captured_at = captured_at.replace(second=0, microsecond=0)
    snapshot_id = f"ops-{captured_at.strftime('%Y%m%d%H%M')}"
    output: list[dict[str, Any]] = []

    for item in task_rows:
        started_at = _as_datetime(item.get("started_at"))
        finished_at = _as_datetime(item.get("finished_at"))
        last_success_at = _as_datetime(item.get("last_success_at"))
        running_age = (
            max(0, int((captured_at - started_at).total_seconds()))
            if started_at and str(item.get("status") or "").lower() == "running"
            else None
        )
        status, severity = _status_projection(item.get("status"), running_age_seconds=running_age)
        source_event_time = finished_at or started_at
        freshness = (
            max(0, int((captured_at - last_success_at).total_seconds()))
            if last_success_at
            else None
        )
        metrics = {
            "recorded_status": item.get("status"),
            "running_age_seconds": running_age,
            "task_metadata": _json_mapping(item.get("metadata_json")),
        }
        output.append(
            {
                "snapshot_id": snapshot_id,
                "captured_at": captured_at,
                "component_type": "task",
                "component_name": str(item.get("task_name") or "unknown")[:96],
                "status": status,
                "severity": severity,
                "logical_trade_date": source_event_time.date() if source_event_time else None,
                "current_batch_id": str(item.get("run_id") or "")[:96] or None,
                "source_event_time": source_event_time,
                "last_success_at": last_success_at,
                "freshness_seconds": freshness,
                "coverage_ratio": None,
                "latency_ms": None,
                "error_code": item.get("error_code"),
                "error_message": str(item.get("message") or "")[:500] or None,
                "metrics_json": _canonical_json(metrics),
            }
        )

    for item in manifest_rows:
        status_source = item.get("quality_status") or item.get("status")
        status, severity = _status_projection(status_source)
        source_event_time = _as_datetime(item.get("source_event_time_max"))
        last_success_at = _as_datetime(item.get("published_at"))
        freshness_reference = source_event_time or last_success_at
        freshness = (
            max(0, int((captured_at - freshness_reference).total_seconds()))
            if freshness_reference
            else None
        )
        component_name = (
            f"{item.get('source_name') or 'unknown'}:{item.get('dataset_name') or 'unknown'}"
        )[:96]
        metrics = {
            "manifest_status": item.get("status"),
            "quality_status": item.get("quality_status"),
            "quality_reason": item.get("quality_reason"),
            "expected_rows": item.get("expected_rows"),
            "actual_rows": item.get("actual_rows"),
            "expected_entities": item.get("expected_entities"),
            "actual_entities": item.get("actual_entities"),
        }
        output.append(
            {
                "snapshot_id": snapshot_id,
                "captured_at": captured_at,
                "component_type": "dataset",
                "component_name": component_name,
                "status": status,
                "severity": severity,
                "logical_trade_date": _as_date(item.get("logical_trade_date")),
                "current_batch_id": str(item.get("batch_id") or "")[:96] or None,
                "source_event_time": source_event_time,
                "last_success_at": last_success_at,
                "freshness_seconds": freshness,
                "coverage_ratio": item.get("coverage_ratio"),
                "latency_ms": None,
                "error_code": None if severity != "error" else "SOURCE_BATCH_REJECTED",
                "error_message": str(item.get("quality_reason") or "")[:500] or None,
                "metrics_json": _canonical_json(metrics),
            }
        )

    if not output:
        output.append(
            {
                "snapshot_id": snapshot_id,
                "captured_at": captured_at,
                "component_type": "pipeline",
                "component_name": "local_read_model_inputs",
                "status": "unknown",
                "severity": "warning",
                "logical_trade_date": None,
                "current_batch_id": None,
                "source_event_time": None,
                "last_success_at": None,
                "freshness_seconds": None,
                "coverage_ratio": None,
                "latency_ms": None,
                "error_code": "NO_LOCAL_STATUS_INPUT",
                "error_message": "task_run_log and source_batch_manifest contain no eligible rows",
                "metrics_json": _canonical_json({"external_provider_calls": False}),
            }
        )
    return snapshot_id, output


class LocalReadModelMaterializer:
    """Atomically refresh operational read models from local MySQL tables only."""

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self):
        return self._connection_factory(dict_cursor=True)

    def refresh_realtime_rank(self, *, limit: int = DEFAULT_RANK_LIMIT) -> dict[str, Any]:
        normalized_limit = min(500, max(1, int(limit)))
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, batch_id, MAX(quote_time) AS quote_time,
                           MAX(source) AS source, COUNT(*) AS row_count
                    FROM stock_realtime_snapshot
                    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_realtime_snapshot)
                    GROUP BY trade_date, batch_id
                    ORDER BY quote_time DESC, (batch_id IS NOT NULL) DESC
                    LIMIT 1
                    """
                )
                batch = cursor.fetchone() or {}
                realtime_rows: list[dict[str, Any]] = []
                if batch.get("trade_date"):
                    if batch.get("batch_id"):
                        cursor.execute(
                            """
                            SELECT code, name, trade_date, quote_time, latest_price, pct_chg,
                                   amount, batch_id, freshness_seconds, is_stale, source
                            FROM stock_realtime_snapshot
                            WHERE trade_date = %s AND batch_id = %s
                            ORDER BY code
                            """,
                            (batch["trade_date"], batch["batch_id"]),
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT code, name, trade_date, quote_time, latest_price, pct_chg,
                                   amount, batch_id, freshness_seconds, is_stale, source
                            FROM stock_realtime_snapshot
                            WHERE trade_date = %s AND batch_id IS NULL
                            ORDER BY code
                            """,
                            (batch["trade_date"],),
                        )
                    realtime_rows = cursor.fetchall() or []

                cursor.execute(
                    """
                    SELECT code, name, trade_date, quote_time, latest_price, pct_chg,
                           amount, net_amount, source
                    FROM stock_realtime_moneyflow_snapshot
                    WHERE quote_time = (SELECT MAX(quote_time) FROM stock_realtime_moneyflow_snapshot)
                    ORDER BY code
                    """
                )
                moneyflow_rows = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT code, name, trade_date, quote_time, pct_chg, popularity_score,
                           source_rank, source_score, source
                    FROM stock_popularity_snapshot
                    WHERE quote_time = (SELECT MAX(quote_time) FROM stock_popularity_snapshot)
                    ORDER BY code
                    """
                )
                popularity_rows = cursor.fetchall() or []

                snapshot_id, rows = build_realtime_rank_rows(
                    realtime_rows=realtime_rows,
                    moneyflow_rows=moneyflow_rows,
                    popularity_rows=popularity_rows,
                    source_batch_id=str(batch.get("batch_id") or "") or None,
                    limit=normalized_limit,
                )
                if not snapshot_id:
                    return {
                        "status": "no_data",
                        "snapshot_id": None,
                        "published_rows": 0,
                        "rank_types": {},
                    }

                cursor.execute(
                    "DELETE FROM stock_realtime_rank_snapshot WHERE snapshot_id = %s",
                    (snapshot_id,),
                )
                cursor.executemany(
                    """
                    INSERT INTO stock_realtime_rank_snapshot (
                        snapshot_id, source_batch_id, trade_date, quote_time, rank_type,
                        rank_no, code, name, latest_price, pct_chg, amount, net_amount,
                        rank_score, is_stale, source, metrics_json
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    [
                        (
                            item["snapshot_id"],
                            item["source_batch_id"],
                            item["trade_date"],
                            item["quote_time"],
                            item["rank_type"],
                            item["rank_no"],
                            item["code"],
                            item["name"],
                            item["latest_price"],
                            item["pct_chg"],
                            item["amount"],
                            item["net_amount"],
                            item["rank_score"],
                            item["is_stale"],
                            item["source"],
                            item["metrics_json"],
                        )
                        for item in rows
                    ],
                )
                latest_trade_date = max(item["trade_date"] for item in rows)
                cursor.execute(
                    """
                    DELETE FROM stock_realtime_rank_snapshot
                    WHERE trade_date < DATE_SUB(%s, INTERVAL 3 DAY)
                    """,
                    (latest_trade_date,),
                )

        counts: dict[str, int] = defaultdict(int)
        for item in rows:
            counts[item["rank_type"]] += 1
        return {
            "status": "success",
            "snapshot_id": snapshot_id,
            "published_rows": len(rows),
            "rank_types": dict(counts),
            "source_batch_id": str(batch.get("batch_id") or "") or None,
            "retention_days": 3,
        }

    def refresh_tracking_summary(
        self,
        *,
        summary_date: str | date | datetime | None = None,
    ) -> dict[str, Any]:
        resolved_date = _as_date(summary_date) if summary_date is not None else date.today()
        if resolved_date is None:
            raise ValueError("summary_date must be an ISO-8601 date")
        calculated_at = datetime.now().replace(microsecond=0)
        window_start = resolved_date - timedelta(days=TRACKING_LOOKBACK_DAYS)

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    WITH ranked_selection AS (
                        SELECT
                            sr.id AS selection_result_id,
                            sr.trade_date,
                            sr.strategy_id,
                            sr.code,
                            sr.include_in_stats,
                            sr.metadata_json,
                            sr.created_at,
                            sb.instrument_type,
                            registry.version AS registry_version,
                            dk0.close AS entry_price,
                            ROW_NUMBER() OVER (
                                PARTITION BY sr.code, sr.trade_date, sr.strategy_id
                                ORDER BY sr.id DESC
                            ) AS business_rank
                        FROM selection_result sr
                        INNER JOIN stock_basic sb ON sb.code = sr.code
                        LEFT JOIN strategy_registry registry ON registry.strategy_id = sr.strategy_id
                        LEFT JOIN daily_kline dk0
                          ON dk0.code = sr.code AND dk0.trade_date = sr.trade_date
                        WHERE sr.trade_date BETWEEN %s AND %s
                          AND COALESCE(sr.include_in_stats, 1) = 1
                    ),
                    selected AS (
                        SELECT * FROM ranked_selection WHERE business_rank = 1
                    ),
                    future_prices AS (
                        SELECT
                            selected.selection_result_id,
                            dk.trade_date,
                            dk.close,
                            ROW_NUMBER() OVER (
                                PARTITION BY selected.selection_result_id
                                ORDER BY dk.trade_date
                            ) AS horizon_no
                        FROM selected
                        INNER JOIN daily_kline dk
                          ON dk.code = selected.code
                         AND dk.trade_date > selected.trade_date
                         AND dk.trade_date <= %s
                         AND dk.close > 0
                    )
                    SELECT
                        selected.selection_result_id,
                        selected.trade_date,
                        selected.strategy_id,
                        selected.registry_version,
                        selected.code,
                        selected.instrument_type,
                        selected.entry_price,
                        selected.include_in_stats,
                        selected.metadata_json,
                        selected.created_at,
                        MAX(CASE WHEN future_prices.horizon_no = 1 THEN future_prices.close END) AS close_1d,
                        MAX(CASE WHEN future_prices.horizon_no = 3 THEN future_prices.close END) AS close_3d,
                        MAX(CASE WHEN future_prices.horizon_no = 5 THEN future_prices.close END) AS close_5d,
                        MAX(CASE WHEN future_prices.horizon_no = 20 THEN future_prices.close END) AS close_20d,
                        MAX(future_prices.trade_date) AS latest_future_date
                    FROM selected
                    LEFT JOIN future_prices
                      ON future_prices.selection_result_id = selected.selection_result_id
                     AND future_prices.horizon_no <= 20
                    GROUP BY
                        selected.selection_result_id, selected.trade_date, selected.strategy_id,
                        selected.registry_version, selected.code, selected.instrument_type,
                        selected.entry_price, selected.include_in_stats,
                        selected.metadata_json, selected.created_at
                    ORDER BY selected.strategy_id, selected.trade_date, selected.code
                    """,
                    (window_start, resolved_date, resolved_date),
                )
                source_rows = cursor.fetchall() or []
                summaries = build_tracking_summary_rows(
                    source_rows,
                    summary_date=resolved_date,
                    calculated_at=calculated_at,
                )
                cursor.execute(
                    "DELETE FROM tracking_summary_daily WHERE summary_date = %s",
                    (resolved_date,),
                )
                if summaries:
                    cursor.executemany(
                        """
                        INSERT INTO tracking_summary_daily (
                            summary_date, strategy_id, strategy_version, instrument_type,
                            selection_count, tradable_count,
                            matured_1d_count, matured_3d_count, matured_5d_count, matured_20d_count,
                            win_rate_1d_pct, win_rate_3d_pct, win_rate_5d_pct, win_rate_20d_pct,
                            avg_return_1d_pct, avg_return_3d_pct, avg_return_5d_pct, avg_return_20d_pct,
                            avg_excess_1d_pct, avg_excess_3d_pct, avg_excess_5d_pct, avg_excess_20d_pct,
                            source_cutoff_at, source_snapshot_hash, quality_status,
                            summary_json, calculated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        [
                            (
                                item["summary_date"],
                                item["strategy_id"],
                                item["strategy_version"],
                                item["instrument_type"],
                                item["selection_count"],
                                item["tradable_count"],
                                item["matured_1d_count"],
                                item["matured_3d_count"],
                                item["matured_5d_count"],
                                item["matured_20d_count"],
                                item["win_rate_1d_pct"],
                                item["win_rate_3d_pct"],
                                item["win_rate_5d_pct"],
                                item["win_rate_20d_pct"],
                                item["avg_return_1d_pct"],
                                item["avg_return_3d_pct"],
                                item["avg_return_5d_pct"],
                                item["avg_return_20d_pct"],
                                item["avg_excess_1d_pct"],
                                item["avg_excess_3d_pct"],
                                item["avg_excess_5d_pct"],
                                item["avg_excess_20d_pct"],
                                item["source_cutoff_at"],
                                item["source_snapshot_hash"],
                                item["quality_status"],
                                item["summary_json"],
                                item["calculated_at"],
                            )
                            for item in summaries
                        ],
                    )

        return {
            "status": "success" if summaries else "no_data",
            "summary_date": resolved_date,
            "published_rows": len(summaries),
            "source_rows": len(source_rows),
            "lookback_days": TRACKING_LOOKBACK_DAYS,
            "excess_return_status": "unavailable_no_replayable_benchmark_series",
        }

    def refresh_operational_status(
        self,
        *,
        captured_at: str | datetime | None = None,
    ) -> dict[str, Any]:
        resolved_capture = _as_datetime(captured_at) if captured_at is not None else datetime.now()
        if resolved_capture is None:
            raise ValueError("captured_at must be an ISO-8601 datetime")
        resolved_capture = resolved_capture.replace(second=0, microsecond=0)

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        latest.task_name, latest.run_id, latest.status, latest.started_at,
                        latest.finished_at, latest.message, latest.error_code,
                        latest.metadata_json, success.last_success_at
                    FROM task_run_log latest
                    INNER JOIN (
                        SELECT task_name, MAX(id) AS max_id
                        FROM task_run_log
                        WHERE started_at <= %s
                          AND task_name <> 'operational_read_models_refresh'
                        GROUP BY task_name
                    ) chosen ON chosen.max_id = latest.id
                    LEFT JOIN (
                        SELECT task_name, MAX(finished_at) AS last_success_at
                        FROM task_run_log
                        WHERE status = 'success' AND finished_at <= %s
                        GROUP BY task_name
                    ) success ON success.task_name = latest.task_name
                    ORDER BY latest.task_name
                    """,
                    (resolved_capture, resolved_capture),
                )
                task_rows = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT latest.*
                    FROM source_batch_manifest latest
                    INNER JOIN (
                        SELECT source_name, dataset_name, MAX(id) AS max_id
                        FROM source_batch_manifest
                        WHERE received_at <= %s
                        GROUP BY source_name, dataset_name
                    ) chosen ON chosen.max_id = latest.id
                    ORDER BY latest.source_name, latest.dataset_name
                    """,
                    (resolved_capture,),
                )
                manifest_rows = cursor.fetchall() or []
                snapshot_id, rows = build_operational_status_rows(
                    task_rows=task_rows,
                    manifest_rows=manifest_rows,
                    captured_at=resolved_capture,
                )
                cursor.execute(
                    "DELETE FROM operational_status_snapshot WHERE snapshot_id = %s",
                    (snapshot_id,),
                )
                cursor.executemany(
                    """
                    INSERT INTO operational_status_snapshot (
                        snapshot_id, captured_at, component_type, component_name, status,
                        severity, logical_trade_date, current_batch_id, source_event_time,
                        last_success_at, freshness_seconds, coverage_ratio, latency_ms,
                        error_code, error_message, metrics_json
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    [
                        (
                            item["snapshot_id"],
                            item["captured_at"],
                            item["component_type"],
                            item["component_name"],
                            item["status"],
                            item["severity"],
                            item["logical_trade_date"],
                            item["current_batch_id"],
                            item["source_event_time"],
                            item["last_success_at"],
                            item["freshness_seconds"],
                            item["coverage_ratio"],
                            item["latency_ms"],
                            item["error_code"],
                            item["error_message"],
                            item["metrics_json"],
                        )
                        for item in rows
                    ],
                )
                cursor.execute(
                    """
                    DELETE FROM operational_status_snapshot
                    WHERE captured_at < DATE_SUB(%s, INTERVAL 7 DAY)
                    """,
                    (resolved_capture,),
                )

        return {
            "status": "success",
            "snapshot_id": snapshot_id,
            "captured_at": resolved_capture,
            "published_rows": len(rows),
            "task_components": len(task_rows),
            "dataset_components": len(manifest_rows),
            "retention_days": 7,
        }

    def refresh(
        self,
        models: Sequence[str],
        *,
        rank_limit: int = DEFAULT_RANK_LIMIT,
        summary_date: str | date | datetime | None = None,
        captured_at: str | datetime | None = None,
    ) -> dict[str, Any]:
        normalized = []
        for model in models:
            name = str(model).strip().lower()
            if name == "all":
                normalized = list(SUPPORTED_MODELS)
                break
            if name not in SUPPORTED_MODELS:
                raise ValueError(f"unsupported read model: {name}")
            if name not in normalized:
                normalized.append(name)
        if not normalized:
            raise ValueError("at least one read model is required")

        results: dict[str, Any] = {}
        for model in normalized:
            if model == "realtime-rank":
                results[model] = self.refresh_realtime_rank(limit=rank_limit)
            elif model == "tracking-summary":
                results[model] = self.refresh_tracking_summary(summary_date=summary_date)
            elif model == "operational-status":
                results[model] = self.refresh_operational_status(captured_at=captured_at)
        return {
            "status": "success",
            "models": normalized,
            "results": results,
            "external_provider_calls": False,
        }
