from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.market_timing.calibration import (  # noqa: E402
    DAILY_WEIGHTS,
    MODEL_ID,
    MODEL_VERSION,
    calibrate_indicator_score,
    compose_timing_state,
    score_signal,
    signal_label,
)
from app.shared.db import mysql_conn  # noqa: E402
from scripts.run_market_timing_daily_update import (  # noqa: E402
    TREND_INDEX_CODES,
    _save_indicators,
    _save_signal,
    _trend_component,
)


SOURCE_MODEL_ID = "huatai_multidim_v18"


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _indicator_schema_is_versioned() -> bool:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS count
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'market_timing_indicator_daily'
                  AND COLUMN_NAME = 'model_id'
                """
            )
            return int((cursor.fetchone() or {}).get("count") or 0) == 1


def _load_source_rows(index_code: str) -> list[dict[str, Any]]:
    versioned = _indicator_schema_is_versioned()
    model_filter = "AND model_id = %s" if versioned else ""
    params: tuple[Any, ...] = (index_code, SOURCE_MODEL_ID) if versioned else (index_code,)
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT trade_date, index_code, dimension, indicator_id, indicator_name,
                       value, value_label, score, signal_value, signal_label,
                       source_status, source, metadata_json
                FROM market_timing_indicator_daily
                WHERE index_code = %s {model_filter}
                ORDER BY trade_date, indicator_id
                """,
                params,
            )
            return cursor.fetchall() or []


def _load_trend_rows() -> dict[str, list[dict[str, Any]]]:
    placeholders = ",".join(["%s"] * len(TREND_INDEX_CODES))
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT trade_date, index_code, close
                FROM market_index_daily
                WHERE index_code IN ({placeholders})
                ORDER BY index_code, trade_date
                """,
                TREND_INDEX_CODES,
            )
            rows = cursor.fetchall() or []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["index_code"])].append(row)
    return dict(grouped)


def _load_source_coverage(index_code: str) -> dict[str, dict[str, Any]]:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT trade_date, coverage_json
                FROM market_timing_signal_daily
                WHERE index_code = %s AND model_id = %s
                ORDER BY trade_date
                """,
                (index_code, SOURCE_MODEL_ID),
            )
            return {
                str(row["trade_date"]): _json_dict(row.get("coverage_json"))
                for row in (cursor.fetchall() or [])
            }


def _source_trade_date(metadata: dict[str, Any]) -> str | None:
    source_date = metadata.get("source_trade_date")
    if source_date:
        return str(source_date)
    item_dates = [
        str(item.get("trade_date"))
        for item in (metadata.get("items") or [])
        if isinstance(item, dict) and item.get("trade_date")
    ]
    return max(item_dates) if item_dates else None


def replay(index_code: str, *, write: bool) -> dict[str, Any]:
    source_rows = _load_source_rows(index_code)
    trend_rows = _load_trend_rows()
    source_coverage = _load_source_coverage(index_code)
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source_rows:
        by_date[str(row["trade_date"])].append(row)

    calibration_history: dict[str, list[float]] = defaultdict(list)
    distribution: Counter[str] = Counter()
    results: list[dict[str, Any]] = []

    for trade_date, rows in sorted(by_date.items()):
        indicators: list[dict[str, Any]] = []
        for row in rows:
            indicator_id = str(row["indicator_id"])
            raw_value = float(row["value"]) if row.get("value") is not None else None
            raw_score = float(row["score"]) if row.get("score") is not None else None
            if indicator_id in {"iv_skew", "futures_holding_net"} and raw_value is not None:
                calibration_history[indicator_id].append(raw_value)
            metadata = _json_dict(row.get("metadata_json"))
            effective_score, calibration = calibrate_indicator_score(
                indicator_id,
                raw_score,
                raw_value,
                history_values=calibration_history.get(indicator_id, []),
                source_status=row.get("source_status"),
                source_trade_date=_source_trade_date(metadata),
                target_trade_date=trade_date,
            )
            metadata["calibration"] = calibration
            signal = score_signal(effective_score)
            indicators.append(
                {
                    "trade_date": trade_date,
                    "index_code": index_code,
                    "model_id": MODEL_ID,
                    "version": MODEL_VERSION,
                    "dimension": row["dimension"],
                    "indicator_id": indicator_id,
                    "indicator_name": row["indicator_name"],
                    "value": raw_value,
                    "value_label": row.get("value_label"),
                    "score": effective_score,
                    "signal": signal,
                    "signal_label": signal_label(signal),
                    "source_status": row.get("source_status"),
                    "source": row.get("source"),
                    "metadata_json": metadata,
                }
            )

        components = []
        for trend_code in TREND_INDEX_CODES:
            component = _trend_component(trend_rows.get(trend_code, []), trade_date)
            if component:
                components.append({"index_code": trend_code, **component})
        if len(components) >= 2:
            raw_score = sum(float(item["score"]) for item in components) / len(components)
            band_pos = sum(float(item["band_pos"]) for item in components) / len(components)
            component_dates = [str(item["trade_date"]) for item in components]
            source_date = min(component_dates)
            source_status = "已接入" if all(item == trade_date for item in component_dates) else "沿用最近收盘"
            score, calibration = calibrate_indicator_score(
                "multi_index_trend",
                raw_score,
                band_pos,
                source_status=source_status,
                source_trade_date=source_date,
                target_trade_date=trade_date,
            )
            signal = score_signal(score)
            indicators.append(
                {
                    "trade_date": trade_date,
                    "index_code": index_code,
                    "model_id": MODEL_ID,
                    "version": MODEL_VERSION,
                    "dimension": "technical",
                    "indicator_id": "multi_index_trend",
                    "indicator_name": "多指数趋势确认",
                    "value": band_pos,
                    "value_label": " / ".join(
                        f"{item['index_code']} {item['score']:.1f}"
                        for item in components
                    ),
                    "score": score,
                    "signal": signal,
                    "signal_label": signal_label(signal),
                    "source_status": source_status,
                    "source": "market_index_daily",
                    "metadata_json": {
                        "components": components,
                        "source_trade_date": source_date,
                        "target_trade_date": trade_date,
                        "calibration": {
                            **calibration,
                            "calibration_method": "multi_index_average",
                        },
                    },
                }
            )

        composition = compose_timing_state(indicators, weights=DAILY_WEIGHTS)
        distribution[composition["state"]] += 1
        result = {
            "trade_date": trade_date,
            "timing_score": composition["timing_score"],
            "state": composition["state"],
            "position_upper_pct": composition["position_upper_pct"],
            "dimension_vote_sum": composition["dimension_vote_sum"],
            "dimension_scores": composition["dimension_scores"],
        }
        results.append(result)

        if write:
            coverage = dict(source_coverage.get(trade_date) or {})
            coverage["multi_index_trend"] = "已接入" if len(components) >= 2 else "待数据"
            _save_indicators(indicators)
            _save_signal(trade_date, index_code, indicators, coverage)

    return {
        "status": "success",
        "mode": "write" if write else "dry_run",
        "source_model_id": SOURCE_MODEL_ID,
        "target_model_id": MODEL_ID,
        "index_code": index_code,
        "days": len(results),
        "distribution": dict(sorted(distribution.items())),
        "first": results[0] if results else None,
        "latest": results[-1] if results else None,
        "risk_on_dates": [
            item["trade_date"]
            for item in results
            if item["state"] in {"risk_on", "strong_risk_on"}
        ],
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay market timing V1.9 from immutable V1.8 daily inputs.")
    parser.add_argument("--index-code", default="000300.SH")
    parser.add_argument("--write", action="store_true", help="Persist versioned V1.9 indicators and signals")
    args = parser.parse_args()
    print(json.dumps(replay(args.index_code, write=args.write), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
