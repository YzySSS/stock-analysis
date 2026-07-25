from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from typing import Any, Callable

from app.shared.db import mysql_conn, mysql_read_conn


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def compute_forward_outcome(
    *,
    signal_trade_date: str,
    horizon_days: int,
    future_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    rows = sorted(
        (
            row
            for row in future_rows
            if str(row.get("trade_date") or "")[:10] > signal_trade_date
        ),
        key=lambda row: str(row.get("trade_date") or ""),
    )
    if not rows:
        return {
            "outcome_status": "pending",
            "block_reason": "next_trade_day_not_available",
        }
    entry_price = _number(rows[0].get("open"))
    if entry_price is None or entry_price <= 0:
        return {
            "outcome_status": "blocked",
            "block_reason": "next_trade_day_open_missing",
            "entry_trade_date": str(rows[0].get("trade_date") or "")[:10],
        }

    observed = rows[: min(len(rows), horizon_days)]
    highs = [
        value
        for row in observed
        if (value := _number(row.get("high"))) is not None
    ]
    lows = [
        value
        for row in observed
        if (value := _number(row.get("low"))) is not None
    ]
    result = {
        "entry_trade_date": str(rows[0].get("trade_date") or "")[:10],
        "entry_price": entry_price,
        "maximum_favorable_excursion_pct": (
            (max(highs) / entry_price - 1) * 100 if highs else None
        ),
        "maximum_adverse_excursion_pct": (
            (min(lows) / entry_price - 1) * 100 if lows else None
        ),
        "metadata": {
            "observed_trade_days": len(observed),
            "required_trade_days": horizon_days,
            "entry_contract": "next_trade_day_open",
            "exit_contract": "horizon_trade_day_close",
        },
    }
    if len(rows) < horizon_days:
        return {
            **result,
            "outcome_status": "entry_observed",
            "block_reason": "horizon_not_mature",
        }
    exit_row = rows[horizon_days - 1]
    exit_price = _number(exit_row.get("close"))
    if exit_price is None:
        return {
            **result,
            "outcome_status": "blocked",
            "block_reason": "horizon_close_missing",
            "exit_trade_date": str(exit_row.get("trade_date") or "")[:10],
        }
    return {
        **result,
        "outcome_status": "mature",
        "block_reason": None,
        "exit_trade_date": str(exit_row.get("trade_date") or "")[:10],
        "exit_price": exit_price,
        "gross_return_pct": (exit_price / entry_price - 1) * 100,
    }


class EtfRotationOutcomeService:
    def __init__(
        self,
        *,
        read_connection_factory: Callable[..., Any] = mysql_read_conn,
        write_connection_factory: Callable[..., Any] = mysql_conn,
    ) -> None:
        self._read_connection_factory = read_connection_factory
        self._write_connection_factory = write_connection_factory

    def _outcomes(self) -> list[dict[str, Any]]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM etf_rotation_forward_outcome
                    WHERE outcome_status<>'mature'
                    ORDER BY signal_trade_date, signal_candidate_id, horizon_days
                    """
                )
                return list(cursor.fetchall() or [])

    def _future_rows(
        self,
        ts_code: str,
        signal_trade_date: str,
    ) -> list[dict[str, Any]]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, open, high, low, close
                    FROM etf_rotation_fund_daily
                    WHERE ts_code=%s AND trade_date>%s
                    ORDER BY trade_date
                    LIMIT 25
                    """,
                    (ts_code, signal_trade_date),
                )
                return list(cursor.fetchall() or [])

    def update(self) -> dict[str, Any]:
        rows = self._outcomes()
        future_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        updates = []
        status_counts: dict[str, int] = {}
        for row in rows:
            cache_key = (
                str(row["ts_code"]),
                str(row["signal_trade_date"])[:10],
            )
            if cache_key not in future_cache:
                future_cache[cache_key] = self._future_rows(*cache_key)
            future_rows = future_cache[cache_key]
            outcome = compute_forward_outcome(
                signal_trade_date=cache_key[1],
                horizon_days=int(row["horizon_days"]),
                future_rows=future_rows,
            )
            stable = {
                "signal_candidate_id": int(row["signal_candidate_id"]),
                "ts_code": row["ts_code"],
                "signal_trade_date": cache_key[1],
                "horizon_days": int(row["horizon_days"]),
                **outcome,
            }
            outcome_hash = (
                hashlib.sha256(_canonical_json(stable).encode("utf-8")).hexdigest()
                if outcome["outcome_status"] == "mature"
                else None
            )
            updates.append(
                {
                    "id": row["id"],
                    "entry_trade_date": outcome.get("entry_trade_date"),
                    "exit_trade_date": outcome.get("exit_trade_date"),
                    "entry_price": outcome.get("entry_price"),
                    "exit_price": outcome.get("exit_price"),
                    "gross_return_pct": outcome.get("gross_return_pct"),
                    "mfe_pct": outcome.get(
                        "maximum_favorable_excursion_pct"
                    ),
                    "mae_pct": outcome.get(
                        "maximum_adverse_excursion_pct"
                    ),
                    "outcome_status": outcome["outcome_status"],
                    "block_reason": outcome.get("block_reason"),
                    "outcome_hash": outcome_hash,
                    "metadata_json": _canonical_json(
                        outcome.get("metadata") or {}
                    ),
                    "computed_at": datetime.now(),
                }
            )
            status = outcome["outcome_status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        if updates:
            with self._write_connection_factory(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """
                        UPDATE etf_rotation_forward_outcome
                        SET entry_trade_date=%(entry_trade_date)s,
                            exit_trade_date=%(exit_trade_date)s,
                            entry_price=%(entry_price)s,
                            exit_price=%(exit_price)s,
                            gross_return_pct=%(gross_return_pct)s,
                            maximum_favorable_excursion_pct=%(mfe_pct)s,
                            maximum_adverse_excursion_pct=%(mae_pct)s,
                            outcome_status=%(outcome_status)s,
                            block_reason=%(block_reason)s,
                            outcome_hash=%(outcome_hash)s,
                            metadata_json=%(metadata_json)s,
                            computed_at=%(computed_at)s
                        WHERE id=%(id)s
                        """,
                        updates,
                    )
        return {
            "status": "success",
            "processed": len(rows),
            "status_counts": status_counts,
            "research_only": True,
        }
