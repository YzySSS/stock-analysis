from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta
from typing import Any, Callable

from app.etf_rotation.model import build_rotation_candidates
from app.etf_rotation.spec import etf_rotation_spec_hash, load_etf_rotation_spec
from app.shared.db import mysql_conn, mysql_read_conn


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _date_text(value: date | str) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)[:10]


class EtfRotationService:
    def __init__(
        self,
        *,
        read_connection_factory: Callable[..., Any] = mysql_read_conn,
        write_connection_factory: Callable[..., Any] = mysql_conn,
        now_factory: Callable[[], datetime] = datetime.now,
    ) -> None:
        self._read_connection_factory = read_connection_factory
        self._write_connection_factory = write_connection_factory
        self._now_factory = now_factory
        self.spec = load_etf_rotation_spec()
        self.spec_hash = etf_rotation_spec_hash()

    def _existing_run_id(self, trade_date: str) -> str | None:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_id
                    FROM etf_rotation_signal_run
                    WHERE model_id=%s AND version=%s AND trade_date=%s
                    LIMIT 1
                    """,
                    (self.spec["model_id"], self.spec["version"], trade_date),
                )
                row = cursor.fetchone()
        return str(row["run_id"]) if row else None

    def _next_open(self, trade_date: str) -> datetime:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cal_date
                    FROM etf_rotation_trade_calendar
                    WHERE exchange_code='SSE'
                      AND is_open=1
                      AND cal_date>%s
                    ORDER BY cal_date
                    LIMIT 1
                    """,
                    (trade_date,),
                )
                row = cursor.fetchone()
        if not row:
            raise RuntimeError(
                f"trade calendar has no next open date after {trade_date}"
            )
        return datetime.combine(row["cal_date"], time(9, 30))

    def _sector_rows(self, trade_date: str) -> list[dict[str, Any]]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, industry_code, industry_name, close,
                           pct_change, company_num, lead_stock,
                           lead_stock_pct_change, lead_stock_close,
                           net_buy_amount, net_sell_amount, net_amount, source
                    FROM etf_rotation_sector_daily
                    WHERE trade_date BETWEEN DATE_SUB(%s, INTERVAL 90 DAY) AND %s
                    ORDER BY trade_date, industry_name
                    """,
                    (trade_date, trade_date),
                )
                return list(cursor.fetchall() or [])

    def _fund_rows_by_code(
        self,
        trade_date: str,
    ) -> dict[str, list[dict[str, Any]]]:
        ts_codes = [item["etf"]["ts_code"] for item in self.spec["sectors"]]
        placeholders = ",".join(["%s"] * len(ts_codes))
        sql = f"""
        SELECT ts_code, trade_date, fund_name, list_date, benchmark,
               open, high, low, close, pre_close, change_amount, pct_chg,
               volume_hand, amount_yuan, fund_share_10k, nav_date,
               unit_nav, accum_nav, net_asset, total_netasset,
               premium_discount_pct, source
        FROM etf_rotation_fund_daily
        WHERE ts_code IN ({placeholders})
          AND trade_date BETWEEN DATE_SUB(%s, INTERVAL 180 DAY) AND %s
        ORDER BY ts_code, trade_date
        """
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (*ts_codes, trade_date, trade_date))
                rows = list(cursor.fetchall() or [])
        grouped = {code: [] for code in ts_codes}
        for row in rows:
            grouped.setdefault(str(row["ts_code"]), []).append(row)
        return grouped

    def _opinion_scores(self, trade_date: str) -> dict[str, dict[str, Any]]:
        aliases = sorted(
            {
                alias
                for sector in self.spec["sectors"]
                for alias in sector["opinion_industries"]
            }
        )
        placeholders = ",".join(["%s"] * len(aliases))
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT MAX(as_of_datetime) AS as_of_datetime
                    FROM sector_opinion_daily
                    WHERE trade_date=%s AND sector_type='industry'
                    """,
                    (trade_date,),
                )
                as_of = (cursor.fetchone() or {}).get("as_of_datetime")
                if as_of is None:
                    return {}
                cursor.execute(
                    f"""
                    SELECT sector_name, sector_score, source_count, news_count
                    FROM sector_opinion_daily
                    WHERE trade_date=%s
                      AND as_of_datetime=%s
                      AND sector_type='industry'
                      AND sector_name IN ({placeholders})
                    """,
                    (trade_date, as_of, *aliases),
                )
                rows = list(cursor.fetchall() or [])
        by_name = {str(row["sector_name"]): row for row in rows}
        result: dict[str, dict[str, Any]] = {}
        for sector in self.spec["sectors"]:
            expected = list(sector["opinion_industries"])
            present = [alias for alias in expected if alias in by_name]
            scores = [
                float(by_name[alias]["sector_score"])
                for alias in present
                if by_name[alias].get("sector_score") is not None
            ]
            result[sector["sector_id"]] = {
                "trade_date": trade_date,
                "as_of_datetime": str(as_of),
                "score": sum(scores) / len(scores) if scores else None,
                "aliases_expected": expected,
                "aliases_present": present,
                "alias_coverage": len(present) / len(expected),
                "source_count": sum(
                    int(by_name[alias].get("source_count") or 0)
                    for alias in present
                ),
                "news_count": sum(
                    int(by_name[alias].get("news_count") or 0)
                    for alias in present
                ),
            }
        return result

    def _timing_signal(self, trade_date: str) -> dict[str, Any] | None:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, index_code, model_id, version,
                           timing_score, state, state_label, position_upper,
                           confidence, coverage_json, source, updated_at
                    FROM market_timing_signal_daily
                    WHERE trade_date=%s AND model_id=%s
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (
                        trade_date,
                        self.spec["risk_overlay"]["timing_model_id"],
                    ),
                )
                row = cursor.fetchone()
        if not row:
            return None
        result = dict(row)
        result["coverage_json"] = _json_value(
            result.get("coverage_json"), {}
        )
        return result

    def _source_lineage(
        self,
        *,
        trade_date: str,
        sector_rows: list[dict[str, Any]],
        fund_rows_by_code: dict[str, list[dict[str, Any]]],
        opinion_scores: dict[str, dict[str, Any]],
        timing_signal: dict[str, Any] | None,
    ) -> dict[str, Any]:
        latest_opinion = max(
            (
                str(value.get("as_of_datetime"))
                for value in opinion_scores.values()
                if value.get("as_of_datetime")
            ),
            default=None,
        )
        return {
            "trade_date": trade_date,
            "sector_source": {
                "source": "tushare.moneyflow_ind_ths",
                "row_count": len(sector_rows),
                "minimum_trade_date": min(
                    (str(row["trade_date"]) for row in sector_rows),
                    default=None,
                ),
                "maximum_trade_date": max(
                    (str(row["trade_date"]) for row in sector_rows),
                    default=None,
                ),
            },
            "fund_source": {
                "source": "tushare.fund_daily+fund_share+fund_nav",
                "row_count": sum(len(rows) for rows in fund_rows_by_code.values()),
                "codes": {
                    code: {
                        "row_count": len(rows),
                        "maximum_trade_date": max(
                            (str(row["trade_date"]) for row in rows),
                            default=None,
                        ),
                    }
                    for code, rows in fund_rows_by_code.items()
                },
            },
            "opinion_source": {
                "source": "sector_opinion_daily",
                "as_of_datetime": latest_opinion,
            },
            "timing_source": {
                "model_id": (timing_signal or {}).get("model_id"),
                "version": (timing_signal or {}).get("version"),
                "trade_date": str(
                    (timing_signal or {}).get("trade_date") or ""
                )[:10]
                or None,
            },
        }

    def _save(
        self,
        *,
        run_payload: dict[str, Any],
        decision_as_of: datetime,
        data_cutoff: datetime,
        earliest_execution_at: datetime,
    ) -> None:
        candidates = run_payload["candidates"]
        payload_hash = run_payload["payload_hash"]
        with self._write_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_id, payload_hash
                    FROM etf_rotation_signal_run
                    WHERE model_id=%s AND version=%s AND trade_date=%s
                    FOR UPDATE
                    """,
                    (
                        self.spec["model_id"],
                        self.spec["version"],
                        run_payload["trade_date"],
                    ),
                )
                existing = cursor.fetchone()
                if existing:
                    if existing["payload_hash"] != payload_hash:
                        raise RuntimeError(
                            "immutable ETF rotation signal already exists "
                            "with a different payload hash"
                        )
                    return

                cursor.execute(
                    """
                    INSERT INTO etf_rotation_signal_run (
                        run_id, model_id, version, spec_hash, trade_date,
                        data_cutoff_datetime, decision_as_of,
                        earliest_execution_at, timing_model_id, timing_state,
                        timing_score, position_upper, candidate_count,
                        eligible_count, selected_count, selection_cap, status,
                        source_lineage_json, diagnostics_json, payload_hash
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s
                    )
                    """,
                    (
                        run_payload["run_id"],
                        self.spec["model_id"],
                        self.spec["version"],
                        self.spec_hash,
                        run_payload["trade_date"],
                        data_cutoff,
                        decision_as_of,
                        earliest_execution_at,
                        self.spec["risk_overlay"]["timing_model_id"],
                        run_payload["timing_state"],
                        run_payload.get("timing_score"),
                        run_payload.get("position_upper"),
                        run_payload["candidate_count"],
                        run_payload["eligible_count"],
                        run_payload["selected_count"],
                        run_payload["selection_cap"],
                        run_payload["status"],
                        _canonical_json(run_payload["source_lineage"]),
                        _canonical_json(run_payload["diagnostics"]),
                        payload_hash,
                    ),
                )
                signal_run_id = cursor.lastrowid
                for candidate in candidates:
                    candidate_hash = _sha256(candidate)
                    cursor.execute(
                        """
                        INSERT INTO etf_rotation_signal_candidate (
                            signal_run_id, run_id, trade_date, rank_no,
                            sector_id, sector_name, ts_code, fund_name,
                            is_eligible, is_selected, sector_score, etf_score,
                            combined_score, flow_strength_score,
                            flow_persistence_score, sector_trend_score,
                            opinion_score, liquidity_score, etf_trend_score,
                            share_change_score, tracking_score, latest_close,
                            average_amount_20d_yuan, share_change_20d_pct,
                            premium_discount_pct, gate_json, evidence_json,
                            payload_hash
                        ) VALUES (
                            %s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,
                            %s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,
                            %s,%s,%s,
                            %s
                        )
                        """,
                        (
                            signal_run_id,
                            run_payload["run_id"],
                            run_payload["trade_date"],
                            candidate["rank_no"],
                            candidate["sector_id"],
                            candidate["sector_name"],
                            candidate["ts_code"],
                            candidate["fund_name"],
                            int(candidate["is_eligible"]),
                            int(candidate["is_selected"]),
                            candidate["sector_score"],
                            candidate["etf_score"],
                            candidate["combined_score"],
                            candidate["sector_components"]["flow_strength"],
                            candidate["sector_components"]["flow_persistence"],
                            candidate["sector_components"]["price_trend"],
                            candidate["sector_components"]["opinion"],
                            candidate["etf_components"]["liquidity"],
                            candidate["etf_components"]["price_trend"],
                            candidate["etf_components"]["share_change"],
                            candidate["etf_components"]["tracking"],
                            candidate["latest_close"],
                            candidate["average_amount_20d_yuan"],
                            candidate["share_change_20d_pct"],
                            candidate["premium_discount_pct"],
                            _canonical_json(candidate["gates"]),
                            _canonical_json(candidate["evidence"]),
                            candidate_hash,
                        ),
                    )
                    candidate_id = cursor.lastrowid
                    if candidate["is_selected"]:
                        cursor.executemany(
                            """
                            INSERT INTO etf_rotation_forward_outcome (
                                signal_candidate_id, run_id, sector_id,
                                ts_code, signal_trade_date, horizon_days,
                                outcome_status
                            ) VALUES (%s,%s,%s,%s,%s,%s,'pending')
                            """,
                            [
                                (
                                    candidate_id,
                                    run_payload["run_id"],
                                    candidate["sector_id"],
                                    candidate["ts_code"],
                                    run_payload["trade_date"],
                                    horizon,
                                )
                                for horizon in self.spec[
                                    "forward_observation"
                                ]["horizons_trade_days"]
                            ],
                        )

    def materialize(self, trade_date: date | str) -> dict[str, Any]:
        trade_date_text = _date_text(trade_date)
        existing_run_id = self._existing_run_id(trade_date_text)
        if existing_run_id:
            existing = self.get_run(existing_run_id)
            existing["idempotent_reuse"] = True
            return existing

        earliest_execution_at = self._next_open(trade_date_text)
        decision_as_of = self._now_factory()
        if decision_as_of >= earliest_execution_at:
            raise RuntimeError(
                "refusing to create a post-close ETF signal after its "
                "next-open execution boundary"
            )
        data_cutoff = datetime.combine(
            date.fromisoformat(trade_date_text), time(15, 0)
        )
        sector_rows = self._sector_rows(trade_date_text)
        fund_rows_by_code = self._fund_rows_by_code(trade_date_text)
        opinion_scores = self._opinion_scores(trade_date_text)
        timing_signal = self._timing_signal(trade_date_text)
        result = build_rotation_candidates(
            spec=self.spec,
            trade_date=trade_date_text,
            sector_rows=sector_rows,
            fund_rows_by_code=fund_rows_by_code,
            opinion_scores=opinion_scores,
            timing_signal=timing_signal,
        )
        source_lineage = self._source_lineage(
            trade_date=trade_date_text,
            sector_rows=sector_rows,
            fund_rows_by_code=fund_rows_by_code,
            opinion_scores=opinion_scores,
            timing_signal=timing_signal,
        )
        if not result["timing_aligned"] or not result["universe_ready"]:
            status = "blocked"
        elif result["selected_count"] == 0:
            status = "cash"
        else:
            status = "ready"
        rejected_gates: dict[str, int] = {}
        for candidate in result["candidates"]:
            for gate_name, passed in candidate["gates"].items():
                if not passed:
                    rejected_gates[gate_name] = rejected_gates.get(gate_name, 0) + 1
        diagnostics = {
            "complete_candidate_count": result["complete_candidate_count"],
            "complete_universe_ratio": result["complete_universe_ratio"],
            "universe_ready": result["universe_ready"],
            "rejected_gate_counts": rejected_gates,
            "allow_cash": True,
            "research_only": True,
        }
        stable_payload = {
            "model_id": self.spec["model_id"],
            "version": self.spec["version"],
            "spec_hash": self.spec_hash,
            "trade_date": trade_date_text,
            "timing_state": result["timing_state"],
            "timing_score": (
                float(timing_signal["timing_score"])
                if timing_signal and timing_signal.get("timing_score") is not None
                else None
            ),
            "position_upper": (
                float(timing_signal["position_upper"])
                if timing_signal and timing_signal.get("position_upper") is not None
                else None
            ),
            "selection_cap": result["selection_cap"],
            "candidate_count": result["candidate_count"],
            "eligible_count": result["eligible_count"],
            "selected_count": result["selected_count"],
            "status": status,
            "source_lineage": source_lineage,
            "diagnostics": diagnostics,
            "candidates": result["candidates"],
        }
        run_payload = {
            **stable_payload,
            "run_id": (
                f"etfrot_{trade_date_text.replace('-', '')}_"
                f"{self.spec_hash[:12]}"
            ),
            "payload_hash": _sha256(stable_payload),
        }
        self._save(
            run_payload=run_payload,
            decision_as_of=decision_as_of,
            data_cutoff=data_cutoff,
            earliest_execution_at=earliest_execution_at,
        )
        return self.get_run(run_payload["run_id"])

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM etf_rotation_signal_run
                    WHERE run_id=%s
                    LIMIT 1
                    """,
                    (run_id,),
                )
                run = cursor.fetchone()
                if not run:
                    raise KeyError(f"unknown ETF rotation run: {run_id}")
                cursor.execute(
                    """
                    SELECT *
                    FROM etf_rotation_signal_candidate
                    WHERE run_id=%s
                    ORDER BY rank_no
                    """,
                    (run_id,),
                )
                candidates = list(cursor.fetchall() or [])
                cursor.execute(
                    """
                    SELECT *
                    FROM etf_rotation_forward_outcome
                    WHERE run_id=%s
                    ORDER BY signal_candidate_id, horizon_days
                    """,
                    (run_id,),
                )
                outcomes = list(cursor.fetchall() or [])
        result = dict(run)
        for key in ("source_lineage_json", "diagnostics_json"):
            result[key.removesuffix("_json")] = _json_value(
                result.pop(key, None), {}
            )
        normalized_candidates = []
        for row in candidates:
            item = dict(row)
            item["gates"] = _json_value(item.pop("gate_json", None), {})
            item["evidence"] = _json_value(
                item.pop("evidence_json", None), {}
            )
            item["is_eligible"] = bool(item["is_eligible"])
            item["is_selected"] = bool(item["is_selected"])
            normalized_candidates.append(item)
        normalized_outcomes = []
        for row in outcomes:
            item = dict(row)
            item["metadata"] = _json_value(
                item.pop("metadata_json", None), {}
            )
            normalized_outcomes.append(item)
        result["candidates"] = normalized_candidates
        result["outcomes"] = normalized_outcomes
        result["research_only"] = True
        result["automatic_trading"] = False
        return result

    def latest(self) -> dict[str, Any] | None:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_id
                    FROM etf_rotation_signal_run
                    WHERE model_id=%s AND version=%s
                    ORDER BY trade_date DESC, created_at DESC
                    LIMIT 1
                    """,
                    (self.spec["model_id"], self.spec["version"]),
                )
                row = cursor.fetchone()
        return self.get_run(str(row["run_id"])) if row else None
