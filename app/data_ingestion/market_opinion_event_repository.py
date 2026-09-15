from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Mapping, Sequence

from app.data_ingestion.market_opinion_events import (
    CONFIRMED_EVENT_STATES,
    build_stock_event_relation,
    normalize_event_evidence,
)
from app.shared.db import mysql_conn


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )


def _datetime_value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value).replace("T", " ")[:19]


def _event_confirmation(cursor, canonical_event_id: str) -> tuple[str, int, int]:
    cursor.execute(
        """
        SELECT
            MAX(CASE
                WHEN is_primary_source=1 AND repost_of_evidence_id IS NULL THEN 1
                ELSE 0
            END) AS has_primary,
            COUNT(DISTINCT CASE
                WHEN credibility_score >= 0.62 AND repost_of_evidence_id IS NULL
                THEN COALESCE(NULLIF(original_publisher, ''), source_id)
                ELSE NULL
            END) AS credible_sources,
            COUNT(DISTINCT CASE
                WHEN repost_of_evidence_id IS NULL
                THEN COALESCE(NULLIF(original_publisher, ''), source_id)
                ELSE NULL
            END) AS sources
        FROM market_opinion_event_evidence
        WHERE canonical_event_id=%s
          AND content_role IN ('original_catalyst', 'catalyst_update', 'risk_event')
        """,
        (canonical_event_id,),
    )
    row = cursor.fetchone() or {}
    if not isinstance(row, Mapping):
        row = {
            "has_primary": row[0] if len(row) > 0 else 0,
            "credible_sources": row[1] if len(row) > 1 else 0,
            "sources": row[2] if len(row) > 2 else 0,
        }
    has_primary = int(row.get("has_primary") or 0)
    credible_sources = int(row.get("credible_sources") or 0)
    source_count = int(row.get("sources") or 0)
    if has_primary:
        return "confirmed_primary", credible_sources, source_count
    if credible_sources >= 2:
        return "confirmed_multi_source", credible_sources, source_count
    return "pending", credible_sources, source_count


def persist_market_opinion_event(
    *,
    raw_id: int,
    evidence: Mapping[str, Any],
    stock_matches: Sequence[Mapping[str, Any]],
    sector_matches: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Persist one ingestion item into the append-only v0.6 evidence ledger.

    The legacy raw/match tables remain the collection source. This function is
    deliberately called after those writes and never mutates their rows.
    """

    stock_codes = sorted(
        {
            str(row.get("code") or "").strip()
            for row in stock_matches
            if str(row.get("code") or "").strip()
        }
    )
    sector_names = sorted(
        {
            str(row.get("sector_name") or "").strip()
            for row in sector_matches
            if str(row.get("sector_name") or "").strip()
        }
    )
    normalized = normalize_event_evidence(
        {**dict(evidence), "raw_id": raw_id},
        stock_codes=stock_codes,
        sectors=sector_names,
    )
    received_at = _datetime_value(normalized.get("received_at"))
    available_at = _datetime_value(normalized.get("available_at"))
    if not received_at or not available_at:
        raise ValueError("market opinion event evidence requires received_at/available_at")

    event_id = str(normalized["canonical_event_id"])
    revision_hash = str(normalized["revision_hash"])
    identity = dict(normalized.get("identity_material") or {})
    event_date = str(identity.get("event_date") or received_at[:10])

    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_revision
                FROM market_opinion_event
                WHERE canonical_event_id=%s
                FOR UPDATE
                """,
                (event_id,),
            )
            existing_event = cursor.fetchone()
            current_revision = int(existing_event[0] or 0) if existing_event else 0
            cursor.execute(
                """
                SELECT event_revision
                FROM market_opinion_event_revision
                WHERE canonical_event_id=%s AND revision_hash=%s
                """,
                (event_id, revision_hash),
            )
            existing_revision = cursor.fetchone()
            if existing_revision:
                event_revision = int(existing_revision[0])
                delta = "same_facts"
            else:
                event_revision = current_revision + 1 if current_revision else 1
                delta = "material_update" if current_revision else "initial"

            primary_subject = ",".join(
                [
                    str(row.get("name") or row.get("code") or "").strip()
                    for row in stock_matches
                    if str(row.get("name") or row.get("code") or "").strip()
                ][:3]
            ) or ",".join(sector_names[:3]) or None
            cursor.execute(
                """
                INSERT INTO market_opinion_event (
                    canonical_event_id, identity_version, event_type, content_role,
                    direction, confirmation_status, primary_subject, event_date,
                    current_revision, first_available_at, last_available_at,
                    effective_until, driver_horizon, next_milestone, is_one_off,
                    is_terminal, identity_material_json, metadata_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    current_revision=GREATEST(current_revision, VALUES(current_revision)),
                    last_available_at=GREATEST(last_available_at, VALUES(last_available_at)),
                    effective_until=COALESCE(VALUES(effective_until), effective_until),
                    is_terminal=GREATEST(is_terminal, VALUES(is_terminal)),
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    event_id,
                    normalized["identity_version"],
                    normalized["event_type"],
                    normalized["content_role"],
                    normalized["direction"],
                    normalized["confirmation_status"],
                    primary_subject,
                    event_date,
                    event_revision,
                    available_at,
                    available_at,
                    _datetime_value(normalized.get("effective_until")),
                    normalized.get("driver_horizon") or "unknown",
                    normalized.get("next_milestone"),
                    int(bool(normalized.get("is_one_off"))),
                    int(bool(normalized.get("is_terminal"))),
                    _json(identity),
                    _json({"producer": "market_opinion_update", "schema": "sentiment-v06"}),
                ),
            )
            if not existing_revision:
                cursor.execute(
                    """
                    INSERT INTO market_opinion_event_revision (
                        canonical_event_id, event_revision, revision_hash,
                        evidence_rule_version, content_role, direction,
                        confirmation_status, delta_from_previous, source_time,
                        received_at, available_at, effective_until, facts_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        event_id,
                        event_revision,
                        revision_hash,
                        normalized["evidence_rule_version"],
                        normalized["content_role"],
                        normalized["direction"],
                        normalized["confirmation_status"],
                        delta,
                        _datetime_value(normalized.get("source_time")),
                        received_at,
                        available_at,
                        _datetime_value(normalized.get("effective_until")),
                        _json(
                            {
                                "revision_material": normalized.get("revision_material") or {},
                                "driver_horizon": normalized.get("driver_horizon"),
                                "next_milestone": normalized.get("next_milestone"),
                                "is_one_off": bool(normalized.get("is_one_off")),
                                "is_terminal": bool(normalized.get("is_terminal")),
                                "persistence_score": normalized.get("persistence_score"),
                            }
                        ),
                    ),
                )

            source_identity = str(
                normalized.get("original_publisher")
                or normalized.get("source_id")
                or ""
            ).strip()
            prior_independent_source = False
            if source_identity and not normalized.get("repost_of_evidence_id"):
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM market_opinion_event_evidence
                    WHERE canonical_event_id=%s
                      AND repost_of_evidence_id IS NULL
                      AND LOWER(TRIM(COALESCE(NULLIF(original_publisher, ''), source_id)))
                          <> LOWER(TRIM(%s))
                    """,
                    (event_id, source_identity),
                )
                prior_row = cursor.fetchone()
                prior_independent_source = bool(prior_row and int(prior_row[0] or 0))
            cursor.execute(
                """
                INSERT INTO market_opinion_event_evidence (
                    evidence_id, canonical_event_id, event_revision, raw_id,
                    original_news_id, source_id, original_publisher,
                    collection_channel, source_type, credibility_rule_version,
                    credibility_score, impact_score, is_primary_source,
                    is_independent_confirmation, repost_of_evidence_id,
                    source_time, published_at, first_seen_at, received_at,
                    available_at, content_role, title, evidence_excerpt,
                    source_url, raw_payload_hash, metadata_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE evidence_id=evidence_id
                """,
                (
                    normalized["evidence_id"],
                    event_id,
                    event_revision,
                    raw_id,
                    normalized.get("original_news_id") or normalized.get("item_id"),
                    normalized.get("source_id"),
                    normalized.get("original_publisher"),
                    normalized.get("collection_channel"),
                    normalized.get("source_type"),
                    normalized["credibility_rule_version"],
                    normalized.get("credibility_score"),
                    normalized.get("impact_score"),
                    int(bool(normalized.get("is_primary_source"))),
                    int(prior_independent_source),
                    normalized.get("repost_of_evidence_id"),
                    _datetime_value(normalized.get("source_time")),
                    _datetime_value(normalized.get("published_at")),
                    _datetime_value(normalized.get("first_seen_at")),
                    received_at,
                    available_at,
                    normalized["content_role"],
                    str(normalized.get("title") or "")[:512],
                    str(
                        normalized.get("relation_context")
                        or normalized.get("summary")
                        or normalized.get("title")
                        or ""
                    )[:1000]
                    or None,
                    normalized.get("url") or normalized.get("source_url"),
                    normalized["raw_payload_hash"],
                    _json(
                        {
                            "availability_status": normalized.get("availability_status"),
                            "received_at_source": normalized.get("received_at_source"),
                            "expectation_status": normalized.get("expectation_status"),
                            "expectation_value": normalized.get("expectation_value"),
                            "actual_value": normalized.get("actual_value"),
                            "expectation_unit": normalized.get("expectation_unit"),
                            "actual_unit": normalized.get("actual_unit"),
                            "surprise_pct": normalized.get("surprise_pct"),
                        }
                    ),
                ),
            )

            confirmation, credible_sources, source_count = _event_confirmation(cursor, event_id)
            cursor.execute(
                """
                UPDATE market_opinion_event
                SET confirmation_status=%s
                WHERE canonical_event_id=%s
                """,
                (confirmation, event_id),
            )
            cursor.execute(
                """
                UPDATE market_opinion_event_revision
                SET confirmation_status=%s
                WHERE canonical_event_id=%s AND event_revision=%s
                """,
                (confirmation, event_id, event_revision),
            )
            event_for_relation = {
                **normalized,
                "event_revision": event_revision,
                "confirmation_status": confirmation,
                "evidence": [normalized],
                "evidence_ids": [normalized["evidence_id"]],
            }
            relation_count = 0
            for match in stock_matches:
                code = str(match.get("code") or "").strip()
                if not code:
                    continue
                relation = build_stock_event_relation(
                    event_for_relation,
                    code=code,
                    stock_name=str(match.get("name") or "") or None,
                    relation_context=str(
                        match.get("relation_context")
                        or normalized.get("relation_context")
                        or normalized.get("summary")
                        or normalized.get("title")
                        or ""
                    ),
                    match_reason=str(match.get("match_reason") or "") or None,
                    match_score=match.get("match_score"),
                )
                cursor.execute(
                    """
                    INSERT INTO market_opinion_stock_relation (
                        relation_id, canonical_event_id, event_revision, code,
                        relation_type, relation_status, relation_score,
                        benefit_scale, benefit_scale_unit, evidence_id,
                        evidence_excerpt, relation_reason, is_adverse_veto,
                        valid_from, valid_until, relation_rule_version,
                        metadata_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE relation_id=relation_id
                    """,
                    (
                        relation["relation_id"],
                        event_id,
                        event_revision,
                        code,
                        relation["relation_type"],
                        relation["relation_status"],
                        relation["relation_score"],
                        relation.get("benefit_scale"),
                        relation.get("benefit_scale_unit"),
                        relation.get("evidence_id"),
                        relation.get("evidence_excerpt"),
                        relation.get("relation_reason"),
                        int(bool(relation.get("is_adverse_veto"))),
                        _datetime_value(relation.get("valid_from")) or available_at,
                        _datetime_value(relation.get("valid_until")),
                        relation["relation_rule_version"],
                        _json({"producer": "market_opinion_update"}),
                    ),
                )
                relation_count += 1

    return {
        "canonical_event_id": event_id,
        "event_revision": event_revision,
        "evidence_id": normalized["evidence_id"],
        "content_role": normalized["content_role"],
        "confirmation_status": confirmation,
        "event_confirmed": confirmation in CONFIRMED_EVENT_STATES,
        "credible_source_count": credible_sources,
        "source_count": source_count,
        "relation_count": relation_count,
        "delta_from_previous": delta,
    }
