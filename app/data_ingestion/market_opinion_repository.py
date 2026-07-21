from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

from app.shared.db import mysql_conn


STOCK_COLUMNS = {
    "code",
    "name",
    "industry",
    "score",
    "news_count",
    "match_type",
    "match_reason",
    "data_trade_date",
    "pct_chg",
    "amount",
    "matched_news",
}

NEWS_SNAPSHOT_COLUMNS = {
    "raw_id",
    "impact_score",
    "signed_score",
    "timeliness_score",
    "timeliness_level",
    "age_days",
    "effective_until",
    "published_at",
    "title",
    "source_id",
    "source_name",
    "direction",
    "event_type",
}


def _decode_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    return value


def _json_object(value: dict[str, Any]) -> str | None:
    cleaned = {key: _json_value(item) for key, item in value.items() if item is not None}
    return json.dumps(cleaned, ensure_ascii=False, default=str) if cleaned else None


def delete_snapshot_payloads(cursor, snapshot_ids: Iterable[int]) -> None:
    ids = [int(item) for item in snapshot_ids]
    if not ids:
        return
    placeholders = ",".join(["%s"] * len(ids))
    for table in (
        "sector_opinion_stock",
        "sector_opinion_news_ref",
        "sector_opinion_source_ref",
    ):
        cursor.execute(f"DELETE FROM {table} WHERE snapshot_id IN ({placeholders})", ids)


def normalized_payload_values(
    snapshot_id: int,
    summary: dict[str, Any],
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    top_stocks = _decode_json(summary.get("top_stocks") or summary.get("top_stocks_json"), [])
    top_news = _decode_json(summary.get("top_news") or summary.get("top_news_json"), [])
    sources = _decode_json(summary.get("sources") or summary.get("source_json"), [])

    stock_values = []
    news_values = []
    source_values = []
    for rank, stock in enumerate(top_stocks, start=1):
        if not isinstance(stock, dict) or not stock.get("code"):
            continue
        extra = {key: value for key, value in stock.items() if key not in STOCK_COLUMNS}
        stock_values.append(
            (
                snapshot_id,
                rank,
                stock.get("code"),
                stock.get("name"),
                stock.get("industry"),
                stock.get("score"),
                int(stock.get("news_count") or 0),
                stock.get("match_type"),
                str(stock.get("match_reason") or "")[:500] or None,
                stock.get("data_trade_date"),
                stock.get("pct_chg"),
                stock.get("amount"),
                _json_object(extra),
            )
        )
        for news_rank, news in enumerate(_decode_json(stock.get("matched_news"), []), start=1):
            if isinstance(news, dict):
                news_values.append(_news_ref_values(snapshot_id, "stock", stock.get("code"), news_rank, news))

    for rank, news in enumerate(top_news, start=1):
        if isinstance(news, dict):
            news_values.append(_news_ref_values(snapshot_id, "sector", "", rank, news))

    for rank, source_id in enumerate(sources, start=1):
        source_text = str(source_id or "").strip()
        if source_text:
            source_values.append((snapshot_id, rank, source_text[:64]))

    return stock_values, news_values, source_values


def insert_normalized_payload_values(
    cursor,
    stock_values: list[tuple[Any, ...]],
    news_values: list[tuple[Any, ...]],
    source_values: list[tuple[Any, ...]],
) -> None:

    if stock_values:
        cursor.executemany(
            """
            INSERT INTO sector_opinion_stock (
                snapshot_id, rank_no, code, name, industry, score, news_count,
                match_type, match_reason, data_trade_date, pct_chg, amount, extra_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                rank_no=VALUES(rank_no), name=VALUES(name), industry=VALUES(industry), score=VALUES(score),
                news_count=VALUES(news_count), match_type=VALUES(match_type), match_reason=VALUES(match_reason),
                data_trade_date=VALUES(data_trade_date), pct_chg=VALUES(pct_chg), amount=VALUES(amount),
                extra_json=VALUES(extra_json)
            """,
            stock_values,
        )
    if news_values:
        cursor.executemany(
            """
            INSERT INTO sector_opinion_news_ref (
                snapshot_id, scope_type, stock_code, rank_no, raw_id,
                impact_score, signed_score, timeliness_score, timeliness_level,
                age_days, effective_until, published_at, fallback_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                raw_id=VALUES(raw_id), impact_score=VALUES(impact_score), signed_score=VALUES(signed_score),
                timeliness_score=VALUES(timeliness_score), timeliness_level=VALUES(timeliness_level),
                age_days=VALUES(age_days), effective_until=VALUES(effective_until),
                published_at=VALUES(published_at), fallback_json=VALUES(fallback_json)
            """,
            news_values,
        )
    if source_values:
        cursor.executemany(
            """
            INSERT INTO sector_opinion_source_ref (snapshot_id, rank_no, source_id)
            VALUES (%s,%s,%s)
            ON DUPLICATE KEY UPDATE rank_no=VALUES(rank_no)
            """,
            source_values,
        )


def store_normalized_payload(cursor, snapshot_id: int, summary: dict[str, Any]) -> dict[str, int]:
    delete_snapshot_payloads(cursor, [snapshot_id])
    stock_values, news_values, source_values = normalized_payload_values(snapshot_id, summary)
    insert_normalized_payload_values(cursor, stock_values, news_values, source_values)
    return {
        "stocks": len(stock_values),
        "news_refs": len(news_values),
        "sources": len(source_values),
    }


def _news_ref_values(
    snapshot_id: int,
    scope_type: str,
    stock_code: str,
    rank_no: int,
    news: dict[str, Any],
) -> tuple[Any, ...]:
    extra = {key: value for key, value in news.items() if key not in NEWS_SNAPSHOT_COLUMNS}
    raw_id = news.get("raw_id")
    fallback = dict(extra)
    if news.get("direction"):
        # `market_opinion_raw.direction` is article-wide.  Snapshot direction is
        # sector/stock-clause local and must survive normalized hydration.
        fallback["direction"] = news.get("direction")
    if news.get("event_type"):
        # The raw event type is also article-wide.  A peer stock mentioned in
        # an earnings article must keep its own local market-attention decay.
        fallback["event_type"] = news.get("event_type")
    if not raw_id:
        fallback.update(news)
    return (
        snapshot_id,
        scope_type,
        stock_code or "",
        rank_no,
        int(raw_id) if raw_id else None,
        news.get("impact_score"),
        news.get("signed_score"),
        news.get("timeliness_score"),
        news.get("timeliness_level"),
        news.get("age_days"),
        news.get("effective_until"),
        news.get("published_at"),
        _json_object(fallback),
    )


def resolve_snapshot_news_direction(
    fallback: dict[str, Any],
    raw_direction: str | None,
    signed_score: Any,
) -> str:
    local_direction = str(fallback.get("direction") or "").strip()
    if local_direction in {"positive", "negative", "neutral"}:
        return local_direction
    try:
        score = float(signed_score)
    except (TypeError, ValueError):
        score = 0.0
    if score > 0:
        return "positive"
    if score < 0:
        return "negative"
    return str(raw_direction or "neutral") if str(raw_direction or "neutral") in {"positive", "negative", "neutral"} else "neutral"


def resolve_snapshot_news_event_type(
    fallback: dict[str, Any],
    raw_event_type: str | None,
) -> str:
    return str(fallback.get("event_type") or raw_event_type or "general")


def save_sector_summaries_normalized(
    summaries: list[dict[str, Any]],
    *,
    trade_date: str,
    as_of_datetime: str,
) -> dict[str, int]:
    totals = {"snapshots": 0, "stocks": 0, "news_refs": 0, "sources": 0}
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM sector_opinion_daily WHERE trade_date=%s AND as_of_datetime=%s",
                (trade_date, as_of_datetime),
            )
            existing_ids = [int(row[0]) for row in cursor.fetchall()]
            delete_snapshot_payloads(cursor, existing_ids)
            cursor.execute(
                "DELETE FROM sector_opinion_daily WHERE trade_date=%s AND as_of_datetime=%s",
                (trade_date, as_of_datetime),
            )
            for row in summaries:
                cursor.execute(
                    """
                    INSERT INTO sector_opinion_daily (
                        trade_date, sector_type, sector_name, as_of_datetime,
                        sector_score, weighted_impact_score, news_count, source_count, stock_count,
                        positive_news_count, negative_news_count,
                        top_stocks_json, top_news_json, source_json,
                        payload_version, payload_migrated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,NULL,2,NOW())
                    """,
                    (
                        row["trade_date"],
                        row["sector_type"],
                        row["sector_name"],
                        row["as_of_datetime"],
                        row["sector_score"],
                        row["weighted_impact_score"],
                        row["news_count"],
                        row["source_count"],
                        row["stock_count"],
                        row["positive_news_count"],
                        row["negative_news_count"],
                    ),
                )
                snapshot_id = int(cursor.lastrowid)
                counts = store_normalized_payload(cursor, snapshot_id, row)
                totals["snapshots"] += 1
                for key in ("stocks", "news_refs", "sources"):
                    totals[key] += counts[key]
    return totals


def hydrate_sector_opinion_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    normalized_ids = [
        int(row["id"])
        for row in rows
        if row.get("id") is not None
        and (
            int(row.get("payload_version") or 1) >= 2
            or (row.get("top_stocks_json") is None and row.get("top_news_json") is None)
        )
    ]
    if not normalized_ids:
        return rows
    placeholders = ",".join(["%s"] * len(normalized_ids))
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT snapshot_id, rank_no, code, name, industry, score, news_count,
                       match_type, match_reason, data_trade_date, pct_chg, amount, extra_json
                FROM sector_opinion_stock
                WHERE snapshot_id IN ({placeholders})
                ORDER BY snapshot_id, rank_no
                """,
                normalized_ids,
            )
            stock_rows = cursor.fetchall() or []
            cursor.execute(
                f"""
                SELECT ref.snapshot_id, ref.scope_type, ref.stock_code, ref.rank_no, ref.raw_id,
                       ref.impact_score, ref.signed_score, ref.timeliness_score,
                       ref.timeliness_level, ref.age_days, ref.effective_until,
                       COALESCE(ref.published_at, raw.published_at, raw.first_seen_at, raw.crawl_time) AS published_at,
                       ref.fallback_json,
                       raw.title, raw.source_id, raw.source_name, raw.direction, raw.event_type
                FROM sector_opinion_news_ref ref
                LEFT JOIN market_opinion_raw raw ON raw.id=ref.raw_id
                WHERE ref.snapshot_id IN ({placeholders})
                ORDER BY ref.snapshot_id, ref.scope_type, ref.stock_code, ref.rank_no
                """,
                normalized_ids,
            )
            news_rows = cursor.fetchall() or []
            cursor.execute(
                f"""
                SELECT snapshot_id, rank_no, source_id
                FROM sector_opinion_source_ref
                WHERE snapshot_id IN ({placeholders})
                ORDER BY snapshot_id, rank_no
                """,
                normalized_ids,
            )
            source_rows = cursor.fetchall() or []

    stocks_by_snapshot: dict[int, list[dict[str, Any]]] = defaultdict(list)
    stock_lookup: dict[tuple[int, str], dict[str, Any]] = {}
    top_news_by_snapshot: dict[int, list[dict[str, Any]]] = defaultdict(list)
    sources_by_snapshot: dict[int, list[str]] = defaultdict(list)

    for row in stock_rows:
        snapshot_id = int(row["snapshot_id"])
        stock = _decode_json(row.get("extra_json"), {})
        stock.update(
            {
                "code": row.get("code"),
                "name": row.get("name"),
                "industry": row.get("industry"),
                "score": _json_value(row.get("score")),
                "news_count": int(row.get("news_count") or 0),
                "match_type": row.get("match_type"),
                "match_reason": row.get("match_reason"),
                "data_trade_date": _json_value(row.get("data_trade_date")),
                "pct_chg": _json_value(row.get("pct_chg")),
                "amount": _json_value(row.get("amount")),
                "matched_news": [],
            }
        )
        stock = {key: value for key, value in stock.items() if value is not None}
        stocks_by_snapshot[snapshot_id].append(stock)
        stock_lookup[(snapshot_id, str(row.get("code") or ""))] = stock

    for row in news_rows:
        snapshot_id = int(row["snapshot_id"])
        news = _decode_json(row.get("fallback_json"), {})
        raw_direction = row.get("direction")
        local_direction = resolve_snapshot_news_direction(news, raw_direction, row.get("signed_score"))
        raw_event_type = row.get("event_type")
        local_event_type = resolve_snapshot_news_event_type(news, raw_event_type)
        news.setdefault("article_direction", raw_direction)
        news.setdefault("article_event_type", raw_event_type)
        materialized = {
            "raw_id": int(row["raw_id"]) if row.get("raw_id") is not None else None,
            "title": row.get("title"),
            "source_id": row.get("source_id"),
            "source_name": row.get("source_name"),
            "impact_score": _json_value(row.get("impact_score")),
            "signed_score": _json_value(row.get("signed_score")),
            "direction": local_direction,
            "event_type": local_event_type,
            "published_at": _json_value(row.get("published_at")),
            "timeliness_score": _json_value(row.get("timeliness_score")),
            "timeliness_level": row.get("timeliness_level"),
            "age_days": _json_value(row.get("age_days")),
            "effective_until": _json_value(row.get("effective_until")),
        }
        news.update({key: value for key, value in materialized.items() if value is not None})
        if row.get("scope_type") == "stock":
            stock = stock_lookup.get((snapshot_id, str(row.get("stock_code") or "")))
            if stock is not None:
                stock.setdefault("matched_news", []).append(news)
        else:
            top_news_by_snapshot[snapshot_id].append(news)

    for row in source_rows:
        sources_by_snapshot[int(row["snapshot_id"])].append(str(row.get("source_id") or ""))

    normalized_set = set(normalized_ids)
    for row in rows:
        snapshot_id = int(row["id"]) if row.get("id") is not None else None
        if snapshot_id not in normalized_set:
            continue
        row["top_stocks_json"] = json.dumps(stocks_by_snapshot[snapshot_id], ensure_ascii=False, default=str)
        row["top_news_json"] = json.dumps(top_news_by_snapshot[snapshot_id], ensure_ascii=False, default=str)
        row["source_json"] = json.dumps(sources_by_snapshot[snapshot_id], ensure_ascii=False, default=str)
    return rows
