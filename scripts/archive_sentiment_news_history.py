"""Bounded historical news retrieval / legacy preservation, outside selectors."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta

from app.data_ingestion.news_history import (
    MajorNewsBackfill, NewsHistoryArchive, capture_news, local_now, strict_asof,
)


def inventory_legacy(start, end, *, archive=None, max_rows=50_000):
    """Preserve current DB versions, not reconstruct their old overwritten text."""
    from app.shared.db import mysql_conn
    if not 0 < (end - start).total_seconds() <= 7 * 86400:
        raise ValueError("legacy inventory must span (0,7] days")
    archive = archive or NewsHistoryArchive()
    refs, counts, windows = [], {}, []
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            # Fixed, validated table contracts; PK pagination bounds JSON loads.
            for table in ("stock_news", "market_opinion_raw"):
                total = 0
                dates = [None] if table == "stock_news" else [
                    (start + timedelta(days=i)).date().isoformat()
                    for i in range((end.date()-start.date()).days)]
                for day in dates:
                    after = 0
                    while total < max_rows:
                        params = [after]
                        where = "id > %s"
                        if day:
                            where += " AND trade_date=%s"
                            params.append(day)
                        params.append(min(500, max_rows-total))
                        cursor.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY id LIMIT %s", params)
                        rows = cursor.fetchall() or []
                        if not rows:
                            break
                        adapted = []
                        for row in rows:
                            adapted.append({"title": row["title"], "code": row.get("code"), "summary": row.get("summary"),
                                "url": row.get("url"), "source": row.get("source") or row.get("source_name"),
                                # market raw published_at may be a crawl-time fallback;
                                # keep it only as a legacy claim in the original DB row.
                                "published_at": row.get("published_at") if table == "stock_news" else None,
                                "item_id": f"{table}:{row['id']}", "legacy_database_row": row,
                                "content_kind": "legacy_current_excerpt"})
                        ref = capture_news(adapted, provider="mysql."+table, mode="legacy_inventory", archive=archive)
                        refs.append(ref)
                        total += len(rows)
                        after = rows[-1]["id"]
                        if ref["status"] != "sealed":
                            raise RuntimeError("legacy inventory failed to seal: "+ref["reason"])
                    windows.append({"table": table, "trade_date": day,
                                    "last_id": after, "possibly_truncated": total >= max_rows})
                    if total >= max_rows:
                        break
                counts[table] = total
    manifest = {"schema": "news_observation_v1", "kind": "legacy_current_versions",
        "sealed_at": local_now(), "start": str(start), "end": str(end), "rows": counts,
        "scope": "stock_news_all_bounded_by_PK;market_raw_trade_date_window",
        "status": "partial" if any(w["possibly_truncated"] for w in windows) else "sealed",
        "windows": windows, "batches": refs, "strict_asof_eligible": False,
        "historical_original_versions_reconstructed": False, "full_history_backtest_available": False}
    return {**manifest, "manifest": archive.put("news_manifest", manifest)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("backfill", "inventory", "inspect"))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date", help="exclusive end date, at most seven days after start")
    parser.add_argument("--max-calls", type=int, default=100)
    parser.add_argument("--archive-directory")
    parser.add_argument("--news-sha256", help="inspect one sealed batch without MySQL/network")
    parser.add_argument("--manifest-sha256", help="inspect a coverage manifest without MySQL/network")
    parser.add_argument("--resume-manifest-sha256", help="backfill only failed windows from a verified manifest")
    parser.add_argument("--as-of", help="strict live-observation visibility cutoff; backfills are excluded")
    args = parser.parse_args(argv)
    archive = NewsHistoryArchive(args.archive_directory) if args.archive_directory else NewsHistoryArchive()
    if args.action == "inspect":
        if args.manifest_sha256:
            result = archive.load("news_manifest", args.manifest_sha256)
        elif args.news_sha256:
            bundle = archive.load("news", args.news_sha256)
            rows = bundle["rows"]
            result = {"provider": bundle["provider"], "mode": bundle["mode"], "rows": len(rows),
                "content_kinds": sorted({r["content_kind"] for r in rows}),
                "strict_asof_rows": len(strict_asof(rows, args.as_of)) if args.as_of else None,
                "full_history_backtest_available": False}
        else:
            parser.error("inspect requires a sealed news/manifest SHA-256")
    else:
        if not args.start_date or not args.end_date or not 1 <= args.max_calls <= 300:
            parser.error("start/end dates and max-calls between 1 and 300 are required")
        start, end = datetime.fromisoformat(args.start_date), datetime.fromisoformat(args.end_date)
        if args.action == "inventory":
            result = inventory_legacy(start, end, archive=archive)
        else:
            # Settings parser loads .env safely; never source it in a shell.
            from app.shared import settings  # noqa: F401
            import tushare as ts
            if not os.environ.get("TUSHARE_TOKEN"):
                raise RuntimeError("TUSHARE_TOKEN unavailable")
            pro = ts.pro_api(os.environ["TUSHARE_TOKEN"], timeout=20)
            previous = archive.load("news_manifest", args.resume_manifest_sha256) if args.resume_manifest_sha256 else None
            result = MajorNewsBackfill(pro, archive=archive, max_calls=args.max_calls).run(start,end,previous=previous)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 2 if result.get("status") in {"warning", "partial"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
