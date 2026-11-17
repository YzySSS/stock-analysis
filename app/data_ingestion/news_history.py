"""Immutable news observations, never a replacement for historical PIT inputs.

Provider publication times are claims, not proof of local availability. Legacy
rows and historical fetches are deliberately excluded by the strict as-of view.
No selection table, frozen strategy or event ledger is written by this module.
"""
from __future__ import annotations

import logging
import re
import time as monotonic_time
from datetime import datetime, time, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from app.backtest.sentiment_evidence import EvidenceArchive
from app.backtest.sentiment_replay import digest

log = logging.getLogger(__name__)
TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_DIRECTORY = Path(__file__).resolve().parents[2] / "logs" / "sentiment-news-history"
SCHEMA = "news_observation_v1"
MAX_BATCH_ROWS = 2000


def local_now() -> str:
    return datetime.now(TZ).isoformat(timespec="microseconds")


def clock(value) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).strip()
        if isinstance(value, (int, float)) or re.fullmatch(r"\d{10}(?:\d{3})?", text):
            number = float(value)
            return datetime.fromtimestamp(number / 1000 if number > 10_000_000_000 else number, TZ)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace("/", "-"))
        except ValueError:
            parsed = parsedate_to_datetime(text)
        return parsed.replace(tzinfo=TZ) if parsed.tzinfo is None else parsed.astimezone(TZ)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


class NewsHistoryArchive(EvidenceArchive):
    def __init__(self, directory=DEFAULT_DIRECTORY, **kwargs):
        super().__init__(directory, max_files=kwargs.pop("max_files", 50_000), **kwargs)

    def _path(self, kind, key):
        if kind not in {"news", "news_manifest"} or not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ValueError("invalid news evidence identity")
        return self.directory / f"{kind}-{key}.json.gz"


def normalize_news(item: dict, *, provider: str, mode: str, observed_at: str,
                   code: str | None = None) -> dict:
    if mode not in {"live_observation", "historical_backfill", "legacy_inventory"}:
        raise ValueError("invalid news acquisition mode")
    observed = clock(observed_at)
    if not observed:
        raise ValueError("a real observation clock is required")
    title = str(item.get("title") or "").strip()
    if not title or len(title) > 16_384:
        raise ValueError("missing or oversized news title")
    url = str(item.get("url") or item.get("source_url") or "").strip()
    parsed_url = urlsplit(url)
    stable_url = urlunsplit((parsed_url.scheme.lower(), parsed_url.netloc.lower(),
                            parsed_url.path, parsed_url.query, "")) if url else ""
    # NewsNow's adapter item_id falls back to URL/title. Preserve that value in
    # raw_payload, but only the actual upstream id/key is a provider ID.
    upstream = item.get("raw") if provider.startswith("newsnow.") else None
    original_id = ((upstream.get("id") or upstream.get("key")) if isinstance(upstream, dict)
                   else (item.get("item_id") or item.get("id")))
    original_id = str(original_id) if original_id is not None else ""
    # Fallback is explicitly synthetic; never call a title hash a provider ID.
    identity_kind = "url" if stable_url else "provider_id" if original_id else "synthetic_title"
    article_id = digest({"provider": provider, "identity_kind": identity_kind,
                         "identity": stable_url or original_id or title})
    published_raw = ((upstream.get("pubDate") or upstream.get("date") or upstream.get("createdAt"))
                     if isinstance(upstream, dict) else
                     (item.get("published_at") or item.get("datetime") or item.get("pub_time")))
    published = clock(published_raw)
    precision = "unknown"
    if published:
        precision = "date" if re.fullmatch(r"\d{4}[-/]\d{2}[-/]\d{2}", str(published_raw).strip()) else "timestamp"
        if precision == "date":
            # A provider date does not prove morning visibility.
            published = datetime.combine(published.date(), time(23, 59, 59), TZ)
    body = str(item.get("content") or item.get("summary") or "")
    content_kind = str(item.get("content_kind") or ("provider_excerpt" if body else "headline_only"))
    content = {"title": title, "body": body, "url": url,
               "publisher": str(item.get("source") or item.get("src") or item.get("source_name") or ""),
               "provider_published_raw": str(published_raw) if published_raw else None,
               "content_kind": content_kind, "raw_payload": item}
    version_hash = digest(content)
    available = max(observed, published) if published else observed
    return {"article_id": article_id, "identity_kind": identity_kind,
            "provider_original_id": original_id or None, "version_sha256": version_hash,
            "provider": provider, "code": code or item.get("code"), "acquisition_mode": mode,
            "observed_at": observed.isoformat(timespec="microseconds"),
            "provider_published_at": published.isoformat() if published else None,
            "publication_precision": precision,
            "available_at": available.isoformat(timespec="microseconds"),
            "historical_first_seen_proven": False,
            "strict_asof_eligible": mode == "live_observation",
            **content}


def capture_news(items, *, provider, mode="live_observation", code=None,
                 archive=None, observed_at=None) -> dict:
    """Archive the exact received payload; errors are explicit, not data gates."""
    try:
        if len(items) > MAX_BATCH_ROWS:
            raise ValueError("news batch exceeds 2000 rows; split before capturing")
        observed_at = observed_at or local_now()
        rows = [normalize_news(dict(item), provider=provider, mode=mode,
                               code=code, observed_at=observed_at) for item in items]
        bundle = {"schema": SCHEMA, "sealed_at": local_now(), "provider": provider,
                  "mode": mode, "rows": rows, "full_history_backtest_available": False}
        ref = (archive or NewsHistoryArchive()).put("news", bundle)
        return {"status": "sealed", "rows": len(rows), **ref}
    except Exception as exc:
        reason = f"{type(exc).__name__}: {str(exc)[:200]}"
        log.warning("新闻版本封存告警 provider=%s: %s", provider, reason)
        return {"status": "warning", "rows": 0, "reason": reason,
                "full_history_backtest_available": False}


def strict_asof(rows, as_of: str) -> list[dict]:
    """Only versions sealed live and truly available by the requested cutoff.

    Caller supplies verified archive rows, not untrusted provider JSON. Return
    newest observed version per article/code, preserving revisions in archive.
    """
    cutoff = clock(as_of)
    if cutoff is None:
        raise ValueError("invalid as-of clock")
    selected = {}
    ambiguous = set()
    for row in rows:
        observed, available = clock(row.get("observed_at")), clock(row.get("available_at"))
        if (row.get("acquisition_mode") != "live_observation"
                or row.get("strict_asof_eligible") is not True or not observed or not available
                or observed > cutoff or available > cutoff or available < observed):
            continue
        key = (row["article_id"], row.get("code"))
        if key not in selected or clock(selected[key]["observed_at"]) < observed:
            selected[key] = row
            ambiguous.discard(key)
        elif (clock(selected[key]["observed_at"]) == observed
              and selected[key]["version_sha256"] != row["version_sha256"]):
            # No trustworthy intra-batch revision ordering was observed.
            ambiguous.add(key)
    return [row for key,row in selected.items() if key not in ambiguous]


class MajorNewsBackfill:
    """Title-only provider archive, with fail-visible bounded split-on-cap reads.

    Tushare major_news has no content column and can cap responses at 800.
    A below-cap response proves only successful queried-window retrieval, never
    all market news or the provider's actual original-publication availability.
    """
    def __init__(self, pro, *, archive=None, max_calls=100, response_cap=800, min_interval=3.2):
        self.pro, self.archive = pro, archive or NewsHistoryArchive()
        self.max_calls, self.response_cap = max_calls, response_cap
        self.calls = 0
        self.min_interval, self.last_call = min_interval, 0.0

    def run(self, start: datetime, end: datetime, *, previous=None) -> dict:
        if not 0 < (end - start).total_seconds() <= 7 * 86400:
            raise ValueError("history request must span (0,7] days")
        if end > datetime.now(TZ).replace(tzinfo=None):
            raise ValueError("future provider windows are not historical data")
        windows, refs, seen = [], [], set()
        provider_pause = None
        if previous:
            if (previous.get("kind") != "historical_provider_windows"
                    or previous.get("provider") != "tushare.major_news"
                    or previous.get("start") != str(start) or previous.get("end") != str(end)):
                raise ValueError("resume manifest scope/provider mismatch")
            throttle = previous.get("provider_throttle") or {}
            retry_not_before = clock(throttle.get("retry_not_before"))
            if retry_not_before and datetime.now(TZ) < retry_not_before:
                provider_pause = throttle
            windows.extend(w for w in previous["windows"] if w["status"] == "retrieved")
            for ref in previous["batches"]:
                if ref["status"] == "sealed":
                    bundle = self.archive.load("news", ref["sha256"])
                    refs.append(ref)
                    for row in bundle["rows"]:
                        payload = dict(row["raw_payload"])
                        payload.pop("content_kind", None)
                        seen.add(digest(payload))

        def fetch(lo, hi):
            nonlocal provider_pause
            if provider_pause:
                windows.append({"start": str(lo), "end": str(hi), "status": "provider_throttle_deferred",
                                "reason": provider_pause["reason"]})
                return
            if self.calls >= self.max_calls:
                windows.append({"start": str(lo), "end": str(hi), "status": "call_budget_exhausted"})
                return
            self.calls += 1
            try:
                wait = self.min_interval - (monotonic_time.monotonic() - self.last_call)
                if wait > 0:
                    monotonic_time.sleep(wait)
                self.last_call = monotonic_time.monotonic()
                frame = self.pro.major_news(start_date=str(lo), end_date=str(hi))
                items = frame.to_dict("records")
                if not {"title", "pub_time", "src", "url"}.issubset(frame.columns):
                    raise ValueError("major_news schema changed")
                if len(items) >= self.response_cap:
                    if (hi - lo).total_seconds() < 2:
                        windows.append({"start": str(lo), "end": str(hi), "status": "unresolved_provider_cap"})
                        return
                    mid = lo + timedelta(seconds=int((hi - lo).total_seconds()) // 2)
                    fetch(lo, mid)
                    fetch(mid, hi)  # inclusive boundary, content dedup below
                    return
                unique = []
                new_keys = set()
                for item in items:
                    published = clock(item.get("pub_time"))
                    if not published or not lo <= published.replace(tzinfo=None) <= hi:
                        raise ValueError("provider returned invalid/out-of-window publication time")
                    if published.replace(tzinfo=None) >= end:
                        continue  # public contract has an exclusive final end
                    key = digest(item)
                    if key not in seen and key not in new_keys:
                        new_keys.add(key)
                        unique.append({**item, "content_kind": "headline_only"})
                ref = capture_news(unique, provider="tushare.major_news", mode="historical_backfill", archive=self.archive)
                refs.append(ref)
                if ref["status"] == "sealed":
                    seen.update(new_keys)
                windows.append({"start": str(lo), "end": str(hi), "provider_rows": len(items),
                                "unique_rows": len(unique), "status": "retrieved" if ref["status"] == "sealed" else "archive_warning"})
            except Exception as exc:
                if "频率超限" in str(exc):
                    provider_pause = {"reason": str(exc)[:200],
                        "retry_not_before": (datetime.now(TZ)+timedelta(seconds=3660 if "小时" in str(exc) else 65)).isoformat()}
                windows.append({"start": str(lo), "end": str(hi), "status": "provider_error",
                                "reason": f"{type(exc).__name__}: {str(exc)[:200]}"})

        if previous:
            for window in previous["windows"]:
                if window["status"] != "retrieved":
                    lo, hi = datetime.fromisoformat(window["start"]), datetime.fromisoformat(window["end"])
                    if not start <= lo < hi <= end:
                        raise ValueError("resume window outside original scope")
                    fetch(lo, hi)
        else:
            lo = start
            while lo < end:
                hi = min(lo + timedelta(hours=6), end)
                fetch(lo, hi)
                lo = hi
        manifest = {"schema": SCHEMA, "kind": "historical_provider_windows", "sealed_at": local_now(),
                    "start": str(start), "end": str(end), "provider": "tushare.major_news",
                    "calls": self.calls, "unique_rows": len(seen), "windows": sorted(windows,key=lambda w:w["start"]), "batches": refs,
                    "status": "retrieved" if all(w["status"] == "retrieved" for w in windows) else "partial",
                    "content_kind": "headline_only", "provider_coverage_complete_proven": False,
                    "historical_first_seen_proven": False, "full_history_backtest_available": False}
        manifest["resumed_from_manifest_sha256"] = digest(previous) if previous else None
        manifest["provider_throttle"] = provider_pause
        return {**manifest, "manifest": self.archive.put("news_manifest", manifest)}
