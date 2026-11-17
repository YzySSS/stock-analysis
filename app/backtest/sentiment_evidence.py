"""Bounded, local immutable evidence, separate from frozen research protocols."""
from __future__ import annotations

import fcntl
import gzip
import json
import os
import re
from datetime import datetime
from pathlib import Path

from app.backtest.sentiment_replay import canonical, digest


DEFAULT_ARCHIVE = Path(__file__).resolve().parents[2] / "logs" / "sentiment-evidence"
MAX_RAW_BYTES = 64 * 1024 * 1024
MAX_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
MAX_ARCHIVE_FILES = 10_000


class EvidenceArchive:
    """Never overwrite/delete: hard quota failure is explicit and recoverable."""
    def __init__(self, directory=DEFAULT_ARCHIVE, *, max_bytes=MAX_ARCHIVE_BYTES,
                 max_files=MAX_ARCHIVE_FILES):
        self.directory = Path(directory)
        self.max_bytes, self.max_files = max_bytes, max_files

    def _path(self, kind: str, key: str) -> Path:
        if kind not in {"inputs", "snapshot", "replay"} or not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ValueError("invalid evidence identity")
        return self.directory / f"{kind}-{key}.json.gz"

    def load(self, kind: str, key: str):
        path = self._path(kind, key)
        with gzip.open(path, "rb") as stream:
            raw = stream.read(MAX_RAW_BYTES + 1)
        if len(raw) > MAX_RAW_BYTES:
            raise ValueError(f"evidence exceeds per-file budget ({len(raw)} > {MAX_RAW_BYTES} bytes)")
        value = json.loads(raw)
        material = value["rows"] if kind == "inputs" else value
        if digest(material) != key:
            raise ValueError("evidence content hash mismatch")
        if kind == "inputs":
            claimed = value.pop("_archive_content_sha256", None)
            if not claimed or digest(value) != claimed:
                raise ValueError("input envelope content hash mismatch")
        return value

    def put(self, kind: str, value, *, key: str | None = None) -> dict:
        identity = digest(value["rows"] if kind == "inputs" else value)
        if key is not None and key != identity:
            raise ValueError("input evidence does not match recorded read-view hash")
        path = self._path(kind, identity)
        if kind == "inputs":
            value = {**value, "_archive_content_sha256": digest(value)}
        raw = canonical(value)
        if len(raw) > MAX_RAW_BYTES:
            raise ValueError(f"evidence exceeds per-file budget ({len(raw)} > {MAX_RAW_BYTES} bytes)")
        compressed = gzip.compress(raw, compresslevel=1, mtime=0)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (self.directory / ".archive.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if path.exists():
                self.load(kind, identity)
                return {"kind": kind, "sha256": identity, "bytes": path.stat().st_size,
                        "reused": True}
            files = list(self.directory.glob("*.json.gz"))
            if len(files) >= self.max_files or sum(p.stat().st_size for p in files)+len(compressed) > self.max_bytes:
                raise ValueError("evidence archive quota reached; preserve files and arrange reviewed export")
            # temp + link publishes atomically and cannot replace an old object.
            temp = self.directory / f".pending-{os.getpid()}"
            try:
                with temp.open("xb") as stream:
                    os.chmod(temp, 0o600)
                    stream.write(compressed)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.link(temp, path)
                directory_fd = os.open(self.directory, os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                temp.unlink(missing_ok=True)
        return {"kind": kind, "sha256": identity, "bytes": len(compressed), "reused": False}


def archiving_materializer(*, archive: EvidenceArchive | None = None):
    """Wrap the existing materializer without changing frozen implementation.

    The whole candidate read view is persisted before its transaction commits.
    It is necessary but not sufficient for a future full-selector replay:
    ancillary repository lookups and raw upstream news are not reconstructed.
    """
    from app.stock_selection.sentiment_snapshot_materializer import (
        MySQLSentimentSnapshotInputRepository, SentimentSnapshotMaterializationService)
    archive = archive or EvidenceArchive()
    events = []

    class ArchivingInputs(MySQLSentimentSnapshotInputRepository):
        def _register_selection_read_view(self, conn, audit, candidates):
            batch = super()._register_selection_read_view(conn, audit, candidates)
            # The latest strategy is the full-input capture target. Older
            # versions run every 15 minutes: retain their selected snapshots,
            # not dozens of duplicate full-market payloads each trading day.
            if audit.strategy_id != "a_share_sentiment_v06":
                events.append({"stage": "inputs", "status": "skipped",
                    "reason": "full_input_capture_scoped_to_latest_strategy",
                    "strategy_id": audit.strategy_id})
                return batch
            # The archive is ancillary evidence, not an input quality gate.
            # Disk/quota/serialization failure must not roll back a valid
            # decision. Keep the failure visible in the enclosing task result.
            try:
                ref = archive.put("inputs", {"rows": list(candidates),
                    "captured_at": str(datetime.now()), "decision_as_of": str(audit.decision_as_of),
                    "source_manifest_ids": audit.source_manifest_ids,
                    "semantics": "actual_candidate_read_view_not_full_raw_news_archive"},
                    key=batch.payload_hash)
                events.append({"stage": "inputs", "status": "sealed", **ref})
            except Exception as exc:
                events.append(archive_warning("inputs", exc))
            return batch

    service = SentimentSnapshotMaterializationService(input_repository=ArchivingInputs())
    service.evidence_archive_events = events
    return service


def archive_warning(stage: str, exc: Exception) -> dict:
    return {"stage": stage, "status": "warning", "error_type": type(exc).__name__,
            "reason": str(exc)[:240], "selection_data_gates_unchanged": True}


def seal_published_result(result: dict, *, archive=None, repository=None,
                          input_events=()) -> dict:
    from app.backtest.sentiment_replay import SentimentReplayRepository, validate_evidence
    archive, repository = archive or EvidenceArchive(), repository or SentimentReplayRepository()
    snapshot_ids = []

    def collect(item):
        if isinstance(item, dict):
            if item.get("snapshot_id") and item.get("snapshot_status") == "ready":
                snapshot_ids.append(item["snapshot_id"])
            for value in item.values():
                if isinstance(value, (dict, list)):
                    collect(value)
        elif isinstance(item, list):
            for value in item:
                collect(value)
    collect(result)
    events = list(input_events)
    for snapshot_id in dict.fromkeys(snapshot_ids):
        try:
            bundle = repository.load_bundle(snapshot_id)
            validate_evidence(bundle)
            events.append({"snapshot_id": snapshot_id, "stage": "snapshot", "status": "sealed",
                           **archive.put("snapshot", bundle)})
        except Exception as exc:
            events.append({"snapshot_id": snapshot_id, **archive_warning("snapshot", exc)})
    status = ("warning" if any(e["status"] == "warning" for e in events) else
              "snapshot_only" if any(e["status"] == "skipped" for e in events) else "sealed")
    return {"status": status,
            "events": events, "full_history_backtest_available": False}
