from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
READ_CHUNK_SIZE = 1024 * 1024
TABLE_NAME = "strategy_factor_snapshot"
COMPACTION_LOCK = "stock_analysis_factor_snapshot_compaction"
MATERIALIZATION_LOCK = "sentiment_snapshot_materialize:all"


class _TokenCounter:
    def __init__(self, tokens: tuple[bytes, ...]) -> None:
        self._tokens = tokens
        self._tails = {token: b"" for token in tokens}
        self.counts = {token: 0 for token in tokens}

    def feed(self, chunk: bytes) -> None:
        for token in self._tokens:
            combined = self._tails[token] + chunk
            self.counts[token] += combined.count(token)
            self._tails[token] = combined[-(len(token) - 1) :]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(READ_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _load_verified_full_backup(manifest_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    backup_path = Path(str(manifest.get("backup_path") or "")).resolve()
    if not backup_path.is_file():
        raise RuntimeError(f"full backup is missing: {backup_path}")
    if int(manifest.get("dump_completed_markers") or 0) != 1:
        raise RuntimeError("full backup completion marker is invalid")
    expected_sha = str(manifest.get("sha256") or "")
    actual_sha = _sha256(backup_path)
    if not expected_sha or actual_sha != expected_sha:
        raise RuntimeError("full backup checksum mismatch")
    return {
        "backup_path": str(backup_path),
        "manifest_path": str(manifest_path.resolve()),
        "sha256": actual_sha,
        "compressed_bytes": backup_path.stat().st_size,
        "uncompressed_bytes": int(manifest.get("uncompressed_bytes") or 0),
        "create_table_statements": int(
            manifest.get("create_table_statements") or 0
        ),
        "insert_statements": int(manifest.get("insert_statements") or 0),
        "dump_completed_markers": 1,
    }


def _mysql_client_env(password: str) -> dict[str, str]:
    child_env = os.environ.copy()
    child_env["MYSQL_PWD"] = password
    return child_env


def _connection_flags(settings: Any) -> list[str]:
    return [
        f"--host={settings.host}",
        f"--port={settings.port}",
        f"--user={settings.user}",
        f"--default-character-set={settings.charset}",
    ]


def _dump_retained_rows(settings: Any, destination: Path) -> dict[str, Any]:
    command = [
        "/usr/bin/mysqldump",
        *_connection_flags(settings),
        "--single-transaction",
        "--skip-lock-tables",
        "--skip-add-locks",
        "--set-gtid-purged=OFF",
        "--no-tablespaces",
        "--quick",
        "--hex-blob",
        "--no-create-info",
        "--skip-triggers",
        "--where=(in_eligible_pool = 1 OR is_selected = 1)",
        settings.database,
        TABLE_NAME,
    ]
    insert_token = b"INSERT INTO "
    completion_token = b"-- Dump completed on "
    counter = _TokenCounter((insert_token, completion_token))
    uncompressed_bytes = 0
    with tempfile.TemporaryFile() as stderr_file:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            env=_mysql_client_env(settings.password),
        )
        if process.stdout is None:  # pragma: no cover - subprocess contract
            raise RuntimeError("mysqldump stdout pipe was not created")
        file_descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            with os.fdopen(file_descriptor, "wb") as raw_file:
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=raw_file,
                    compresslevel=6,
                    mtime=0,
                ) as compressed_file:
                    while True:
                        chunk = process.stdout.read(READ_CHUNK_SIZE)
                        if not chunk:
                            break
                        counter.feed(chunk)
                        compressed_file.write(chunk)
                        uncompressed_bytes += len(chunk)
            return_code = process.wait()
        except BaseException:
            process.terminate()
            process.wait(timeout=30)
            destination.unlink(missing_ok=True)
            raise
        if return_code != 0:
            stderr_file.seek(0)
            error = stderr_file.read().decode("utf-8", errors="replace").strip()
            destination.unlink(missing_ok=True)
            raise RuntimeError(
                f"retained-row mysqldump failed with exit {return_code}: {error[:1000]}"
            )
    insert_statements = counter.counts[insert_token]
    completion_markers = counter.counts[completion_token]
    if uncompressed_bytes <= 0 or insert_statements <= 0 or completion_markers != 1:
        destination.unlink(missing_ok=True)
        raise RuntimeError(
            "retained-row dump failed validation: "
            f"bytes={uncompressed_bytes}, inserts={insert_statements}, "
            f"completion_markers={completion_markers}"
        )
    return {
        "backup_path": str(destination),
        "compressed_bytes": destination.stat().st_size,
        "uncompressed_bytes": uncompressed_bytes,
        "insert_statements": insert_statements,
        "dump_completed_markers": completion_markers,
        "sha256": _sha256(destination),
    }


def _restore_retained_rows(settings: Any, source: Path) -> None:
    command = [
        "/usr/bin/mysql",
        *_connection_flags(settings),
        settings.database,
    ]
    with tempfile.TemporaryFile() as stderr_file:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
            env=_mysql_client_env(settings.password),
        )
        if process.stdin is None:  # pragma: no cover - subprocess contract
            raise RuntimeError("mysql stdin pipe was not created")
        try:
            with gzip.open(source, "rb") as compressed_file:
                while chunk := compressed_file.read(READ_CHUNK_SIZE):
                    process.stdin.write(chunk)
            process.stdin.close()
            return_code = process.wait()
        except BaseException:
            process.terminate()
            process.wait(timeout=30)
            raise
        if return_code != 0:
            stderr_file.seek(0)
            error = stderr_file.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(
                f"retained-row restore failed with exit {return_code}: {error[:1000]}"
            )


def _snapshot_counts(cursor: Any) -> dict[str, int]:
    cursor.execute(
        f"""
        SELECT COUNT(*) AS total_rows,
               SUM(in_eligible_pool=1) AS eligible_rows,
               SUM(is_selected=1) AS selected_rows,
               SUM(in_eligible_pool=0 AND is_selected=1) AS selected_outside_eligible,
               MIN(id) AS min_id,
               MAX(id) AS max_id
        FROM {TABLE_NAME}
        """
    )
    row = cursor.fetchone() or {}
    return {
        key: int(row.get(key) or 0)
        for key in (
            "total_rows",
            "eligible_rows",
            "selected_rows",
            "selected_outside_eligible",
            "min_id",
            "max_id",
        )
    }


def _table_status(cursor: Any) -> dict[str, Any]:
    cursor.execute(f"SHOW TABLE STATUS LIKE '{TABLE_NAME}'")
    row = cursor.fetchone() or {}
    return {
        key: row.get(key)
        for key in (
            "Engine",
            "Rows",
            "Data_length",
            "Index_length",
            "Data_free",
            "Create_time",
            "Update_time",
        )
    }


def _acquire_lock(cursor: Any, name: str) -> None:
    cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (name,))
    if int((cursor.fetchone() or {}).get("acquired") or 0) != 1:
        raise RuntimeError(f"maintenance lock is busy: {name}")


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    file_descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    with os.fdopen(file_descriptor, "w", encoding="utf-8") as target:
        json.dump(payload, target, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        target.write("\n")
    os.replace(temporary, path)


def compact(
    *,
    expected_database: str,
    full_backup_manifest: Path,
    output_dir: Path,
    apply: bool,
) -> dict[str, Any]:
    from app.orchestration.factor_snapshot_compaction import (
        reset_strategy_factor_snapshot,
    )
    from app.shared.db import mysql_maintenance_conn
    from app.shared.settings import mysql_settings

    if mysql_settings.database != expected_database:
        raise RuntimeError(
            "database safety check failed: configured database does not match "
            f"the explicit target {expected_database!r}"
        )
    output_dir = output_dir.resolve()
    if output_dir in {Path("/"), Path("/root"), Path.home().resolve()}:
        raise RuntimeError(f"unsafe output directory: {output_dir}")

    with mysql_maintenance_conn(timeout_seconds=300) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DATABASE() AS database_name")
            actual_database = str((cursor.fetchone() or {}).get("database_name") or "")
            if actual_database != expected_database:
                raise RuntimeError(
                    f"connected database mismatch: expected {expected_database}, got {actual_database}"
                )
            before_counts = _snapshot_counts(cursor)
            before_status = _table_status(cursor)
            if before_counts["selected_outside_eligible"] != 0:
                raise RuntimeError(
                    "selected rows exist outside the eligible pool; compaction filter is unsafe"
                )
            dry_run = {
                "status": "dry_run",
                "database": actual_database,
                "table": TABLE_NAME,
                "before_counts": before_counts,
                "before_status": before_status,
                "rows_to_retain": before_counts["eligible_rows"],
                "rows_to_archive": (
                    before_counts["total_rows"] - before_counts["eligible_rows"]
                ),
            }
            if not apply:
                return dry_run

            full_backup = _load_verified_full_backup(full_backup_manifest)
            output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(output_dir, 0o700)
            timestamp = datetime.now().astimezone()
            slug = timestamp.strftime("%Y%m%dT%H%M%S%z")
            retained_path = output_dir / f"{TABLE_NAME}-retained-{slug}.sql.gz"
            manifest_path = retained_path.with_suffix(retained_path.suffix + ".json")

            _acquire_lock(cursor, COMPACTION_LOCK)
            _acquire_lock(cursor, MATERIALIZATION_LOCK)
            retained_backup = _dump_retained_rows(mysql_settings, retained_path)

            original_next_id = before_counts["max_id"] + 1
            reset_strategy_factor_snapshot(
                cursor,
                auto_increment_floor=original_next_id,
            )
            _restore_retained_rows(mysql_settings, retained_path)

            after_counts = _snapshot_counts(cursor)
            if after_counts["total_rows"] != before_counts["eligible_rows"]:
                raise RuntimeError(
                    "retained row count mismatch after restore: "
                    f"expected {before_counts['eligible_rows']}, "
                    f"got {after_counts['total_rows']}"
                )
            if after_counts["eligible_rows"] != before_counts["eligible_rows"]:
                raise RuntimeError("eligible row count changed during compaction")
            if after_counts["selected_rows"] != before_counts["selected_rows"]:
                raise RuntimeError("selected row count changed during compaction")

            cursor.execute(
                f"""
                SELECT COUNT(*) AS orphan_rows
                FROM strategy_factor_outcome o
                LEFT JOIN {TABLE_NAME} s ON s.id=o.factor_snapshot_id
                WHERE s.id IS NULL
                """
            )
            orphan_rows = int((cursor.fetchone() or {}).get("orphan_rows") or 0)
            if orphan_rows:
                raise RuntimeError(
                    f"factor outcome references became orphaned: {orphan_rows}"
                )
            after_status = _table_status(cursor)

    result = {
        "status": "compacted",
        "database": expected_database,
        "table": TABLE_NAME,
        "started_at": timestamp.isoformat(),
        "completed_at": datetime.now().astimezone().isoformat(),
        "before_counts": before_counts,
        "after_counts": after_counts,
        "rows_archived_from_online_table": (
            before_counts["total_rows"] - after_counts["total_rows"]
        ),
        "before_status": before_status,
        "after_status": after_status,
        "orphan_outcome_rows": orphan_rows,
        "full_backup": full_backup,
        "retained_backup": retained_backup,
        "auto_increment_floor": original_next_id,
        "recovery_note": (
            "The verified full backup contains the archived pre-filter rows; "
            "the retained backup can repopulate the compact online table with original IDs."
        ),
    }
    _write_manifest(manifest_path, result)
    return {**result, "manifest_path": str(manifest_path)}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Archive redundant pre-filter factor traces and keep only immutable "
            "eligible/selected research rows online. Dry-run by default."
        )
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-database", default="stock")
    parser.add_argument("--full-backup-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    result = compact(
        expected_database=args.expected_database,
        full_backup_manifest=args.full_backup_manifest,
        output_dir=args.output_dir,
        apply=bool(args.apply),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
