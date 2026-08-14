from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol


DEFAULT_OUTPUT_DIR = Path("/root/stock-analysis-backups/automated")
DEFAULT_RETENTION_COUNT = 7
BACKUP_PREFIX = "stock-full-"
BACKUP_SUFFIX = ".sql.gz"
READ_CHUNK_SIZE = 1024 * 1024


class DatabaseSettings(Protocol):
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str


@dataclass(frozen=True)
class BackupValidation:
    sha256: str
    compressed_bytes: int
    uncompressed_bytes: int
    create_table_statements: int
    insert_statements: int
    completion_markers: int


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


def _safe_output_dir(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    forbidden = {Path("/"), Path("/root"), Path.home().resolve()}
    if resolved in forbidden:
        raise ValueError(f"unsafe backup output directory: {resolved}")
    return resolved


def _build_dump_command(settings: DatabaseSettings) -> list[str]:
    return [
        "/usr/bin/mysqldump",
        f"--host={settings.host}",
        f"--port={settings.port}",
        f"--user={settings.user}",
        f"--default-character-set={settings.charset}",
        "--single-transaction",
        "--skip-lock-tables",
        "--set-gtid-purged=OFF",
        "--no-tablespaces",
        "--quick",
        "--hex-blob",
        settings.database,
    ]


def _run_dump(settings: DatabaseSettings, destination: Path) -> int:
    command = _build_dump_command(settings)
    child_env = os.environ.copy()
    child_env["MYSQL_PWD"] = settings.password
    uncompressed_bytes = 0

    with tempfile.TemporaryFile() as stderr_file:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            env=child_env,
        )
        try:
            if process.stdout is None:  # pragma: no cover - subprocess contract
                raise RuntimeError("mysqldump stdout pipe was not created")
            file_descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
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
                        compressed_file.write(chunk)
                        uncompressed_bytes += len(chunk)
            return_code = process.wait()
        except Exception:
            process.terminate()
            process.wait(timeout=30)
            raise

        if return_code != 0:
            stderr_file.seek(0)
            error = stderr_file.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"mysqldump failed with exit {return_code}: {error[:1000]}")

    return uncompressed_bytes


def _validate_backup(path: Path) -> BackupValidation:
    schema_token = b"CREATE" + b" TABLE "
    tokens = (schema_token, b"INSERT INTO ", b"-- Dump completed on ")
    counter = _TokenCounter(tokens)
    uncompressed_bytes = 0
    try:
        with gzip.open(path, "rb") as backup_file:
            while True:
                chunk = backup_file.read(READ_CHUNK_SIZE)
                if not chunk:
                    break
                counter.feed(chunk)
                uncompressed_bytes += len(chunk)
    except (OSError, EOFError) as exc:
        raise RuntimeError(f"gzip integrity validation failed: {exc}") from exc

    create_count = counter.counts[tokens[0]]
    insert_count = counter.counts[tokens[1]]
    completion_count = counter.counts[tokens[2]]
    if uncompressed_bytes <= 0:
        raise RuntimeError("database backup is empty")
    if create_count <= 0:
        raise RuntimeError("database backup contains no table-definition statements")
    if insert_count <= 0:
        raise RuntimeError("database backup contains no INSERT INTO statements")
    if completion_count != 1:
        raise RuntimeError(
            f"database backup completion marker count must be 1, got {completion_count}"
        )

    digest = hashlib.sha256()
    with path.open("rb") as backup_file:
        while True:
            chunk = backup_file.read(READ_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)

    compressed_bytes = path.stat().st_size
    if compressed_bytes <= 0:
        raise RuntimeError("compressed database backup is empty")
    return BackupValidation(
        sha256=digest.hexdigest(),
        compressed_bytes=compressed_bytes,
        uncompressed_bytes=uncompressed_bytes,
        create_table_statements=create_count,
        insert_statements=insert_count,
        completion_markers=completion_count,
    )


def _write_manifest(path: Path, payload: dict) -> None:
    temporary_path = path.with_suffix(path.suffix + ".partial")
    file_descriptor = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    with os.fdopen(file_descriptor, "w", encoding="utf-8") as manifest_file:
        json.dump(payload, manifest_file, ensure_ascii=False, indent=2, sort_keys=True)
        manifest_file.write("\n")
    os.replace(temporary_path, path)


def _prune_backups(output_dir: Path, retention_count: int) -> list[str]:
    if retention_count < 1:
        raise ValueError("retention_count must be at least 1")
    backups = sorted(
        output_dir.glob(f"{BACKUP_PREFIX}*{BACKUP_SUFFIX}"),
        key=lambda candidate: candidate.name,
        reverse=True,
    )
    removed: list[str] = []
    for backup_path in backups[retention_count:]:
        manifest_path = backup_path.with_suffix(backup_path.suffix + ".json")
        backup_path.unlink()
        manifest_path.unlink(missing_ok=True)
        removed.append(backup_path.name)
    return removed


def create_backup(
    *,
    settings: DatabaseSettings,
    output_dir: Path,
    retention_count: int,
) -> dict:
    safe_output_dir = _safe_output_dir(output_dir)
    safe_output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(safe_output_dir, 0o700)

    timestamp = datetime.now().astimezone()
    timestamp_slug = timestamp.strftime("%Y%m%dT%H%M%S%z")
    backup_name = f"{BACKUP_PREFIX}{timestamp_slug}{BACKUP_SUFFIX}"
    final_path = safe_output_dir / backup_name
    partial_path = safe_output_dir / f".{backup_name}.{os.getpid()}.partial"
    if final_path.exists():
        raise RuntimeError(f"backup already exists: {final_path}")

    try:
        streamed_bytes = _run_dump(settings, partial_path)
        validation = _validate_backup(partial_path)
        if validation.uncompressed_bytes != streamed_bytes:
            raise RuntimeError(
                "database backup byte count changed between creation and validation"
            )
        os.replace(partial_path, final_path)
        manifest_path = final_path.with_suffix(final_path.suffix + ".json")
        manifest = {
            "backup_path": str(final_path),
            "completed_at": datetime.now().astimezone().isoformat(),
            "compressed_bytes": validation.compressed_bytes,
            "create_table_statements": validation.create_table_statements,
            "database": settings.database,
            "dump_completed_markers": validation.completion_markers,
            "format": "mysqldump+gzip",
            "insert_statements": validation.insert_statements,
            "required_dump_flags": [
                "--single-transaction",
                "--skip-lock-tables",
                "--set-gtid-purged=OFF",
                "--no-tablespaces",
            ],
            "sha256": validation.sha256,
            "started_at": timestamp.isoformat(),
            "uncompressed_bytes": validation.uncompressed_bytes,
        }
        _write_manifest(manifest_path, manifest)
        removed = _prune_backups(safe_output_dir, retention_count)
        return {
            **manifest,
            "manifest_path": str(manifest_path),
            "retention_count": retention_count,
            "removed_backups": removed,
        }
    finally:
        partial_path.unlink(missing_ok=True)


def main() -> None:
    from app.shared.settings import mysql_settings

    parser = argparse.ArgumentParser(
        description="Create and validate a compressed full MySQL backup."
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--retention-count",
        type=int,
        default=DEFAULT_RETENTION_COUNT,
        help="Keep this many successful automated full backups.",
    )
    args = parser.parse_args()
    if args.retention_count < 1:
        parser.error("--retention-count must be at least 1")

    os.umask(0o077)
    output_dir = _safe_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = output_dir / ".database-backup.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SystemExit("another database backup is already running") from exc
        result = create_backup(
            settings=mysql_settings,
            output_dir=output_dir,
            retention_count=args.retention_count,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
