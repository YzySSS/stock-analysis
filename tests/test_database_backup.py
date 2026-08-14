from __future__ import annotations

import gzip
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from scripts.run_database_backup import (
    _build_dump_command,
    _prune_backups,
    _validate_backup,
)


class DatabaseBackupTest(unittest.TestCase):
    def test_dump_command_uses_required_safe_flags_without_password(self) -> None:
        settings = SimpleNamespace(
            host="db.internal",
            port=3306,
            user="app_user",
            password="not-on-the-command-line",
            database="stock",
            charset="utf8mb4",
        )

        command = _build_dump_command(settings)

        self.assertIn("--single-transaction", command)
        self.assertIn("--skip-lock-tables", command)
        self.assertIn("--set-gtid-purged=OFF", command)
        self.assertIn("--no-tablespaces", command)
        self.assertNotIn(settings.password, command)

    def test_validate_backup_checks_integrity_schema_data_and_end_marker(self) -> None:
        dump = (
            b"CREATE TABLE `example` (`id` int);\n"
            b"INSERT INTO `example` VALUES (1);\n"
            b"-- Dump completed on 2026-08-14 15:00:00\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stock-full-test.sql.gz"
            with gzip.open(path, "wb") as backup_file:
                backup_file.write(dump)

            validation = _validate_backup(path)

        self.assertEqual(validation.create_table_statements, 1)
        self.assertEqual(validation.insert_statements, 1)
        self.assertEqual(validation.completion_markers, 1)
        self.assertEqual(validation.uncompressed_bytes, len(dump))
        self.assertEqual(len(validation.sha256), 64)

    def test_validate_backup_rejects_schema_only_dump(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stock-full-test.sql.gz"
            with gzip.open(path, "wb") as backup_file:
                backup_file.write(
                    b"CREATE TABLE `example` (`id` int);\n"
                    b"-- Dump completed on 2026-08-14 15:00:00\n"
                )

            with self.assertRaisesRegex(RuntimeError, "no INSERT INTO"):
                _validate_backup(path)

    def test_prune_only_removes_old_matching_backup_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            names = [
                "stock-full-20260812T003000+0800.sql.gz",
                "stock-full-20260813T003000+0800.sql.gz",
                "stock-full-20260814T003000+0800.sql.gz",
            ]
            for name in names:
                (output_dir / name).write_bytes(b"backup")
                (output_dir / f"{name}.json").write_text("{}", encoding="utf-8")
            unrelated = output_dir / "manual-backup.sql.gz"
            unrelated.write_bytes(b"manual")

            removed = _prune_backups(output_dir, retention_count=2)

            self.assertEqual(removed, [names[0]])
            self.assertFalse((output_dir / names[0]).exists())
            self.assertFalse((output_dir / f"{names[0]}.json").exists())
            self.assertTrue((output_dir / names[1]).exists())
            self.assertTrue((output_dir / names[2]).exists())
            self.assertTrue(unrelated.exists())


if __name__ == "__main__":
    unittest.main()
