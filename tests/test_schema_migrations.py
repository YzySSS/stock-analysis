from __future__ import annotations

import io
import re
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from app.orchestration import migrate
from app.orchestration.migration_smoke import validate_smoke_database_name


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SchemaMigrationRegistryTests(unittest.TestCase):
    def test_empty_database_smoke_rejects_production_or_wrong_database(self):
        with self.assertRaises(ValueError):
            validate_smoke_database_name("stock", "stock")
        with self.assertRaises(ValueError):
            validate_smoke_database_name(
                "stock_migration_smokeevil",
                "stock_migration_smokeevil",
            )
        with self.assertRaises(ValueError):
            validate_smoke_database_name(
                "stock_migration_smoke_",
                "stock_migration_smoke_",
            )
        with self.assertRaises(RuntimeError):
            validate_smoke_database_name("stock", "stock_migration_smoke_test")
        validate_smoke_database_name(
            "stock_migration_smoke",
            "stock_migration_smoke",
        )
        validate_smoke_database_name(
            "stock_migration_smoke_test",
            "stock_migration_smoke_test",
        )

    def test_registry_is_ordered_unique_and_covers_current_schema_modules(self):
        versions = [item.version for item in migrate.MIGRATIONS]
        names = [item.name for item in migrate.MIGRATIONS]
        self.assertEqual(versions, sorted(versions))
        self.assertEqual(len(versions), len(set(versions)))
        self.assertEqual(versions[0], "0001")
        self.assertEqual(versions[-1], "0020")
        self.assertTrue(any("market opinion" in name for name in names))
        self.assertTrue(any("realtime" in name for name in names))
        self.assertTrue(any("job state" in name for name in names))
        self.assertTrue(any("point-in-time stock" in name for name in names))
        self.assertTrue(any("point-in-time fundamental" in name for name in names))
        self.assertTrue(any("point-in-time index constituent" in name for name in names))
        self.assertTrue(any("out-of-sample validation" in name for name in names))

    def test_core_checksum_is_stable_when_module_is_imported(self):
        core = migrate.MIGRATIONS[0]
        expected_payload = f"{core.version}\n{core.name}\n__main__._run_core"
        self.assertEqual(
            core.checksum,
            __import__("hashlib").sha256(expected_payload.encode("utf-8")).hexdigest(),
        )

    def test_plan_detects_applied_pending_and_checksum_mismatch(self):
        first, second = migrate.MIGRATIONS[:2]
        applied = {
            first.version: {"status": "success", "checksum": first.checksum, "error_message": None},
            second.version: {"status": "success", "checksum": "wrong", "error_message": None},
        }
        with patch.object(migrate, "_applied_rows", return_value=applied), patch.object(
            migrate, "ping_mysql", return_value={"db": "test_db"}
        ):
            plan = migrate.migration_plan(target=second.version)

        self.assertEqual(plan["database"], "test_db")
        self.assertEqual(plan["applied"], 1)
        self.assertEqual(plan["pending"], 1)
        self.assertEqual(plan["items"][1]["status"], "checksum_mismatch")

    def test_apply_runs_only_pending_migrations_under_one_lock(self):
        calls: list[str] = []

        def first_runner():
            calls.append("first")
            return {"ok": 1}

        def second_runner():
            calls.append("second")
            return {"ok": 2}

        items = (
            migrate.Migration("0001", "first", first_runner),
            migrate.Migration("0002", "second", second_runner),
        )
        existing = {"0001": {"status": "success", "checksum": items[0].checksum}}
        finished = []

        with patch.object(migrate, "MIGRATIONS", items), patch.object(
            migrate, "acquire_mysql_advisory_lock", return_value=object()
        ), patch.object(migrate, "release_mysql_advisory_lock") as release, patch.object(
            migrate, "_ensure_migration_table"
        ), patch.object(migrate, "_applied_rows", return_value=existing), patch.object(
            migrate, "_mark_started"
        ) as mark_started, patch.object(
            migrate,
            "_mark_finished",
            side_effect=lambda item, **kwargs: finished.append((item.version, kwargs["status"])),
        ), patch.object(
            migrate,
            "migration_plan",
            return_value={"ready": True, "pending": 0, "items": []},
        ):
            result = migrate.apply_migrations()

        self.assertEqual(calls, ["second"])
        mark_started.assert_called_once_with(items[1])
        self.assertEqual(finished, [("0002", "success")])
        release.assert_called_once()
        self.assertEqual(result["applied_now"][0]["version"], "0002")

    def test_check_mode_is_nonzero_when_schema_is_pending(self):
        output = io.StringIO()
        with patch.object(migrate, "migration_plan", return_value={"ready": False, "pending": 1}), redirect_stdout(output):
            exit_code = migrate.main(["--check"])
        self.assertEqual(exit_code, 1)
        self.assertIn('"pending": 1', output.getvalue())


class SchemaBoundaryTests(unittest.TestCase):
    def test_request_and_sync_paths_do_not_execute_schema_ddl(self):
        roots = [
            PROJECT_ROOT / "app" / "api",
            PROJECT_ROOT / "app" / "backtest",
            PROJECT_ROOT / "app" / "portfolio",
            PROJECT_ROOT / "app" / "stock_selection",
            PROJECT_ROOT / "app" / "strategies",
            PROJECT_ROOT / "scripts",
        ]
        offenders: list[str] = []
        ensure_pattern = re.compile(r"ensure_[A-Za-z0-9_]*schema\s*\(")
        for root in roots:
            for path in root.rglob("*.py"):
                text = path.read_text(encoding="utf-8")
                if "CREATE TABLE" in text or "ALTER TABLE" in text or ensure_pattern.search(text):
                    offenders.append(str(path.relative_to(PROJECT_ROOT)))
        self.assertEqual(offenders, [])

    def test_ingestion_ddl_is_limited_to_partition_lifecycle(self):
        offenders: list[str] = []
        for path in (PROJECT_ROOT / "app" / "data_ingestion").glob("*.py"):
            text = path.read_text(encoding="utf-8")
            if path.name == "realtime_lifecycle.py":
                continue
            if "CREATE TABLE" in text or "ALTER TABLE" in text:
                offenders.append(path.name)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
