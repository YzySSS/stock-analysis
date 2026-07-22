from __future__ import annotations

import unittest
from unittest.mock import patch

from app.shared import db
from app.shared.settings import MySQLSettings


class FakeConnection:
    def __init__(self, *, fail_commit: bool = False, fail_ping: bool = False) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0
        self.pings = 0
        self.fail_commit = fail_commit
        self.fail_ping = fail_ping

    def commit(self) -> None:
        self.commits += 1
        if self.fail_commit:
            raise ConnectionError("commit failed")

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closes += 1

    def ping(self, reconnect: bool = False) -> None:
        self.pings += 1
        if self.fail_ping:
            raise ConnectionError("ping failed")


class CursorRecordingConnection(FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.cursor_classes = []

    def cursor(self, cursor_class=None):
        self.cursor_classes.append(cursor_class)
        return object()


def settings(*, pool_enabled: bool = True, pool_pre_ping: bool = False) -> MySQLSettings:
    return MySQLSettings(
        host="db.local",
        port=3307,
        user="stock",
        password="secret",
        database="stock_analysis",
        connect_timeout_seconds=3,
        read_timeout_seconds=11,
        write_timeout_seconds=12,
        pool_enabled=pool_enabled,
        pool_size=1,
        pool_max_overflow=0,
        pool_timeout_seconds=0.2,
        pool_recycle_seconds=60,
        pool_pre_ping=pool_pre_ping,
    )


class SharedDatabaseTests(unittest.TestCase):
    def tearDown(self) -> None:
        db.dispose_mysql_pools()

    def test_pool_reuses_connection_and_commits_each_successful_context(self):
        connection = FakeConnection()
        with patch.object(db, "mysql_settings", settings()), patch.object(
            db.pymysql,
            "connect",
            return_value=connection,
        ) as connect:
            with db.mysql_conn() as first:
                self.assertIsNotNone(first)
            with db.mysql_conn() as second:
                self.assertIsNotNone(second)

        connect.assert_called_once()
        self.assertEqual(connection.commits, 2)
        self.assertEqual(connection.rollbacks, 0)

    def test_body_error_rolls_back_and_does_not_commit(self):
        connection = FakeConnection()
        with patch.object(db, "mysql_settings", settings()), patch.object(
            db.pymysql,
            "connect",
            return_value=connection,
        ):
            with self.assertRaisesRegex(ValueError, "boom"):
                with db.mysql_conn():
                    raise ValueError("boom")

        self.assertEqual(connection.commits, 0)
        self.assertGreaterEqual(connection.rollbacks, 1)

    def test_commit_error_rolls_back_and_propagates(self):
        connection = FakeConnection(fail_commit=True)
        with patch.object(db, "mysql_settings", settings()), patch.object(
            db.pymysql,
            "connect",
            return_value=connection,
        ):
            with self.assertRaisesRegex(ConnectionError, "commit failed"):
                with db.mysql_conn():
                    pass

        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 1)

    def test_pre_ping_discards_stale_connection(self):
        stale = FakeConnection(fail_ping=True)
        healthy = FakeConnection()
        with patch.object(db, "mysql_settings", settings(pool_pre_ping=True)), patch.object(
            db.pymysql,
            "connect",
            side_effect=[stale, healthy],
        ) as connect:
            with db.mysql_conn():
                pass

        self.assertEqual(connect.call_count, 2)
        self.assertEqual(stale.pings, 1)
        self.assertGreaterEqual(stale.closes, 1)
        self.assertEqual(healthy.pings, 1)
        self.assertEqual(healthy.commits, 1)

    def test_dict_and_tuple_cursor_modes_share_one_bounded_pool(self):
        connection = FakeConnection()
        with patch.object(db, "mysql_settings", settings()), patch.object(
            db.pymysql,
            "connect",
            return_value=connection,
        ) as connect:
            with db.mysql_conn(dict_cursor=True):
                pass
            with db.mysql_conn(dict_cursor=False):
                pass

        self.assertEqual(connect.call_count, 1)
        for call in connect.call_args_list:
            self.assertNotIn("cursorclass", call.kwargs)
            self.assertEqual(call.kwargs["connect_timeout"], 3)
            self.assertEqual(call.kwargs["read_timeout"], 11)
            self.assertEqual(call.kwargs["write_timeout"], 12)
            self.assertFalse(call.kwargs["autocommit"])

    def test_pool_can_be_disabled_without_changing_context_contract(self):
        connection = FakeConnection()
        with patch.object(db, "mysql_settings", settings(pool_enabled=False)), patch.object(
            db.pymysql,
            "connect",
            return_value=connection,
        ):
            with db.mysql_conn(dict_cursor=False):
                pass

        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.closes, 1)

    def test_maintenance_context_uses_dedicated_long_timeouts(self):
        connection = FakeConnection()
        with patch.object(db, "mysql_settings", settings()), patch.object(
            db.pymysql,
            "connect",
            return_value=connection,
        ) as connect:
            with db.mysql_maintenance_conn(dict_cursor=False, timeout_seconds=90):
                pass

        connect.assert_called_once()
        self.assertEqual(connect.call_args.kwargs["read_timeout"], 90)
        self.assertEqual(connect.call_args.kwargs["write_timeout"], 90)
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.closes, 1)

    def test_cursor_view_selects_dict_or_tuple_cursor_without_new_connections(self):
        connection = CursorRecordingConnection()

        db._CursorModeConnection(connection, dict_cursor=True).cursor()
        db._CursorModeConnection(connection, dict_cursor=False).cursor()

        self.assertIs(connection.cursor_classes[0], db.DictCursor)
        self.assertIs(connection.cursor_classes[1], db.Cursor)

    def test_pool_diagnostics_do_not_expose_credentials(self):
        connection = FakeConnection()
        with patch.object(db, "mysql_settings", settings()), patch.object(
            db.pymysql,
            "connect",
            return_value=connection,
        ):
            with db.mysql_conn():
                diagnostics = db.mysql_pool_diagnostics()

        self.assertTrue(diagnostics["enabled"])
        self.assertIn("shared", diagnostics["pools"])
        self.assertNotIn("password", str(diagnostics).lower())
        self.assertNotIn("secret", str(diagnostics))

    def test_read_context_rolls_back_instead_of_committing(self):
        connection = FakeConnection()
        with patch.object(db, "_checkout_mysql_connection", return_value=connection):
            with db.mysql_read_conn() as yielded:
                self.assertIs(yielded, connection)

        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.closes, 1)


if __name__ == "__main__":
    unittest.main()
