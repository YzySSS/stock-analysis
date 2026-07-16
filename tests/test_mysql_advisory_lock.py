from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock


class MysqlAdvisoryLockTests(unittest.TestCase):
    def _connection_fixture(self, rows):
        conn_context = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        conn_context.__enter__.return_value = conn
        conn.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.side_effect = rows
        return conn_context, conn, cursor

    def test_acquired_lock_keeps_owning_connection_open_until_release(self):
        conn_context, conn, cursor = self._connection_fixture(
            [{"locked": 1, "connection_id": 42}, {"released": 1}]
        )

        with patch("app.shared.mysql_lock.mysql_conn", return_value=conn_context):
            handle = acquire_mysql_advisory_lock("test_task_lock")

        self.assertIsNotNone(handle)
        self.assertEqual(handle.connection_id, 42)
        conn_context.__exit__.assert_not_called()

        release_error = release_mysql_advisory_lock(handle)

        self.assertIsNone(release_error)
        self.assertTrue(handle.released)
        conn_context.__exit__.assert_called_once_with(None, None, None)
        self.assertEqual(cursor.execute.call_count, 2)
        self.assertIs(handle.conn, conn)

    def test_failed_acquire_closes_temporary_connection(self):
        conn_context, _conn, _cursor = self._connection_fixture(
            [{"locked": 0, "connection_id": 43}]
        )

        with patch("app.shared.mysql_lock.mysql_conn", return_value=conn_context):
            handle = acquire_mysql_advisory_lock("test_task_lock")

        self.assertIsNone(handle)
        conn_context.__exit__.assert_called_once_with(None, None, None)

    def test_release_is_idempotent(self):
        conn_context, _conn, _cursor = self._connection_fixture(
            [{"locked": 1, "connection_id": 44}, {"released": 1}]
        )

        with patch("app.shared.mysql_lock.mysql_conn", return_value=conn_context):
            handle = acquire_mysql_advisory_lock("test_task_lock")

        self.assertIsNone(release_mysql_advisory_lock(handle))
        self.assertIsNone(release_mysql_advisory_lock(handle))
        conn_context.__exit__.assert_called_once_with(None, None, None)

    def test_invalid_lock_name_is_rejected_before_database_access(self):
        with patch("app.shared.mysql_lock.mysql_conn") as mysql_conn_mock:
            with self.assertRaises(ValueError):
                acquire_mysql_advisory_lock("")
        mysql_conn_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
