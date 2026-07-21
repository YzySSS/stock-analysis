from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from app.shared.settings import CacheSettings, MySQLSettings


class SharedSettingsTests(unittest.TestCase):
    def test_mysql_pool_and_timeout_settings_are_loaded_from_environment(self):
        environment = {
            "DB_HOST": "db.local",
            "DB_PORT": "3307",
            "DB_USER": "stock",
            "DB_PASSWORD": "secret",
            "DB_NAME": "stock_analysis",
            "DB_CONNECT_TIMEOUT_SECONDS": "4",
            "DB_READ_TIMEOUT_SECONDS": "21",
            "DB_WRITE_TIMEOUT_SECONDS": "22",
            "DB_POOL_ENABLED": "false",
            "DB_POOL_SIZE": "7",
            "DB_POOL_MAX_OVERFLOW": "3",
            "DB_POOL_TIMEOUT_SECONDS": "1.5",
            "DB_POOL_RECYCLE_SECONDS": "120",
            "DB_POOL_PRE_PING": "false",
        }
        with patch.dict(os.environ, environment, clear=True):
            settings = MySQLSettings.from_env()

        self.assertFalse(settings.pool_enabled)
        self.assertEqual(settings.pool_size, 7)
        self.assertEqual(settings.pool_max_overflow, 3)
        self.assertEqual(settings.pool_timeout_seconds, 1.5)
        self.assertEqual(settings.pool_recycle_seconds, 120)
        self.assertFalse(settings.pool_pre_ping)
        self.assertEqual(settings.to_pymysql_dict()["connect_timeout"], 4)
        self.assertEqual(settings.to_pymysql_dict()["read_timeout"], 21)
        self.assertEqual(settings.to_pymysql_dict()["write_timeout"], 22)

    def test_cache_defaults_to_enabled_memory_backend(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = CacheSettings.from_env()

        self.assertTrue(settings.enabled)
        self.assertEqual(settings.backend, "memory")
        self.assertIsNone(settings.redis_url)
        self.assertTrue(settings.redis_fallback_to_memory)

    def test_redis_plan_environment_names_enable_redis_backend(self):
        with patch.dict(
            os.environ,
            {
                "CACHE_BACKEND": "memory",
                "REDIS_CACHE_ENABLED": "true",
                "REDIS_URL": "redis://127.0.0.1:6379/0",
            },
            clear=True,
        ):
            settings = CacheSettings.from_env()

        self.assertTrue(settings.enabled)
        self.assertEqual(settings.backend, "redis")
        self.assertEqual(settings.redis_url, "redis://127.0.0.1:6379/0")

    def test_invalid_boolean_is_rejected(self):
        environment = {
            "DB_HOST": "db.local",
            "DB_USER": "stock",
            "DB_PASSWORD": "secret",
            "DB_NAME": "stock_analysis",
            "DB_POOL_ENABLED": "sometimes",
        }
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "DB_POOL_ENABLED"):
                MySQLSettings.from_env()


if __name__ == "__main__":
    unittest.main()
