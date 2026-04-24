from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


BASE_DIR = Path(__file__).resolve().parents[2]
ENV_CANDIDATES = [
    BASE_DIR / ".env",
    BASE_DIR / "config" / ".env",
]

if load_dotenv:
    for env_path in ENV_CANDIDATES:
        if env_path.exists():
            load_dotenv(env_path, override=False)


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


@dataclass(frozen=True)
class MySQLSettings:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"

    @classmethod
    def from_env(cls) -> "MySQLSettings":
        return cls(
            host=_get_required_env("DB_HOST"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=_get_required_env("DB_USER"),
            password=_get_required_env("DB_PASSWORD"),
            database=_get_required_env("DB_NAME"),
            charset=os.getenv("DB_CHARSET", "utf8mb4"),
        )

    def to_pymysql_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
        }


@dataclass(frozen=True)
class SqliteSettings:
    stock_history_path: str = os.getenv(
        "SQLITE_STOCK_HISTORY_PATH",
        str(BASE_DIR / "src" / "data_cache" / "stock_history.db"),
    )
    sentiment_cache_path: str = os.getenv(
        "SQLITE_SENTIMENT_CACHE_PATH",
        str(BASE_DIR / "src" / "data_cache" / "sentiment_cache.db"),
    )


mysql_settings = MySQLSettings.from_env()
sqlite_settings = SqliteSettings()
