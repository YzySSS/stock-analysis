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


def _get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"环境变量 {name} 必须是 true/false")


def _get_int_env(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise RuntimeError(f"环境变量 {name} 必须是整数") from exc
    if value < minimum:
        raise RuntimeError(f"环境变量 {name} 不能小于 {minimum}")
    return value


def _get_float_env(name: str, default: float, *, minimum: float = 0) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError as exc:
        raise RuntimeError(f"环境变量 {name} 必须是数字") from exc
    if value < minimum:
        raise RuntimeError(f"环境变量 {name} 不能小于 {minimum}")
    return value


@dataclass(frozen=True)
class MySQLSettings:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"
    connect_timeout_seconds: int = 3
    read_timeout_seconds: int = 10
    write_timeout_seconds: int = 10
    pool_enabled: bool = True
    pool_size: int = 4
    pool_max_overflow: int = 0
    pool_timeout_seconds: float = 3.0
    pool_recycle_seconds: int = 1800
    pool_pre_ping: bool = True

    @classmethod
    def from_env(cls) -> "MySQLSettings":
        return cls(
            host=_get_required_env("DB_HOST"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=_get_required_env("DB_USER"),
            password=_get_required_env("DB_PASSWORD"),
            database=_get_required_env("DB_NAME"),
            charset=os.getenv("DB_CHARSET", "utf8mb4"),
            connect_timeout_seconds=_get_int_env("DB_CONNECT_TIMEOUT_SECONDS", 3, minimum=1),
            read_timeout_seconds=_get_int_env("DB_READ_TIMEOUT_SECONDS", 10, minimum=1),
            write_timeout_seconds=_get_int_env("DB_WRITE_TIMEOUT_SECONDS", 10, minimum=1),
            pool_enabled=_get_bool_env("DB_POOL_ENABLED", True),
            pool_size=_get_int_env("DB_POOL_SIZE", 4, minimum=1),
            pool_max_overflow=_get_int_env("DB_POOL_MAX_OVERFLOW", 0, minimum=0),
            pool_timeout_seconds=_get_float_env("DB_POOL_TIMEOUT_SECONDS", 3.0, minimum=0.1),
            pool_recycle_seconds=_get_int_env("DB_POOL_RECYCLE_SECONDS", 1800, minimum=1),
            pool_pre_ping=_get_bool_env("DB_POOL_PRE_PING", True),
        )

    def to_pymysql_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
            "connect_timeout": self.connect_timeout_seconds,
            "read_timeout": self.read_timeout_seconds,
            "write_timeout": self.write_timeout_seconds,
            "autocommit": False,
        }


@dataclass(frozen=True)
class CacheSettings:
    enabled: bool = True
    backend: str = "memory"
    default_ttl_seconds: int = 60
    key_prefix: str = "stock-analysis"
    redis_url: str | None = None
    redis_socket_connect_timeout_seconds: float = 0.5
    redis_socket_timeout_seconds: float = 0.5
    redis_fallback_to_memory: bool = True

    @classmethod
    def from_env(cls) -> "CacheSettings":
        backend = str(os.getenv("CACHE_BACKEND", "memory")).strip().lower()
        if backend not in {"memory", "redis"}:
            raise RuntimeError("环境变量 CACHE_BACKEND 必须是 memory 或 redis")
        redis_enabled = _get_bool_env("REDIS_CACHE_ENABLED", backend == "redis")
        if redis_enabled:
            backend = "redis"
        elif backend == "redis":
            backend = "memory"
        redis_url = str(os.getenv("REDIS_URL") or os.getenv("CACHE_REDIS_URL") or "").strip() or None
        key_prefix = str(os.getenv("CACHE_KEY_PREFIX", "stock-analysis")).strip() or "stock-analysis"
        return cls(
            enabled=_get_bool_env("CACHE_ENABLED", True),
            backend=backend,
            default_ttl_seconds=_get_int_env("CACHE_DEFAULT_TTL_SECONDS", 60, minimum=1),
            key_prefix=key_prefix,
            redis_url=redis_url,
            redis_socket_connect_timeout_seconds=_get_float_env(
                "CACHE_REDIS_CONNECT_TIMEOUT_SECONDS", 0.5, minimum=0.1
            ),
            redis_socket_timeout_seconds=_get_float_env(
                "CACHE_REDIS_TIMEOUT_SECONDS", 0.5, minimum=0.1
            ),
            redis_fallback_to_memory=_get_bool_env("CACHE_REDIS_FALLBACK_TO_MEMORY", True),
        )


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
cache_settings = CacheSettings.from_env()
