from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


PASSWORD_SCHEME = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 600_000
SESSION_VERSION = 1
UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")
PUBLIC_EXACT_PATHS = frozenset(
    {
        "/api/health",
        "/favicon.ico",
        "/login",
        "/login/",
    }
)
PUBLIC_PATH_PREFIXES = ("/static/",)


def _encode_base64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode_base64(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.b64decode(
        f"{value}{padding}".encode("ascii"),
        altchars=b"-_",
        validate=True,
    )


def hash_password(
    password: str,
    *,
    iterations: int = PASSWORD_ITERATIONS,
    salt: bytes | None = None,
) -> str:
    if not password:
        raise ValueError("password must not be empty")
    if iterations < 100_000:
        raise ValueError("password hash iterations are too low")
    selected_salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        selected_salt,
        iterations,
    )
    return (
        f"{PASSWORD_SCHEME}${iterations}"
        f"${_encode_base64(selected_salt)}${_encode_base64(digest)}"
    )


def verify_password(password: str, encoded_hash: str) -> bool:
    try:
        scheme, raw_iterations, raw_salt, raw_digest = encoded_hash.split("$", 3)
        if scheme != PASSWORD_SCHEME:
            return False
        iterations = int(raw_iterations)
        if iterations < 100_000 or iterations > 5_000_000:
            return False
        salt = _decode_base64(raw_salt)
        expected_digest = _decode_base64(raw_digest)
        if len(salt) < 16 or len(expected_digest) != 32:
            return False
        actual_digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt,
            iterations,
        )
        return hmac.compare_digest(actual_digest, expected_digest)
    except (binascii.Error, TypeError, ValueError):
        return False


def safe_next_path(value: str | None, *, default: str = "/") -> str:
    candidate = str(value or "").strip()
    if not candidate or len(candidate) > 2048:
        return default
    if any(ord(character) < 32 for character in candidate):
        return default
    if "\\" in candidate or candidate.lower().startswith(("/%2f", "/%5c")):
        return default
    parsed = urlsplit(candidate)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/"):
        return default
    if candidate.startswith("//"):
        return default
    return candidate


def _required_environment_value(environment: Mapping[str, str], name: str) -> str:
    value = str(environment.get(name, "")).strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


@dataclass(frozen=True)
class SiteAuthSettings:
    username: str
    password_hash: str
    session_secret: str
    session_ttl_seconds: int = 7 * 24 * 60 * 60
    cookie_name: str = "stock_analysis_session"
    csrf_cookie_name: str = "stock_analysis_csrf"
    cookie_secure: bool = True
    cookie_samesite: str = "lax"
    max_failed_attempts: int = 5
    failed_attempt_window_seconds: int = 5 * 60

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> "SiteAuthSettings":
        values = os.environ if environment is None else environment
        username = _required_environment_value(values, "SITE_AUTH_USERNAME")
        if not USERNAME_PATTERN.fullmatch(username):
            raise RuntimeError(
                "环境变量 SITE_AUTH_USERNAME 只能包含字母、数字、点、下划线、@ 或连字符"
            )

        password_hash = _required_environment_value(values, "SITE_AUTH_PASSWORD_HASH")
        try:
            scheme, raw_iterations, raw_salt, raw_digest = password_hash.split("$", 3)
            password_hash_valid = (
                scheme == PASSWORD_SCHEME
                and 100_000 <= int(raw_iterations) <= 5_000_000
                and len(_decode_base64(raw_salt)) >= 16
                and len(_decode_base64(raw_digest)) == 32
            )
        except (binascii.Error, TypeError, ValueError):
            password_hash_valid = False
        if not password_hash_valid:
            raise RuntimeError("环境变量 SITE_AUTH_PASSWORD_HASH 格式无效")

        session_secret = _required_environment_value(values, "SITE_AUTH_SESSION_SECRET")
        if len(session_secret) < 32:
            raise RuntimeError("环境变量 SITE_AUTH_SESSION_SECRET 至少需要 32 个字符")

        try:
            session_ttl_seconds = int(values.get("SITE_AUTH_SESSION_TTL_SECONDS", 604800))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("环境变量 SITE_AUTH_SESSION_TTL_SECONDS 必须是整数") from exc
        if not 300 <= session_ttl_seconds <= 31 * 24 * 60 * 60:
            raise RuntimeError("SITE_AUTH_SESSION_TTL_SECONDS 必须在 300 秒到 31 天之间")

        raw_secure = str(values.get("SITE_AUTH_COOKIE_SECURE", "true")).strip().lower()
        if raw_secure not in {"true", "false", "1", "0", "yes", "no", "on", "off"}:
            raise RuntimeError("环境变量 SITE_AUTH_COOKIE_SECURE 必须是 true/false")
        cookie_secure = raw_secure in {"true", "1", "yes", "on"}

        return cls(
            username=username,
            password_hash=password_hash,
            session_secret=session_secret,
            session_ttl_seconds=session_ttl_seconds,
            cookie_secure=cookie_secure,
        )


@dataclass(frozen=True)
class AuthSession:
    username: str
    issued_at: int
    expires_at: int
    csrf_token: str


class LoginAttemptLimiter:
    def __init__(
        self,
        *,
        max_failed_attempts: int,
        window_seconds: int,
        max_clients: int = 1024,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.max_failed_attempts = max_failed_attempts
        self.window_seconds = window_seconds
        self.max_clients = max_clients
        self.clock = clock
        self._failures: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, client_key: str) -> tuple[bool, int]:
        now = self.clock()
        with self._lock:
            failures = self._active_failures(client_key, now)
            if len(failures) < self.max_failed_attempts:
                return True, 0
            retry_after = max(1, int(self.window_seconds - (now - failures[0])))
            return False, retry_after

    def register_failure(self, client_key: str) -> None:
        now = self.clock()
        with self._lock:
            self._active_failures(client_key, now).append(now)
            self._prune_clients(now)

    def clear(self, client_key: str) -> None:
        with self._lock:
            self._failures.pop(client_key, None)

    def _active_failures(self, client_key: str, now: float) -> deque[float]:
        failures = self._failures[client_key]
        cutoff = now - self.window_seconds
        while failures and failures[0] <= cutoff:
            failures.popleft()
        return failures

    def _prune_clients(self, now: float) -> None:
        if len(self._failures) <= self.max_clients:
            return
        cutoff = now - self.window_seconds
        stale_keys = [
            key
            for key, failures in self._failures.items()
            if not failures or failures[-1] <= cutoff
        ]
        for key in stale_keys:
            self._failures.pop(key, None)
        while len(self._failures) > self.max_clients:
            self._failures.pop(next(iter(self._failures)))


class SiteAuthenticator:
    def __init__(
        self,
        settings: SiteAuthSettings,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.clock = clock
        self._session_secret = settings.session_secret.encode("utf-8")
        self.login_limiter = LoginAttemptLimiter(
            max_failed_attempts=settings.max_failed_attempts,
            window_seconds=settings.failed_attempt_window_seconds,
            clock=clock,
        )

    def authenticate(self, username: str, password: str) -> bool:
        password_matches = verify_password(password, self.settings.password_hash)
        username_matches = hmac.compare_digest(
            str(username).encode("utf-8"),
            self.settings.username.encode("utf-8"),
        )
        return password_matches and username_matches

    def issue_session(self) -> tuple[str, AuthSession]:
        issued_at = int(self.clock())
        session = AuthSession(
            username=self.settings.username,
            issued_at=issued_at,
            expires_at=issued_at + self.settings.session_ttl_seconds,
            csrf_token=secrets.token_urlsafe(24),
        )
        payload = {
            "v": SESSION_VERSION,
            "sub": session.username,
            "iat": session.issued_at,
            "exp": session.expires_at,
            "csrf": session.csrf_token,
        }
        encoded_payload = _encode_base64(
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )
        signature = hmac.new(
            self._session_secret,
            encoded_payload.encode("ascii"),
            hashlib.sha256,
        ).digest()
        return f"{encoded_payload}.{_encode_base64(signature)}", session

    def read_session(self, token: str | None) -> AuthSession | None:
        if not token or len(token) > 4096:
            return None
        try:
            encoded_payload, encoded_signature = token.split(".", 1)
            expected_signature = hmac.new(
                self._session_secret,
                encoded_payload.encode("ascii"),
                hashlib.sha256,
            ).digest()
            supplied_signature = _decode_base64(encoded_signature)
            if not hmac.compare_digest(expected_signature, supplied_signature):
                return None

            payload: dict[str, Any] = json.loads(_decode_base64(encoded_payload))
            if payload.get("v") != SESSION_VERSION:
                return None
            issued_at = int(payload["iat"])
            expires_at = int(payload["exp"])
            now = int(self.clock())
            if issued_at > now + 60 or expires_at <= now:
                return None
            if expires_at - issued_at > self.settings.session_ttl_seconds:
                return None
            username = str(payload["sub"])
            csrf_token = str(payload["csrf"])
            if not hmac.compare_digest(
                username.encode("utf-8"),
                self.settings.username.encode("utf-8"),
            ):
                return None
            if len(csrf_token) < 24:
                return None
            return AuthSession(
                username=username,
                issued_at=issued_at,
                expires_at=expires_at,
                csrf_token=csrf_token,
            )
        except (
            binascii.Error,
            KeyError,
            TypeError,
            ValueError,
            UnicodeError,
            json.JSONDecodeError,
        ):
            return None

    def set_session_cookies(
        self,
        response: Response,
        *,
        token: str,
        session: AuthSession,
    ) -> None:
        cookie_options = {
            "path": "/",
            "max_age": self.settings.session_ttl_seconds,
            "secure": self.settings.cookie_secure,
            "samesite": self.settings.cookie_samesite,
        }
        response.set_cookie(
            self.settings.cookie_name,
            token,
            httponly=True,
            **cookie_options,
        )
        response.set_cookie(
            self.settings.csrf_cookie_name,
            session.csrf_token,
            httponly=False,
            **cookie_options,
        )

    def clear_session_cookies(self, response: Response) -> None:
        response.delete_cookie(
            self.settings.cookie_name,
            path="/",
            secure=self.settings.cookie_secure,
            httponly=True,
            samesite=self.settings.cookie_samesite,
        )
        response.delete_cookie(
            self.settings.csrf_cookie_name,
            path="/",
            secure=self.settings.cookie_secure,
            httponly=False,
            samesite=self.settings.cookie_samesite,
        )


def is_public_path(path: str) -> bool:
    return path in PUBLIC_EXACT_PATHS or path.startswith(PUBLIC_PATH_PREFIXES)


def request_client_key(request: Request) -> str:
    real_ip = str(request.headers.get("X-Real-IP", "")).strip()
    if real_ip:
        return real_ip[:128]
    if request.client:
        return str(request.client.host)[:128]
    return "unknown"


def _add_security_headers(response: Response, *, no_store: bool) -> Response:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    if no_store:
        response.headers["Cache-Control"] = "private, no-store"
    return response


class SiteAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: Any, *, authenticator: SiteAuthenticator) -> None:
        super().__init__(app)
        self.authenticator = authenticator

    async def dispatch(self, request: Request, call_next: Callable[..., Any]) -> Response:
        session = self.authenticator.read_session(
            request.cookies.get(self.authenticator.settings.cookie_name)
        )
        request.state.auth_session = session
        public_path = is_public_path(request.url.path)

        if not public_path and session is None:
            if request.url.path.startswith("/api/"):
                return _add_security_headers(
                    JSONResponse(
                        {
                            "detail": "authentication_required",
                            "login_url": "/login",
                        },
                        status_code=401,
                    ),
                    no_store=True,
                )
            next_path = request.url.path
            if request.url.query:
                next_path = f"{next_path}?{request.url.query}"
            return _add_security_headers(
                RedirectResponse(
                    url=f"/login?next={quote(next_path, safe='')}",
                    status_code=303,
                ),
                no_store=True,
            )

        if (
            not public_path
            and session is not None
            and request.method.upper() in UNSAFE_METHODS
        ):
            supplied_header = str(request.headers.get("X-CSRF-Token", ""))
            supplied_cookie = str(
                request.cookies.get(self.authenticator.settings.csrf_cookie_name, "")
            )
            if not (
                supplied_header
                and supplied_cookie
                and hmac.compare_digest(supplied_header, session.csrf_token)
                and hmac.compare_digest(supplied_cookie, session.csrf_token)
            ):
                return _add_security_headers(
                    JSONResponse({"detail": "csrf_validation_failed"}, status_code=403),
                    no_store=True,
                )

        response = await call_next(request)
        no_store = not public_path or request.url.path in {"/login", "/login/"}
        return _add_security_headers(response, no_store=no_store)
