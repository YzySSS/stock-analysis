from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from http.cookies import SimpleCookie
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.api.auth import (
    LoginAttemptLimiter,
    SiteAuthenticator,
    SiteAuthMiddleware,
    SiteAuthSettings,
    hash_password,
    safe_next_path,
    verify_password,
)
from app.api.routes.auth import router as auth_router
from scripts.configure_site_auth import _replace_env_values


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build_settings(*, password: str = "correct horse battery staple") -> SiteAuthSettings:
    return SiteAuthSettings(
        username="dax",
        password_hash=hash_password(
            password,
            iterations=100_000,
            salt=b"0123456789abcdef",
        ),
        session_secret="test-session-secret-that-is-long-enough",
        session_ttl_seconds=600,
        cookie_secure=False,
    )


class PasswordHashTests(unittest.TestCase):
    def test_password_hash_round_trip_and_wrong_password(self):
        encoded = hash_password(
            "private-site-password",
            iterations=100_000,
            salt=b"0123456789abcdef",
        )

        self.assertTrue(verify_password("private-site-password", encoded))
        self.assertFalse(verify_password("wrong-password", encoded))
        self.assertFalse(verify_password("private-site-password", "broken"))

    def test_authenticator_requires_both_username_and_password(self):
        authenticator = SiteAuthenticator(build_settings())

        self.assertTrue(authenticator.authenticate("dax", "correct horse battery staple"))
        self.assertFalse(authenticator.authenticate("other", "correct horse battery staple"))
        self.assertFalse(authenticator.authenticate("dax", "wrong-password"))


class SessionTests(unittest.TestCase):
    def test_signed_session_round_trip_tamper_and_expiry(self):
        now = [1_000.0]
        authenticator = SiteAuthenticator(build_settings(), clock=lambda: now[0])

        token, issued = authenticator.issue_session()
        restored = authenticator.read_session(token)

        self.assertIsNotNone(restored)
        self.assertEqual(restored.username, "dax")
        self.assertEqual(restored.csrf_token, issued.csrf_token)
        payload, signature = token.rsplit(".", 1)
        replacement = "A" if signature[0] != "A" else "B"
        tampered_token = f"{payload}.{replacement}{signature[1:]}"
        self.assertIsNone(authenticator.read_session(tampered_token))

        now[0] = issued.expires_at
        self.assertIsNone(authenticator.read_session(token))

    def test_environment_settings_fail_closed_on_invalid_values(self):
        valid_hash = build_settings().password_hash
        base_environment = {
            "SITE_AUTH_USERNAME": "dax",
            "SITE_AUTH_PASSWORD_HASH": valid_hash,
            "SITE_AUTH_SESSION_SECRET": "x" * 32,
        }

        settings = SiteAuthSettings.from_env(base_environment)
        self.assertTrue(settings.cookie_secure)

        with self.assertRaises(RuntimeError):
            SiteAuthSettings.from_env({**base_environment, "SITE_AUTH_PASSWORD_HASH": "broken"})
        with self.assertRaises(RuntimeError):
            SiteAuthSettings.from_env({**base_environment, "SITE_AUTH_SESSION_SECRET": "short"})
        with self.assertRaises(RuntimeError):
            SiteAuthSettings.from_env({**base_environment, "SITE_AUTH_USERNAME": "bad name"})


class AuthBoundaryTests(unittest.TestCase):
    def test_auth_configuration_script_runs_directly(self):
        result = subprocess.run(
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "configure_site_auth.py"),
                "--help",
            ],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--username", result.stdout)

    def test_auth_configuration_removes_duplicate_environment_values(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            env_path = Path(temporary_directory) / ".env"
            env_path.write_text(
                "SITE_AUTH_USERNAME=first\n"
                "DB_NAME=stock\n"
                "SITE_AUTH_USERNAME=stale-last-value\n",
                encoding="utf-8",
            )

            _replace_env_values(
                env_path,
                {
                    "SITE_AUTH_USERNAME": "dax",
                    "SITE_AUTH_SESSION_SECRET": "x" * 48,
                },
            )

            lines = env_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(lines.count("SITE_AUTH_USERNAME=dax"), 1)
            self.assertFalse(any("stale-last-value" in line for line in lines))
            self.assertEqual(lines.count(f"SITE_AUTH_SESSION_SECRET={'x' * 48}"), 1)
            self.assertEqual(env_path.stat().st_mode & 0o777, 0o600)

    def test_next_path_only_allows_same_site_absolute_paths(self):
        self.assertEqual(safe_next_path("/selection?run_id=abc"), "/selection?run_id=abc")
        self.assertEqual(safe_next_path("https://evil.example/"), "/")
        self.assertEqual(safe_next_path("//evil.example/"), "/")
        self.assertEqual(safe_next_path(r"/\evil.example/"), "/")
        self.assertEqual(safe_next_path("/%2f%2fevil.example/"), "/")
        self.assertEqual(safe_next_path("selection"), "/")

    def test_failed_login_limiter_unlocks_after_window(self):
        now = [1_000.0]
        limiter = LoginAttemptLimiter(
            max_failed_attempts=2,
            window_seconds=60,
            clock=lambda: now[0],
        )

        limiter.register_failure("client")
        limiter.register_failure("client")
        allowed, retry_after = limiter.check("client")
        self.assertFalse(allowed)
        self.assertGreaterEqual(retry_after, 1)

        now[0] = 1_061.0
        self.assertEqual(limiter.check("client"), (True, 0))

    def test_login_page_and_frontend_auth_hooks_are_present(self):
        login_page = (
            PROJECT_ROOT / "app" / "api" / "web" / "pages" / "login.html"
        ).read_text(encoding="utf-8")
        common_js = (
            PROJECT_ROOT / "app" / "api" / "web" / "js" / "common.js"
        ).read_text(encoding="utf-8")
        layout_css = (
            PROJECT_ROOT / "app" / "api" / "web" / "css" / "layout.css"
        ).read_text(encoding="utf-8")

        self.assertIn('autocomplete="username"', login_page)
        self.assertIn('autocomplete="current-password"', login_page)
        self.assertIn("X-CSRF-Token", common_js)
        self.assertIn("'/api/auth/session'", common_js)
        self.assertIn("data-session-username", common_js)
        self.assertIn('aria-label="退出登录"', common_js)
        self.assertIn("session-logout", common_js)
        self.assertIn("window.location.replace(currentLoginUrl())", common_js)
        self.assertIn(".session-dock {", layout_css)
        self.assertIn("left: 18px;", layout_css)

    def test_all_product_pages_pin_the_authenticated_common_script(self):
        pages_dir = PROJECT_ROOT / "app" / "api" / "web" / "pages"
        product_pages = {
            "home.html",
            "selection.html",
            "tracking.html",
            "portfolio.html",
            "backtest.html",
            "strategies.html",
            "trade-strategies.html",
            "stock-detail.html",
            "system.html",
        }
        for filename in product_pages:
            source = (pages_dir / filename).read_text(encoding="utf-8")
            self.assertIn("/static/js/common.js?v=20260819marketalert1", source)

    def test_product_pages_use_the_stock_research_brand_icon(self):
        web_dir = PROJECT_ROOT / "app" / "api" / "web"
        pages_dir = web_dir / "pages"
        branded_pages = {
            "home.html",
            "selection.html",
            "tracking.html",
            "portfolio.html",
            "backtest.html",
            "strategies.html",
            "trade-strategies.html",
            "etf-rotation.html",
            "stock-detail.html",
            "system.html",
            "login.html",
        }

        for filename in branded_pages:
            source = (pages_dir / filename).read_text(encoding="utf-8")
            self.assertIn("/favicon.ico?v=20260729brand1", source)
            self.assertIn("/static/css/brand.css?v=20260729brand1", source)
            self.assertIn("stock-research-apple-touch.png?v=20260729brand1", source)

        for filename in (
            "stock-research-icon.png",
            "stock-research-favicon.png",
            "stock-research-apple-touch.png",
        ):
            asset = web_dir / "assets" / filename
            self.assertTrue(asset.is_file())
            self.assertGreater(asset.stat().st_size, 1_000)


class AuthHttpIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.authenticator = SiteAuthenticator(build_settings())
        app = FastAPI()
        app.state.site_authenticator = self.authenticator
        app.add_middleware(SiteAuthMiddleware, authenticator=self.authenticator)
        app.include_router(auth_router)

        @app.get("/protected")
        def protected_page() -> dict:
            return {"ok": True}

        @app.get("/api/protected")
        def protected_api() -> dict:
            return {"ok": True}

        @app.post("/api/protected")
        def protected_write() -> dict:
            return {"ok": True}

        web_dir = PROJECT_ROOT / "app" / "api" / "web"
        app.mount("/static/css", StaticFiles(directory=web_dir / "css"), name="test-css")
        app.mount("/static/js", StaticFiles(directory=web_dir / "js"), name="test-js")
        app.mount(
            "/static/assets",
            StaticFiles(directory=web_dir / "assets"),
            name="test-assets",
        )
        self.app = app

    async def _request(
        self,
        method: str,
        target: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: dict | None = None,
    ) -> tuple[int, list[tuple[str, str]], bytes]:
        parsed = urlsplit(target)
        body = json.dumps(json_body).encode("utf-8") if json_body is not None else b""
        raw_headers = [
            (key.lower().encode("latin-1"), value.encode("latin-1"))
            for key, value in (headers or {}).items()
        ]
        if json_body is not None:
            raw_headers.append((b"content-type", b"application/json"))
            raw_headers.append((b"content-length", str(len(body)).encode("ascii")))
        messages: list[dict] = []
        request_delivered = False

        async def receive() -> dict:
            nonlocal request_delivered
            if not request_delivered:
                request_delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.sleep(0)
            return {"type": "http.disconnect"}

        async def send(message: dict) -> None:
            messages.append(message)

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method.upper(),
            "scheme": "http",
            "path": parsed.path,
            "raw_path": parsed.path.encode("ascii"),
            "query_string": parsed.query.encode("ascii"),
            "root_path": "",
            "headers": raw_headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
        await self.app(scope, receive, send)
        response_start = next(
            message for message in messages if message["type"] == "http.response.start"
        )
        response_headers = [
            (key.decode("latin-1"), value.decode("latin-1"))
            for key, value in response_start["headers"]
        ]
        response_body = b"".join(
            message.get("body", b"")
            for message in messages
            if message["type"] == "http.response.body"
        )
        return response_start["status"], response_headers, response_body

    @staticmethod
    def _header(headers: list[tuple[str, str]], name: str) -> str | None:
        lowered = name.lower()
        return next((value for key, value in headers if key.lower() == lowered), None)

    @staticmethod
    def _cookies(headers: list[tuple[str, str]]) -> dict[str, str]:
        cookies: dict[str, str] = {}
        for key, value in headers:
            if key.lower() != "set-cookie":
                continue
            parsed = SimpleCookie()
            parsed.load(value)
            cookies.update({name: morsel.value for name, morsel in parsed.items()})
        return cookies

    async def test_full_login_csrf_and_logout_flow(self):
        status, headers, _ = await self._request("GET", "/protected?view=latest")
        self.assertEqual(status, 303)
        self.assertEqual(
            self._header(headers, "location"),
            "/login?next=%2Fprotected%3Fview%3Dlatest",
        )

        status, _, body = await self._request("GET", "/api/protected")
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body)["detail"], "authentication_required")

        status, _, _ = await self._request("GET", "/static/pages/home.html")
        self.assertEqual(status, 404)

        status, _, body = await self._request(
            "POST",
            "/login",
            json_body={
                "username": "dax",
                "password": "wrong-password",
                "next": "/protected",
            },
        )
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body)["detail"], "invalid_credentials")

        status, login_headers, body = await self._request(
            "POST",
            "/login",
            json_body={
                "username": "dax",
                "password": "correct horse battery staple",
                "next": "/protected?view=latest",
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["redirect_to"], "/protected?view=latest")
        raw_set_cookie = "\n".join(
            value for key, value in login_headers if key.lower() == "set-cookie"
        )
        self.assertIn("HttpOnly", raw_set_cookie)
        self.assertIn("SameSite=lax", raw_set_cookie)
        cookies = self._cookies(login_headers)
        cookie_header = "; ".join(f"{key}={value}" for key, value in cookies.items())

        status, _, _ = await self._request(
            "GET",
            "/protected",
            headers={"Cookie": cookie_header},
        )
        self.assertEqual(status, 200)

        status, _, body = await self._request(
            "GET",
            "/api/auth/session",
            headers={"Cookie": cookie_header},
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["username"], "dax")

        status, _, body = await self._request(
            "POST",
            "/api/protected",
            headers={"Cookie": cookie_header},
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body)["detail"], "csrf_validation_failed")

        csrf_token = cookies["stock_analysis_csrf"]
        status, _, body = await self._request(
            "POST",
            "/api/protected",
            headers={"Cookie": cookie_header, "X-CSRF-Token": csrf_token},
        )
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(body)["ok"])

        status, logout_headers, body = await self._request(
            "POST",
            "/logout",
            headers={"Cookie": cookie_header, "X-CSRF-Token": csrf_token},
        )
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(body)["ok"])
        self.assertIn("Max-Age=0", "\n".join(value for _, value in logout_headers))


if __name__ == "__main__":
    unittest.main()
