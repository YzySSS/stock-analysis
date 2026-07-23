from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

from app.api.auth import SiteAuthenticator, request_client_key, safe_next_path


router = APIRouter(tags=["authentication"])
WEB_DIR = Path(__file__).resolve().parents[1] / "web"
PAGES_DIR = WEB_DIR / "pages"


class LoginPayload(BaseModel):
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=1024)
    next: str | None = Field(default=None, max_length=2048)


def _authenticator(request: Request) -> SiteAuthenticator:
    return request.app.state.site_authenticator


@router.get("/login", include_in_schema=False)
def login_page(request: Request, next: str | None = None):  # noqa: A002, ANN201
    destination = safe_next_path(next)
    if getattr(request.state, "auth_session", None) is not None:
        return RedirectResponse(destination, status_code=303)
    return FileResponse(
        PAGES_DIR / "login.html",
        headers={
            "Cache-Control": "private, no-store",
            "Content-Security-Policy": (
                "default-src 'self'; "
                "script-src 'self'; "
                "style-src 'self'; "
                "img-src 'self' data:; "
                "connect-src 'self'; "
                "frame-ancestors 'none'; "
                "base-uri 'none'; "
                "form-action 'self'"
            ),
        },
    )


@router.post("/login", include_in_schema=False)
def login(request: Request, payload: LoginPayload) -> JSONResponse:
    authenticator = _authenticator(request)
    client_key = request_client_key(request)
    allowed, retry_after = authenticator.login_limiter.check(client_key)
    if not allowed:
        return JSONResponse(
            {"detail": "too_many_login_attempts"},
            status_code=429,
            headers={"Retry-After": str(retry_after), "Cache-Control": "private, no-store"},
        )

    if not authenticator.authenticate(payload.username, payload.password):
        authenticator.login_limiter.register_failure(client_key)
        return JSONResponse(
            {"detail": "invalid_credentials"},
            status_code=401,
            headers={"Cache-Control": "private, no-store"},
        )

    authenticator.login_limiter.clear(client_key)
    token, session = authenticator.issue_session()
    response = JSONResponse(
        {
            "ok": True,
            "redirect_to": safe_next_path(payload.next),
            "expires_at": session.expires_at,
        },
        headers={"Cache-Control": "private, no-store"},
    )
    authenticator.set_session_cookies(response, token=token, session=session)
    return response


@router.post("/logout", include_in_schema=False)
def logout(request: Request) -> JSONResponse:
    response = JSONResponse(
        {"ok": True, "redirect_to": "/login"},
        headers={"Cache-Control": "private, no-store"},
    )
    _authenticator(request).clear_session_cookies(response)
    return response


@router.get("/api/auth/session", include_in_schema=False)
def auth_session(request: Request) -> dict:
    session = request.state.auth_session
    return {
        "authenticated": True,
        "username": session.username,
        "expires_at": session.expires_at,
    }
