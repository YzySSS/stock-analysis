from __future__ import annotations

import logging
import time
import uuid

from fastapi import FastAPI
from fastapi import Request
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from app.api.auth import SiteAuthenticator, SiteAuthMiddleware, SiteAuthSettings
from app.api.routes.auth import router as auth_router
from app.api.routes.backtest import router as backtest_router
from app.api.routes.dashboard import router as dashboard_router
from app.api.routes.health import router as health_router
from app.api.routes.portfolio import router as portfolio_router
from app.api.routes.selection import router as selection_router
from app.api.routes.stocks import router as stocks_router
from app.api.routes.strategies import router as strategies_router
from app.api.routes.system import router as system_router
from app.api.routes.trade_strategies import router as trade_strategies_router
from app.api.routes.tracking import router as tracking_router
from app.api.routes.web import router as web_router
from app.shared.observability import request_metrics


app = FastAPI(
    title="Stock Analysis API",
    version="0.1.0",
    description="股票分析项目第一版 Web API",
)
logger = logging.getLogger(__name__)
site_authenticator = SiteAuthenticator(SiteAuthSettings.from_env())
app.state.site_authenticator = site_authenticator
app.add_middleware(SiteAuthMiddleware, authenticator=site_authenticator)


@app.middleware("http")
async def observe_request(request: Request, call_next):  # noqa: ANN001
    started = time.perf_counter()
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception:
        duration_ms = (time.perf_counter() - started) * 1000
        route = getattr(request.scope.get("route"), "path", request.url.path)
        request_metrics.observe(request.method, route, status_code, duration_ms)
        logger.exception(
            "request failed method=%s route=%s status=%s duration_ms=%.3f request_id=%s",
            request.method,
            route,
            status_code,
            duration_ms,
            request_id,
        )
        raise

    duration_ms = (time.perf_counter() - started) * 1000
    route = getattr(request.scope.get("route"), "path", request.url.path)
    request_metrics.observe(request.method, route, status_code, duration_ms)
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Response-Time-Ms"] = f"{duration_ms:.3f}"
    response.headers["Server-Timing"] = f"app;dur={duration_ms:.3f}"
    if duration_ms >= 500:
        logger.warning(
            "slow request method=%s route=%s status=%s duration_ms=%.3f request_id=%s",
            request.method,
            route,
            status_code,
            duration_ms,
            request_id,
        )
    return response

WEB_DIR = Path(__file__).resolve().parent / "web"

app.include_router(auth_router)
app.include_router(web_router)
app.include_router(health_router, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(system_router, prefix="/api")
app.include_router(strategies_router, prefix="/api")
app.include_router(selection_router, prefix="/api")
app.include_router(tracking_router, prefix="/api")
app.include_router(portfolio_router, prefix="/api")
app.include_router(backtest_router, prefix="/api")
app.include_router(trade_strategies_router, prefix="/api")
app.include_router(stocks_router, prefix="/api")
app.mount("/static/css", StaticFiles(directory=WEB_DIR / "css"), name="static-css")
app.mount("/static/js", StaticFiles(directory=WEB_DIR / "js"), name="static-js")
app.mount("/static/assets", StaticFiles(directory=WEB_DIR / "assets"), name="static-assets")
