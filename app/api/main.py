from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from app.api.routes.backtest import router as backtest_router
from app.api.routes.dashboard import router as dashboard_router
from app.api.routes.health import router as health_router
from app.api.routes.selection import router as selection_router
from app.api.routes.stocks import router as stocks_router
from app.api.routes.strategies import router as strategies_router
from app.api.routes.system import router as system_router
from app.api.routes.tracking import router as tracking_router
from app.api.routes.web import router as web_router


app = FastAPI(
    title="Stock Analysis API",
    version="0.1.0",
    description="股票分析项目第一版 Web API",
)

WEB_DIR = Path(__file__).resolve().parent / "web"

app.include_router(web_router)
app.include_router(health_router, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(system_router, prefix="/api")
app.include_router(strategies_router, prefix="/api")
app.include_router(selection_router, prefix="/api")
app.include_router(tracking_router, prefix="/api")
app.include_router(backtest_router, prefix="/api")
app.include_router(stocks_router, prefix="/api")
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
