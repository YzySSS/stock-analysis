from __future__ import annotations

from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.selection import router as selection_router
from app.api.routes.strategies import router as strategies_router
from app.api.routes.tracking import router as tracking_router


app = FastAPI(
    title="Stock Analysis API",
    version="0.1.0",
    description="股票分析项目第一版 Web API",
)

app.include_router(health_router, prefix="/api")
app.include_router(strategies_router, prefix="/api")
app.include_router(selection_router, prefix="/api")
app.include_router(tracking_router, prefix="/api")
