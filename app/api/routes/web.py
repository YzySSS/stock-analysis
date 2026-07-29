from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse
from pathlib import Path

router = APIRouter(tags=["web"])

WEB_DIR = Path(__file__).resolve().parents[1] / "web"
PAGES_DIR = WEB_DIR / "pages"


def serve_page(filename: str) -> FileResponse:
    return FileResponse(PAGES_DIR / filename, headers={"Cache-Control": "no-cache"})


@router.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
def home() -> FileResponse:
    return serve_page("home.html")


@router.api_route("/favicon.ico", methods=["GET", "HEAD"], include_in_schema=False)
def favicon() -> FileResponse:
    return FileResponse(
        WEB_DIR / "assets" / "stock-research-favicon.png",
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


@router.get("/selection", include_in_schema=False)
def selection_page() -> FileResponse:
    return serve_page("selection.html")


@router.get("/tracking", include_in_schema=False)
def tracking_page() -> FileResponse:
    return serve_page("tracking.html")


@router.get("/portfolio", include_in_schema=False)
def portfolio_page() -> FileResponse:
    return serve_page("portfolio.html")


@router.get("/strategies", include_in_schema=False)
def strategies_page() -> FileResponse:
    return serve_page("strategies.html")


@router.get("/backtest", include_in_schema=False)
def backtest_page() -> FileResponse:
    return serve_page("backtest.html")


@router.get("/trade-strategies", include_in_schema=False)
def trade_strategies_page() -> FileResponse:
    return serve_page("trade-strategies.html")


@router.get("/etf-rotation", include_in_schema=False)
def etf_rotation_page() -> FileResponse:
    return serve_page("etf-rotation.html")


@router.get("/system", include_in_schema=False)
def system_page() -> FileResponse:
    return serve_page("system.html")


@router.get("/stocks/{code}", include_in_schema=False)
def stock_detail_page(code: str) -> FileResponse:
    _ = code
    return serve_page("stock-detail.html")
