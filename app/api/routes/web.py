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
    return FileResponse(WEB_DIR / "assets" / "favicon.png", media_type="image/png")


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


@router.get("/system", include_in_schema=False)
def system_page() -> FileResponse:
    return serve_page("system.html")


@router.get("/ui-reference/stock-detail", include_in_schema=False)
def stock_detail_ui_reference_page() -> FileResponse:
    return serve_page("ui-reference.html")


@router.get("/ui-reference/image2", include_in_schema=False)
def stock_detail_image2_ui_reference_page() -> FileResponse:
    return serve_page("ui-image2-preview.html")


@router.get("/ui-reference/pages", include_in_schema=False)
def pages_image2_ui_reference_page() -> FileResponse:
    return serve_page("ui-pages-preview.html")


@router.get("/ui-reference/terminal", include_in_schema=False)
def terminal_ui_reference_page() -> FileResponse:
    return serve_page("ui-terminal-preview.html")


@router.api_route("/preview", methods=["GET", "HEAD"], include_in_schema=False)
def preview_page() -> FileResponse:
    return serve_page("ui-terminal-preview.html")


@router.api_route("/ui-preview", methods=["GET", "HEAD"], include_in_schema=False)
def ui_preview_page() -> FileResponse:
    return serve_page("ui-terminal-preview.html")


@router.get("/ui-reference/simple", include_in_schema=False)
def simple_image2_ui_reference_page() -> FileResponse:
    return serve_page("ui-simple-preview.html")


@router.get("/stocks/{code}", include_in_schema=False)
def stock_detail_page(code: str) -> FileResponse:
    _ = code
    return serve_page("stock-detail.html")
