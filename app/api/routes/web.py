from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse
from pathlib import Path

router = APIRouter(tags=["web"])

WEB_DIR = Path(__file__).resolve().parents[1] / "web"
PAGES_DIR = WEB_DIR / "pages"


def serve_page(filename: str) -> FileResponse:
    return FileResponse(PAGES_DIR / filename)


@router.get("/", include_in_schema=False)
def home() -> FileResponse:
    return serve_page("home.html")


@router.get("/selection", include_in_schema=False)
def selection_page() -> FileResponse:
    return serve_page("selection.html")


@router.get("/tracking", include_in_schema=False)
def tracking_page() -> FileResponse:
    return serve_page("tracking.html")


@router.get("/strategies", include_in_schema=False)
def strategies_page() -> FileResponse:
    return serve_page("strategies.html")


@router.get("/system", include_in_schema=False)
def system_page() -> FileResponse:
    return serve_page("system.html")


@router.get("/stocks/{code}", include_in_schema=False)
def stock_detail_page(code: str) -> FileResponse:
    _ = code
    return serve_page("stock-detail.html")
