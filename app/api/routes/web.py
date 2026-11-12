from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse, HTMLResponse
from html import escape
from pathlib import Path

router = APIRouter(tags=["web"])

WEB_DIR = Path(__file__).resolve().parents[1] / "web"
PAGES_DIR = WEB_DIR / "pages"


def serve_page(filename: str) -> FileResponse:
    return FileResponse(PAGES_DIR / filename, headers={"Cache-Control": "no-cache"})


@router.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
def home() -> FileResponse:
    return serve_page("home.html")


@router.api_route("/ui-reference/pages", methods=["GET", "HEAD"], include_in_schema=False)
def ui_reference_pages() -> HTMLResponse:
    # Historical, machine-local drafts are optional and are not part of a fresh checkout.
    legacy_previews = (
        ("首页", "市场概览 / 板块资金 / 短线情绪 / 最近跟踪", "home.svg"),
        ("选股中心", "运行选股 / 结果池 / 原因风险", "selection.png"),
        ("跟踪复盘", "收益统计 / 胜率 / 不统计切换", "tracking.png"),
        ("持仓管理", "手动持仓 / 实时建议 / 买卖点", "portfolio.svg"),
        ("回测中心", "异步任务 / 收益曲线 / 交易明细", "backtest.png"),
        ("选股策略", "策略注册 / 状态 / 权重说明", "strategies.png"),
        ("交易策略", "买卖规则 / 真实计算 / 交易后分析", "trade-strategy.png"),
        ("数据状态", "数据覆盖 / 任务日志 / 舆情质量", "system.png"),
        ("个股详情", "分时 / 日K / 基本面 / 舆情新闻", "stock-detail.png"),
    )
    cards = []
    for title, subtitle, filename in legacy_previews:
        if not (WEB_DIR / "assets" / "ui-preview" / "pages" / filename).is_file():
            continue
        cards.append(
            '<article class="card"><div class="card-head">'
            f'<h3>{escape(title)}</h3><span>{escape(subtitle)}</span></div>'
            f'<img src="/static/assets/ui-preview/pages/{escape(filename)}" '
            f'alt="{escape(title)}历史 UI 预览" loading="lazy"></article>'
        )
    legacy_gallery = (
        '<section class="previous" aria-labelledby="previous-heading">'
        '<h2 id="previous-heading">历史设计参考</h2>'
        '<p>保留之前的视觉方向，便于对照。</p>'
        f'<div class="grid">{"".join(cards)}</div></section>'
        if cards else ""
    )
    page = (PAGES_DIR / "ui-pages-preview.html").read_text(encoding="utf-8")
    return HTMLResponse(
        page.replace("<!-- LEGACY_PREVIEW_GALLERY -->", legacy_gallery),
        headers={"Cache-Control": "no-cache"},
    )


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
