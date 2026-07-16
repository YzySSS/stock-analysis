from __future__ import annotations

import json
import re
import time
from threading import Lock
from typing import Annotated, Any

from fastapi import APIRouter, Query

from app.dashboard.repository import DashboardRepository
from app.data_ingestion.market_opinion_repository import hydrate_sector_opinion_rows
from app.error_learning.tracker import SelectionResultTracker
from app.market_timing.service import build_market_timing_signal

router = APIRouter(tags=["dashboard"])
_DASHBOARD_REPOSITORY = DashboardRepository()


_DASHBOARD_CACHE_TTL_SECONDS = 30.0
_DASHBOARD_CACHE: dict[int, tuple[float, dict[str, Any]]] = {}
_DASHBOARD_CACHE_LOCK = Lock()


def _to_float(value) -> float | None:
    return float(value) if value is not None else None


def _to_int(value) -> int:
    return int(value or 0)


def _project(item: dict[str, Any] | None, fields: tuple[str, ...]) -> dict[str, Any]:
    item = item or {}
    return {field: item.get(field) for field in fields}


def _compact_tracking_preview_item(item: dict[str, Any]) -> dict[str, Any]:
    return _project(
        item,
        (
            "code",
            "name",
            "rank_no",
            "strategy_id",
            "strategy_display_name",
            "selection_date",
            "selection_datetime",
            "tracking_days",
            "selected_open_price",
            "selected_close_price",
            "current_price",
            "score",
            "price_change_pct",
            "realtime_quote_time",
        ),
    )


def _compact_sector_item(item: dict[str, Any]) -> dict[str, Any]:
    return _project(
        item,
        (
            "name",
            "sector_type_label",
            "stock_count",
            "net_amount",
            "source_unit",
            "leading_stock",
            "leading_stock_pct_chg",
            "avg_pct_chg",
            "amount_weighted_pct_chg",
            "up_count",
            "down_count",
        ),
    )


def _compact_hot_theme_item(item: dict[str, Any]) -> dict[str, Any]:
    result = _project(
        item,
        (
            "sector_name",
            "sector_type",
            "sector_type_label",
            "net_amount",
            "source_unit",
            "pct_chg",
            "leading_stock",
            "hot_score",
            "sector_score",
            "fund_flow_score",
            "opinion_match_score",
            "ths_score",
            "ths_member_count",
            "stock_count",
            "source_count",
            "news_count",
            "positive_news_count",
            "negative_news_count",
        ),
    )
    result["top_stocks"] = [
        _project(stock, ("name", "stock_name", "code")) if isinstance(stock, dict) else stock
        for stock in (item.get("top_stocks") or [])[:3]
    ]
    result["top_news"] = [
        _project(news, ("title", "summary")) if isinstance(news, dict) else news
        for news in (item.get("top_news") or [])[:1]
    ]
    return result


def _compact_dashboard_payload(payload: dict[str, Any]) -> dict[str, Any]:
    market_overview = dict(payload.get("market_overview") or {})
    market_overview["strong_sectors"] = [
        _compact_sector_item(item) for item in (market_overview.get("strong_sectors") or [])[:5]
    ]
    market_overview["weak_sectors"] = [
        _compact_sector_item(item) for item in (market_overview.get("weak_sectors") or [])[:5]
    ]

    market_timing_source = payload.get("market_timing") or {}
    market_timing = _project(
        market_timing_source,
        (
            "model_name",
            "as_of",
            "state",
            "state_label",
            "timing_score",
            "position_upper_pct",
            "action_label",
            "reasons",
            "risk_notes",
        ),
    )
    market_timing["signals"] = [
        _project(
            item,
            (
                "label",
                "dimension",
                "article_dimension",
                "signal",
                "signal_label",
                "score",
                "value",
                "value_label",
                "source_status",
            ),
        )
        for item in (market_timing_source.get("signals") or [])
    ]
    market_timing["article_factor_coverage"] = [
        _project(item, ("factor", "status", "reason"))
        for item in (market_timing_source.get("article_factor_coverage") or [])[:8]
    ]

    hot_themes_source = payload.get("hot_themes") or {}
    hot_themes = _project(hot_themes_source, ("as_of",))
    hot_themes["items"] = [
        _compact_hot_theme_item(item) for item in (hot_themes_source.get("items") or [])[:8]
    ]

    emotion_source = payload.get("emotion_board") or {}
    emotion_board = _project(emotion_source, ("source", "note", "as_observation"))
    emotion_board["limit_up_pool"] = [
        _project(
            item,
            (
                "code",
                "name",
                "industry",
                "board_height_label",
                "recent_pattern_label",
                "open_board_count",
                "open_board_label",
                "last_open_time",
                "status_label",
                "amount",
                "pct_chg",
                "turnover_rate",
                "risk_tags",
            ),
        )
        for item in (emotion_source.get("limit_up_pool") or [])[:10]
    ]
    emotion_board["hot_limit_watch_pool"] = [
        _project(
            item,
            (
                "code",
                "name",
                "industry",
                "limit_gap_pct",
                "theme_name",
                "theme_score",
                "watch_points",
                "status_label",
                "theme_match_reason",
                "pct_chg",
                "hot_score",
                "net_amount_yi",
                "popularity_rank",
            ),
        )
        for item in (emotion_source.get("hot_limit_watch_pool") or [])[:6]
    ]
    emotion_board["reversal_watch_pool"] = [
        _project(
            item,
            (
                "code",
                "name",
                "industry",
                "is_limit_up",
                "limit_gap_pct",
                "status_label",
                "previous_board_label",
                "prev_divergence_pct",
                "reversal_pct",
                "support_score",
                "risk_tags",
            ),
        )
        for item in (emotion_source.get("reversal_watch_pool") or [])[:6]
    ]

    return {
        "latest_trade_date": payload.get("latest_trade_date"),
        "market_overview": market_overview,
        "market_timing": market_timing,
        "hot_themes": hot_themes,
        "emotion_board": emotion_board,
        "latest_tracking_count": payload.get("latest_tracking_count"),
        "latest_tracking_avg_price_change_pct": payload.get("latest_tracking_avg_price_change_pct"),
        "latest_tracking_preview": [
            _compact_tracking_preview_item(item)
            for item in (payload.get("latest_tracking_preview") or [])
        ],
        "latest_selection_summary": payload.get("latest_selection_summary"),
    }


def _get_cached_dashboard(limit: int) -> dict[str, Any] | None:
    now = time.monotonic()
    with _DASHBOARD_CACHE_LOCK:
        cached = _DASHBOARD_CACHE.get(limit)
        if not cached or cached[0] <= now:
            _DASHBOARD_CACHE.pop(limit, None)
            return None
        return cached[1]


def _cache_dashboard(limit: int, payload: dict[str, Any]) -> None:
    with _DASHBOARD_CACHE_LOCK:
        _DASHBOARD_CACHE[limit] = (time.monotonic() + _DASHBOARD_CACHE_TTL_SECONDS, payload)


def _clean_industry_name(name: str | None) -> str:
    if not name:
        return "未分类"
    # stock_basic.industry 当前带证监会行业编码，如 C39计算机、通信和其他电子设备制造业。
    return re.sub(r"^[A-Z]\d{2}", "", str(name)).strip() or str(name)


def _market_state(strength: float | None, up_ratio: float | None) -> str:
    if strength is None or up_ratio is None:
        return "unknown"
    if strength >= 70 and up_ratio >= 0.6:
        return "strong"
    if strength >= 58 and up_ratio >= 0.52:
        return "warm"
    if strength <= 35 and up_ratio <= 0.4:
        return "weak"
    if strength <= 45:
        return "cold"
    return "neutral"


def _market_state_label(state: str) -> str:
    return {
        "strong": "强势",
        "warm": "偏强",
        "neutral": "震荡",
        "cold": "偏弱",
        "weak": "弱势",
        "unknown": "未知",
    }.get(state, state)


def _limit_rate_for_code(code: str | None, name: str | None = None) -> float:
    text = name or ""
    if code and code.startswith("bj."):
        return 0.30
    if code and (code.startswith("sz.300") or code.startswith("sz.301") or code.startswith("sh.688")):
        return 0.20
    if text.startswith("*ST") or text.startswith("ST") or text.startswith("退市"):
        return 0.05
    return 0.10


def _is_limit_up(price: float | None, pre_close: float | None, code: str | None, name: str | None = None) -> bool:
    if price is None or pre_close is None or price <= 0 or pre_close <= 0:
        return False
    limit_price = round(pre_close * (1 + _limit_rate_for_code(code, name)), 2)
    return price >= limit_price


def _limit_open_board_stats(rows: list[dict[str, Any]]) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row.get("code"), []).append(row)

    stats: dict[str, dict] = {}
    for code, items in grouped.items():
        was_sealed = False
        ever_sealed = False
        open_count = 0
        first_limit_time = None
        last_open_time = None
        trade_date = None
        for row in items:
            price = _to_float(row.get("latest_price"))
            pre_close = _to_float(row.get("pre_close"))
            is_sealed = _is_limit_up(price, pre_close, code, row.get("name"))
            if is_sealed and not ever_sealed:
                first_limit_time = row.get("quote_minute")
            if ever_sealed and was_sealed and not is_sealed:
                open_count += 1
                last_open_time = row.get("quote_minute")
            ever_sealed = ever_sealed or is_sealed
            was_sealed = is_sealed
            trade_date = row.get("trade_date")
        stats[code] = {
            "open_board_count": open_count,
            "open_board_label": f"开板{open_count}次" if open_count else None,
            "first_limit_time": str(first_limit_time) if first_limit_time else None,
            "last_open_time": str(last_open_time) if last_open_time else None,
            "intraday_trade_date": str(trade_date) if trade_date else None,
        }
    return stats


def _board_label(code: str | None) -> str:
    if not code:
        return "A股"
    if code.startswith("bj."):
        return "北交所"
    if code.startswith("sh.688"):
        return "科创板"
    if code.startswith("sz.300") or code.startswith("sz.301"):
        return "创业板"
    return "主板"


def _risk_tags(row: dict, *, high_turnover_threshold: float = 35) -> list[str]:
    tags: list[str] = []
    name = str(row.get("name") or "")
    code = str(row.get("code") or "")
    turnover = _to_float(row.get("turnover_rate"))
    pe = _to_float(row.get("pe_tushare"))
    eps = _to_float(row.get("eps"))
    roe = _to_float(row.get("roe"))
    profit_yoy = _to_float(row.get("profit_yoy"))
    if name.startswith("*ST") or name.startswith("ST") or name.startswith("退市"):
        tags.append("ST")
    if code.startswith("bj."):
        tags.append("北交所")
    if turnover is not None and turnover >= high_turnover_threshold:
        tags.append("高换手")
    if pe is None:
        if eps is not None and eps <= 0:
            tags.append("PE不适用")
        else:
            tags.append("PE暂缺")
    if roe is not None and roe < 3:
        tags.append("ROE低")
    if profit_yoy is not None and profit_yoy < 0:
        tags.append("利润下滑")
    return tags[:4]


def _parse_json_list(value) -> list:
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        data = json.loads(value)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _hot_stock_theme_context(rows: list[dict[str, Any]]) -> dict[str, dict]:
    hydrate_sector_opinion_rows(rows)
    by_code: dict[str, dict] = {}
    for row in rows:
        sector_score = _to_float(row.get("sector_score")) or 0
        top_news = _parse_json_list(row.get("top_news_json"))
        for stock in _parse_json_list(row.get("top_stocks_json")):
            code = stock.get("code")
            if not code:
                continue
            stock_score = _to_float(stock.get("score")) or 0
            combined = sector_score * 0.72 + min(stock_score / 5, 100) * 0.28
            existing = by_code.get(code)
            if existing and existing.get("_combined", 0) >= combined:
                continue
            by_code[code] = {
                "_combined": combined,
                "theme_name": row.get("sector_name"),
                "theme_score": round(sector_score, 2),
                "theme_as_of": str(row.get("as_of_datetime")) if row.get("as_of_datetime") else None,
                "theme_match_type": stock.get("match_type"),
                "theme_match_reason": stock.get("match_reason"),
                "theme_stock_score": round(stock_score, 2),
                "theme_news_count": _to_int(stock.get("news_count")),
                "theme_top_news": (stock.get("matched_news") or top_news)[:2],
            }
    return by_code


def _limit_board_history(
    rows: list[dict[str, Any]],
    code: str,
    name: str | None,
    window_days: int = 6,
) -> dict:
    rows = list(rows[-(window_days + 1) :])
    if len(rows) < 2:
        return {
            "latest_streak": 0,
            "recent_limit_count": 0,
            "recent_window_days": 0,
            "recent_pattern_label": None,
        }

    limit_flags: list[bool] = []
    for index in range(1, len(rows)):
        current_close = _to_float(rows[index].get("close"))
        previous_close = _to_float(rows[index - 1].get("close"))
        limit_flags.append(_is_limit_up(current_close, previous_close, code, name))
    recent_flags = limit_flags[-window_days:]
    latest_streak = 0
    for flag in reversed(recent_flags):
        if flag:
            latest_streak += 1
        else:
            break
    recent_limit_count = sum(1 for flag in recent_flags if flag)
    actual_window = len(recent_flags)
    pattern_label = (
        f"{actual_window}天{recent_limit_count}板"
        if actual_window >= 3 and recent_limit_count >= 3 and recent_limit_count != latest_streak
        else None
    )
    return {
        "latest_streak": latest_streak,
        "recent_limit_count": recent_limit_count,
        "recent_window_days": actual_window,
        "recent_pattern_label": pattern_label,
    }


def _consecutive_limit_days_before(
    rows: list[dict[str, Any]],
    code: str,
    name: str | None,
    before_trade_date,
) -> int:
    before_value = str(before_trade_date)
    rows = [row for row in rows if str(row.get("trade_date") or "") < before_value][-8:]
    if len(rows) < 2:
        return 0
    streak = 0
    for index in range(len(rows) - 1, 0, -1):
        current_close = _to_float(rows[index].get("close"))
        previous_close = _to_float(rows[index - 1].get("close"))
        if _is_limit_up(current_close, previous_close, code, name):
            streak += 1
        else:
            break
    return streak


def _dashboard_emotion_board(limit: int = 8) -> dict:
    inputs = _DASHBOARD_REPOSITORY.load_emotion_board_inputs(limit)
    limit_rows = inputs["limit_rows"]
    theme_context_by_code = _hot_stock_theme_context(inputs["theme_rows"])
    hot_limit_rows = inputs["hot_limit_rows"]
    latest_kline_date = inputs["latest_kline_date"]
    reversal_rows = inputs["reversal_rows"]
    history_by_code = inputs["history_by_code"]

    limit_pool = []
    open_board_stats = _limit_open_board_stats(inputs["intraday_rows"])
    for row in limit_rows:
        code = row.get("code")
        name = row.get("name")
        history = _limit_board_history(history_by_code.get(str(code), []), code, name, window_days=5) if code else {}
        open_board = open_board_stats.get(code or "", {})
        board_height = int(history.get("latest_streak") or 0) + 1
        recent_window_days = int(history.get("recent_window_days") or 0) + 1
        recent_limit_count = int(history.get("recent_limit_count") or 0) + 1
        recent_pattern_label = (
            f"{recent_window_days}天{recent_limit_count}板"
            if recent_window_days >= 3 and recent_limit_count >= 3 and recent_limit_count != board_height
            else None
        )
        item = {
            "code": code,
            "name": name,
            "industry": _clean_industry_name(row.get("industry")),
            "board": _board_label(code),
            "board_height": board_height,
            "board_height_label": f"{board_height}板" if board_height > 1 else "首板",
            "recent_limit_count": recent_limit_count,
            "recent_window_days": recent_window_days,
            "recent_pattern_label": recent_pattern_label,
            "status_label": "封板/涨停",
            "open_board_count": int(open_board.get("open_board_count") or 0),
            "open_board_label": open_board.get("open_board_label"),
            "first_limit_time": open_board.get("first_limit_time"),
            "last_open_time": open_board.get("last_open_time"),
            "latest_price": _to_float(row.get("latest_price")),
            "pct_chg": round(_to_float(row.get("pct_chg")) or 0, 2),
            "amount": _to_float(row.get("realtime_amount")),
            "turnover_rate": _to_float(row.get("turnover_rate")),
            "volume_ratio": _to_float(row.get("volume_ratio")),
            "risk_tags": _risk_tags(row),
            "quote_time": str(row.get("quote_time")) if row.get("quote_time") else None,
        }
        limit_pool.append(item)
    limit_pool.sort(
        key=lambda item: (
            item.get("recent_limit_count") or 0,
            item.get("board_height") or 0,
            item.get("amount") or 0,
        ),
        reverse=True,
    )

    hot_limit_watch_pool = []
    for row in hot_limit_rows:
        code = row.get("code")
        name = row.get("name")
        latest_price = _to_float(row.get("latest_price"))
        pre_close = _to_float(row.get("pre_close"))
        pct_chg = _to_float(row.get("pct_chg")) or 0
        limit_rate = _limit_rate_for_code(code, name)
        limit_price = round(pre_close * (1 + limit_rate), 2) if pre_close else None
        is_today_limit = _is_limit_up(latest_price, pre_close, code, name)
        if is_today_limit:
            continue
        limit_gap_pct = (
            round((limit_price - latest_price) / latest_price * 100, 2)
            if limit_price and latest_price
            else None
        )
        theme_context = theme_context_by_code.get(code or "")
        popularity_score = _to_float(row.get("popularity_score"))
        net_amount = _to_float(row.get("realtime_net_amount"))
        net_amount_yi = net_amount if row.get("realtime_moneyflow_unit") == "亿元" else (net_amount / 100000000 if net_amount is not None else None)
        if net_amount_yi is not None and net_amount_yi <= 0:
            continue
        volume_ratio = _to_float(row.get("volume_ratio"))
        turnover_rate = _to_float(row.get("turnover_rate"))
        has_money_confirmation = net_amount_yi is not None and net_amount_yi > 0
        has_technical_confirmation = (
            (volume_ratio is not None and volume_ratio >= 1.2)
            or (turnover_rate is not None and turnover_rate >= 4)
        )
        if not theme_context and not has_money_confirmation and (popularity_score is None or popularity_score < 70):
            continue
        if pct_chg < 5 and not (theme_context and (has_money_confirmation or has_technical_confirmation)):
            continue
        fund_score = 50 + max(min(net_amount_yi or 0, 20), -20) * 1.5
        if net_amount_yi is None:
            fund_score = 50
        fund_score = max(0, min(100, fund_score))
        theme_score = _to_float((theme_context or {}).get("theme_score")) or 0
        stock_heat = min((_to_float((theme_context or {}).get("theme_stock_score")) or 0) / 5, 100)
        sprint_score = max(0, min(100, pct_chg / (limit_rate * 100) * 100))
        technical_score = 50
        if volume_ratio is not None:
            technical_score += max(min((volume_ratio - 1) * 18, 30), -15)
        if turnover_rate is not None:
            technical_score += max(min((turnover_rate - 3) * 3, 20), -10)
        technical_score = max(0, min(100, technical_score))
        hot_score = (
            theme_score * 0.36
            + stock_heat * 0.12
            + sprint_score * 0.22
            + fund_score * 0.18
            + technical_score * 0.08
            + (popularity_score or 50) * 0.04
        )
        if theme_context and theme_context.get("theme_match_type") == "direct_news_match":
            hot_score += 4
        hot_score = round(max(0, min(100, hot_score)), 1)
        if limit_gap_pct is not None and limit_gap_pct <= 3:
            status_label = "冲刺涨停"
        elif pct_chg >= 6:
            status_label = "强势冲板"
        else:
            status_label = "水上蓄势"
        watch_points = []
        if theme_context and theme_context.get("theme_name"):
            watch_points.append(f"热点：{theme_context.get('theme_name')}")
        if limit_gap_pct is not None:
            watch_points.append(f"距涨停{limit_gap_pct:.2f}%")
        if net_amount_yi is not None:
            watch_points.append(f"实时净流入{net_amount_yi:.2f}亿")
        if volume_ratio is not None:
            watch_points.append(f"量比{volume_ratio:.2f}")
        hot_limit_watch_pool.append({
            "code": code,
            "name": name,
            "industry": _clean_industry_name(row.get("industry")),
            "board": _board_label(code),
            "status_label": status_label,
            "is_limit_up": is_today_limit,
            "latest_price": latest_price,
            "pct_chg": round(pct_chg, 2),
            "limit_gap_pct": limit_gap_pct,
            "amount": _to_float(row.get("realtime_amount")),
            "turnover_rate": turnover_rate,
            "volume_ratio": volume_ratio,
            "net_amount": net_amount,
            "net_amount_yi": round(net_amount_yi, 2) if net_amount_yi is not None else None,
            "popularity_rank": _to_int(row.get("popularity_rank")) if row.get("popularity_rank") is not None else None,
            "popularity_score": round(popularity_score, 2) if popularity_score is not None else None,
            "hot_score": hot_score,
            "sprint_score": round(sprint_score, 1),
            "fund_score": round(fund_score, 1),
            "technical_score": round(technical_score, 1),
            "theme_name": (theme_context or {}).get("theme_name"),
            "theme_score": (theme_context or {}).get("theme_score"),
            "theme_match_type": (theme_context or {}).get("theme_match_type"),
            "theme_match_reason": (theme_context or {}).get("theme_match_reason"),
            "theme_news_count": (theme_context or {}).get("theme_news_count"),
            "theme_top_news": (theme_context or {}).get("theme_top_news") or [],
            "watch_points": watch_points[:3],
            "risk_tags": _risk_tags(row),
            "quote_time": str(row.get("quote_time")) if row.get("quote_time") else None,
        })
    hot_limit_watch_pool.sort(
        key=lambda item: (
            item.get("hot_score") or 0,
            -(item.get("limit_gap_pct") or 99),
            item.get("amount") or 0,
        ),
        reverse=True,
    )

    reversal_pool = []
    for row in reversal_rows:
        code = row.get("code")
        name = row.get("name")
        previous_board_streak = (
            _consecutive_limit_days_before(
                history_by_code.get(str(code), []),
                code,
                name,
                latest_kline_date,
            )
            if code and latest_kline_date else 0
        )
        if previous_board_streak < 2:
            continue
        prev_high = _to_float(row.get("prev_high"))
        prev_close = _to_float(row.get("prev_close"))
        prev_open = _to_float(row.get("prev_open"))
        prev_low = _to_float(row.get("prev_low"))
        latest_price = _to_float(row.get("latest_price"))
        pre_close = _to_float(row.get("pre_close"))
        pct_chg = _to_float(row.get("pct_chg")) or 0
        divergence = round((prev_high - prev_close) / prev_high * 100, 2) if prev_high and prev_close else None
        amplitude = round((prev_high - prev_low) / prev_close * 100, 2) if prev_high and prev_low and prev_close else None
        repair = round((latest_price - prev_close) / prev_close * 100, 2) if latest_price and prev_close else None
        is_today_limit = _is_limit_up(latest_price, pre_close, code, name)
        limit_rate = _limit_rate_for_code(code, name)
        limit_price = round(pre_close * (1 + limit_rate), 2) if pre_close else None
        limit_gap_pct = (
            round((limit_price - latest_price) / latest_price * 100, 2)
            if limit_price and latest_price and not is_today_limit
            else 0 if is_today_limit else None
        )
        turnover = _to_float(row.get("turnover_rate"))
        support_score = 42 + min(max(pct_chg, 0) * 3, 30)
        if divergence is not None:
            support_score += min(divergence * 1.35, 16)
        if not is_today_limit:
            support_score += 5
        if _to_float(row.get("realtime_amount")) is not None:
            support_score += min((_to_float(row.get("realtime_amount")) or 0) / 2000000000, 5)
        if turnover is not None:
            support_score -= min(max(turnover - 30, 0) * 0.35, 6)
        support_score += min(previous_board_streak * 3, 9)
        support_score = round(max(0, min(100, support_score)), 1)
        if is_today_limit:
            status_label = "已反包涨停"
        elif limit_gap_pct is not None and limit_gap_pct <= 3:
            status_label = "临近封板"
        elif pct_chg >= 7:
            status_label = "强修复"
        elif pct_chg >= 5:
            status_label = "强修复"
        else:
            status_label = "承接修复"
        reversal_pool.append({
            "code": code,
            "name": name,
            "industry": _clean_industry_name(row.get("industry")),
            "board": _board_label(code),
            "previous_board_streak": previous_board_streak,
            "previous_board_label": f"{previous_board_streak}连板",
            "status_label": status_label,
            "is_limit_up": is_today_limit,
            "prev_divergence_pct": divergence,
            "prev_amplitude_pct": amplitude,
            "reversal_pct": round(pct_chg, 2),
            "repair_from_prev_close_pct": repair,
            "support_score": support_score,
            "limit_gap_pct": limit_gap_pct,
            "latest_price": latest_price,
            "amount": _to_float(row.get("realtime_amount")),
            "turnover_rate": turnover,
            "volume_ratio": _to_float(row.get("volume_ratio")),
            "risk_tags": _risk_tags(row),
            "quote_time": str(row.get("quote_time")) if row.get("quote_time") else None,
            "prev_open": prev_open,
            "prev_high": prev_high,
            "prev_low": prev_low,
            "prev_close": prev_close,
        })
    reversal_pool.sort(
        key=lambda item: (
            0 if item.get("is_limit_up") else 1,
            item.get("support_score") or 0,
            item.get("amount") or 0,
        ),
        reverse=True,
    )

    return {
        "source": "stock_realtime_snapshot + daily_kline + factor_input_daily",
        "as_observation": True,
        "note": "短线情绪观察池，不等同正式选股策略",
        "limit_up_pool": limit_pool[:limit],
        "hot_limit_watch_pool": hot_limit_watch_pool[:limit],
        "reversal_watch_pool": reversal_pool[:limit],
    }


def _dashboard_market_overview() -> dict:
    """Compute today's market breadth/sector strength from MySQL snapshots."""

    inputs = _DASHBOARD_REPOSITORY.load_market_overview_inputs()
    breadth = inputs["breadth"]
    fund_strong_rows = inputs["fund_strong_rows"]
    fund_weak_rows = inputs["fund_weak_rows"]
    fund_flow_meta = inputs["fund_flow_meta"]
    strong_rows = inputs["strong_rows"]
    weak_rows = inputs["weak_rows"]
    previous_strength = inputs["previous_strength"]

    total = _to_int(breadth.get("total"))
    up_count = _to_int(breadth.get("up_count"))
    down_count = _to_int(breadth.get("down_count"))
    flat_count = _to_int(breadth.get("flat_count"))
    limit_up_count = _to_int(breadth.get("limit_up_count"))
    limit_down_count = _to_int(breadth.get("limit_down_count"))
    board30_limit_up_count = _to_int(breadth.get("board30_limit_up_count"))
    board30_limit_down_count = _to_int(breadth.get("board30_limit_down_count"))
    hs_limit_up_count = limit_up_count - board30_limit_up_count
    hs_limit_down_count = limit_down_count - board30_limit_down_count
    avg_pct_chg = _to_float(breadth.get("avg_pct_chg"))
    amount_weighted_pct_chg = _to_float(breadth.get("amount_weighted_pct_chg"))
    total_amount = _to_float(breadth.get("total_amount"))
    up_amount = _to_float(breadth.get("up_amount")) or 0
    down_amount = _to_float(breadth.get("down_amount")) or 0
    amount_pressure = round((up_amount - down_amount) / total_amount, 4) if total_amount else None
    up_ratio = round(up_count / total, 4) if total else None
    down_ratio = round(down_count / total, 4) if total else None

    if total and avg_pct_chg is not None and up_ratio is not None:
        weighted_pct = amount_weighted_pct_chg if amount_weighted_pct_chg is not None else avg_pct_chg
        raw_strength = 50 + weighted_pct * 7 + (up_ratio - 0.5) * 45 + ((amount_pressure or 0) * 18) + ((limit_up_count - limit_down_count) / total) * 80
        market_strength = round(max(0, min(100, raw_strength)), 2)
    else:
        market_strength = None

    state = _market_state(market_strength, up_ratio)
    strength_change = None
    if market_strength is not None and previous_strength.get("market_strength") is not None:
        strength_change = round(market_strength - float(previous_strength["market_strength"]), 2)

    def format_sector(row: dict) -> dict:
        stock_count = _to_int(row.get("stock_count"))
        sector_up_count = _to_int(row.get("up_count"))
        sector_down_count = _to_int(row.get("down_count"))
        return {
            "name": _clean_industry_name(row.get("industry")),
            "raw_name": row.get("industry"),
            "stock_count": stock_count,
            "avg_pct_chg": round(_to_float(row.get("avg_pct_chg")) or 0, 2),
            "amount_weighted_pct_chg": round(_to_float(row.get("amount_weighted_pct_chg")) or 0, 2),
            "up_count": sector_up_count,
            "down_count": sector_down_count,
            "up_ratio": round(sector_up_count / stock_count, 4) if stock_count else None,
            "amount": _to_float(row.get("amount")),
            "up_amount": _to_float(row.get("up_amount")),
            "down_amount": _to_float(row.get("down_amount")),
            "net_amount_proxy": (_to_float(row.get("up_amount")) or 0) - (_to_float(row.get("down_amount")) or 0),
        }

    def format_fund_sector(row: dict) -> dict:
        inflow = _to_float(row.get("inflow_amount")) or 0
        outflow = _to_float(row.get("outflow_amount")) or 0
        net_amount = _to_float(row.get("net_amount")) or 0
        company_count = _to_int(row.get("company_count"))
        return {
            "name": row.get("sector_name"),
            "raw_name": row.get("sector_name"),
            "sector_type": row.get("sector_type"),
            "sector_type_label": "概念" if row.get("sector_type") == "concept" else "行业",
            "stock_count": company_count,
            "avg_pct_chg": round(_to_float(row.get("pct_chg")) or 0, 2),
            "amount_weighted_pct_chg": round(_to_float(row.get("pct_chg")) or 0, 2),
            "up_count": None,
            "down_count": None,
            "up_ratio": None,
            "amount": inflow + outflow,
            "up_amount": inflow,
            "down_amount": outflow,
            "net_amount_proxy": net_amount,
            "net_amount": net_amount,
            "inflow_amount": inflow,
            "outflow_amount": outflow,
            "leading_stock": row.get("leading_stock"),
            "leading_stock_pct_chg": _to_float(row.get("leading_stock_pct_chg")),
            "source_unit": row.get("source_unit") or "亿元",
            "quote_time": str(row.get("quote_time")) if row.get("quote_time") else None,
        }

    has_fund_flow = bool(fund_strong_rows or fund_weak_rows)

    return {
        "source": "stock_realtime_snapshot + market_sector_fund_flow_snapshot",
        "board_api_status": "akshare_board_spot_unstable_remote_disconnected",
        "trade_date": str(breadth.get("trade_date")) if breadth.get("trade_date") else None,
        "latest_quote_time": str(breadth.get("latest_quote_time")) if breadth.get("latest_quote_time") else None,
        "market_strength": market_strength,
        "previous_market_strength": previous_strength.get("market_strength"),
        "previous_market_strength_trade_date": previous_strength.get("trade_date"),
        "market_strength_change": strength_change,
        "market_state": state,
        "market_state_label": _market_state_label(state),
        "total": total,
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "up_ratio": up_ratio,
        "down_ratio": down_ratio,
        "avg_pct_chg": round(avg_pct_chg, 2) if avg_pct_chg is not None else None,
        "amount_weighted_pct_chg": round(amount_weighted_pct_chg, 2) if amount_weighted_pct_chg is not None else None,
        "up_amount": up_amount,
        "down_amount": down_amount,
        "net_amount_proxy": up_amount - down_amount,
        "amount_pressure": amount_pressure,
        "limit_up_count": hs_limit_up_count,
        "limit_down_count": hs_limit_down_count,
        "limit_up_like": hs_limit_up_count,
        "limit_down_like": hs_limit_down_count,
        "all_limit_up_count": limit_up_count,
        "all_limit_down_count": limit_down_count,
        "limit_rule": "price_equals_exchange_limit_by_bucket: 北交所=30%, 创业板/科创板=20%, 主板ST/退市=5%, others=10%; latest_price must be > 0; based on realtime name + code prefix",
        "limit_breakdown": {
            "st": {"up": _to_int(breadth.get("st_limit_up_count")), "down": _to_int(breadth.get("st_limit_down_count"))},
            "board10": {"up": _to_int(breadth.get("board10_limit_up_count")), "down": _to_int(breadth.get("board10_limit_down_count"))},
            "board20": {"up": _to_int(breadth.get("board20_limit_up_count")), "down": _to_int(breadth.get("board20_limit_down_count"))},
            "board30": {"up": board30_limit_up_count, "down": board30_limit_down_count},
        },
        "strong_up_count": _to_int(breadth.get("strong_up_count")),
        "strong_down_count": _to_int(breadth.get("strong_down_count")),
        "total_amount": total_amount,
        "sector_source": "akshare_realtime_fund_flow" if has_fund_flow else "stock_realtime_snapshot_industry_fallback",
        "sector_source_label": "即时行业/概念资金流" if has_fund_flow else "本地行业成交额加权统计",
        "sector_fund_flow_time": str(fund_flow_meta.get("latest_fund_flow_time")) if fund_flow_meta.get("latest_fund_flow_time") else None,
        "sector_fund_flow_rows": _to_int(fund_flow_meta.get("fund_flow_rows")),
        "strong_sectors": [format_fund_sector(row) for row in fund_strong_rows] if has_fund_flow else [format_sector(row) for row in strong_rows],
        "weak_sectors": [format_fund_sector(row) for row in fund_weak_rows] if has_fund_flow else [format_sector(row) for row in weak_rows],
    }


def _dashboard_hot_themes(limit: int = 8) -> dict:
    excluded_topics = {
        "融资融券",
        "深股通",
        "沪股通",
        "转融券标的",
        "富时罗素",
        "MSCI中国",
        "标普道琼斯A股",
        "人民币贬值受益",
        "2025年报预增",
        "2026一季报预增",
        "专精特新",
        "金融",
        "银行",
        "证券",
    }
    short_term_theme_keywords = ("AI", "PCB", "CPO", "算力", "数据中心", "机器人", "人形机器人", "芯片", "半导体")
    inputs = _DASHBOARD_REPOSITORY.load_hot_theme_inputs()
    opinion_rows = inputs["opinion_rows"]
    fund_rows = inputs["fund_rows"]
    ths_rows = inputs["ths_rows"]
    hydrate_sector_opinion_rows(opinion_rows)
    def parse_json(value) -> list:
        if not value:
            return []
        if isinstance(value, list):
            return value
        try:
            data = json.loads(value)
        except Exception:
            return []
        return data if isinstance(data, list) else []

    by_name: dict[str, dict] = {}
    opinion_theme_scores: dict[str, float] = {}
    for row in opinion_rows:
        top_stocks = parse_json(row.get("top_stocks_json"))
        top_news = parse_json(row.get("top_news_json"))
        name = row.get("sector_name")
        if not name or name in excluded_topics:
            continue
        sector_score = round(_to_float(row.get("sector_score")) or 0, 2)
        opinion_theme_scores[name] = sector_score
        by_name[name] = {
            "trade_date": str(row.get("trade_date")) if row.get("trade_date") else None,
            "sector_type": row.get("sector_type"),
            "sector_type_label": "主题",
            "sector_name": name,
            "as_of": str(row.get("as_of_datetime")) if row.get("as_of_datetime") else None,
            "sector_score": sector_score,
            "weighted_impact_score": round(_to_float(row.get("weighted_impact_score")) or 0, 2),
            "fund_flow_score": None,
            "net_amount": None,
            "pct_chg": None,
            "leading_stock": None,
            "leading_stock_pct_chg": None,
            "news_count": _to_int(row.get("news_count")),
            "source_count": _to_int(row.get("source_count")),
            "stock_count": _to_int(row.get("stock_count")),
            "positive_news_count": _to_int(row.get("positive_news_count")),
            "negative_news_count": _to_int(row.get("negative_news_count")),
            "top_stocks": top_stocks[:4],
            "top_news": top_news[:2],
            "hot_score": sector_score,
        }

    def matched_opinion_score(concept_name: str) -> float | None:
        matches = [
            score
            for theme_name, score in opinion_theme_scores.items()
            if theme_name and (theme_name in concept_name or concept_name in theme_name)
        ]
        return max(matches) if matches else None

    ths_theme_keywords = {
        "AI算力": ("AI", "人工智能", "大模型", "算力", "数据中心", "智能体"),
        "机器人": ("机器人", "人形机器人", "具身智能"),
        "半导体": ("半导体", "芯片", "先进封装", "光刻", "PCB", "存储"),
        "新能源车": ("新能源车", "智能驾驶", "汽车", "车路云"),
        "锂电池": ("锂电", "电池", "固态电池", "储能"),
        "绿电": ("绿电", "电力", "光伏", "风电", "水电", "核电"),
        "低空经济": ("低空", "无人机", "飞行汽车", "eVTOL"),
        "军工航天": ("军工", "航天", "卫星", "商业航天", "北斗"),
        "医药": ("医药", "创新药", "医疗", "药"),
        "有色金属": ("有色", "铜", "铝", "黄金", "稀土", "锂矿"),
        "房地产": ("房地产", "地产", "物业", "城中村"),
        "消费": ("消费", "白酒", "食品", "旅游", "零售"),
        "传媒游戏": ("传媒", "游戏", "短剧", "影视"),
    }

    def matched_theme_name(concept_name: str) -> str | None:
        for theme_name in by_name:
            if theme_name and (theme_name in concept_name or concept_name in theme_name):
                return theme_name
        for theme_name, keywords in ths_theme_keywords.items():
            if theme_name in by_name and any(keyword in concept_name for keyword in keywords):
                return theme_name
        return None

    for row in fund_rows:
        name = row.get("sector_name")
        if not name or name in excluded_topics:
            continue
        pct_chg = _to_float(row.get("pct_chg")) or 0
        net_amount = _to_float(row.get("net_amount")) or 0
        fund_score = max(0, min(100, 45 + min(max(net_amount, 0) / 4, 35) + max(min(pct_chg, 8), -8) * 3))
        opinion_match_score = matched_opinion_score(name)
        if opinion_match_score is not None:
            fund_score = max(fund_score, min(100, fund_score * 0.72 + opinion_match_score * 0.28 + 6))
        if any(keyword in name for keyword in short_term_theme_keywords):
            fund_score = min(100, fund_score + 8)
        current = by_name.get(name)
        if current:
            current["fund_flow_score"] = round(fund_score, 2)
            current["opinion_match_score"] = round(opinion_match_score, 2) if opinion_match_score is not None else None
            current["net_amount"] = net_amount
            current["pct_chg"] = round(pct_chg, 2)
            current["leading_stock"] = row.get("leading_stock")
            current["leading_stock_pct_chg"] = _to_float(row.get("leading_stock_pct_chg"))
            current["hot_score"] = round(current["hot_score"] * 0.62 + fund_score * 0.38, 2)
        else:
            by_name[name] = {
                "trade_date": None,
                "sector_type": "concept",
                "sector_type_label": "概念",
                "sector_name": name,
                "as_of": str(row.get("quote_time")) if row.get("quote_time") else None,
                "sector_score": round(fund_score, 2),
                "weighted_impact_score": None,
                "fund_flow_score": round(fund_score, 2),
                "opinion_match_score": round(opinion_match_score, 2) if opinion_match_score is not None else None,
                "net_amount": net_amount,
                "pct_chg": round(pct_chg, 2),
                "leading_stock": row.get("leading_stock"),
                "leading_stock_pct_chg": _to_float(row.get("leading_stock_pct_chg")),
                "news_count": 0,
                "source_count": 1,
                "stock_count": _to_int(row.get("company_count")),
                "positive_news_count": 0,
                "negative_news_count": 0,
                "top_stocks": [{"name": row.get("leading_stock")} ] if row.get("leading_stock") else [],
                "top_news": [],
                "hot_score": round(fund_score, 2),
            }
    for row in ths_rows:
        name = row.get("concept_name")
        if not name or name in excluded_topics:
            continue
        ths_score = _to_float(row.get("ths_score")) or 0
        matched_name = matched_theme_name(name)
        current = by_name.get(name) or (by_name.get(matched_name) if matched_name else None)
        ths_payload = {
            "ths_score": round(ths_score, 2),
            "ths_concept_name": name,
            "ths_driver_event": row.get("driver_event"),
            "ths_leading_stock": row.get("leading_stock"),
            "ths_member_count": _to_int(row.get("member_count")),
            "ths_concept_code": row.get("concept_code"),
            "ths_summary_date": str(row.get("summary_date")) if row.get("summary_date") else None,
        }
        if current:
            if current.get("ths_score") is not None and (_to_float(current.get("ths_score")) or 0) >= ths_score:
                continue
            existing_hot_score = _to_float(current.get("hot_score")) or 0
            current.update(ths_payload)
            current["hot_score"] = round(max(existing_hot_score, existing_hot_score * 0.82 + ths_score * 0.18), 2)
            if not current.get("leading_stock") and row.get("leading_stock"):
                current["leading_stock"] = row.get("leading_stock")
            if not current.get("top_news") and row.get("driver_event"):
                current["top_news"] = [{"title": row.get("driver_event"), "source": "同花顺概念事件"}]
            if row.get("quote_time") and not current.get("as_of"):
                current["as_of"] = str(row.get("quote_time"))
        else:
            by_name[name] = {
                "trade_date": str(row.get("summary_date")) if row.get("summary_date") else None,
                "sector_type": "ths_concept",
                "sector_type_label": "同花顺概念",
                "sector_name": name,
                "as_of": str(row.get("quote_time")) if row.get("quote_time") else None,
                "sector_score": round(ths_score, 2),
                "weighted_impact_score": None,
                "fund_flow_score": None,
                "opinion_match_score": None,
                "net_amount": None,
                "pct_chg": None,
                "leading_stock": row.get("leading_stock"),
                "leading_stock_pct_chg": None,
                "news_count": 0,
                "source_count": 1,
                "stock_count": _to_int(row.get("member_count")),
                "positive_news_count": 0,
                "negative_news_count": 0,
                "top_stocks": [{"name": row.get("leading_stock")}] if row.get("leading_stock") else [],
                "top_news": [{"title": row.get("driver_event"), "source": "同花顺概念事件"}] if row.get("driver_event") else [],
                "hot_score": round(ths_score, 2),
                **ths_payload,
            }
    themes = sorted(by_name.values(), key=lambda item: item.get("hot_score") or 0, reverse=True)[:limit]
    return {
        "source": "sector_opinion_daily(theme only) + market_sector_fund_flow_snapshot(concept) + ths_concept_hot_snapshot",
        "schedule": "舆情主题每 15 分钟，概念资金每 3 分钟，同花顺概念每 30 分钟",
        "method": "热点排序按舆情热度、概念资金和同花顺概念事件综合展示；RRG/扩散过滤仍在讨论中，暂不参与",
        "items": themes,
        "as_of": themes[0].get("as_of") if themes else None,
    }


@router.get("/dashboard/summary")
def dashboard_summary(
    limit: Annotated[int, Query(ge=1, le=20)] = 5,
    compact: Annotated[bool, Query()] = False,
) -> dict:
    if compact:
        cached = _get_cached_dashboard(limit)
        if cached is not None:
            return cached

    tracker = SelectionResultTracker()

    preview_records = tracker.build_latest_selection_snapshot(limit=limit, instrument_type="stock")
    preview_items = tracker.to_dict_list(preview_records)

    price_values = [item["price_change_pct"] for item in preview_items if item.get("price_change_pct") is not None]
    avg_price_change_pct = round(sum(price_values) / len(price_values), 2) if price_values else None

    latest_trade_date = preview_items[0].get("selection_date") if preview_items else None
    latest_selection_summary = None
    if preview_items:
        top_items = preview_items[:3]
        latest_selection_summary = {
            "run_id": preview_items[0].get("run_id"),
            "strategy_display_name": preview_items[0].get("strategy_display_name") or preview_items[0].get("strategy_id"),
            "selected_trade_date": preview_items[0].get("selection_date"),
            "pick_count": len(preview_items),
            "top_items": [
                {
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "score": item.get("score"),
                    "price_change_pct": item.get("price_change_pct"),
                }
                for item in top_items
            ],
        }

    market_overview = _dashboard_market_overview()

    payload = {
        "latest_trade_date": latest_trade_date,
        "market_overview": market_overview,
        "market_timing": build_market_timing_signal(market_overview),
        "hot_themes": _dashboard_hot_themes(limit=8),
        "emotion_board": _dashboard_emotion_board(limit=10),
        "latest_tracking_count": len(preview_items),
        "latest_tracking_avg_price_change_pct": avg_price_change_pct,
        "latest_tracking_preview": preview_items,
        "latest_selection_summary": latest_selection_summary,
    }
    if compact:
        payload = _compact_dashboard_payload(payload)
        _cache_dashboard(limit, payload)
    return payload
