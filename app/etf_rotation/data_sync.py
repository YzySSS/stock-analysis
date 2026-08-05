from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from app.etf_rotation.spec import load_etf_rotation_spec
from app.shared.db import mysql_conn, mysql_read_conn


SECTOR_SOURCE = "tushare.moneyflow_ind_ths"
FUND_SOURCE = "tushare.fund_daily+fund_share+fund_nav"
CALENDAR_SOURCE = "tushare.trade_cal"


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _date_text(value: Any) -> str | None:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        return None
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def latest_market_trade_date() -> date:
    with mysql_read_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(dk.trade_date) AS trade_date
                FROM daily_kline dk
                INNER JOIN stock_basic sb ON sb.code=dk.code
                WHERE sb.instrument_type='stock'
                  AND sb.is_delisted=0
                  AND dk.close IS NOT NULL
                """
            )
            value = (cursor.fetchone() or {}).get("trade_date")
    if value is None:
        raise RuntimeError("stock daily_kline has no trade date")
    return value


def _date_chunks(start_date: date, end_date: date, days: int = 30) -> Iterable[tuple[date, date]]:
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _records(frame: Any) -> list[dict[str, Any]]:
    if frame is None or getattr(frame, "empty", True):
        return []
    return list(frame.to_dict("records"))


def _fetch_sector_rows(pro: Any, start_date: date, end_date: date) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    fields = (
        "trade_date,ts_code,industry,lead_stock,close,pct_change,company_num,"
        "pct_change_stock,close_price,net_buy_amount,net_sell_amount,net_amount"
    )
    for chunk_start, chunk_end in _date_chunks(start_date, end_date):
        frame = pro.moneyflow_ind_ths(
            start_date=chunk_start.strftime("%Y%m%d"),
            end_date=chunk_end.strftime("%Y%m%d"),
            fields=fields,
        )
        for row in _records(frame):
            trade_date = _date_text(row.get("trade_date"))
            industry_code = str(row.get("ts_code") or "").strip()
            industry_name = str(row.get("industry") or "").strip()
            if not trade_date or not industry_code or not industry_name:
                continue
            by_key[(trade_date, industry_code)] = {
                "trade_date": trade_date,
                "industry_code": industry_code,
                "industry_name": industry_name,
                "close": _number(row.get("close")),
                "pct_change": _number(row.get("pct_change")),
                "company_num": int(row["company_num"]) if _number(row.get("company_num")) is not None else None,
                "lead_stock": str(row.get("lead_stock") or "").strip()[:64] or None,
                "lead_stock_pct_change": _number(row.get("pct_change_stock")),
                "lead_stock_close": _number(row.get("close_price")),
                "net_buy_amount": _number(row.get("net_buy_amount")),
                "net_sell_amount": _number(row.get("net_sell_amount")),
                "net_amount": _number(row.get("net_amount")),
                "source": SECTOR_SOURCE,
            }
    if not by_key:
        raise RuntimeError(
            f"moneyflow_ind_ths returned no rows for {start_date}..{end_date}"
        )
    return list(by_key.values())


def _fetch_trade_calendar(
    pro: Any,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    frame = pro.trade_cal(
        exchange="SSE",
        start_date=start_date.strftime("%Y%m%d"),
        end_date=(end_date + timedelta(days=45)).strftime("%Y%m%d"),
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    rows = []
    for row in _records(frame):
        cal_date = _date_text(row.get("cal_date"))
        if not cal_date:
            continue
        rows.append(
            {
                "exchange_code": str(row.get("exchange") or "SSE"),
                "cal_date": cal_date,
                "is_open": 1 if int(row.get("is_open") or 0) else 0,
                "pretrade_date": _date_text(row.get("pretrade_date")),
                "source": CALENDAR_SOURCE,
            }
        )
    if not rows:
        raise RuntimeError("trade_cal returned no rows")
    return rows


def merge_fund_rows(
    *,
    fund: dict[str, Any],
    daily_rows: list[dict[str, Any]],
    share_rows: list[dict[str, Any]],
    nav_rows: list[dict[str, Any]],
    maximum_nav_staleness_days: int,
) -> list[dict[str, Any]]:
    share_by_date = {
        trade_date: _number(row.get("fd_share"))
        for row in share_rows
        if (trade_date := _date_text(row.get("trade_date")))
    }
    nav_values = sorted(
        (
            trade_date,
            {
                "unit_nav": _number(row.get("unit_nav")),
                "accum_nav": _number(row.get("accum_nav")),
                "net_asset": _number(row.get("net_asset")),
                "total_netasset": _number(row.get("total_netasset")),
            },
        )
        for row in nav_rows
        if (trade_date := _date_text(row.get("nav_date")))
    )
    list_date = _date_text(fund.get("list_date"))
    if not list_date:
        raise RuntimeError(f"{fund.get('ts_code')} has no valid list_date")

    merged: list[dict[str, Any]] = []
    for daily in sorted(daily_rows, key=lambda item: str(item.get("trade_date") or "")):
        trade_date = _date_text(daily.get("trade_date"))
        close = _number(daily.get("close"))
        if not trade_date or close is None:
            continue
        trade_day = date.fromisoformat(trade_date)
        nav_date: str | None = None
        nav: dict[str, float | None] = {}
        for current_date, current_nav in nav_values:
            if current_date > trade_date:
                break
            if (trade_day - date.fromisoformat(current_date)).days <= maximum_nav_staleness_days:
                nav_date = current_date
                nav = current_nav
        unit_nav = nav.get("unit_nav")
        premium_discount = (
            (close / unit_nav - 1) * 100
            if unit_nav not in {None, 0}
            else None
        )
        raw_amount = _number(daily.get("amount"))
        merged.append(
            {
                "ts_code": fund["ts_code"],
                "trade_date": trade_date,
                "fund_name": fund["name"],
                "list_date": list_date,
                "benchmark": fund.get("benchmark"),
                "open": _number(daily.get("open")),
                "high": _number(daily.get("high")),
                "low": _number(daily.get("low")),
                "close": close,
                "pre_close": _number(daily.get("pre_close")),
                "change_amount": _number(daily.get("change")),
                "pct_chg": _number(daily.get("pct_chg")),
                "volume_hand": _number(daily.get("vol")),
                # Tushare fund_daily.amount is reported in thousand yuan.
                "amount_yuan": raw_amount * 1000 if raw_amount is not None else None,
                # Tushare fund_share.fd_share is reported in 10,000 shares.
                "fund_share_10k": share_by_date.get(trade_date),
                "nav_date": nav_date,
                "unit_nav": unit_nav,
                "accum_nav": nav.get("accum_nav"),
                "net_asset": nav.get("net_asset"),
                "total_netasset": nav.get("total_netasset"),
                "premium_discount_pct": premium_discount,
                "source": FUND_SOURCE,
            }
        )
    return merged


def _fetch_fund_rows(
    pro: Any,
    start_date: date,
    end_date: date,
    spec: dict[str, Any],
) -> list[dict[str, Any]]:
    basic_frame = pro.fund_basic(
        market="E",
        status="L",
        fields="ts_code,name,list_date,benchmark,status,invest_type,fund_type",
    )
    basics = {
        str(row.get("ts_code") or ""): row
        for row in _records(basic_frame)
    }
    start = start_date.strftime("%Y%m%d")
    end = end_date.strftime("%Y%m%d")
    maximum_nav_staleness_days = int(
        spec["data_contract"]["maximum_nav_staleness_calendar_days"]
    )
    all_rows: list[dict[str, Any]] = []
    for sector in spec["sectors"]:
        frozen = sector["etf"]
        ts_code = frozen["ts_code"]
        basic = basics.get(ts_code)
        if not basic:
            raise RuntimeError(f"fund_basic is missing frozen ETF {ts_code}")
        if str(basic.get("name") or "") != frozen["name"]:
            raise RuntimeError(
                f"fund_basic name mismatch for {ts_code}: "
                f"{basic.get('name')!r} != {frozen['name']!r}"
            )
        if str(basic.get("list_date") or "") != frozen["list_date"]:
            raise RuntimeError(
                f"fund_basic list_date mismatch for {ts_code}: "
                f"{basic.get('list_date')!r} != {frozen['list_date']!r}"
            )
        daily = _records(
            pro.fund_daily(
                ts_code=ts_code,
                start_date=start,
                end_date=end,
                fields=(
                    "ts_code,trade_date,open,high,low,close,pre_close,"
                    "change,pct_chg,vol,amount"
                ),
            )
        )
        share = _records(
            pro.fund_share(
                ts_code=ts_code,
                start_date=start,
                end_date=end,
                fields="ts_code,trade_date,fd_share,fund_type,market",
            )
        )
        nav = _records(
            pro.fund_nav(
                ts_code=ts_code,
                start_date=start,
                end_date=end,
                fields=(
                    "ts_code,ann_date,nav_date,unit_nav,accum_nav,"
                    "net_asset,total_netasset"
                ),
            )
        )
        if not daily:
            raise RuntimeError(
                f"fund_daily returned no rows for frozen ETF {ts_code}"
            )
        all_rows.extend(
            merge_fund_rows(
                fund=basic,
                daily_rows=daily,
                share_rows=share,
                nav_rows=nav,
                maximum_nav_staleness_days=maximum_nav_staleness_days,
            )
        )
    return all_rows


def _save_rows(
    calendar_rows: list[dict[str, Any]],
    sector_rows: list[dict[str, Any]],
    fund_rows: list[dict[str, Any]],
) -> dict[str, int]:
    calendar_sql = """
    INSERT INTO etf_rotation_trade_calendar (
        exchange_code, cal_date, is_open, pretrade_date, source
    ) VALUES (
        %(exchange_code)s, %(cal_date)s, %(is_open)s,
        %(pretrade_date)s, %(source)s
    )
    ON DUPLICATE KEY UPDATE
        is_open=VALUES(is_open),
        pretrade_date=VALUES(pretrade_date),
        source=VALUES(source)
    """
    sector_sql = """
    INSERT INTO etf_rotation_sector_daily (
        trade_date, industry_code, industry_name, close, pct_change,
        company_num, lead_stock, lead_stock_pct_change, lead_stock_close,
        net_buy_amount, net_sell_amount, net_amount, source
    ) VALUES (
        %(trade_date)s, %(industry_code)s, %(industry_name)s, %(close)s,
        %(pct_change)s, %(company_num)s, %(lead_stock)s,
        %(lead_stock_pct_change)s, %(lead_stock_close)s,
        %(net_buy_amount)s, %(net_sell_amount)s, %(net_amount)s, %(source)s
    )
    ON DUPLICATE KEY UPDATE
        industry_name=VALUES(industry_name),
        close=VALUES(close),
        pct_change=VALUES(pct_change),
        company_num=VALUES(company_num),
        lead_stock=VALUES(lead_stock),
        lead_stock_pct_change=VALUES(lead_stock_pct_change),
        lead_stock_close=VALUES(lead_stock_close),
        net_buy_amount=VALUES(net_buy_amount),
        net_sell_amount=VALUES(net_sell_amount),
        net_amount=VALUES(net_amount),
        source=VALUES(source)
    """
    fund_sql = """
    INSERT INTO etf_rotation_fund_daily (
        ts_code, trade_date, fund_name, list_date, benchmark,
        open, high, low, close, pre_close, change_amount, pct_chg,
        volume_hand, amount_yuan, fund_share_10k, nav_date,
        unit_nav, accum_nav, net_asset, total_netasset,
        premium_discount_pct, source
    ) VALUES (
        %(ts_code)s, %(trade_date)s, %(fund_name)s, %(list_date)s,
        %(benchmark)s, %(open)s, %(high)s, %(low)s, %(close)s,
        %(pre_close)s, %(change_amount)s, %(pct_chg)s,
        %(volume_hand)s, %(amount_yuan)s, %(fund_share_10k)s,
        %(nav_date)s, %(unit_nav)s, %(accum_nav)s, %(net_asset)s,
        %(total_netasset)s, %(premium_discount_pct)s, %(source)s
    )
    ON DUPLICATE KEY UPDATE
        fund_name=VALUES(fund_name),
        list_date=VALUES(list_date),
        benchmark=VALUES(benchmark),
        open=VALUES(open),
        high=VALUES(high),
        low=VALUES(low),
        close=VALUES(close),
        pre_close=VALUES(pre_close),
        change_amount=VALUES(change_amount),
        pct_chg=VALUES(pct_chg),
        volume_hand=VALUES(volume_hand),
        amount_yuan=VALUES(amount_yuan),
        fund_share_10k=VALUES(fund_share_10k),
        nav_date=VALUES(nav_date),
        unit_nav=VALUES(unit_nav),
        accum_nav=VALUES(accum_nav),
        net_asset=VALUES(net_asset),
        total_netasset=VALUES(total_netasset),
        premium_discount_pct=VALUES(premium_discount_pct),
        source=VALUES(source)
    """
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(calendar_sql, calendar_rows)
            calendar_affected = cursor.rowcount
            cursor.executemany(sector_sql, sector_rows)
            sector_affected = cursor.rowcount
            cursor.executemany(fund_sql, fund_rows)
            fund_affected = cursor.rowcount
    return {
        "calendar_rows": len(calendar_rows),
        "calendar_affected": calendar_affected,
        "sector_rows": len(sector_rows),
        "sector_affected": sector_affected,
        "fund_rows": len(fund_rows),
        "fund_affected": fund_affected,
    }


def sync_etf_rotation_data(
    *,
    pro: Any,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    spec = load_etf_rotation_spec()
    calendar_rows = _fetch_trade_calendar(pro, start_date, end_date)
    sector_rows = _fetch_sector_rows(pro, start_date, end_date)
    fund_rows = _fetch_fund_rows(pro, start_date, end_date, spec)
    result = _save_rows(calendar_rows, sector_rows, fund_rows)
    end_date_text = end_date.isoformat()
    target_is_open = any(
        str(row.get("cal_date") or "")[:10] == end_date_text
        and int(row.get("is_open") or 0) == 1
        for row in calendar_rows
    )
    sector_latest_trade_date = max(
        (str(row.get("trade_date") or "")[:10] for row in sector_rows),
        default=None,
    )
    sector_target_aligned = (
        not target_is_open or sector_latest_trade_date == end_date_text
    )
    return {
        "status": "success" if sector_target_aligned else "partial_success",
        "model_id": spec["model_id"],
        "start_date": start_date.isoformat(),
        "end_date": end_date_text,
        "etf_count": len(spec["sectors"]),
        "target_is_open": target_is_open,
        "sector_latest_trade_date": sector_latest_trade_date,
        "sector_target_aligned": sector_target_aligned,
        "partial_reason": (
            None
            if sector_target_aligned
            else "provider sector data has not published the target trade date"
        ),
        **result,
    }
