"""Add validated independent calendar dates without rewriting existing history."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.shared.db import mysql_conn


def validate_calendar_records(records: list[dict], start: date, end: date) -> list[tuple]:
    normalized = []
    for row in records:
        text = str(row.get('cal_date') or '')
        day = date.fromisoformat(text if '-' in text else f'{text[:4]}-{text[4:6]}-{text[6:8]}')
        if str(row.get('exchange')) != 'SSE' or row.get('is_open') not in (0, 1, '0', '1'):
            raise ValueError('交易日历来源或开市标志异常')
        normalized.append((str(day), int(row['is_open']), 'tushare.trade_cal'))
    normalized.sort()
    expected = [str(start + timedelta(days=i)) for i in range((end-start).days+1)]
    if [row[0] for row in normalized] != expected:
        raise ValueError('交易日历日期不连续或重复，拒绝写入')
    return normalized


def sync_calendar(start_date: str, end_date: str, *, apply: bool = False,
                  provider: Any = None, connection_factory=mysql_conn) -> dict:
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    if end < start or (end-start).days > 1461:
        raise ValueError('日历范围最多1462个自然日')
    if provider is None:
        from app.data_ingestion.adj_factor_sync import AdjFactorSync
        provider = AdjFactorSync().pro
    records = []
    # Avoid upstream row limits; every requested date must be accounted for.
    batch_start = start
    while batch_start <= end:
        batch_end = min(end, date(batch_start.year, 12, 31))
        frame = provider.trade_cal(exchange='SSE', start_date=batch_start.strftime('%Y%m%d'),
                                   end_date=batch_end.strftime('%Y%m%d'),
                                   fields='exchange,cal_date,is_open')
        records.extend(frame.to_dict('records') if frame is not None else [])
        batch_start = batch_end + timedelta(days=1)
    rows = validate_calendar_records(records, start, end)
    with connection_factory() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT cal_date,is_open,source FROM etf_rotation_trade_calendar
                WHERE exchange_code='SSE' AND cal_date BETWEEN %s AND %s ORDER BY cal_date""",
                (start_date,end_date))
            existing = {str(row['cal_date']):(int(row['is_open']),row['source']) for row in cursor.fetchall()}
            for day, is_open, source in rows:
                if day in existing and existing[day] != (is_open, source):
                    raise ValueError(f'{day} 已有日历与来源冲突，拒绝覆盖')
            missing = [row for row in rows if row[0] not in existing]
            if apply and missing:
                cursor.executemany("""INSERT INTO etf_rotation_trade_calendar
                    (exchange_code,cal_date,is_open,source) VALUES ('SSE',%s,%s,%s)""", missing)
    return {'start_date':start_date,'end_date':end_date,'source':'tushare.trade_cal',
            'verified_days':len(rows),'existing_days':len(existing),'missing_days':len(missing),
            'inserted_days':len(missing) if apply else 0,'dry_run':not apply}
