from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import date, datetime
from typing import Any, Callable

from app.shared.db import mysql_conn


ConnectionFactory = Callable[..., AbstractContextManager]


REFRESH_STOCK_TECHNICAL_FEATURE_SQL = """
INSERT INTO stock_technical_feature_daily (
    code, trade_date, source_trade_date, latest_close,
    ma5, ma10, ma20, ma30, ma60,
    close_5d, close_20d, prev_close_1d, max_close_20, min_close_20,
    pct_chg_1d, return_5d_pct, return_20d_pct, std_return_20,
    latest_amount, avg_amount_5, avg_amount_20, median_amount_20,
    amount_ratio_5_20, kline_count_20, kline_count_60, computed_at
)
WITH recent_dates AS (
    SELECT trade_date
    FROM daily_kline
    WHERE trade_date <= %s
    GROUP BY trade_date
    ORDER BY trade_date DESC
    LIMIT 90
),
ranked AS (
    SELECT
        dk.code,
        dk.trade_date,
        dk.close,
        dk.amount,
        LAG(dk.close) OVER (PARTITION BY dk.code ORDER BY dk.trade_date) AS prev_close,
        ROW_NUMBER() OVER (PARTITION BY dk.code ORDER BY dk.trade_date DESC) AS rn
    FROM daily_kline dk
    WHERE dk.trade_date >= (SELECT MIN(trade_date) FROM recent_dates)
      AND dk.trade_date <= %s
),
aggregated AS (
    SELECT
        code,
        MAX(CASE WHEN rn = 1 THEN trade_date END) AS source_trade_date,
        MAX(CASE WHEN rn = 1 THEN close END) AS latest_close,
        AVG(CASE WHEN rn <= 5 THEN close END) AS ma5,
        AVG(CASE WHEN rn <= 10 THEN close END) AS ma10,
        AVG(CASE WHEN rn <= 20 THEN close END) AS ma20,
        AVG(CASE WHEN rn <= 30 THEN close END) AS ma30,
        AVG(CASE WHEN rn <= 60 THEN close END) AS ma60,
        MAX(CASE WHEN rn = 6 THEN close END) AS close_5d,
        MAX(CASE WHEN rn = 20 THEN close END) AS close_20d,
        MAX(CASE WHEN rn = 21 THEN close END) AS return_base_20d,
        MAX(CASE WHEN rn = 2 THEN close END) AS prev_close_1d,
        MAX(CASE WHEN rn <= 20 THEN close END) AS max_close_20,
        MIN(CASE WHEN rn <= 20 THEN close END) AS min_close_20,
        MAX(CASE WHEN rn = 1 THEN amount END) AS latest_amount,
        AVG(CASE WHEN rn <= 5 THEN amount END) AS avg_amount_5,
        AVG(CASE WHEN rn <= 20 THEN amount END) AS avg_amount_20,
        SUM(CASE WHEN rn <= 20 THEN 1 ELSE 0 END) AS kline_count_20,
        SUM(CASE WHEN rn <= 60 THEN 1 ELSE 0 END) AS kline_count_60,
        STDDEV_SAMP(
            CASE
                WHEN rn <= 20 AND prev_close IS NOT NULL AND prev_close > 0
                THEN close / prev_close - 1
            END
        ) AS std_return_20,
        MAX(
            CASE
                WHEN rn = 1 AND prev_close IS NOT NULL AND prev_close > 0
                THEN (close - prev_close) / prev_close * 100
            END
        ) AS pct_chg_1d
    FROM ranked
    WHERE rn <= 60
    GROUP BY code
),
amount_ranked AS (
    SELECT
        code,
        amount,
        ROW_NUMBER() OVER (PARTITION BY code ORDER BY amount) AS amount_rank,
        COUNT(*) OVER (PARTITION BY code) AS amount_count
    FROM ranked
    WHERE rn <= 20 AND amount IS NOT NULL
),
medians AS (
    SELECT code, AVG(amount) AS median_amount_20
    FROM amount_ranked
    WHERE amount_rank IN (
        FLOOR((amount_count + 1) / 2),
        FLOOR((amount_count + 2) / 2)
    )
    GROUP BY code
)
SELECT
    aggregated.code,
    %s AS trade_date,
    aggregated.source_trade_date,
    aggregated.latest_close,
    aggregated.ma5,
    aggregated.ma10,
    aggregated.ma20,
    aggregated.ma30,
    aggregated.ma60,
    aggregated.close_5d,
    aggregated.close_20d,
    aggregated.prev_close_1d,
    aggregated.max_close_20,
    aggregated.min_close_20,
    aggregated.pct_chg_1d,
    CASE
        WHEN aggregated.close_5d > 0
        THEN (aggregated.latest_close / aggregated.close_5d - 1) * 100
    END AS return_5d_pct,
    CASE
        WHEN aggregated.return_base_20d > 0
        THEN (aggregated.latest_close / aggregated.return_base_20d - 1) * 100
    END AS return_20d_pct,
    aggregated.std_return_20,
    aggregated.latest_amount,
    aggregated.avg_amount_5,
    aggregated.avg_amount_20,
    medians.median_amount_20,
    aggregated.avg_amount_5 / NULLIF(aggregated.avg_amount_20, 0) AS amount_ratio_5_20,
    aggregated.kline_count_20,
    aggregated.kline_count_60,
    NOW()
FROM aggregated
LEFT JOIN medians ON medians.code = aggregated.code
ON DUPLICATE KEY UPDATE
    source_trade_date=VALUES(source_trade_date),
    latest_close=VALUES(latest_close),
    ma5=VALUES(ma5),
    ma10=VALUES(ma10),
    ma20=VALUES(ma20),
    ma30=VALUES(ma30),
    ma60=VALUES(ma60),
    close_5d=VALUES(close_5d),
    close_20d=VALUES(close_20d),
    prev_close_1d=VALUES(prev_close_1d),
    max_close_20=VALUES(max_close_20),
    min_close_20=VALUES(min_close_20),
    pct_chg_1d=VALUES(pct_chg_1d),
    return_5d_pct=VALUES(return_5d_pct),
    return_20d_pct=VALUES(return_20d_pct),
    std_return_20=VALUES(std_return_20),
    latest_amount=VALUES(latest_amount),
    avg_amount_5=VALUES(avg_amount_5),
    avg_amount_20=VALUES(avg_amount_20),
    median_amount_20=VALUES(median_amount_20),
    amount_ratio_5_20=VALUES(amount_ratio_5_20),
    kline_count_20=VALUES(kline_count_20),
    kline_count_60=VALUES(kline_count_60),
    computed_at=VALUES(computed_at)
"""


def _normalize_trade_date(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value).strip()).isoformat()


class TechnicalFeatureDailyRefreshService:
    """Build the reusable technical snapshot using only local daily bars."""

    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or mysql_conn

    def _connect(self):
        return self._connection_factory(dict_cursor=True)

    def refresh(self, trade_date: str | date | datetime | None = None) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                resolved_trade_date = self._resolve_trade_date(cursor, trade_date)
                if resolved_trade_date is None:
                    return {
                        "status": "no_data",
                        "trade_date": None,
                        "published_rows": 0,
                        "source": "daily_kline",
                    }

                cursor.execute(
                    REFRESH_STOCK_TECHNICAL_FEATURE_SQL,
                    (resolved_trade_date, resolved_trade_date, resolved_trade_date),
                )
                affected_rows = max(0, int(cursor.rowcount or 0))
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM stock_technical_feature_daily WHERE trade_date = %s",
                    (resolved_trade_date,),
                )
                published_rows = int((cursor.fetchone() or {}).get("count") or 0)

        return {
            "status": "success",
            "trade_date": resolved_trade_date,
            "published_rows": published_rows,
            "affected_rows": affected_rows,
            "source": "daily_kline",
        }

    @staticmethod
    def _resolve_trade_date(cursor: Any, requested: str | date | datetime | None) -> str | None:
        if requested is not None:
            return _normalize_trade_date(requested)
        cursor.execute("SELECT MAX(trade_date) AS trade_date FROM daily_kline")
        value = (cursor.fetchone() or {}).get("trade_date")
        return _normalize_trade_date(value) if value else None
