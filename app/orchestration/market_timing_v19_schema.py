from __future__ import annotations

from app.shared.db import mysql_conn


TABLE_NAME = "market_timing_indicator_daily"
MODEL_ID = "huatai_multidim_v18"
MODEL_VERSION = "v1.8"


def _column_names(cursor) -> set[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """,
        (TABLE_NAME,),
    )
    return {str(row["COLUMN_NAME"]) for row in (cursor.fetchall() or [])}


def _index_columns(cursor) -> dict[str, tuple[str, ...]]:
    cursor.execute(
        """
        SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """,
        (TABLE_NAME,),
    )
    grouped: dict[str, list[tuple[int, str]]] = {}
    for row in cursor.fetchall() or []:
        grouped.setdefault(str(row["INDEX_NAME"]), []).append(
            (int(row["SEQ_IN_INDEX"]), str(row["COLUMN_NAME"]))
        )
    return {
        name: tuple(column for _, column in sorted(items))
        for name, items in grouped.items()
    }


def ensure_market_timing_v19_schema() -> dict:
    with mysql_conn() as conn:
        with conn.cursor() as cursor:
            columns = _column_names(cursor)
            if "model_id" not in columns:
                cursor.execute(
                    """
                    ALTER TABLE market_timing_indicator_daily
                    ADD COLUMN model_id VARCHAR(64) NOT NULL
                        DEFAULT 'huatai_multidim_v18'
                        AFTER index_code
                    """
                )
            if "version" not in columns:
                cursor.execute(
                    """
                    ALTER TABLE market_timing_indicator_daily
                    ADD COLUMN version VARCHAR(32) NOT NULL
                        DEFAULT 'v1.8'
                        AFTER model_id
                    """
                )

            cursor.execute(
                """
                UPDATE market_timing_indicator_daily
                SET model_id = %s, version = %s
                WHERE model_id IS NULL OR model_id = '' OR version IS NULL OR version = ''
                """,
                (MODEL_ID, MODEL_VERSION),
            )

            indexes = _index_columns(cursor)
            desired_unique = ("trade_date", "index_code", "model_id", "indicator_id")
            existing_unique = indexes.get("uniq_market_timing_indicator")
            if existing_unique != desired_unique:
                if existing_unique:
                    cursor.execute(
                        """
                        ALTER TABLE market_timing_indicator_daily
                        DROP INDEX uniq_market_timing_indicator,
                        ADD UNIQUE KEY uniq_market_timing_indicator
                            (trade_date, index_code, model_id, indicator_id)
                        """
                    )
                else:
                    cursor.execute(
                        """
                        ALTER TABLE market_timing_indicator_daily
                        ADD UNIQUE KEY uniq_market_timing_indicator
                            (trade_date, index_code, model_id, indicator_id)
                        """
                    )

            indexes = _index_columns(cursor)
            if "idx_market_timing_indicator_model_date" not in indexes:
                cursor.execute(
                    """
                    ALTER TABLE market_timing_indicator_daily
                    ADD KEY idx_market_timing_indicator_model_date
                        (model_id, index_code, trade_date)
                    """
                )

    return {
        "status": "ok",
        "table": TABLE_NAME,
        "columns": ["model_id", "version"],
        "unique_key": ["trade_date", "index_code", "model_id", "indicator_id"],
    }
