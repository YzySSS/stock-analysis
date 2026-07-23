from __future__ import annotations

from app.shared.db import mysql_conn


STATS_REINCLUDED_REASON = "all_saved_selections_within_14_days_across_versions"
LEGACY_INVALIDATION_REASONS = (
    "pre_v0.4.0_market_opinion_semantics_invalidated",
    "pre_v0.4.4_market_opinion_semantics_invalidated",
)


def ensure_selection_result_version_schema() -> dict:
    """Persist strategy lineage and restore recoverable in-window statistics."""

    applied_columns: list[str] = []
    version_backfilled = 0
    stats_reincluded = 0
    with mysql_conn(dict_cursor=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM selection_result")
            rows = cursor.fetchall() or []
            columns = {str(row[0]): row for row in rows}

            version_column = columns.get("strategy_version")
            if version_column is None:
                cursor.execute(
                    "ALTER TABLE selection_result "
                    "ADD COLUMN strategy_version VARCHAR(32) DEFAULT NULL AFTER strategy_id"
                )
                applied_columns.append("strategy_version")

            cursor.execute(
                """
                UPDATE selection_result
                SET strategy_version = COALESCE(
                    NULLIF(strategy_version, ''),
                    NULLIF(
                        JSON_UNQUOTE(
                            JSON_EXTRACT(metadata_json, '$.strategy_version')
                        ),
                        ''
                    ),
                    'unknown'
                )
                WHERE strategy_version IS NULL OR strategy_version = ''
                """
            )
            version_backfilled = int(cursor.rowcount or 0)

            if version_column is None or str(version_column[2]).upper() == "YES":
                cursor.execute(
                    "ALTER TABLE selection_result "
                    "MODIFY COLUMN strategy_version VARCHAR(32) NOT NULL"
                )

            placeholders = ", ".join(["%s"] * len(LEGACY_INVALIDATION_REASONS))
            cursor.execute(
                f"""
                UPDATE selection_result
                SET include_in_stats = 1,
                    metadata_json = JSON_SET(
                        COALESCE(metadata_json, JSON_OBJECT()),
                        '$.stats_reincluded_reason', %s,
                        '$.stats_reincluded_at',
                        DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:%%i:%%s')
                    )
                WHERE strategy_id = 'a_share_sentiment'
                  AND COALESCE(include_in_stats, 1) = 0
                  AND created_at >= DATE_SUB(NOW(), INTERVAL 14 DAY)
                  AND JSON_UNQUOTE(
                        JSON_EXTRACT(
                            metadata_json,
                            '$.legacy_include_in_stats_before_invalidation'
                        )
                      ) = '1'
                  AND JSON_UNQUOTE(
                        JSON_EXTRACT(
                            metadata_json,
                            '$.evidence_invalidated_reason'
                        )
                      ) IN ({placeholders})
                """,
                (STATS_REINCLUDED_REASON, *LEGACY_INVALIDATION_REASONS),
            )
            stats_reincluded = int(cursor.rowcount or 0)

    return {
        "status": "ok",
        "tables": ["selection_result"],
        "applied_columns": applied_columns,
        "strategy_versions_backfilled": version_backfilled,
        "stats_reincluded": stats_reincluded,
        "stats_window_days": 14,
    }
