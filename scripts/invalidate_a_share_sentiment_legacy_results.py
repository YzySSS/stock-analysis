from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.db import mysql_conn  # noqa: E402
from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock  # noqa: E402
from app.shared.task_log import TaskRunLogger  # noqa: E402


TASK_NAME = "invalidate_a_share_sentiment_legacy_results"
LOCK_NAME = "invalidate_a_share_sentiment_legacy_results"
STRATEGY_ID = "a_share_sentiment"
CURRENT_STRATEGY_VERSION = "0.4.1"
INVALIDATION_REASON = "pre_v0.4.1_market_opinion_semantics_invalidated"


LEGACY_WHERE_SQL = """
strategy_id = %s
AND COALESCE(
    JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.strategy_version')),
    'legacy_unknown'
) <> %s
"""


def audit_legacy_results(conn: Any) -> dict[str, Any]:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                COUNT(*) AS result_count,
                SUM(CASE WHEN COALESCE(include_in_stats, 1) = 1 THEN 1 ELSE 0 END) AS included_count,
                MIN(trade_date) AS first_trade_date,
                MAX(trade_date) AS last_trade_date
            FROM selection_result
            WHERE {LEGACY_WHERE_SQL}
            """,
            (STRATEGY_ID, CURRENT_STRATEGY_VERSION),
        )
        summary = cursor.fetchone() or {}
        cursor.execute(
            f"""
            SELECT
                COALESCE(
                    JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.strategy_version')),
                    'legacy_unknown'
                ) AS strategy_version,
                COUNT(*) AS result_count,
                SUM(CASE WHEN COALESCE(include_in_stats, 1) = 1 THEN 1 ELSE 0 END) AS included_count
            FROM selection_result
            WHERE {LEGACY_WHERE_SQL}
            GROUP BY strategy_version
            ORDER BY strategy_version
            """,
            (STRATEGY_ID, CURRENT_STRATEGY_VERSION),
        )
        version_rows = cursor.fetchall() or []
    versions = [
        {
            "strategy_version": str(row.get("strategy_version") or "legacy_unknown"),
            "result_count": int(row.get("result_count") or 0),
            "included_count": int(row.get("included_count") or 0),
        }
        for row in version_rows
    ]
    first_trade_date = summary.get("first_trade_date")
    last_trade_date = summary.get("last_trade_date")
    return {
        "strategy_id": STRATEGY_ID,
        "current_strategy_version": CURRENT_STRATEGY_VERSION,
        "result_count": int(summary.get("result_count") or 0),
        "included_count": int(summary.get("included_count") or 0),
        "first_trade_date": str(first_trade_date) if first_trade_date else None,
        "last_trade_date": str(last_trade_date) if last_trade_date else None,
        "versions": versions,
    }


def invalidate_legacy_results(conn: Any, invalidated_at: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE selection_result
            SET metadata_json = JSON_SET(
                    COALESCE(metadata_json, JSON_OBJECT()),
                    '$.evidence_validity', 'invalidated',
                    '$.evidence_invalidated_reason', %s,
                    '$.evidence_invalidated_at', %s,
                    '$.legacy_include_in_stats_before_invalidation',
                    IF(
                        JSON_CONTAINS_PATH(
                            COALESCE(metadata_json, JSON_OBJECT()),
                            'one',
                            '$.legacy_include_in_stats_before_invalidation'
                        ),
                        JSON_EXTRACT(
                            metadata_json,
                            '$.legacy_include_in_stats_before_invalidation'
                        ),
                        COALESCE(include_in_stats, 1)
                    )
                ),
                include_in_stats = 0
            WHERE {LEGACY_WHERE_SQL}
            """,
            (
                INVALIDATION_REASON,
                invalidated_at,
                STRATEGY_ID,
                CURRENT_STRATEGY_VERSION,
            ),
        )
        return int(cursor.rowcount or 0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Preserve legacy A-share sentiment selections but remove pre-v0.4.1 "
            "rows from effective statistics. Dry-run is the default."
        )
    )
    parser.add_argument("--apply", action="store_true", help="apply the recoverable metadata/statistics invalidation")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    lock = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock is None:
        print(json.dumps({"status": "skipped", "reason": "lock_unavailable"}, ensure_ascii=False))
        return

    logger = TaskRunLogger() if args.apply else None
    run_id = f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    invalidated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with mysql_conn() as conn:
            before = audit_legacy_results(conn)
            if not args.apply:
                print(json.dumps({"status": "dry_run", "before": before}, ensure_ascii=False, default=str, indent=2))
                return

            logger.start(TASK_NAME, run_id, {"before": before, "reason": INVALIDATION_REASON})
            updated = invalidate_legacy_results(conn, invalidated_at)
        with mysql_conn() as conn:
            after = audit_legacy_results(conn)
        result = {
            "status": "success",
            "updated_rows": updated,
            "reason": INVALIDATION_REASON,
            "invalidated_at": invalidated_at,
            "before": before,
            "after": after,
        }
        logger.finish(TASK_NAME, run_id, "success", f"invalidated {updated} legacy sentiment rows", result)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    except Exception as exc:
        if logger is not None:
            logger.finish(TASK_NAME, run_id, "failed", str(exc), {"reason": INVALIDATION_REASON})
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock)
        if release_error:
            print(json.dumps({"lock_release_warning": release_error}, ensure_ascii=False), file=sys.stderr)


if __name__ == "__main__":
    main()
