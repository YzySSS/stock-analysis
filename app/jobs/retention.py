from __future__ import annotations

from dataclasses import asdict, dataclass

from app.jobs.errors import error_fingerprint, infer_error_code, sanitize_error_message
from app.shared.db import mysql_conn


TERMINAL_JOB_STATUSES = ("success", "succeeded", "failed", "cancelled")


@dataclass(frozen=True)
class JobRetentionPolicy:
    task_detail_days: int = 90
    selection_task_days: int = 90
    backtest_system_test_days: int = 90
    portfolio_raw_response_days: int = 30
    portfolio_snapshot_days: int = 90
    error_summary_days: int = 365
    obsolete_worker_days: int = 30
    abandoned_task_hours: int = 24

    def validate(self) -> None:
        for field, value in asdict(self).items():
            if value < 1:
                raise ValueError(f"{field} must be positive")


class JobRetentionService:
    """Apply bounded retention without deleting formal product history.

    Formal backtests, validation baselines, portfolio advice summaries/outcomes,
    and saved selection results remain intact. Only replaceable queue details,
    old system smoke tests, and large expired AI payloads are pruned.
    """

    def __init__(self, policy: JobRetentionPolicy | None = None) -> None:
        self.policy = policy or JobRetentionPolicy()
        self.policy.validate()

    def preview(self) -> dict:
        queries = {
            "abandoned_task_runs": (
                """
                SELECT COUNT(*) AS count
                FROM task_run_log
                WHERE status='running'
                  AND started_at < DATE_SUB(NOW(), INTERVAL %s HOUR)
                """,
                (self.policy.abandoned_task_hours,),
            ),
            "task_run_log_delete": (
                """
                SELECT COUNT(*) AS count
                FROM task_run_log
                WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                  AND id NOT IN (
                      SELECT max_id FROM (
                          SELECT MAX(id) AS max_id FROM task_run_log GROUP BY task_name
                      ) latest_per_task
                  )
                """,
                (self.policy.task_detail_days,),
            ),
            "selection_run_delete": (
                """
                SELECT COUNT(*) AS count
                FROM selection_run
                WHERE status IN ('success','failed','cancelled')
                  AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                """,
                (self.policy.selection_task_days,),
            ),
            "backtest_system_test_delete": (
                """
                SELECT COUNT(*) AS count
                FROM backtest_run
                WHERE is_system_test=1
                  AND validation_baseline_id IS NULL
                  AND status IN ('success','failed','cancelled')
                  AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                """,
                (self.policy.backtest_system_test_days,),
            ),
            "portfolio_raw_response_prune": (
                """
                SELECT COUNT(*) AS count
                FROM portfolio_advice_run
                WHERE raw_response IS NOT NULL
                  AND status IN ('succeeded','failed','cancelled')
                  AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                """,
                (self.policy.portfolio_raw_response_days,),
            ),
            "portfolio_snapshot_prune": (
                """
                SELECT COUNT(*) AS count
                FROM portfolio_advice_run
                WHERE (input_snapshot_json IS NOT NULL OR error_message IS NOT NULL)
                  AND status IN ('succeeded','failed','cancelled')
                  AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                """,
                (self.policy.portfolio_snapshot_days,),
            ),
            "error_summary_delete": (
                """
                SELECT COUNT(*) AS count
                FROM job_error_daily_summary
                WHERE error_date < DATE_SUB(CURDATE(), INTERVAL %s DAY)
                """,
                (self.policy.error_summary_days,),
            ),
        }
        counts: dict[str, int] = {}
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                for key, (sql, params) in queries.items():
                    cursor.execute(sql, params)
                    counts[key] = int((cursor.fetchone() or {}).get("count") or 0)
        return {
            "mode": "dry_run",
            "policy": asdict(self.policy),
            "candidates": counts,
            "protected": {
                "formal_backtests": True,
                "validation_baselines": True,
                "validation_protocols": True,
                "portfolio_advice_summaries": True,
                "portfolio_advice_outcomes": True,
                "saved_selection_results": True,
            },
        }

    def apply(self) -> dict:
        before = self.preview()
        changed: dict[str, int] = {}
        changed["abandoned_task_runs"] = self._mark_abandoned_task_runs()
        changed["task_run_error_fields_backfilled"] = self._backfill_task_error_fields()
        changed["task_daily_summary_rows"] = self._aggregate_task_daily_summary()
        changed["scheduled_error_summary_rows"] = self._aggregate_scheduled_errors()
        changed["worker_error_summary_rows"] = self._aggregate_worker_errors()
        changed["task_run_log_deleted"] = self._delete_old_task_details()
        changed["selection_run_deleted"] = self._delete_old_selection_tasks()
        changed.update(self._delete_old_backtest_system_tests())
        changed["portfolio_raw_response_pruned"] = self._prune_portfolio_raw_responses()
        changed["portfolio_snapshot_pruned"] = self._prune_portfolio_snapshots()
        changed["obsolete_worker_rows_deleted"] = self._delete_obsolete_worker_rows()
        changed["error_summary_deleted"] = self._delete_old_error_summaries()
        return {
            "mode": "apply",
            "policy": asdict(self.policy),
            "preview": before["candidates"],
            "changed": changed,
            "protected": before["protected"],
        }

    def _execute(self, sql: str, params: tuple = ()) -> int:
        with mysql_conn(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return int(cursor.rowcount)

    def _mark_abandoned_task_runs(self) -> int:
        message = "task logger entry abandoned without a terminal update"
        return self._execute(
            """
            UPDATE task_run_log
            SET status='failed', finished_at=NOW(), message=%s,
                error_code='abandoned_task_log', error_fingerprint=%s
            WHERE status='running'
              AND started_at < DATE_SUB(NOW(), INTERVAL %s HOUR)
            """,
            (message, error_fingerprint(message), self.policy.abandoned_task_hours),
        )

    def _backfill_task_error_fields(self) -> int:
        return self._execute(
            """
            UPDATE task_run_log
            SET error_code=COALESCE(error_code, 'legacy_task_failed'),
                error_fingerprint=COALESCE(error_fingerprint, SHA2(COALESCE(message, ''), 256))
            WHERE status IN ('failed','killed')
              AND (error_code IS NULL OR error_fingerprint IS NULL)
            """
        )

    def _aggregate_task_daily_summary(self) -> int:
        return self._execute(
            """
            INSERT INTO task_run_daily_summary (
                run_date, task_name, status, run_count, first_started_at, last_finished_at
            )
            SELECT DATE(started_at), task_name, status, COUNT(*), MIN(started_at), MAX(finished_at)
            FROM task_run_log
            GROUP BY DATE(started_at), task_name, status
            ON DUPLICATE KEY UPDATE
                run_count=VALUES(run_count),
                first_started_at=LEAST(first_started_at, VALUES(first_started_at)),
                last_finished_at=CASE
                    WHEN VALUES(last_finished_at) IS NULL THEN last_finished_at
                    WHEN last_finished_at IS NULL THEN VALUES(last_finished_at)
                    ELSE GREATEST(last_finished_at, VALUES(last_finished_at))
                END
            """
        )

    def _aggregate_scheduled_errors(self) -> int:
        return self._execute(
            """
            INSERT INTO job_error_daily_summary (
                error_date, source_kind, job_type, error_code, error_fingerprint,
                occurrence_count, first_seen_at, last_seen_at, last_message
            )
            SELECT
                DATE(COALESCE(finished_at, started_at)), 'scheduled_task', task_name,
                COALESCE(error_code, 'legacy_task_failed'),
                COALESCE(error_fingerprint, SHA2(COALESCE(message, ''), 256)),
                COUNT(*), MIN(started_at), MAX(COALESCE(finished_at, started_at)),
                MAX(LEFT(COALESCE(message, ''), 500))
            FROM task_run_log
            WHERE status IN ('failed','killed')
            GROUP BY DATE(COALESCE(finished_at, started_at)), task_name,
                     COALESCE(error_code, 'legacy_task_failed'),
                     COALESCE(error_fingerprint, SHA2(COALESCE(message, ''), 256))
            ON DUPLICATE KEY UPDATE
                occurrence_count=GREATEST(occurrence_count, VALUES(occurrence_count)),
                first_seen_at=LEAST(first_seen_at, VALUES(first_seen_at)),
                last_seen_at=GREATEST(last_seen_at, VALUES(last_seen_at)),
                last_message=VALUES(last_message)
            """
        )

    def _aggregate_worker_errors(self) -> int:
        total = 0
        sources = (
            ("selection", "selection_run"),
            ("portfolio_advice", "portfolio_advice_run"),
            ("backtest", "backtest_run"),
        )
        for source_kind, table in sources:
            with mysql_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT error_code, error_message, created_at, finished_at
                        FROM {table}
                        WHERE status='failed'
                        """
                    )
                    rows = cursor.fetchall() or []

            groups: dict[tuple, dict] = {}
            for row in rows:
                seen_at = row.get("finished_at") or row.get("created_at")
                if not seen_at:
                    continue
                message = sanitize_error_message(row.get("error_message"))
                code = str(row.get("error_code") or infer_error_code(message, "legacy_job_failed"))[:64]
                fingerprint = error_fingerprint(message)
                key = (seen_at.date(), source_kind, source_kind, code, fingerprint)
                group = groups.setdefault(
                    key,
                    {
                        "count": 0,
                        "first_seen_at": row.get("created_at") or seen_at,
                        "last_seen_at": seen_at,
                        "last_message": message,
                    },
                )
                group["count"] += 1
                if (row.get("created_at") or seen_at) < group["first_seen_at"]:
                    group["first_seen_at"] = row.get("created_at") or seen_at
                if seen_at >= group["last_seen_at"]:
                    group["last_seen_at"] = seen_at
                    group["last_message"] = message

            if not groups:
                continue
            values = [
                (*key, item["count"], item["first_seen_at"], item["last_seen_at"], item["last_message"])
                for key, item in groups.items()
            ]
            with mysql_conn(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """
                INSERT INTO job_error_daily_summary (
                    error_date, source_kind, job_type, error_code, error_fingerprint,
                    occurrence_count, first_seen_at, last_seen_at, last_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    occurrence_count=GREATEST(occurrence_count, VALUES(occurrence_count)),
                    first_seen_at=LEAST(first_seen_at, VALUES(first_seen_at)),
                    last_seen_at=GREATEST(last_seen_at, VALUES(last_seen_at)),
                    last_message=VALUES(last_message)
                        """,
                        values,
                    )
                    total += int(cursor.rowcount)
        return total

    def _delete_old_task_details(self) -> int:
        return self._execute(
            """
            DELETE FROM task_run_log
            WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
              AND id NOT IN (
                  SELECT max_id FROM (
                      SELECT MAX(id) AS max_id FROM task_run_log GROUP BY task_name
                  ) latest_per_task
              )
            """,
            (self.policy.task_detail_days,),
        )

    def _delete_old_selection_tasks(self) -> int:
        return self._execute(
            """
            DELETE FROM selection_run
            WHERE status IN ('success','failed','cancelled')
              AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """,
            (self.policy.selection_task_days,),
        )

    def _delete_old_backtest_system_tests(self) -> dict[str, int]:
        condition = """
            br.is_system_test=1
            AND br.validation_baseline_id IS NULL
            AND br.status IN ('success','failed','cancelled')
            AND br.created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
        """
        changed: dict[str, int] = {}
        for table in (
            "backtest_trade_order",
            "backtest_trade_analysis",
            "backtest_pick",
            "backtest_trade",
            "backtest_summary_daily",
        ):
            changed[f"{table}_deleted"] = self._execute(
                f"""
                DELETE child
                FROM {table} child
                INNER JOIN backtest_run br ON br.run_id=child.run_id
                WHERE {condition}
                """,
                (self.policy.backtest_system_test_days,),
            )
        changed["backtest_system_test_deleted"] = self._execute(
            f"DELETE br FROM backtest_run br WHERE {condition}",
            (self.policy.backtest_system_test_days,),
        )
        return changed

    def _prune_portfolio_raw_responses(self) -> int:
        return self._execute(
            """
            UPDATE portfolio_advice_run
            SET raw_response=NULL
            WHERE raw_response IS NOT NULL
              AND status IN ('succeeded','failed','cancelled')
              AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """,
            (self.policy.portfolio_raw_response_days,),
        )

    def _prune_portfolio_snapshots(self) -> int:
        return self._execute(
            """
            UPDATE portfolio_advice_run
            SET input_snapshot_json=NULL, error_message=NULL
            WHERE (input_snapshot_json IS NOT NULL OR error_message IS NOT NULL)
              AND status IN ('succeeded','failed','cancelled')
              AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """,
            (self.policy.portfolio_snapshot_days,),
        )

    def _delete_obsolete_worker_rows(self) -> int:
        return self._execute(
            """
            DELETE old
            FROM worker_runtime_heartbeat old
            INNER JOIN worker_runtime_heartbeat newer
              ON newer.worker_type=old.worker_type
             AND (
                  newer.heartbeat_at > old.heartbeat_at
                  OR (newer.heartbeat_at=old.heartbeat_at AND newer.worker_id > old.worker_id)
             )
            WHERE old.heartbeat_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """,
            (self.policy.obsolete_worker_days,),
        )

    def _delete_old_error_summaries(self) -> int:
        return self._execute(
            """
            DELETE FROM job_error_daily_summary
            WHERE error_date < DATE_SUB(CURDATE(), INTERVAL %s DAY)
            """,
            (self.policy.error_summary_days,),
        )
