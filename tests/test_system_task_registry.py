from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.api.routes.system import (
    TASK_NAME_LABELS,
    TASK_RUNNING_STALE_SECONDS,
    TASK_SCHEDULES,
    TRACKED_TASKS,
    _data_quality_status,
    _latest_task_runs,
)


class SystemTaskRegistryTests(unittest.TestCase):
    def test_schedule_registry_is_unique_and_covers_high_frequency_tasks(self):
        self.assertEqual(len(TRACKED_TASKS), len(set(TRACKED_TASKS)))
        self.assertEqual(set(TRACKED_TASKS), set(TASK_NAME_LABELS))
        self.assertGreaterEqual(len(TASK_SCHEDULES), 24)
        self.assertTrue(
            {
                "stock_realtime_snapshot_update",
                "market_fund_flow_update",
                "stock_realtime_moneyflow_update",
                "ths_concept_hot_update",
                "stock_popularity_update",
                "portfolio_etf_quote_update",
                "data_quality_audit",
                "fundamental_pit_backfill",
                "index_constituent_pit_backfill",
                "automatic_strategy_observation",
                "strategy_forward_outcome_update",
                "adj_factor_history_backfill",
                "stock_technical_feature_daily_refresh",
                "sentiment_candidate_snapshot_materialize",
                "operational_read_models_refresh",
            }.issubset(TRACKED_TASKS)
        )

    def test_technical_feature_refresh_is_in_print_only_cron_output(self):
        source = Path("scripts/setup_kline_cron.sh").read_text(encoding="utf-8")
        self.assertIn('TECHNICAL_FEATURE_JOB="5 2 * * *', source)
        self.assertIn("refresh_stock_technical_feature_daily.py", source)
        self.assertIn("stock_technical_feature_daily_refresh.log", source)
        self.assertIn('"$TECHNICAL_FEATURE_JOB"', source)
        self.assertIn("materialize_sentiment_candidate_snapshot.py", source)
        self.assertIn("materialize_sentiment_candidate_snapshot.py --dual-run", source)
        self.assertIn("sentiment_candidate_snapshot_materialize.log", source)
        self.assertIn('"$SENTIMENT_SNAPSHOT_JOB"', source)
        self.assertIn("refresh_operational_read_models.py", source)
        self.assertIn("operational_read_models_refresh.log", source)
        self.assertIn('"$OPERATIONAL_READ_MODELS_JOB"', source)
        self.assertIn('AUTOMATIC_OBSERVATION_JOB="25 9 * * 1-5', source)
        self.assertIn("run_automatic_strategy_observation.py", source)
        self.assertIn("automatic_strategy_observation.log", source)
        self.assertIn('"$AUTOMATIC_OBSERVATION_JOB"', source)
        self.assertNotIn('FORWARD_OBSERVATION_JOB="20 16', source)

    def test_old_running_task_is_exposed_as_stale(self):
        conn_context = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        conn_context.__enter__.return_value = conn
        conn.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchall.return_value = [
            {
                "task_name": "stock_popularity_update",
                "run_id": "stale-run",
                "status": "running",
                "started_at": "2026-07-15 10:00:00",
                "finished_at": None,
                "message": None,
                "metadata_json": "{}",
                "running_age_seconds": TASK_RUNNING_STALE_SECONDS + 1,
            }
        ]

        with patch("app.api.routes.system.mysql_read_conn", return_value=conn_context):
            items = _latest_task_runs()

        self.assertEqual(items[0]["status"], "stale")
        self.assertEqual(items[0]["recorded_status"], "running")
        self.assertTrue(items[0]["stale"])

    def test_data_quality_status_reuses_persisted_task_metadata(self):
        payload = _data_quality_status(
            [
                {
                    "task_name": "data_quality_audit",
                    "status": "partial_success",
                    "run_id": "dq-1",
                    "started_at": "2026-07-17 04:05:00",
                    "finished_at": "2026-07-17 04:05:03",
                    "message": "completed with warnings",
                    "metadata": {
                        "health": "warning",
                        "status": "warn",
                        "counts": {"pass": 8, "warn": 3, "fail": 0},
                        "checks": [{"check_id": "daily_kline_coverage", "status": "warn"}],
                    },
                }
            ]
        )

        self.assertEqual(payload["health"], "warning")
        self.assertEqual(payload["task_status"], "partial_success")
        self.assertEqual(payload["counts"]["warn"], 3)


if __name__ == "__main__":
    unittest.main()
