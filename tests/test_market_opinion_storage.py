from __future__ import annotations

import unittest

from app.data_ingestion.market_opinion_storage import classify_storage_reclamation


class MarketOpinionStorageDecisionTests(unittest.TestCase):
    def test_shared_tablespace_refuses_single_table_rebuild(self):
        result = classify_storage_reclamation(file_per_table=False, logical_cleanup_complete=True)

        self.assertEqual(result["status"], "shared_tablespace_reusable")
        self.assertFalse(result["local_rebuild_recommended"])
        self.assertFalse(result["optimize_table_recommended"])

    def test_file_per_table_still_requires_separate_maintenance_window(self):
        result = classify_storage_reclamation(file_per_table=True, logical_cleanup_complete=True)

        self.assertEqual(result["status"], "file_per_table_maintenance_window_required")
        self.assertFalse(result["local_rebuild_recommended"])
        self.assertIn("shadow rebuild", result["next_action"])

    def test_logical_cleanup_must_finish_before_storage_work(self):
        result = classify_storage_reclamation(file_per_table=False, logical_cleanup_complete=False)

        self.assertEqual(result["status"], "blocked_pending_logical_cleanup")
        self.assertIn("lifecycle", result["next_action"])


if __name__ == "__main__":
    unittest.main()
