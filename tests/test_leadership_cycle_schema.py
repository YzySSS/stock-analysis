from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.orchestration.leadership_cycle_schema import (
    ensure_leadership_cycle_schema,
)


class LeadershipCycleSchemaTests(unittest.TestCase):
    def test_adds_cycle_evidence_columns_and_expands_label(self) -> None:
        connection_factory = MagicMock()
        cursor = (
            connection_factory.return_value.__enter__.return_value
            .cursor.return_value.__enter__.return_value
        )
        cursor.fetchall.return_value = [
            ("model_id", "varchar(64)"),
            ("state_label", "varchar(16)"),
            ("source_lineage_json", "json"),
        ]

        with patch(
            "app.orchestration.leadership_cycle_schema.mysql_conn",
            connection_factory,
        ):
            result = ensure_leadership_cycle_schema()

        statements = [
            str(call.args[0]).strip()
            for call in cursor.execute.call_args_list
        ]
        alter_sql = "\n".join(
            statement for statement in statements if statement.startswith("ALTER")
        )
        self.assertIn("MODIFY COLUMN state_label VARCHAR(32)", alter_sql)
        self.assertIn("ADD COLUMN cycle_state", alter_sql)
        self.assertIn("ADD COLUMN price_metrics_json", alter_sql)
        self.assertIn("ADD COLUMN breadth_metrics_json", alter_sql)
        self.assertIn("ADD COLUMN data_quality_json", alter_sql)
        self.assertIn("cycle_state", result["added"])


if __name__ == "__main__":
    unittest.main()
