from __future__ import annotations

import unittest
from unittest.mock import patch

from pymysql.err import OperationalError, ProgrammingError

from scripts.refresh_stock_technical_feature_daily import (
    is_retryable_refresh_error,
    main,
)


class TechnicalFeatureRefreshJobTests(unittest.TestCase):
    def test_only_transient_mysql_failures_are_retryable(self):
        self.assertTrue(is_retryable_refresh_error(OperationalError(2013, "timed out")))
        self.assertTrue(is_retryable_refresh_error(OperationalError(1213, "deadlock")))
        self.assertFalse(is_retryable_refresh_error(ProgrammingError(1064, "bad sql")))
        self.assertFalse(is_retryable_refresh_error(RuntimeError("bad data")))

    def test_transient_failure_retries_and_then_succeeds(self):
        service = patch("scripts.refresh_stock_technical_feature_daily.TechnicalFeatureDailyRefreshService")
        logger = patch("scripts.refresh_stock_technical_feature_daily.TaskRunLogger")
        with service as service_cls, logger as logger_cls, patch(
            "scripts.refresh_stock_technical_feature_daily.time.sleep"
        ) as sleep:
            service_cls.return_value.refresh.side_effect = [
                OperationalError(2013, "timed out"),
                {"status": "success", "published_rows": 5529},
            ]
            exit_code = main(["--attempts", "3", "--retry-seconds", "0"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(service_cls.return_value.refresh.call_count, 2)
        sleep.assert_called_once_with(0.0)
        payload = logger_cls.return_value.finish.call_args.kwargs["metadata"]
        self.assertEqual(payload["attempts_used"], 2)


if __name__ == "__main__":
    unittest.main()
