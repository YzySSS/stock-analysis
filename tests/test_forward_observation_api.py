from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi import HTTPException

from app.api.routes.strategies import (
    ForwardActionRequest,
    get_forward_evidence,
    record_forward_action,
)


class ForwardObservationApiTests(unittest.TestCase):
    def test_forward_evidence_endpoint_returns_service_payload(self):
        payload = {
            "status": "collecting",
            "validation_status": "unvalidated",
            "strategy_id": "a_share_sentiment",
        }
        with patch("app.api.routes.strategies.ForwardObservationService") as service_cls:
            service_cls.return_value.evidence_summary.return_value = payload

            result = get_forward_evidence("a_share_sentiment")

        self.assertEqual(result, {"forward_evidence": payload})
        service_cls.return_value.evidence_summary.assert_called_once_with("a_share_sentiment")

    def test_forward_action_endpoint_persists_supported_action(self):
        action = {"id": 7, "action_type": "saved"}
        request = ForwardActionRequest(
            observation_id="obs-v1",
            code="sh.600000",
            action_type="saved",
            note="继续观察",
        )
        with patch("app.api.routes.strategies.ForwardObservationRepository") as repository_cls:
            repository_cls.return_value.record_action.return_value = action

            result = record_forward_action(request)

        self.assertEqual(result, {"action": action})
        repository_cls.return_value.record_action.assert_called_once_with(
            observation_id="obs-v1",
            code="sh.600000",
            action_type="saved",
            action_price=None,
            note="继续观察",
        )

    def test_forward_action_endpoint_maps_validation_error_to_400(self):
        request = ForwardActionRequest(
            observation_id="obs-v1",
            code="sh.600000",
            action_type="unknown",
        )
        with patch("app.api.routes.strategies.ForwardObservationRepository") as repository_cls:
            repository_cls.return_value.record_action.side_effect = ValueError("unsupported forward action")

            with self.assertRaises(HTTPException) as raised:
                record_forward_action(request)

        self.assertEqual(raised.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
