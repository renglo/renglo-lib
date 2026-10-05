"""Peer entry must call describe() when the payload asks for it. No AWS."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.schd.peer_lambda import lambda_handler  # noqa: E402


class _Report:
    def __init__(self) -> None:
        self.describe_payload = None
        self.ran = False

    def describe(self, payload):
        self.describe_payload = dict(payload)
        return {
            "success": True,
            "action": "describe",
            "output": {"described": True, "handler": "client_production_report"},
        }

    def run(self, payload):
        self.ran = True
        return {"success": True, "action": "run", "output": {"rows": []}}


class _NoDescribe:
    def __init__(self) -> None:
        self.ran = False

    def run(self, payload):
        self.ran = True
        return {"success": True, "output": {"rows": []}}


class PeerDescribeTests(unittest.TestCase):
    def test_describe_flag_calls_describe_not_run(self) -> None:
        handler = _Report()
        with patch("renglo.schd.peer_lambda.get_handler", return_value=handler):
            response = lambda_handler(
                {
                    "handler": "client_production_report",
                    "payload": {
                        "_describe": True,
                        "_stack": True,
                        "_jwt_claims": {"sub": "user"},
                        "portfolio": "p",
                    },
                },
                None,
            )
        self.assertEqual(response["statusCode"], 200)
        self.assertTrue(response["body"]["output"]["described"])
        self.assertFalse(handler.ran)
        self.assertNotIn("_describe", handler.describe_payload)
        self.assertNotIn("_stack", handler.describe_payload)
        self.assertNotIn("_jwt_claims", handler.describe_payload)
        self.assertEqual(handler.describe_payload["portfolio"], "p")

    def test_string_true_is_describe(self) -> None:
        handler = _Report()
        with patch("renglo.schd.peer_lambda.get_handler", return_value=handler):
            response = lambda_handler(
                {"handler": "client_production_report", "payload": {"_describe": "true"}},
                None,
            )
        self.assertTrue(response["body"]["output"]["described"])
        self.assertFalse(handler.ran)

    def test_missing_describe_does_not_run(self) -> None:
        handler = _NoDescribe()
        with patch("renglo.schd.peer_lambda.get_handler", return_value=handler):
            response = lambda_handler(
                {"handler": "demo", "payload": {"_describe": True}},
                None,
            )
        self.assertEqual(response["statusCode"], 200)
        self.assertFalse(response["body"]["output"]["described"])
        self.assertFalse(handler.ran)

    def test_without_flag_calls_run(self) -> None:
        handler = _Report()
        with patch("renglo.schd.peer_lambda.get_handler", return_value=handler):
            response = lambda_handler(
                {"handler": "client_production_report", "payload": {"period": "yesterday"}},
                None,
            )
        self.assertTrue(handler.ran)
        self.assertEqual(response["body"]["action"], "run")


if __name__ == "__main__":
    unittest.main()
