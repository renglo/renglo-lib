#!/usr/bin/env python3
"""Handle → peer routing (peer-routes only; overflow fallback removed)."""

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.schd.external_handlers_config import (  # noqa: E402
    get_ecs_config,
    get_lambda_config,
    get_peer_route,
    load_extension_config,
    peer_routing_enabled,
    resolve_handlers_docker_image_base,
    _resolve_handlers_lambda_function_name,
)

LAB_ARN = "arn:aws:lambda:us-east-1:123:function:arbitium0813-peer-lab"
TRIAGE_ARN = "arn:aws:lambda:eu-west-1:123:function:arbitium0813-peer-triage"

TWO_PEERS = json.dumps(
    {
        "arbitiumlab": {
            "lambda_arn": LAB_ARN,
            "ecs_cluster": "arbitium0813-peer-lab",
            "ecs_task_definition": "arbitium0813-peer-lab-ecs",
            "ecs_results_bucket": "arbitium0813-peer-lab-ecs-123",
            "region": "us-east-1",
            "subnets": ["subnet-lab"],
            "security_groups": ["sg-lab"],
        },
        "arbitiumtriage": {
            "lambda_arn": TRIAGE_ARN,
            "ecs_cluster": "arbitium0813-peer-triage",
            "ecs_task_definition": "arbitium0813-peer-triage-ecs",
            "ecs_results_bucket": "arbitium0813-peer-triage-ecs-123",
            "region": "eu-west-1",
            "subnets": ["subnet-triage"],
            "security_groups": ["sg-triage"],
        },
    }
)

SAME_PEER = json.dumps(
    {
        "arbitiumlab": {
            "lambda_arn": LAB_ARN,
            "ecs_cluster": "shared-lab",
            "ecs_results_bucket": "b",
            "subnets": ["subnet-shared"],
            "security_groups": ["sg-shared"],
        },
        "other": {
            "lambda_arn": LAB_ARN,
            "ecs_cluster": "shared-lab",
            "ecs_results_bucket": "b",
            "subnets": ["subnet-shared"],
            "security_groups": ["sg-shared"],
        },
    }
)


class PeerRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = patch.dict(
            os.environ,
            {
                "EXTERNAL_HANDLERS": "arbitiumlab,arbitiumtriage",
                "AWS_REGION": "us-east-1",
            },
            clear=False,
        )
        self._env.start()
        for key in (
            "EXTERNAL_HANDLERS_PEER_MAP",
            "EXTERNAL_HANDLERS_PEER_ROUTING",
        ):
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        self._env.stop()

    def test_empty_map_uses_handle_convention(self) -> None:
        self.assertTrue(peer_routing_enabled())
        self.assertIsNone(get_peer_route("arbitiumlab"))
        self.assertEqual(
            _resolve_handlers_lambda_function_name("arbitiumlab"),
            "arbitiumlab-handlers",
        )
        self.assertEqual(
            _resolve_handlers_lambda_function_name("arbitiumtriage"),
            "arbitiumtriage-handlers",
        )
        cfg = get_lambda_config("arbitiumlab")
        self.assertEqual(cfg["function_name"], "arbitiumlab-handlers")
        self.assertIsNone(get_ecs_config("arbitiumlab"))
        self.assertEqual(
            resolve_handlers_docker_image_base("arbitiumlab"),
            "arbitiumlab-lambda-builder",
        )

    def test_peer_ecs_ignores_overflow_launch_type(self) -> None:
        os.environ["ECS_LAUNCH_TYPE"] = "ec2"
        os.environ["ECS_NETWORK_MODE"] = "bridge"
        os.environ["ECS_CLUSTER"] = "overflow-handlers"
        os.environ["EXTERNAL_HANDLERS_PEER_MAP"] = json.dumps(
            {
                "arbitiumtriage": {
                    "lambda_arn": TRIAGE_ARN,
                    "ecs_cluster": "arbitium0813-peer-lab",
                    "ecs_task_definition": "arbitium0813-peer-lab-ecs",
                    "ecs_results_bucket": "arbitium0813-peer-lab-ecs-123",
                    "region": "us-east-1",
                    "subnets": ["subnet-peer"],
                    "security_groups": ["sg-peer"],
                }
            }
        )
        ecs = get_ecs_config("arbitiumtriage")
        self.assertIsNotNone(ecs)
        assert ecs is not None
        self.assertEqual(ecs["launch_type"], "fargate")
        self.assertEqual(ecs["network_mode"], "awsvpc")
        self.assertEqual(ecs["subnets"], ["subnet-peer"])
        self.assertEqual(ecs["security_groups"], ["sg-peer"])

    def test_two_handles_two_peers(self) -> None:
        os.environ["EXTERNAL_HANDLERS_PEER_MAP"] = TWO_PEERS
        self.assertEqual(
            _resolve_handlers_lambda_function_name("arbitiumlab"),
            "arbitium0813-peer-lab",
        )
        self.assertEqual(
            _resolve_handlers_lambda_function_name("arbitiumtriage"),
            "arbitium0813-peer-triage",
        )
        self.assertEqual(
            resolve_handlers_docker_image_base("arbitiumlab"),
            "arbitium0813-peer-lab-lambda-builder",
        )
        self.assertEqual(
            resolve_handlers_docker_image_base("arbitiumtriage"),
            "arbitium0813-peer-triage-lambda-builder",
        )
        lab = get_ecs_config("arbitiumlab")
        triage = get_ecs_config("arbitiumtriage")
        self.assertEqual(lab["cluster"], "arbitium0813-peer-lab")
        self.assertEqual(triage["cluster"], "arbitium0813-peer-triage")
        self.assertEqual(triage["region"], "eu-west-1")
        self.assertEqual(load_extension_config("arbitiumtriage")["lambda_region"], "eu-west-1")

    def test_two_handles_same_peer(self) -> None:
        os.environ["EXTERNAL_HANDLERS"] = "arbitiumlab,other"
        os.environ["EXTERNAL_HANDLERS_PEER_MAP"] = SAME_PEER
        self.assertEqual(_resolve_handlers_lambda_function_name("arbitiumlab"), "arbitium0813-peer-lab")
        self.assertEqual(_resolve_handlers_lambda_function_name("other"), "arbitium0813-peer-lab")
        self.assertEqual(get_ecs_config("arbitiumlab")["cluster"], "shared-lab")
        self.assertEqual(get_ecs_config("other")["cluster"], "shared-lab")

    def test_partial_map_unmapped_handle_uses_convention(self) -> None:
        os.environ["EXTERNAL_HANDLERS_PEER_MAP"] = json.dumps(
            {"arbitiumlab": {"lambda_arn": LAB_ARN, "ecs_cluster": "lab", "ecs_results_bucket": "b"}}
        )
        self.assertEqual(_resolve_handlers_lambda_function_name("arbitiumlab"), "arbitium0813-peer-lab")
        self.assertEqual(
            _resolve_handlers_lambda_function_name("arbitiumtriage"),
            "arbitiumtriage-handlers",
        )
        self.assertIsNone(get_ecs_config("arbitiumtriage"))

    def test_kill_switch_fails_closed(self) -> None:
        os.environ["EXTERNAL_HANDLERS_PEER_MAP"] = TWO_PEERS
        os.environ["EXTERNAL_HANDLERS_PEER_ROUTING"] = "off"
        self.assertFalse(peer_routing_enabled())
        self.assertIsNone(get_peer_route("arbitiumlab"))
        self.assertEqual(
            _resolve_handlers_lambda_function_name("arbitiumlab"),
            "arbitiumlab-handlers",
        )
        self.assertIsNone(get_ecs_config("arbitiumlab"))

    def test_docker_stem_not_first_external_handlers_name(self) -> None:
        self.assertEqual(
            resolve_handlers_docker_image_base("arbitiumtriage"),
            "arbitiumtriage-lambda-builder",
        )


if __name__ == "__main__":
    unittest.main()
