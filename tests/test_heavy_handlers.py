#!/usr/bin/env python3
"""heavy_handlers catalog: new key, legacy ecs_handlers, env overlay."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.schd.external_handlers_config import (  # noqa: E402
    get_heavy_handlers,
    is_heavy_handler,
    is_ecs_handler,
)


class HeavyHandlersTests(unittest.TestCase):
    def test_package_prefers_heavy_handlers_and_accepts_legacy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pkg = root / "extensions" / "acmewidget" / "package"
            pkg.mkdir(parents=True)
            (pkg / "handlers_config.json").write_text(
                '{"heavy_handlers": ["orch"], "ecs_handlers": ["legacy_only"]}',
                encoding="utf-8",
            )
            with unittest.mock.patch(
                "renglo.schd.external_handlers_config._get_workspace_root",
                return_value=root,
            ):
                names = get_heavy_handlers("acmewidget")
                self.assertEqual(names, ["orch", "legacy_only"])
                self.assertTrue(is_heavy_handler("acmewidget", "orch/start"))
                self.assertTrue(is_ecs_handler("acmewidget", "legacy_only"))

    def test_env_overlay_new_key_wins_over_legacy(self) -> None:
        old_new = os.environ.get("EXTERNAL_HANDLERS_HEAVY")
        old_legacy = os.environ.get("EXTERNAL_HANDLERS_ECS_HANDLERS")
        os.environ["EXTERNAL_HANDLERS_HEAVY"] = "acmewidget:from_new"
        os.environ["EXTERNAL_HANDLERS_ECS_HANDLERS"] = "acmewidget:from_legacy"
        try:
            with unittest.mock.patch(
                "renglo.schd.external_handlers_config._get_workspace_root",
                return_value=None,
            ):
                self.assertEqual(get_heavy_handlers("acmewidget"), ["from_new"])
        finally:
            if old_new is None:
                os.environ.pop("EXTERNAL_HANDLERS_HEAVY", None)
            else:
                os.environ["EXTERNAL_HANDLERS_HEAVY"] = old_new
            if old_legacy is None:
                os.environ.pop("EXTERNAL_HANDLERS_ECS_HANDLERS", None)
            else:
                os.environ["EXTERNAL_HANDLERS_ECS_HANDLERS"] = old_legacy

    def test_peer_route_heavy_handlers_wins(self) -> None:
        old = os.environ.get("EXTERNAL_HANDLERS_PEER_MAP")
        os.environ["EXTERNAL_HANDLERS_PEER_MAP"] = (
            '{"acmewidget":{"lambda_arn":"arn:aws:lambda:us-east-1:1:function:peer",'
            '"heavy_handlers":["from_route"]}}'
        )
        try:
            with unittest.mock.patch(
                "renglo.schd.external_handlers_config._get_workspace_root",
                return_value=None,
            ):
                self.assertEqual(get_heavy_handlers("acmewidget"), ["from_route"])
        finally:
            if old is None:
                os.environ.pop("EXTERNAL_HANDLERS_PEER_MAP", None)
            else:
                os.environ["EXTERNAL_HANDLERS_PEER_MAP"] = old


if __name__ == "__main__":
    unittest.main()
