#!/usr/bin/env python3
"""Installed-package blueprint index and code-first resolve."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.blueprint.extension_blueprints import (  # noqa: E402
    collect_blueprint_documents,
    get_installed_blueprint,
    resolve_blueprint,
)


def _write_blueprint(folder: Path, name: str, version: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{name}.json").write_text(
        json.dumps(
            {
                "handle": "irma",
                "name": name,
                "version": version,
                "fields": [{"name": "portfolio"}],
                "irn": f"irn:blueprint:irma:{name}",
            }
        ),
        encoding="utf-8",
    )


class InstalledBlueprintTests(unittest.TestCase):
    def test_collects_handle_and_name(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            blueprints = Path(raw) / "blueprints"
            _write_blueprint(blueprints, "data_onboardings", "0.0.1")
            index = collect_blueprint_documents([blueprints])
            doc = index[("irma", "data_onboardings")]
            self.assertEqual(doc["version"], "0.0.1")

    def test_last_returns_current_tag(self) -> None:
        index = {
            ("irma", "data_onboardings"): {
                "handle": "irma",
                "name": "data_onboardings",
                "version": "0.0.1",
                "fields": [],
            }
        }
        with patch(
            "renglo.blueprint.extension_blueprints.installed_blueprint_index",
            return_value=index,
        ):
            found = get_installed_blueprint("irma", "data_onboardings", "last")
            self.assertEqual(found["version"], "0.0.1")
            missing = get_installed_blueprint("irma", "data_onboardings", "9.9.9")
            self.assertIsNone(missing)
            exact = get_installed_blueprint("irma", "data_onboardings", "0.0.1")
            self.assertEqual(exact["name"], "data_onboardings")


class ResolveOrderTests(unittest.TestCase):
    def test_code_wins_over_dynamo(self) -> None:
        wheel = {
            "handle": "irma",
            "name": "data_onboardings",
            "version": "0.0.1",
            "fields": [],
            "irn": "irn:blueprint:irma:data_onboardings",
        }
        dynamo = {
            "handle": "irma",
            "name": "data_onboardings",
            "version": "dynamo",
            "irn": "irn:blueprint:irma:data_onboardings",
        }
        with patch(
            "renglo.blueprint.extension_blueprints.get_installed_blueprint",
            return_value=wheel,
        ):
            result = resolve_blueprint(
                "irma", "data_onboardings", "last", dynamo=dynamo
            )
        self.assertEqual(result["version"], "0.0.1")

    def test_dynamo_when_wheel_misses(self) -> None:
        dynamo = {
            "handle": "irma",
            "name": "legacy_ring",
            "version": "0.0.9",
            "irn": "irn:blueprint:irma:legacy_ring",
        }
        with patch(
            "renglo.blueprint.extension_blueprints.get_installed_blueprint",
            return_value=None,
        ):
            result = resolve_blueprint("irma", "legacy_ring", "0.0.9", dynamo=dynamo)
        self.assertEqual(result["version"], "0.0.9")

    def test_public_after_dynamo_miss(self) -> None:
        public = {
            "handle": "irma",
            "name": "open_ring",
            "version": "1.0.0",
            "irn": "irn:blueprint:irma:open_ring",
        }
        dynamo = {"success": False, "message": "Document not found"}
        with patch(
            "renglo.blueprint.extension_blueprints.get_installed_blueprint",
            return_value=None,
        ):
            result = resolve_blueprint(
                "irma", "open_ring", "last", dynamo=dynamo, public=public
            )
        self.assertEqual(result["version"], "1.0.0")


if __name__ == "__main__":
    unittest.main()
