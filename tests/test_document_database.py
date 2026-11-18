"""Blueprint semver on create and update."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.blueprint.blueprint_controller import BlueprintController  # noqa: E402
from renglo.blueprint.semver import classify_change, next_version  # noqa: E402


BLUEPRINT = {
    "name": "projects",
    "fields": [
        {"name": "status", "type": "string"},
        {"name": "ownerId", "type": "string"},
        {"name": "createdAt", "type": "timestamp"},
        {"name": "location", "type": "string"},
    ],
    "indexes": {
        "path": ["status", "ownerId"],
        "time": ["createdAt"],
        "geo": ["location"],
    },
}


class SemverTests(unittest.TestCase):
    def test_added_field_is_minor_and_removed_field_is_major(self):
        added = {
            "fields": BLUEPRINT["fields"] + [{"name": "region", "type": "string"}],
            "indexes": BLUEPRINT["indexes"],
        }
        self.assertEqual(classify_change(BLUEPRINT, added), "minor")
        self.assertEqual(next_version("1.2.3", "minor"), "1.3.0")
        removed = {
            "fields": BLUEPRINT["fields"][:2],
            "indexes": BLUEPRINT["indexes"],
        }
        self.assertEqual(classify_change(BLUEPRINT, removed), "major")
        self.assertEqual(next_version("1.2.3", "major"), "2.0.0")

    def test_reorder_or_type_change_is_major_and_label_is_patch(self):
        reordered = {
            "fields": list(reversed(BLUEPRINT["fields"])),
            "indexes": BLUEPRINT["indexes"],
        }
        self.assertEqual(classify_change(BLUEPRINT, reordered), "major")
        retyped = {
            "fields": [
                {"name": "status", "type": "number"},
                *BLUEPRINT["fields"][1:],
            ],
            "indexes": BLUEPRINT["indexes"],
        }
        self.assertEqual(classify_change(BLUEPRINT, retyped), "major")
        labeled = dict(BLUEPRINT)
        labeled["label"] = "Projects"
        self.assertEqual(classify_change(BLUEPRINT, labeled), "patch")
        self.assertEqual(next_version("1.2.3", "patch"), "1.2.4")

    def test_post_ignores_the_incoming_version(self):
        saved = {}

        class Store:
            def get_blueprint(self, handle, name, version):
                item = saved.get((handle, name))
                if not item:
                    return {"success": False, "message": "Document not found"}
                return item

            def put_blueprint(self, data):
                saved[(data["handle"], data["name"])] = dict(data)
                return {"success": True, "document": dict(data), "status": 201}

        with patch("renglo.blueprint.blueprint_controller.BlueprintModel", return_value=Store()), \
                patch("renglo.auth.auth_controller.AuthModel", return_value=object()):
            controller = BlueprintController(config={})
        created = controller.create_blueprint(
            {"name": "projects", "version": "9.9.9", "label": "Projects", "fields": []},
            user_handle="portfolio",
        )
        self.assertEqual(created["document"]["version"], "1.0.0")
        updated = controller.update_blueprint(
            "portfolio",
            "projects",
            data={"version": "0.0.1", "label": "Renamed"},
        )
        self.assertEqual(updated["document"]["version"], "1.0.1")
        self.assertEqual(saved[("portfolio", "projects")]["version"], "1.0.1")
        self.assertEqual(saved[("portfolio", "projects")]["label"], "Renamed")


if __name__ == "__main__":
    unittest.main()
