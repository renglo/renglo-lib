#!/usr/bin/env python3
"""Preferences are a single-value map, separate from multi-value tags."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.common import sanitize_entity_preferences, sanitize_entity_tags  # noqa: E402


class SanitizeEntityPreferencesTests(unittest.TestCase):
    def test_one_string_per_key(self):
        clean = sanitize_entity_preferences({
            "Default_Extension": "Elements",
            "theme": ["dark", "light"],
        })
        self.assertEqual(clean["default_extension"], "Elements")
        self.assertEqual(clean["theme"], "light")

    def test_drops_unsafe_and_empty_entries(self):
        clean = sanitize_entity_preferences({
            "ok": "value",
            "bad:key": "nope",
            "path": "a/b",
            "blank": "   ",
            "": "missing",
            1: "numeric-key",
        })
        self.assertEqual(clean, {"ok": "value"})

    def test_non_dict_is_empty(self):
        self.assertEqual(sanitize_entity_preferences(None), {})
        self.assertEqual(sanitize_entity_preferences(["default_extension"]), {})

    def test_tags_stay_multi_value(self):
        tags = sanitize_entity_tags({"location": ["london", "paris"]})
        self.assertEqual(tags["location"], ["london", "paris"])


if __name__ == "__main__":
    unittest.main()
