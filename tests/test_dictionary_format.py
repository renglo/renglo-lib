"""Dictionary documents name terms, and relationship terms cite their inverse."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.blueprint.dictionary_format import dictionary_errors, dictionary_skeleton  # noqa: E402


def _term(dictionary, key, label, inverse=None, mappings=None, status="active"):
    item = {
        "id": f"{dictionary}.{key}",
        "label": label,
        "description": label,
        "external_mappings": list(mappings or []),
        "status": status,
    }
    if inverse:
        item["inverse"] = f"{dictionary}.{inverse}"
    return item


def _food():
    document = dictionary_skeleton("food")
    document["description"] = "Terms for edible things."
    document["terms"] = {
        "food": _term("food", "food", "Food"),
        "contains": _term("food", "contains", "Contains", inverse="belongs_to"),
        "belongs_to": _term("food", "belongs_to", "Belongs To", inverse="contains"),
    }
    return document


class DictionaryFormatTests(unittest.TestCase):
    def test_an_empty_draft_is_a_dictionary(self):
        document = dictionary_skeleton("infrastructure")
        self.assertIsNone(dictionary_errors(document))
        self.assertEqual(document["id"], "infrastructure.x.y.z")
        self.assertEqual(document["terms"], {})

    def test_relationship_terms_have_to_reverse_each_other(self):
        self.assertIsNone(dictionary_errors(_food()))
        broken = _food()
        broken["terms"]["contains"]["inverse"] = "food.food"
        self.assertIn("do not reverse each other", dictionary_errors(broken))
        missing = _food()
        missing["terms"]["contains"]["inverse"] = "food.missing"
        self.assertIn("is not a term in this dictionary", dictionary_errors(missing))

    def test_a_term_id_follows_the_dictionary_name(self):
        renamed = _food()
        renamed["terms"]["food"]["id"] = "menu.food"
        self.assertIn('terms.food.id must be "food.food"', dictionary_errors(renamed))
        extra = _food()
        extra["license"] = "CC BY"
        self.assertIn('unknown property "license"', dictionary_errors(extra))

    def test_the_infrastructure_and_evidence_examples_match_the_shape(self):
        fixtures = Path(__file__).resolve().parent / "fixtures"
        for name in ("infrastructure.dictionary.json", "evidence.dictionary.json"):
            document = json.loads((fixtures / name).read_text())
            self.assertIsNone(dictionary_errors(document), dictionary_errors(document))
