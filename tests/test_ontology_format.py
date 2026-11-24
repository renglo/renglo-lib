"""Ontology documents follow one shape, whatever subject they describe."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.blueprint.ontology_format import ontology_errors, ontology_skeleton  # noqa: E402


def _type(name, semantic, parent=None, abstract=False, description=None):
    item = {
        "type": name,
        "semantic": semantic,
        "description": description or name,
        "examples": [],
    }
    if parent:
        item["parent_type"] = parent
    if abstract:
        item["abstract"] = True
    return item


def _domain(domain_id, root, types, semantic=None):
    return {
        "id": domain_id,
        "label": domain_id.replace("_", " ").title(),
        "semantic": semantic or f"food.domain.{domain_id}",
        "root_type": root,
        "types": types,
    }


def _food(**overrides):
    document = ontology_skeleton("food.groups")
    document["description"] = "Edible things grouped for a menu."
    document["node_types"] = [
        _type("Food", "food.item", abstract=True, description="Anything eaten."),
        _type("Fruit", "food.fruit", parent="Food", description="A sweet plant food."),
        _type("Vegetable", "food.vegetable", parent="Food", description="A savory plant food."),
    ]
    document["functional_domains"] = [
        _domain("sweet", "Food", ["Fruit"]),
        _domain("savory", "Food", ["Vegetable"]),
    ]
    document.update(overrides)
    return document


class OntologyFormatTests(unittest.TestCase):
    def test_an_empty_draft_is_an_ontology(self):
        self.assertIsNone(ontology_errors(ontology_skeleton("food.groups")))

    def test_domains_can_cut_across_the_type_tree(self):
        self.assertIsNone(ontology_errors(_food()))

    def test_the_infrastructure_example_matches_the_shape(self):
        path = Path(__file__).resolve().parent / "fixtures" / "infrastructure.ontology.json"
        document = json.loads(path.read_text())
        self.assertIsNone(ontology_errors(document), ontology_errors(document))

    def test_every_instantiable_type_belongs_to_one_domain(self):
        missing = _food()
        missing["functional_domains"] = [_domain("sweet", "Food", ["Fruit"])]
        self.assertIn('node type "Vegetable" is not listed in a functional domain', ontology_errors(missing))
        shared = _food()
        shared["functional_domains"] = [
            _domain("sweet", "Food", ["Fruit", "Vegetable"]),
            _domain("savory", "Food", ["Vegetable"]),
        ]
        self.assertIn('listed in more than one functional domain', ontology_errors(shared))

    def test_unknown_fields_and_a_missing_root_are_refused(self):
        extra = _food(license="CC BY")
        self.assertIn('unknown property "license"', ontology_errors(extra))
        two_roots = _food()
        two_roots["node_types"] = [
            _type("Fruit", "food.fruit"),
            _type("Vegetable", "food.vegetable"),
        ]
        two_roots["functional_domains"] = [
            _domain("sweet", "Fruit", ["Fruit"]),
            _domain("savory", "Vegetable", ["Vegetable"]),
        ]
        self.assertIn("node_types needs one type with no parent_type", ontology_errors(two_roots))
