"""Blueprint documents have to match the extension schema shape."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parents[1]
sys.path.insert(0, str(ROOT))

from renglo.blueprint.schema_format import schema_errors  # noqa: E402


def _field(name="title", **overrides):
    field = {
        "name": name,
        "label": "Title",
        "type": "string",
        "widget": "text",
        "cardinality": "single",
        "layer": "0",
        "required": False,
        "default": "",
    }
    field.update(overrides)
    return field


def _schema(**overrides):
    document = {"name": "projects", "label": "Projects", "fields": [_field()]}
    document.update(overrides)
    return document


class SchemaFormatTests(unittest.TestCase):
    def test_extension_blueprints_match_the_shape(self):
        paths = [
            "extensions/breakdown/blueprints/breakdown_items.json",
            "extensions/breakdown/blueprints/breakdown_scene.json",
            "extensions/elements/blueprints/element_prop.json",
            "extensions/budget/blueprints/budget_items.json",
            "extensions/elements/blueprints/element_crew.json",
        ]
        for relative in paths:
            document = json.loads((WORKSPACE / relative).read_text())
            self.assertIsNone(schema_errors(document), relative)

    def test_empty_field_list_is_a_schema(self):
        self.assertIsNone(schema_errors({"name": "projects", "label": "Projects", "fields": []}))

    def test_cardinality_and_layer_are_closed_sets(self):
        message = schema_errors(_schema(fields=[_field(cardinality="many", layer=1)]))
        self.assertIn('cardinality must be "single" or "multiple"', message)
        self.assertIn('layer must be "0", "1", or "2"', message)

    def test_select_needs_options_unless_it_points_at_a_source(self):
        bare = schema_errors(_schema(fields=[_field(widget="select")]))
        self.assertIn("needs options", bare)
        sourced = _field(
            widget="select",
            source={
                "target": "breakdown_file",
                "target_key": "_id",
                "preview": ["title"],
                "label": ["ORIGINATED_FROM", "ORIGINATES"],
                "projection": ["title"],
            },
        )
        self.assertIsNone(schema_errors(_schema(fields=[sourced])))
        shorthand = schema_errors(_schema(fields=[_field(widget="select", source="breakdown_file:_id:title")]))
        self.assertIn("source must be an object", shorthand)

    def test_multiple_defaults_are_lists_and_indexes_name_real_fields(self):
        tags = _field(name="tags", label="Tags", cardinality="multiple", default="")
        self.assertIn("list of strings", schema_errors(_schema(fields=[tags])))
        indexed = _schema(
            fields=[_field(name="status", label="Status"), sourced_field()],
            indexes={"path": ["missing"], "time": ["script_id"]},
        )
        message = schema_errors(indexed)
        self.assertIn('indexes.path "missing" is not a field', message)
        self.assertIn('indexes.time "script_id" is a source field', message)

    def test_unknown_properties_are_refused(self):
        message = schema_errors(_schema(owner="portfolio", fields=[_field(widget_hint="big")]))
        self.assertIn('unknown property "owner"', message)
        self.assertIn('unknown property "widget_hint"', message)


def sourced_field():
    return _field(
        name="script_id",
        label="Script",
        widget="select",
        layer="2",
        source={
            "target": "breakdown_file",
            "target_key": "_id",
            "preview": ["title"],
            "label": ["ORIGINATED_FROM", "ORIGINATES"],
            "projection": ["title"],
        },
    )


if __name__ == "__main__":
    unittest.main()
