#!/usr/bin/env python3
"""Blueprint v1 embed flags → CRUD fingerprint and vector sync."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.vector.vector_index_service import VectorIndexService


INFRA_BLUEPRINT = {
    "irn": "irn:blueprint:irma:infrastructure_elements",
    "version": "1",
    "fields": [
        {"name": "provider", "embed": True, "order": "1", "type": "string"},
        {"name": "universal_domain", "embed": True, "type": "string"},
        {"name": "external_id", "embed": False, "type": "string"},
        {"name": "name", "embed": True, "type": "string"},
        {"name": "parameters", "embed": True, "type": "object"},
        {
            "name": "links",
            "embed": True,
            "type": "string",
            "source": {"target": "infrastructure_elements"},
        },
        {"name": "aliases", "embed": 2, "cardinality": "multiple", "type": "string"},
    ],
}


class TestVectorIndexPlan(unittest.TestCase):
    def setUp(self):
        self.svc = VectorIndexService(config={})

    def test_embed_true_and_weight_include_in_order(self):
        plan = self.svc.get_index_plan(
            "infrastructure_elements", blueprint=INFRA_BLUEPRINT
        )
        self.assertTrue(plan["enabled"])
        self.assertEqual(
            plan["embed_fields"],
            ["provider", "universal_domain", "name", "aliases"],
        )
        self.assertEqual(plan["entity_type"], "infrastructure_elements")
        self.assertNotIn("external_id", plan["embed_fields"])
        self.assertNotIn("parameters", plan["embed_fields"])
        self.assertNotIn("links", plan["embed_fields"])

    def test_enable_embed_false_disables(self):
        blueprint = {**INFRA_BLUEPRINT, "enable_embed": False}
        plan = self.svc.get_index_plan("infrastructure_elements", blueprint=blueprint)
        self.assertFalse(plan["enabled"])
        self.assertEqual(plan["embed_fields"], [])

    def test_fingerprint_concat_skips_empty_and_ids(self):
        plan = self.svc.get_index_plan(
            "infrastructure_elements", blueprint=INFRA_BLUEPRINT
        )
        text = self.svc.build_fingerprint(
            plan,
            {
                "provider": "aws",
                "universal_domain": "identity",
                "external_id": "arn:aws:iam::123:role/x",
                "name": "BreakGlass",
                "aliases": ["bg", "break-glass"],
                "links": ["other-id"],
            },
        )
        self.assertEqual(
            text,
            "[provider=aws] [universal_domain=identity] [name=BreakGlass] [aliases=bg,break-glass]",
        )
        self.assertNotIn("arn:aws", text)
        self.assertEqual(plan["used_fields"], ["provider", "universal_domain", "name", "aliases"])

    def test_empty_fingerprint_when_no_values(self):
        plan = self.svc.get_index_plan(
            "infrastructure_elements", blueprint=INFRA_BLUEPRINT
        )
        self.assertEqual(self.svc.build_fingerprint(plan, {"external_id": "arn"}), "")


class TestVectorIndexSync(unittest.TestCase):
    def setUp(self):
        self.svc = VectorIndexService(config={})
        self.vectors = MagicMock()
        self.vectors.put_vector.return_value = {"success": True, "key": "infrastructure_elements/doc-1"}
        self.vectors.delete_vector.return_value = {"success": True}

    def test_put_uses_ring_and_doc_id(self):
        result = self.svc.sync_document(
            self.vectors,
            portfolio="p1",
            org="o1",
            ring="infrastructure_elements",
            doc_id="doc-1",
            attributes={"provider": "aws", "name": "api"},
            verb="POST",
            blueprint=INFRA_BLUEPRINT,
        )
        self.assertTrue(result["success"])
        self.vectors.put_vector.assert_called_once()
        kwargs = self.vectors.put_vector.call_args.kwargs
        self.assertEqual(kwargs["entity_type"], "infrastructure_elements")
        self.assertEqual(kwargs["entity_id"], "doc-1")
        self.assertEqual(kwargs["text"], "[provider=aws] [name=api]")
        self.assertEqual(kwargs["attrs"]["source"], "blueprint")

    def test_delete_same_key(self):
        result = self.svc.sync_document(
            self.vectors,
            portfolio="p1",
            org="o1",
            ring="infrastructure_elements",
            doc_id="doc-1",
            verb="DELETE",
            blueprint=INFRA_BLUEPRINT,
        )
        self.assertTrue(result["success"])
        self.vectors.delete_vector.assert_called_once_with(
            portfolio="p1",
            org="o1",
            entity_type="infrastructure_elements",
            entity_id="doc-1",
        )
        self.vectors.put_vector.assert_not_called()

    def test_skip_when_no_embed_fields(self):
        result = self.svc.sync_document(
            self.vectors,
            portfolio="p1",
            org="o1",
            ring="other_ring",
            doc_id="doc-1",
            attributes={"name": "x"},
            verb="PUT",
            blueprint={"fields": [{"name": "name", "type": "string"}]},
        )
        self.assertTrue(result["skipped"])
        self.vectors.put_vector.assert_not_called()


HANDLER_BLUEPRINT = {
    "irn": "irn:blueprint:irma:arbitium_threat_events",
    "version": "0.0.7",
    "embed_handler": "arbitiumtriage/threat_fingerprint_builder/prepare_embed",
    "fields": [{"name": "provider", "type": "string"}],
}

HANDLER_AND_FIELDS_BLUEPRINT = {
    **HANDLER_BLUEPRINT,
    "fields": [{"name": "provider", "embed": True, "type": "string"}],
}


class TestEmbedHandlerSync(unittest.TestCase):
    def setUp(self):
        def _invoke(ref, payload):
            self.last_ref = ref
            self.last_payload = payload
            return {
                "success": True,
                "text": "[provider=aws] [intent=exfil]",
                "attrs": {"kind": "threat_event"},
            }

        self.svc = VectorIndexService(config={}, embed_handler_invoker=_invoke)
        self.vectors = MagicMock()
        self.vectors.put_vector.return_value = {
            "success": True,
            "key": "arbitium_threat_events/te-1",
        }
        self.vectors.delete_vector.return_value = {"success": True}

    def test_plan_records_handler(self):
        plan = self.svc.get_index_plan("arbitium_threat_events", blueprint=HANDLER_BLUEPRINT)
        self.assertTrue(plan["enabled"])
        self.assertEqual(
            plan["embed_handler"],
            "arbitiumtriage/threat_fingerprint_builder/prepare_embed",
        )
        self.assertEqual(plan["embed_fields"], [])

    def test_put_uses_handler_text_not_field_concat(self):
        result = self.svc.sync_document(
            self.vectors,
            portfolio="p1",
            org="o1",
            ring="arbitium_threat_events",
            doc_id="te-1",
            attributes={"provider": "azure"},
            verb="PUT",
            blueprint=HANDLER_AND_FIELDS_BLUEPRINT,
        )
        self.assertTrue(result["success"])
        self.vectors.put_vector.assert_called_once()
        kwargs = self.vectors.put_vector.call_args.kwargs
        self.assertEqual(kwargs["entity_type"], "arbitium_threat_events")
        self.assertEqual(kwargs["entity_id"], "te-1")
        self.assertEqual(kwargs["text"], "[provider=aws] [intent=exfil]")
        self.assertNotIn("[provider=azure]", kwargs["text"])
        self.assertEqual(kwargs["attrs"]["source"], "embed_handler")
        self.assertEqual(kwargs["attrs"]["kind"], "threat_event")
        self.assertEqual(self.last_payload["ring"], "arbitium_threat_events")
        self.assertEqual(self.last_payload["_id"], "te-1")
        self.assertEqual(self.last_payload["attributes"]["provider"], "azure")

    def test_handler_skip_does_not_put(self):
        svc = VectorIndexService(
            config={},
            embed_handler_invoker=lambda ref, payload: {"success": True, "skip": True},
        )
        result = svc.sync_document(
            self.vectors,
            portfolio="p1",
            org="o1",
            ring="arbitium_threat_events",
            doc_id="te-1",
            verb="PUT",
            blueprint=HANDLER_BLUEPRINT,
        )
        self.assertTrue(result["success"])
        self.assertTrue(result["skipped"])
        self.vectors.put_vector.assert_not_called()

    def test_handler_failure_does_not_put(self):
        svc = VectorIndexService(
            config={},
            embed_handler_invoker=lambda ref, payload: {
                "success": False,
                "error": "threat_event not found",
            },
        )
        result = svc.sync_document(
            self.vectors,
            portfolio="p1",
            org="o1",
            ring="arbitium_threat_events",
            doc_id="te-1",
            verb="PUT",
            blueprint=HANDLER_BLUEPRINT,
        )
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "threat_event not found")
        self.vectors.put_vector.assert_not_called()

    def test_delete_handler_only_blueprint(self):
        result = self.svc.sync_document(
            self.vectors,
            portfolio="p1",
            org="o1",
            ring="arbitium_threat_events",
            doc_id="te-1",
            verb="DELETE",
            blueprint=HANDLER_BLUEPRINT,
        )
        self.assertTrue(result["success"])
        self.vectors.delete_vector.assert_called_once_with(
            portfolio="p1",
            org="o1",
            entity_type="arbitium_threat_events",
            entity_id="te-1",
        )
        self.vectors.put_vector.assert_not_called()


if __name__ == "__main__":
    unittest.main()
