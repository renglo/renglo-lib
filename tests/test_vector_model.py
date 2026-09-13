#!/usr/bin/env python3
"""Org-scoped VectorModel: keys, stamps, isolation, list/count."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.vector.vector_controller import VectorController
from renglo.vector.vector_model import VectorModel, org_index_name, vector_key


class FakeAUC:
    def authorize(self, *args, **kwargs):
        return {"success": True, "roles": ["admin"], "status": 200}


class TestOrgIndexName(unittest.TestCase):
    def test_hex_ids_fit(self):
        name = org_index_name("8c38e9a6bd5d", "e023fcb12bf8")
        self.assertEqual(name, "8c38e9a6bd5d-e023fcb12bf8")
        self.assertGreaterEqual(len(name), 3)
        self.assertLessEqual(len(name), 63)

    def test_strips_invalid_chars(self):
        name = org_index_name("Acme Corp!", "Org_1")
        self.assertTrue(name.replace("-", "").replace(".", "").isalnum())
        self.assertNotIn("_", name)


class TestVectorModelLocal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.model = VectorModel(
            config={"S3_VECTORS_BUCKET": "", "S3_VECTORS_LOCAL_DIR": self.tmp.name}
        )
        self.vec_a = self.model._local_hash_embedding("alpha")
        self.vec_b = self.model._local_hash_embedding("beta")

    def tearDown(self):
        self.tmp.cleanup()

    def test_put_key_and_stamp_order(self):
        result = self.model.put_vector(
            portfolio="p1",
            org="o1",
            entity_type="threat_event",
            entity_id="evt-1",
            vector=self.vec_a,
            attrs={
                "provider": "aws",
                "portfolio": "forged",
                "org": "forged",
                "entity_type": "forged",
                "entity_id": "forged",
            },
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["key"], "threat_event/evt-1")
        self.assertEqual(result["index"], org_index_name("p1", "o1"))
        store = self.model._local_load(result["index"])
        meta = store["threat_event/evt-1"]["metadata"]
        self.assertEqual(meta["portfolio"], "p1")
        self.assertEqual(meta["org"], "o1")
        self.assertEqual(meta["entity_type"], "threat_event")
        self.assertEqual(meta["entity_id"], "evt-1")
        self.assertIn("updated_at", meta)
        attrs = __import__("json").loads(meta["attrs"])
        self.assertEqual(attrs["provider"], "aws")
        self.assertNotIn("portfolio", attrs)
        self.assertNotIn("entity_id", attrs)

    def test_same_id_different_type_are_distinct(self):
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="threat_event", entity_id="same",
            vector=self.vec_a,
        )
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="catalog", entity_id="same",
            vector=self.vec_b,
        )
        listed = self.model.list_vectors(portfolio="p1", org="o1")
        keys = {item["key"] for item in listed["items"]}
        self.assertEqual(keys, {"threat_event/same", "catalog/same"})

    def test_query_does_not_cross_org(self):
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="catalog", entity_id="c1",
            vector=self.vec_a, attrs={"label": "a"},
        )
        self.model.put_vector(
            portfolio="p1", org="o2", entity_type="catalog", entity_id="c2",
            vector=self.vec_a, attrs={"label": "b"},
        )
        hits = self.model.query(
            portfolio="p1", org="o1", vector=self.vec_a, entity_type="catalog"
        )
        self.assertEqual([h["entity_id"] for h in hits], ["c1"])
        self.assertNotEqual(
            org_index_name("p1", "o1"),
            org_index_name("p1", "o2"),
        )

    def test_query_without_type_is_org_wide(self):
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="threat_event", entity_id="e1",
            vector=self.vec_a,
        )
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="catalog", entity_id="c1",
            vector=self.vec_a,
        )
        hits = self.model.query(portfolio="p1", org="o1", vector=self.vec_a, top_k=10)
        types = {h["entity_type"] for h in hits}
        self.assertEqual(types, {"threat_event", "catalog"})

    def test_query_with_type_slices(self):
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="threat_event", entity_id="e1",
            vector=self.vec_a,
        )
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="catalog", entity_id="c1",
            vector=self.vec_a,
        )
        hits = self.model.query(
            portfolio="p1", org="o1", vector=self.vec_a, entity_type="catalog"
        )
        self.assertEqual([h["entity_id"] for h in hits], ["c1"])

    def test_list_count_and_purge(self):
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="threat_event", entity_id="e1",
            vector=self.vec_a,
        )
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="catalog", entity_id="c1",
            vector=self.vec_b,
        )
        listed = self.model.list_vectors(portfolio="p1", org="o1")
        self.assertEqual(listed["count"], 2)
        status = self.model.status("p1", "o1")
        self.assertEqual(status["count"], 2)
        self.assertEqual(status["counts"]["threat_event"], 1)
        self.model.purge_index(portfolio="p1", org="o1")
        listed = self.model.list_vectors(portfolio="p1", org="o1")
        self.assertEqual(listed["count"], 0)

    def test_get_vector_returns_embedding_and_attrs(self):
        self.model.put_vector(
            portfolio="p1",
            org="o1",
            entity_type="infrastructure_elements",
            entity_id="ie-1",
            vector=self.vec_a,
            attrs={"source": "blueprint", "fields": ["provider", "name"]},
        )
        got = self.model.get_vector(
            portfolio="p1",
            org="o1",
            entity_type="infrastructure_elements",
            entity_id="ie-1",
        )
        self.assertTrue(got["found"])
        self.assertEqual(got["key"], "infrastructure_elements/ie-1")
        self.assertEqual(got["vector"], self.vec_a)
        self.assertEqual(got["dim"], len(self.vec_a))
        self.assertEqual(got["attrs"]["fields"], ["provider", "name"])
        missing = self.model.get_vector(
            portfolio="p1",
            org="o1",
            entity_type="infrastructure_elements",
            entity_id="missing",
        )
        self.assertTrue(missing["success"])
        self.assertFalse(missing["found"])
        other_org = self.model.get_vector(
            portfolio="p1",
            org="o2",
            entity_type="infrastructure_elements",
            entity_id="ie-1",
        )
        self.assertFalse(other_org["found"])

    def test_extra_filter_cannot_override_reserved(self):
        self.model.put_vector(
            portfolio="p1", org="o1", entity_type="catalog", entity_id="c1",
            vector=self.vec_a,
        )
        hits = self.model.query(
            portfolio="p1",
            org="o1",
            vector=self.vec_a,
            extra_filter={"org": "someone-else", "entity_type": "campaign"},
        )
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["entity_id"], "c1")


class TestVectorControllerAuth(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctl = VectorController(
            config={"S3_VECTORS_BUCKET": "", "S3_VECTORS_LOCAL_DIR": self.tmp.name}
        )
        self.ctl.AUC = FakeAUC()

    def tearDown(self):
        self.tmp.cleanup()

    def test_put_and_query_without_extension(self):
        put = self.ctl.put_vector(
            portfolio="p1",
            org="o1",
            entity_type="catalog",
            entity_id="c1",
            text="exfil s3",
            attrs={"mitre": "T1567"},
        )
        self.assertTrue(put["success"], put)
        self.assertEqual(put["key"], vector_key("catalog", "c1"))
        queried = self.ctl.query(
            portfolio="p1", org="o1", entity_type="catalog", text="exfil s3"
        )
        self.assertTrue(queried["success"])
        self.assertEqual(queried["hits"][0]["entity_id"], "c1")
        self.assertNotIn("extension", queried)
        self.assertEqual(queried["hits"][0]["attrs"]["mitre"], "T1567")
        got = self.ctl.get_vector(
            portfolio="p1", org="o1", entity_type="catalog", entity_id="c1"
        )
        self.assertTrue(got["success"], got)
        self.assertTrue(got["found"])
        self.assertEqual(got["entity_id"], "c1")
        self.assertTrue(got.get("vector"))

    def test_reserved_metadata_rejected(self):
        result = self.ctl.put_vector(
            portfolio="p1",
            org="o1",
            entity_type="catalog",
            entity_id="c1",
            text="x",
            metadata={"org": "other", "foo": "bar"},
        )
        self.assertFalse(result["success"])
        self.assertIn("reserved", result["error"])

    def test_status_requires_authorize(self):
        denied = MagicMock()
        denied.authorize.return_value = {
            "success": False,
            "message": "Not authorized",
            "status": 403,
            "roles": [],
        }
        self.ctl.AUC = denied
        result = self.ctl.status(portfolio="p1", org="o1")
        self.assertFalse(result["success"])
        self.assertEqual(result.get("status"), 403)


if __name__ == "__main__":
    unittest.main()
