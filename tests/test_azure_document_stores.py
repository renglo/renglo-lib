"""Cosmos document store and the Azure submodels that call it."""

from __future__ import annotations

import ast
import importlib
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


LIB_ROOT = Path(__file__).resolve().parents[1] / "renglo"

FACADES = (
    "blueprint/blueprint_model.py",
    "auth/auth_model.py",
    "data/data_model.py",
    "graph/graph_model.py",
    "search/search_model.py",
    "session/session_model.py",
    "timeseries/timeseries_model.py",
)


class StatusError(Exception):
    def __init__(self, status):
        super().__init__(str(status))
        self.status_code = status


class Pager:
    def __init__(self, chunk, token):
        self.continuation_token = token
        self._chunk = chunk
        self._done = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._done:
            raise StopIteration
        self._done = True
        return self._chunk


class Page:
    def __init__(self, items, limit):
        self.items = items
        self.limit = limit

    def by_page(self, continuation=None):
        start = int(continuation or 0)
        if self.limit is None:
            chunk = self.items[start:]
            token = None
        else:
            chunk = self.items[start : start + self.limit]
            nxt = start + len(chunk)
            token = str(nxt) if nxt < len(self.items) else None
        return Pager(chunk, token)


class MemoryContainer:
    def __init__(self, partition_field, missing=False):
        self.partition_field = partition_field
        self.docs = {}
        self.revision = 0
        self.missing = missing

    def read_item(self, item, partition_key):
        found = self.docs.get((partition_key, item))
        if found is None:
            raise StatusError(404)
        return dict(found)

    def upsert_item(self, body):
        self.revision += 1
        stored = dict(body)
        stored["_etag"] = f'"{self.revision}"'
        stored["_rid"] = "rid"
        self.docs[(stored[self.partition_field], stored["id"])] = stored
        return stored

    def create_item(self, body):
        key = (body[self.partition_field], body["id"])
        if key in self.docs:
            raise StatusError(409)
        return self.upsert_item(body)

    def replace_item(self, item, body, etag=None, match_condition=None):
        key = (body[self.partition_field], item)
        current = self.docs.get(key)
        if current is None:
            raise StatusError(404)
        if etag is not None and current.get("_etag") != etag:
            raise StatusError(412)
        return self.upsert_item(body)

    def delete_item(self, item, partition_key, etag=None, match_condition=None):
        key = (partition_key, item)
        current = self.docs.get(key)
        if current is None:
            raise StatusError(404)
        if etag is not None and current.get("_etag") != etag:
            raise StatusError(412)
        del self.docs[key]

    def read(self):
        if self.missing:
            raise StatusError(404)
        return {"id": "container"}

    def query_items(self, query, parameters=None, partition_key=None, max_item_count=None, enable_cross_partition_query=False):
        import re

        params = {item["name"]: item["value"] for item in parameters or []}
        order = re.search(r'ORDER BY c\["([A-Za-z0-9_]+)"\] (ASC|DESC)', query)
        field = order.group(1) if order else None
        matched = []
        for (partition, _item_id), doc in self.docs.items():
            if partition_key is not None and partition != partition_key:
                continue
            value = doc.get(field)
            if "@prefix" in params and not str(value or "").startswith(params["@prefix"]):
                continue
            if "@equals" in params and value != params["@equals"]:
                continue
            if "@low" in params and (value is None or value < params["@low"]):
                continue
            if "@high" in params and (value is None or value > params["@high"]):
                continue
            if "@after" in params and (value is None or not value > params["@after"]):
                continue
            if "@before" in params and (value is None or not value < params["@before"]):
                continue
            matched.append(doc)
        if field:
            matched.sort(key=lambda doc: doc.get(field) or "", reverse=bool(order and order.group(2) == "DESC"))
        return Page(matched, max_item_count)


class DocumentStoreTests(unittest.TestCase):
    def setUp(self):
        from renglo.document_stores.document_store_azure import DocumentStore

        self.container = MemoryContainer("index")
        self.store = DocumentStore(
            {},
            "demo_entities",
            "index",
            "_id",
            container=self.container,
        )

    def test_legal_sort_key_is_the_document_id(self):
        from renglo.document_stores.document_store_azure import cosmos_id

        self.assertEqual(cosmos_id("user-1"), "user-1")
        self.assertNotEqual(cosmos_id("a/b"), "a/b")
        self.assertEqual(len(cosmos_id("a/b")), 64)

    def test_round_trip_strips_system_fields(self):
        self.store.put({"index": "users", "_id": "u1", "name": "Ada"})
        item = self.store.get("users", "u1")
        self.assertEqual(item["name"], "Ada")
        self.assertNotIn("_etag", item)
        self.assertNotIn("_rid", item)

    def test_prefix_query_orders_matches(self):
        self.store.put({"index": "users", "_id": "b", "name": "B"})
        self.store.put({"index": "users", "_id": "a-2", "name": "A2"})
        self.store.put({"index": "other", "_id": "a-9", "name": "no"})
        self.store.put({"index": "users", "_id": "a-1", "name": "A1"})
        items, token = self.store.query("users", prefix="a-")
        self.assertIsNone(token)
        self.assertEqual([item["_id"] for item in items], ["a-1", "a-2"])

    def test_exclusive_create_conflicts(self):
        from renglo.document_stores.document_store_azure import Conflict

        self.store.create({"index": "users", "_id": "u1"})
        with self.assertRaises(Conflict):
            self.store.create({"index": "users", "_id": "u1"})

    def test_replace_rejects_a_stale_etag(self):
        from renglo.document_stores.document_store_azure import PreconditionFailed

        self.store.put({"index": "users", "_id": "u1", "name": "Ada"})
        _item, etag = self.store.read("users", "u1")
        self.store.put({"index": "users", "_id": "u1", "name": "Grace"})
        with self.assertRaises(PreconditionFailed):
            self.store.replace({"index": "users", "_id": "u1", "name": "Ada"}, etag=etag)

    def test_limit_returns_a_continuation(self):
        for name in ("a", "b", "c"):
            self.store.put({"index": "users", "_id": name})
        first, token = self.store.query("users", limit=2)
        self.assertEqual([item["_id"] for item in first], ["a", "b"])
        self.assertIsNotNone(token)
        second, next_token = self.store.query("users", limit=2, continuation=token)
        self.assertEqual([item["_id"] for item in second], ["c"])
        self.assertIsNone(next_token)

    def test_missing_container_describe_raises(self):
        from renglo.document_stores.document_store_azure import DocumentStore

        store = DocumentStore({}, "missing", "index", "_id", container=MemoryContainer("index", missing=True))
        with self.assertRaises(RuntimeError):
            store.describe("Entities")


class _Conditional(Exception):
    def __init__(self):
        super().__init__("exists")
        self.response = {
            "Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"},
            "ResponseMetadata": {"HTTPStatusCode": 400},
        }


class _FakeTable:
    def __init__(self):
        self.items = {}

    def put_item(self, **kwargs):
        item = kwargs["Item"]
        key = (item["index"], item["_id"])
        if "ConditionExpression" in kwargs and key in self.items:
            raise _Conditional()
        self.items[key] = dict(item)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_item(self, **kwargs):
        key = kwargs["Key"]
        item = self.items.get((key["index"], key["_id"]))
        response = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        if item:
            response["Item"] = item
        return response


class AwsStoreShapeTests(unittest.TestCase):
    def test_aws_store_exposes_the_same_methods(self):
        from decimal import Decimal

        from renglo.document_stores import document_store_aws as aws_store
        from renglo.document_stores import document_store_azure as azure_store
        from renglo.document_stores.document_store_aws import Conflict

        names = ("get", "read", "query", "put", "create", "replace", "delete", "scan", "describe")
        for name in names:
            self.assertTrue(hasattr(aws_store.DocumentStore, name))
            self.assertTrue(hasattr(azure_store.DocumentStore, name))
        store = aws_store.DocumentStore({}, "entities", "index", "_id", table=_FakeTable())
        store.put({"index": "users", "_id": "u1", "score": 1.5})
        self.assertEqual(store.get("users", "u1")["score"], Decimal("1.5"))
        with self.assertRaises(Conflict):
            store.create({"index": "users", "_id": "u1"})


class FacadeBoundaryTests(unittest.TestCase):
    def test_facades_do_not_import_a_cloud_module(self):
        banned = ("boto3", "azure", "_aws", "_azure")
        for relative in FACADES:
            tree = ast.parse((LIB_ROOT / relative).read_text(encoding="utf-8"), filename=relative)
            for node in ast.walk(tree):
                modules = []
                if isinstance(node, ast.Import):
                    modules.extend(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    modules.append(node.module)
                for module in modules:
                    self.assertFalse(
                        any(token in module for token in banned),
                        f"{relative} imports {module}",
                    )

    def test_graph_keys_do_not_load_the_aws_submodel(self):
        sys.modules.pop("renglo.graph.submodels.graph_model_aws", None)
        module = importlib.import_module("renglo.graph.graph_model")
        importlib.reload(module)
        self.assertEqual(module.GraphModel.make_pk("p", "o"), "irn:edge:p:o")
        self.assertNotIn("renglo.graph.submodels.graph_model_aws", sys.modules)


class AzureModelTests(unittest.TestCase):
    def test_blueprint_put_uses_irn_and_version(self):
        from renglo.blueprint.blueprint_model import BlueprintModel
        from renglo.document_stores.document_store_azure import DocumentStore

        container = MemoryContainer("irn")

        store = DocumentStore({}, "blueprints", "irn", "version", container=container)
        model = BlueprintModel(store=store)
        model.put_blueprint({"irn": "irn:blueprint:h:n", "version": "1.0.0", "name": "n"})
        found = model.get_blueprint("h", "n", "last")
        self.assertEqual(found["version"], "1.0.0")

    def test_auth_entity_create_and_identity_placeholder(self):
        from renglo.auth.auth_model import AuthModel
        from renglo.document_stores.document_store_azure import DocumentStore

        entities = DocumentStore({}, "entities", "index", "_id", container=MemoryContainer("index"))
        rels = DocumentStore({}, "rel", "index", "rel", container=MemoryContainer("index"))
        model = AuthModel(config={"CLOUD": "azure"}, entity_store=entities, rel_store=rels)
        created = model.put_entity_exclusive({"index": "irn:entity:user:*", "_id": "u1", "type": "user"})
        self.assertTrue(created["success"])
        self.assertEqual(created["status"], 200)
        again = model.put_entity_exclusive({"index": "irn:entity:user:*", "_id": "u1"})
        self.assertEqual(again["status"], 409)
        self.assertEqual(
            model._identity().__class__.__module__,
            "renglo.auth.providers.identity_azure",
        )
        missing = model.lookup_by_email("a@example.com")
        self.assertFalse(missing["success"])
        with self.assertRaises(NotImplementedError):
            model.send_email("from", "to", "subject", "text", "html")

    def test_data_path_index_query(self):
        from renglo.data.data_model import DataModel
        from renglo.document_stores.document_store_azure import DocumentStore

        store = DocumentStore({}, "data", "portfolio_index", "doc_index", container=MemoryContainer("portfolio_index"))
        model = DataModel(config={"DATA_TABLE": "data"}, bind_auth=False, store=store)
        model.post_a_b("p", "o", "ring", {"_id": "1", "path_index": "irn:h_index:o:ring:alpha", "attributes": {}})
        result = model.get_a_b_beginswith(
            {
                "portfolio": "p",
                "org": "o",
                "ring": "ring",
                "value": "alpha",
                "limit": 10,
                "lastkey": None,
                "sort": "desc",
            }
        )
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["items"][0]["path_index"], "irn:h_index:o:ring:alpha")

    def test_graph_outgoing_prefix(self):
        from renglo.graph.graph_model import GraphModel
        from renglo.document_stores.document_store_azure import DocumentStore

        store = DocumentStore({}, "graph", "graph_index", "forward_index", container=MemoryContainer("graph_index"))
        model = GraphModel(config={"GRAPH_TABLE": "graph"}, store=store)
        model.put_edge("p", "o", "relates", "User/a", "User/b", properties={"label": "knows"})
        page = model.list_outgoing_edges("p", "o", "relates", "User/a")
        self.assertEqual(len(page.items), 1)
        self.assertEqual(page.items[0].to_node_id, "User/b")
        self.assertEqual(page.items[0].properties["label"], "knows")

    def test_search_token_partition(self):
        import types

        if "renglo.search" not in sys.modules:
            package = types.ModuleType("renglo.search")
            package.__path__ = [str(LIB_ROOT / "search")]
            package.__package__ = "renglo.search"
            sys.modules["renglo.search"] = package
        from renglo.search.search_model import SearchModel
        from renglo.document_stores.document_store_azure import DocumentStore

        store = DocumentStore({}, "search", "index", "search_index", container=MemoryContainer("index"))
        model = SearchModel(config={"SEARCH_TABLE": "search"}, store=store)
        count = model.put_index_rows(
            [
                {
                    "portfolio_id": "p",
                    "org_id": "o",
                    "ring": "ring",
                    "token": "ada",
                    "field": "name",
                    "doc_id": "1",
                    "positions": [0],
                }
            ]
        )
        self.assertEqual(count, 1)
        found = model.query_token("p", "o", "ring", "ada")
        self.assertEqual(found["items"][0]["doc_id"], "1")
        self.assertEqual(found["items"][0]["field"], "name")

    def test_session_appends_events(self):
        from renglo.session.session_model import SessionModel
        from renglo.document_stores.document_store_azure import DocumentStore

        store = DocumentStore({}, "session", "index", "entity_index", container=MemoryContainer("index"))
        model = SessionModel(store=store)
        model.create_session({"index": "turn", "entity_index": "e1", "events": [{"n": 1}]})
        result = model.append_events("turn", "e1", {"n": 2})
        self.assertTrue(result["success"])
        saved = model.get_session("turn", "e1", None)
        self.assertEqual(saved["item"]["events"], [{"n": 1}, {"n": 2}])

    def test_timeseries_put_writes_metric_index(self):
        from renglo.timeseries.timeseries_model import TimeSeriesModel
        from renglo.document_stores.document_store_azure import DocumentStore

        store = DocumentStore({}, "timeseries", "index", "metric_index", container=MemoryContainer("index"))
        model = TimeSeriesModel(config={"TIMESERIES_TABLE": "timeseries"}, store=store)
        stored = model.put("p", "o", "temp", 3, timestamp=1_700_000_000, resolution="day")
        self.assertTrue(stored["appended"])
        self.assertIn("irn:metric:p:o:temp", stored["index"])
        self.assertEqual(stored["reading_count"], 1)


class AzureCliPathTests(unittest.TestCase):
    def test_local_cli_directory_is_put_on_path(self):
        from renglo.document_stores import document_store_azure as azure_store

        with tempfile.TemporaryDirectory() as directory:
            az = Path(directory) / "az"
            az.write_text("#!/bin/sh\n", encoding="utf-8")
            az.chmod(0o755)
            with mock.patch.object(azure_store, "_azure_cli_directories", return_value=[Path(directory)]):
                with mock.patch.dict(os.environ, {"PATH": ""}, clear=False):
                    azure_store._ensure_azure_cli()
                    self.assertEqual(shutil.which("az"), str(az))


if __name__ == "__main__":
    unittest.main()
