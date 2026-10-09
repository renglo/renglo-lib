#!/usr/bin/env python3
"""Soft-delete portfolios and orgs without removing their relationships."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.auth.entity_status import (  # noqa: E402
    ENTITY_STATUS_ACTIVE,
    ENTITY_STATUS_DELETED,
    HARD_DELETE_REFUSED,
    entity_forbids_hard_delete,
    entity_is_deleted,
    entity_status,
)
from renglo.auth.auth_controller import AuthController  # noqa: E402


def _doc(index, entity_id, entity_type, name, status=None):
    document = {
        "_id": entity_id,
        "index": index,
        "type": entity_type,
        "name": name,
        "handle": name.lower(),
    }
    if status is not None:
        document["status"] = status
    return document


class EntityStatusTests(unittest.TestCase):
    def test_missing_blank_and_active_are_live(self):
        self.assertEqual(entity_status({}), ENTITY_STATUS_ACTIVE)
        self.assertEqual(entity_status({"status": ""}), ENTITY_STATUS_ACTIVE)
        self.assertEqual(entity_status({"status": "  "}), ENTITY_STATUS_ACTIVE)
        self.assertEqual(entity_status({"status": "Active"}), ENTITY_STATUS_ACTIVE)
        self.assertFalse(entity_is_deleted({}))
        self.assertFalse(entity_is_deleted({"status": "archived"}))

    def test_deleted_status_is_case_insensitive(self):
        self.assertTrue(entity_is_deleted({"status": "deleted"}))
        self.assertTrue(entity_is_deleted({"status": " Deleted "}))

    def test_hard_delete_guard_matches_portfolio_and_org_only(self):
        self.assertTrue(entity_forbids_hard_delete({"type": "portfolio"}))
        self.assertTrue(entity_forbids_hard_delete({"type": "Org"}))
        self.assertTrue(
            entity_forbids_hard_delete({"index": "irn:entity:portfolio:*"})
        )
        self.assertTrue(
            entity_forbids_hard_delete(
                {"index": "irn:unentity:portfolio/org:p1/*"}
            )
        )
        self.assertFalse(
            entity_forbids_hard_delete(
                {"type": "team", "index": "irn:entity:portfolio/team:p1/*"}
            )
        )
        self.assertFalse(
            entity_forbids_hard_delete(
                {
                    "type": "extension",
                    "index": "irn:entity:portfolio/extension:p1/*",
                }
            )
        )
        self.assertFalse(
            entity_forbids_hard_delete(
                {"type": "user", "index": "irn:entity:user:*"}
            )
        )


class FakeAuthModel:
    def __init__(self):
        self.entities = {}
        self.rels = {}
        self.deleted_entities = []
        self.deleted_rels = []

    def get_entity(self, index, entity_id):
        document = self.entities.get((index, entity_id))
        if not document:
            return {"success": False, "message": "Entity not found", "status": 404}
        return {
            "success": True,
            "message": "Entity found",
            "document": document,
            "status": 200,
        }

    def list_entity(self, index, limit=50, lastkey=None):
        items = [
            document
            for (stored_index, _entity_id), document in self.entities.items()
            if stored_index == index
        ]
        return {
            "success": True,
            "message": "Documents found",
            "document": {"items": items, "lastkey": None},
            "status": 200,
        }

    def create_entity(self, data):
        self.entities[(data["index"], data["_id"])] = data
        return {
            "success": True,
            "message": "Entity created",
            "document": data,
            "status": 200,
        }

    def update_entity(self, data):
        self.entities[(data["index"], data["_id"])] = data
        return {
            "success": True,
            "message": "Entity updated",
            "document": data,
            "status": 200,
        }

    def delete_entity(self, **entity_document):
        if entity_forbids_hard_delete(entity_document):
            return {
                "success": False,
                "message": HARD_DELETE_REFUSED,
                "document": entity_document,
                "status": 400,
            }
        self.deleted_entities.append(dict(entity_document))
        self.entities.pop((entity_document["index"], entity_document["_id"]), None)
        return {
            "success": True,
            "message": "Entity deleted",
            "document": entity_document,
            "status": 200,
        }

    def get_rel(self, index, rel):
        return {"success": False, "message": "Entity not found", "status": 404}

    def list_rel(self, index, limit=50, lastkey=None):
        items = [dict(item) for item in self.rels.get(index, [])]
        return {
            "success": True,
            "message": "Documents found",
            "document": {"items": items, "lastkey": None},
            "status": 200,
        }

    def delete_rel(self, **rel_document):
        self.deleted_rels.append(dict(rel_document))
        return {
            "success": True,
            "message": "Rel deleted",
            "document": rel_document,
            "status": 200,
        }


def _controller(store):
    with patch("renglo.auth.auth_controller.AuthModel", return_value=store):
        return AuthController(config={})


class SoftDeleteControllerTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeAuthModel()
        self.portfolio_index = "irn:entity:portfolio:*"
        self.org_index = "irn:entity:portfolio/org:p_live/*"
        self.team_index = "irn:entity:portfolio/team:p_live/*"
        self.extension_index = "irn:entity:portfolio/extension:p_live/*"
        self.store.entities[(self.portfolio_index, "p_live")] = _doc(
            self.portfolio_index, "p_live", "portfolio", "Live"
        )
        self.store.entities[(self.portfolio_index, "p_dead")] = _doc(
            self.portfolio_index, "p_dead", "portfolio", "Dead", status="deleted"
        )
        self.store.entities[(self.portfolio_index, "p_arch")] = _doc(
            self.portfolio_index, "p_arch", "portfolio", "Old", status="archived"
        )
        self.store.entities[(self.team_index, "t1")] = _doc(
            self.team_index, "t1", "team", "Admin"
        )
        self.store.entities[(self.org_index, "o_live")] = _doc(
            self.org_index, "o_live", "org", "Shoot"
        )
        self.store.entities[(self.org_index, "o_arch")] = _doc(
            self.org_index, "o_arch", "org", "Wrapped", status="archived"
        )
        self.store.entities[(self.org_index, "o_del")] = _doc(
            self.org_index, "o_del", "org", "Cut", status="deleted"
        )
        self.store.entities[(self.extension_index, "e1")] = _doc(
            self.extension_index, "e1", "extension", "Breakdown"
        )
        self.store.entities[(self.extension_index, "e2")] = _doc(
            self.extension_index, "e2", "extension", "OnlyCut"
        )
        self.store.rels["irn:rel:user:team:user-1:*"] = [{"rel": "t1"}]
        self.store.rels["irn:rel:team:portfolio:t1:*"] = [
            {"rel": "p_dead"},
            {"rel": "p_live"},
            {"rel": "p_missing"},
        ]
        self.store.rels["irn:rel:team/extension:org:t1/e1:*"] = [
            {"rel": "o_del"},
            {"rel": "o_live"},
        ]
        self.store.rels["irn:rel:team/extension:org:t1/e2:*"] = [{"rel": "o_del"}]
        self.auth = _controller(self.store)

    def test_deleted_portfolio_and_org_are_hidden_by_id(self):
        missing_portfolio = self.auth.get_entity("portfolio", portfolio_id="p_dead")
        self.assertFalse(missing_portfolio["success"])
        self.assertEqual(missing_portfolio["status"], 404)
        self.assertNotIn("document", missing_portfolio)

        live = self.auth.get_entity("portfolio", portfolio_id="p_live")
        self.assertTrue(live["success"])
        self.assertEqual(live["document"]["name"], "Live")

        archived = self.auth.get_entity("portfolio", portfolio_id="p_arch")
        self.assertTrue(archived["success"])

        hidden_org = self.auth.get_entity(
            "org", portfolio_id="p_live", org_id="o_del"
        )
        self.assertEqual(hidden_org["status"], 404)
        visible_org = self.auth.get_entity(
            "org", portfolio_id="p_live", org_id="o_arch"
        )
        self.assertTrue(visible_org["success"])

    def test_lists_skip_deleted_and_keep_unmarked_rows(self):
        orgs = self.auth.list_entity("org", portfolio_id="p_live")
        names = [item["name"] for item in orgs["document"]["items"]]
        self.assertEqual(names, ["Shoot", "Wrapped"])

        portfolios = self.auth.list_entity("portfolio", user_id="user-1")
        rels = portfolios["document"][0]["document"]["items"]
        self.assertEqual(
            [item["rel"] for item in rels],
            ["p_live", "p_missing"],
        )
        self.assertEqual(
            self.auth.user_portfolios("user-1"),
            ["p_live", "p_missing"],
        )
        stored_dead = self.store.rels["irn:rel:team:portfolio:t1:*"]
        self.assertEqual(
            [item["rel"] for item in stored_dead],
            ["p_dead", "p_live", "p_missing"],
        )

    def test_tree_omits_deleted_portfolio_org_and_grant(self):
        tree = self.auth.get_tree_full(user_id="user-1")["document"]
        self.assertEqual(list(tree["portfolios"]), ["p_live"])
        orgs = tree["portfolios"]["p_live"]["orgs"]
        self.assertIn("o_live", orgs)
        self.assertIn("o_arch", orgs)
        self.assertNotIn("o_del", orgs)
        self.assertIn("_all", orgs)

        team_extensions = tree["portfolios"]["p_live"]["teams"]["t1"]["extensions"]
        self.assertEqual(team_extensions["e1"]["orgs"], ["o_live"])
        self.assertNotIn("o_del", team_extensions["e2"].get("orgs") or [])
        self.assertTrue(tree["portfolios"]["p_live"]["extensions"]["e1"]["active"])
        self.assertNotEqual(
            tree["portfolios"]["p_live"]["extensions"]["e2"].get("active"),
            True,
        )
        self.assertEqual(self.store.deleted_rels, [])

    def test_soft_delete_keeps_the_row_and_its_rels(self):
        deleted = self.auth.remove_org_funnel(portfolio_id="p_live", org_id="o_live")
        self.assertTrue(deleted["success"])
        stored = self.store.entities[(self.org_index, "o_live")]
        self.assertEqual(stored["status"], ENTITY_STATUS_DELETED)
        self.assertEqual(self.store.deleted_entities, [])
        self.assertEqual(self.store.deleted_rels, [])
        self.assertEqual(
            self.auth.get_entity("org", portfolio_id="p_live", org_id="o_live")["status"],
            404,
        )

        again = self.auth.remove_portfolio_funnel(portfolio_id="p_dead")
        self.assertTrue(again["success"])
        self.assertEqual(again["message"], "Portfolio already deleted")
        self.assertEqual(self.store.deleted_entities, [])

    def test_update_cannot_change_status(self):
        updated = self.auth.update_entity(
            "portfolio",
            portfolio_id="p_live",
            payload={"name": "Renamed", "status": "deleted"},
        )
        self.assertTrue(updated["success"])
        stored = self.store.entities[(self.portfolio_index, "p_live")]
        self.assertEqual(stored["name"], "Renamed")
        self.assertNotIn("status", stored)
        self.assertTrue(
            self.auth.get_entity("portfolio", portfolio_id="p_live")["success"]
        )

    def test_new_entities_are_marked_active(self):
        created = self.auth.create_entity("org", portfolio_id="p_live", name="New")
        self.assertEqual(created["document"]["status"], ENTITY_STATUS_ACTIVE)

    def test_team_delete_still_removes_a_team(self):
        removed = self.auth.remove_team_funnel(portfolio_id="p_live", team_id="t1")
        self.assertTrue(removed["success"])
        self.assertNotIn((self.team_index, "t1"), self.store.entities)
        self.assertTrue(self.store.deleted_entities)

    def test_team_delete_refuses_a_portfolio_document(self):
        self.store.entities[(self.team_index, "t1")]["type"] = "portfolio"
        refused = self.auth.remove_team_funnel(portfolio_id="p_live", team_id="t1")
        self.assertFalse(refused["success"])
        self.assertEqual(refused["message"], HARD_DELETE_REFUSED)
        self.assertIn((self.team_index, "t1"), self.store.entities)
        self.assertEqual(self.store.deleted_entities, [])
        self.assertEqual(self.store.deleted_rels, [])

    def test_extension_delete_refuses_an_org_document(self):
        self.store.entities[(self.extension_index, "e1")]["type"] = "org"
        refused = self.auth.remove_tool_funnel(portfolio_id="p_live", tool_id="e1")
        self.assertFalse(refused["success"])
        self.assertEqual(refused["message"], HARD_DELETE_REFUSED)
        self.assertEqual(
            self.store.entities[(self.extension_index, "e1")]["type"],
            "org",
        )
        self.assertEqual(self.store.deleted_rels, [])

    def test_extension_delete_still_removes_an_extension(self):
        removed = self.auth.remove_tool_funnel(portfolio_id="p_live", tool_id="e2")
        self.assertTrue(removed["success"])
        self.assertNotIn((self.extension_index, "e2"), self.store.entities)
        self.assertIn((self.portfolio_index, "p_live"), self.store.entities)
        self.assertIn((self.org_index, "o_live"), self.store.entities)


class AuthModelHardDeleteTests(unittest.TestCase):
    def test_model_refuses_portfolio_and_org_and_deletes_a_team(self):
        with patch("renglo.auth.auth_model.boto3"):
            from renglo.auth.auth_model import AuthModel

            model = AuthModel(config={})
        model.entity_table = MagicMock()

        refused = model.delete_entity(
            index="irn:entity:portfolio:*",
            _id="p1",
            type="portfolio",
        )
        self.assertFalse(refused["success"])
        self.assertEqual(refused["status"], 400)
        model.entity_table.delete_item.assert_not_called()

        refused_org = model.delete_entity(
            index="irn:entity:portfolio/org:p1/*",
            _id="o1",
        )
        self.assertEqual(refused_org["message"], HARD_DELETE_REFUSED)
        model.entity_table.delete_item.assert_not_called()

        model.entity_table.delete_item.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        deleted = model.delete_entity(
            index="irn:entity:portfolio/team:p1/*",
            _id="t1",
            type="team",
        )
        self.assertTrue(deleted["success"])
        model.entity_table.delete_item.assert_called_once()


if __name__ == "__main__":
    unittest.main()
