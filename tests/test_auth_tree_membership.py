"""Shared user, team, and portfolio trees stay aligned with relationship writes."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.auth.auth_controller import AuthController  # noqa: E402
from renglo.auth.auth_trees import AuthTreeStore, MemoryTreeS3  # noqa: E402


def _doc(index, entity_id, entity_type, name):
    return {
        "_id": entity_id,
        "index": index,
        "type": entity_type,
        "name": name,
        "handle": name.lower(),
        "preferences": {},
    }


class FakeAuth:
    def __init__(self):
        self.entities = {}
        self.rels = {}

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
            "document": {"items": items, "lastkey": None},
            "status": 200,
        }

    def list_rel(self, index, limit=50, lastkey=None):
        return {
            "success": True,
            "document": {"items": [dict(item) for item in self.rels.get(index, [])]},
            "status": 200,
        }

    def create_rel(self, index=None, rel=None, **_kwargs):
        bucket = self.rels.setdefault(index, [])
        if not any(item.get("rel") == rel for item in bucket):
            bucket.append({"rel": rel})
        return {"success": True, "document": {"index": index, "rel": rel}, "status": 200}

    def delete_rel(self, index=None, rel=None, **_kwargs):
        self.rels[index] = [
            item for item in self.rels.get(index, []) if item.get("rel") != rel
        ]
        return {"success": True, "document": {"index": index, "rel": rel}, "status": 200}


def _controller(store, s3=None):
    with patch("renglo.auth.auth_controller.AuthModel", return_value=store):
        controller = AuthController(config={"S3_BUCKET_NAME": "bucket"})
    controller.AUM = store
    controller.trees._client = s3 or MemoryTreeS3()
    return controller


def _seed_portfolio(store, portfolio_id="p1", org_id="org-1", team_id="t1"):
    store.entities[("irn:entity:portfolio:*", portfolio_id)] = _doc(
        "irn:entity:portfolio:*", portfolio_id, "portfolio", "Movies"
    )
    store.entities[(f"irn:entity:portfolio/org:{portfolio_id}/*", org_id)] = _doc(
        f"irn:entity:portfolio/org:{portfolio_id}/*", org_id, "org", "Shoot"
    )
    store.entities[(f"irn:entity:portfolio/team:{portfolio_id}/*", team_id)] = _doc(
        f"irn:entity:portfolio/team:{portfolio_id}/*", team_id, "team", "Admin"
    )
    store.rels[f"irn:rel:team:portfolio:{team_id}:*"] = [{"rel": portfolio_id}]


class SharedAuthTreeTests(unittest.TestCase):
    def test_a_team_grant_is_visible_to_another_member_without_rewriting_their_tree(self):
        store = FakeAuth()
        _seed_portfolio(store)
        store.entities[("irn:entity:portfolio/tool:p1/*", "tool-1")] = _doc(
            "irn:entity:portfolio/tool:p1/*", "tool-1", "tool", "Breakdown"
        )
        s3 = MemoryTreeS3()
        controller = _controller(store, s3)
        controller.create_rel("user:team", user_id="user-a", team_id="t1")
        controller.create_rel("user:team", user_id="user-b", team_id="t1")
        user_b = AuthTreeStore.key("user", "user-b")
        puts_before = s3.puts.count(user_b)

        controller.create_rel("team:tool", team_id="t1", tool_id="tool-1")
        controller.create_rel(
            "team/tool:org", team_id="t1", tool_id="tool-1", org_id="org-1"
        )

        self.assertEqual(s3.puts.count(user_b), puts_before)
        self.assertNotIn("auth/tree/user-b", s3.puts)
        assembled = controller.trees.assemble("user-b")
        org = assembled["document"]["portfolios"]["p1"]["orgs"]["org-1"]
        self.assertIn("tool-1", org["tools"])
        self.assertTrue(org["active"])

    def test_removing_membership_drops_the_team_and_denies_the_portfolio(self):
        store = FakeAuth()
        _seed_portfolio(store)
        controller = _controller(store)
        controller.create_rel("user:team", user_id="user-a", team_id="t1")
        removed = controller.delete_rel("user:team", user_id="user-a", team_id="t1")
        self.assertTrue(removed["success"])

        user = json.loads(
            controller.trees._client.objects[AuthTreeStore.key("user", "user-a")]
        )
        self.assertEqual(user["teams"], [])
        denied = controller.authorize("p1", "org-1", resource="org", user_id="user-a")
        self.assertFalse(denied["success"])
        self.assertEqual(denied["status"], 403)

    def test_two_teams_union_into_one_org_tool_list_only_at_assembly(self):
        store = FakeAuth()
        _seed_portfolio(store, team_id="team-a")
        store.entities[("irn:entity:portfolio/team:p1/*", "team-b")] = _doc(
            "irn:entity:portfolio/team:p1/*", "team-b", "team", "Crew"
        )
        store.rels["irn:rel:team:portfolio:team-b:*"] = [{"rel": "p1"}]
        store.entities[("irn:entity:portfolio/tool:p1/*", "tool-a")] = _doc(
            "irn:entity:portfolio/tool:p1/*", "tool-a", "tool", "Breakdown"
        )
        store.entities[("irn:entity:portfolio/tool:p1/*", "tool-b")] = _doc(
            "irn:entity:portfolio/tool:p1/*", "tool-b", "tool", "CallSheet"
        )
        controller = _controller(store)
        controller.create_rel("user:team", user_id="user-1", team_id="team-a")
        controller.create_rel("user:team", user_id="user-1", team_id="team-b")
        for team_id, tool_id in (("team-a", "tool-a"), ("team-b", "tool-b")):
            controller.create_rel("team:tool", team_id=team_id, tool_id=tool_id)
            controller.create_rel(
                "team/tool:org", team_id=team_id, tool_id=tool_id, org_id="org-1"
            )
        self.assertTrue(controller.trees.note("portfolio", "p1")["success"])

        stored = json.loads(
            controller.trees._client.objects[AuthTreeStore.key("portfolio", "p1")]
        )
        self.assertNotIn("tools", stored["orgs"]["org-1"])
        team_a = json.loads(
            controller.trees._client.objects[AuthTreeStore.key("team", "team-a")]
        )
        self.assertEqual(team_a["tools"]["tool-a"]["orgs"], ["org-1"])
        self.assertEqual(team_a["tools"]["tool-b"].get("orgs") or [], [])

        assembled = controller.trees.assemble("user-1")["document"]
        self.assertCountEqual(
            assembled["portfolios"]["p1"]["orgs"]["org-1"]["tools"],
            ["tool-a", "tool-b"],
        )

    def test_a_failed_s3_patch_fails_create_rel(self):
        store = FakeAuth()
        _seed_portfolio(store)
        s3 = MemoryTreeS3(fail_put=True)
        controller = _controller(store, s3)
        result = controller.create_rel("team:tool", team_id="t1", tool_id="tool-1")
        self.assertFalse(result["success"])
        self.assertEqual(result["status"], 500)

    def test_a_funnel_flushes_each_dirty_tree_once(self):
        store = FakeAuth()
        _seed_portfolio(store)
        s3 = MemoryTreeS3()
        controller = _controller(store, s3)

        controller.trees.begin()
        controller.trees.note("team", "t1")
        controller.trees.note("team", "t1")
        controller.trees.note("user", "user-a")
        controller.trees.note("user", "user-a")
        self.assertEqual(s3.puts, [])
        flushed = controller.trees.end()
        self.assertTrue(flushed["success"])
        self.assertEqual(
            sorted(s3.puts),
            [
                AuthTreeStore.key("team", "t1"),
                AuthTreeStore.key("user", "user-a"),
            ],
        )

        s3.puts.clear()
        applied = controller._apply_installable_rels(
            "POST", "assign", "t1", "tool-1"
        )
        self.assertTrue(applied["success"])
        self.assertEqual(s3.puts, [AuthTreeStore.key("team", "t1")])

    def test_authorize_for_a_known_extension_does_not_load_other_portfolios(self):
        store = FakeAuth()
        s3 = MemoryTreeS3()
        controller = _controller(store, s3)
        s3.objects[AuthTreeStore.key("user", "u1")] = json.dumps({
            "user_id": "u1",
            "preferences": {},
            "teams": [
                {"team_id": "t1", "portfolio_id": "p1"},
                {"team_id": "t2", "portfolio_id": "p2"},
            ],
        }).encode()
        s3.objects[AuthTreeStore.key("team", "t1")] = json.dumps({
            "team_id": "t1",
            "portfolio_id": "p1",
            "name": "Admin",
            "preferences": {},
            "tools": {},
            "extensions": {"ext-1": {"roles": ["editor"], "orgs": ["org-1"]}},
            "tools_access": [],
            "extensions_access": ["ext-1"],
        }).encode()
        s3.objects[AuthTreeStore.key("team", "t2")] = json.dumps({
            "team_id": "t2",
            "portfolio_id": "p2",
            "name": "Other",
            "preferences": {},
            "tools": {},
            "extensions": {},
        }).encode()
        s3.objects[AuthTreeStore.key("portfolio", "p1")] = json.dumps({
            "portfolio_id": "p1",
            "name": "Movies",
            "preferences": {},
            "team_ids": ["t1"],
            "orgs": {
                "org-1": {
                    "org_id": "org-1",
                    "name": "Shoot",
                    "handle": "shoot",
                    "tags": {},
                    "preferences": {},
                }
            },
            "tools": {},
            "extensions": {
                "ext-1": {
                    "extension_id": "ext-1",
                    "name": "Breakdown",
                    "handle": "breakdown",
                    "roles": ["editor"],
                    "entity_type": "extension",
                    "preferences": {},
                }
            },
        }).encode()
        s3.objects[AuthTreeStore.key("portfolio", "p2")] = json.dumps({
            "portfolio_id": "p2",
            "name": "Other",
            "preferences": {},
            "team_ids": ["t2"],
            "orgs": {},
            "tools": {},
            "extensions": {},
        }).encode()
        s3.gets.clear()

        allowed = controller.authorize(
            "p1",
            "org-1",
            resource="extension",
            tool_id="ext-1",
            user_id="u1",
        )
        self.assertTrue(allowed["success"])
        self.assertEqual(allowed["roles"], ["editor"])
        self.assertEqual(allowed["tool_id"], "ext-1")
        self.assertNotIn(AuthTreeStore.key("portfolio", "p2"), s3.gets)
        self.assertNotIn(AuthTreeStore.key("team", "t2"), s3.gets)
        self.assertNotIn("auth/tree/u1", s3.gets)
        self.assertIn(AuthTreeStore.key("portfolio", "p1"), s3.gets)
