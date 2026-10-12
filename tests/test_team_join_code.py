"""Open team codes request access. They do not grant membership."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.auth.auth_controller import AuthController  # noqa: E402
from renglo.auth.auth_trees import INVITE_REL_TYPES, MemoryTreeS3  # noqa: E402


class _RelTable:
    def __init__(self):
        self.rows = []

    def create_rel(self, **document):
        self.rows.append({"index": document["index"], "rel": document["rel"]})
        return {"success": True, "document": document, "status": 200}

    def delete_rel(self, **document):
        self.rows = [
            row for row in self.rows
            if not (row["index"] == document["index"] and row["rel"] == document["rel"])
        ]
        return {"success": True, "status": 200}

    def list_rel(self, index, limit=50, lastkey=None):
        items = [row for row in self.rows if row["index"] == index]
        return {
            "success": True,
            "document": {"items": items, "lastkey": None},
            "status": 200,
        }


def _controller(store=None):
    store = store or _RelTable()
    with patch("renglo.auth.auth_controller.AuthModel", return_value=store):
        controller = AuthController(config={"S3_BUCKET_NAME": "bucket"})
    controller.AUM = store
    controller.trees._client = MemoryTreeS3()
    return controller


class OpenJoinCodeTests(unittest.TestCase):
    def _ready(self):
        controller = _controller()
        controller._invite_context = lambda *args, **kwargs: {"success": True}
        controller._invite_place_names = lambda team_id: {
            "team_name": "Northstar Staff",
            "portfolio_name": "Northstar",
        }
        return controller

    def test_issue_regenerate_and_revoke_keep_one_code(self):
        controller = self._ready()
        issued = controller.issue_team_join_code("team-1", "port-1", "owner")
        code = issued["document"]["code"]
        self.assertTrue(code.startswith("FRM-"))
        again = controller.issue_team_join_code("team-1", "port-1", "owner")
        self.assertEqual(again["document"]["code"], code)

        replaced = controller.issue_team_join_code("team-1", "port-1", "owner", replace=True)
        self.assertNotEqual(replaced["document"]["code"], code)
        self.assertEqual(len(controller._team_join_rows("team-1")), 1)
        lookup = controller.list_rel("join:team", code=code)
        self.assertEqual(lookup["document"]["items"], [])

        revoked = controller.revoke_team_join_code("team-1", "port-1", "owner")
        self.assertTrue(revoked["success"])
        self.assertEqual(controller._team_join_rows("team-1"), [])

    def test_redeem_requests_access_without_membership(self):
        controller = self._ready()
        issued = controller.issue_team_join_code("team-1", "port-1", "owner")
        code = issued["document"]["code"]
        added = []
        controller.add_user_to_team_funnel = lambda **kwargs: added.append(kwargs) or {"success": True}

        first = controller.redeem_join_code("user-2", code.lower())
        second = controller.redeem_join_code("user-2", code)
        self.assertEqual(first["document"]["status"], "access-requested")
        self.assertEqual(second["message"], "Access requested.")
        self.assertEqual(controller._access_user_ids("team-1"), ["user-2"])
        self.assertEqual(added, [])
        self.assertEqual(controller.trees._dirty, set())

    def test_unknown_code_stays_generic(self):
        controller = self._ready()
        response = controller.redeem_join_code("user-2", "FRM-XXXX-YYYY")
        self.assertFalse(response["success"])
        self.assertNotIn("Northstar", response["message"])

    def test_approve_adds_the_user_and_reject_does_not(self):
        controller = self._ready()
        controller._team_portfolio_or_error = lambda team_id: ("port-1", None)
        controller.create_rel("team:access", team_id="team-1", user_id="user-2")
        added = []
        controller.add_user_to_team_funnel = lambda **kwargs: added.append(kwargs) or {"success": True}

        rejected = controller.reject_team_access("team-1", "user-2", "owner")
        self.assertTrue(rejected["success"])
        self.assertEqual(added, [])
        self.assertEqual(controller._access_user_ids("team-1"), [])

        controller.create_rel("team:access", team_id="team-1", user_id="user-2")
        approved = controller.approve_team_access("team-1", "user-2", "owner")
        self.assertTrue(approved["success"])
        self.assertEqual(added, [{"user_id": "user-2", "team_id": "team-1"}])
        self.assertEqual(controller._access_user_ids("team-1"), [])

    def test_pending_request_is_not_an_auth_tree_membership(self):
        self.assertIn("team:access", INVITE_REL_TYPES)
        self.assertIn("team:join", INVITE_REL_TYPES)
        self.assertIn("join:team", INVITE_REL_TYPES)
        controller = _controller()
        controller.trees.note_rel("team:access", {"team_id": "team-1", "user_id": "user-2"})
        controller.trees.note_rel("team:join", {"team_id": "team-1", "code": "FRM-ABCD-EFGH"})
        self.assertEqual(controller.trees._dirty, set())

    def test_new_portfolio_admin_team_is_named_and_separate(self):
        controller = _controller()
        created = []
        rels = []

        def create_entity(kind, **kwargs):
            document = {"_id": f"{kind}-new", "name": kwargs.get("name"), "type": kind}
            created.append((kind, kwargs.get("name"), document["_id"]))
            return {"success": True, "document": document}

        controller.create_entity = create_entity
        controller.create_rel = lambda reltype, **data: rels.append((reltype, dict(data))) or {"success": True}
        response = controller.create_portfolio_funnel(name="Northstar", user_id="user-1")
        self.assertTrue(response["success"])
        self.assertIn(("portfolio", "Northstar", "portfolio-new"), created)
        self.assertIn(("team", "Northstar Admin", "team-new"), created)
        memberships = [data for reltype, data in rels if reltype == "user:team"]
        self.assertEqual(memberships, [{"user_id": "user-1", "team_id": "team-new"}])

    def test_staff_membership_does_not_select_another_portfolios_admin(self):
        admin, staff = AuthController._pick_core_teams("Northstar", [
            {"_id": "staff-1", "name": "Northstar Staff"},
            {"_id": "camera", "name": "Camera"},
        ])
        self.assertIsNone(admin)
        self.assertEqual(staff["_id"], "staff-1")

        admin, staff = AuthController._pick_core_teams("Northstar", [
            {"_id": "legacy", "name": "Admin"},
            {"_id": "named", "name": "Northstar Admin"},
            {"_id": "legacy-staff", "name": "Staff"},
        ])
        self.assertEqual(admin["_id"], "named")
        self.assertEqual(staff["_id"], "legacy-staff")


if __name__ == "__main__":
    unittest.main()
