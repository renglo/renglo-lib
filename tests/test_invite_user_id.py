"""Existing-user invites must use the same id sign-in uses."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.auth.auth_controller import AuthController  # noqa: E402
from renglo.auth.auth_trees import MemoryTreeS3  # noqa: E402
from renglo.common import create_md5_hash  # noqa: E402

GOOGLE_USERNAME = "Google_123"
GOOGLE_SUB = "516d3d3c-0e25-4d02-8957-93383f0036f0"
PASSWORD_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


class _Store:
    def __init__(self, entities=None, matches=None):
        self.entities = entities or {}
        self.matches = matches or []
        self.deleted = []

    def check_user_by_email(self, email):
        return {
            "success": True,
            "message": "User found",
            "document": {"email": email, "matches": self.matches},
            "status": 200,
        }

    def get_entity(self, index, entity_id):
        document = self.entities.get((index, entity_id))
        if not document:
            return {"success": False, "message": "Entity not found", "status": 404}
        return {"success": True, "message": "Entity found", "document": document, "status": 200}

    def delete_rel(self, **rel_document):
        self.deleted.append(rel_document)
        return {"success": True, "message": "Rel deleted", "document": rel_document, "status": 200}

    def list_rel(self, index, limit=50, lastkey=None):
        return {
            "success": True,
            "message": "Documents found",
            "document": {"items": [], "lastkey": None},
            "status": 200,
        }


def _controller(store):
    with patch("renglo.auth.auth_controller.AuthModel", return_value=store):
        controller = AuthController(config={"S3_BUCKET_NAME": "bucket"})
    controller.AUM = store
    controller.trees._client = MemoryTreeS3()
    return controller


class InvitePresentationTests(unittest.TestCase):
    def test_opening_the_app_is_not_the_same_as_creating_the_row(self):
        self.assertTrue(AuthController._user_has_opened_app({"last_login": True}))
        self.assertFalse(AuthController._user_has_opened_app({"last_login": "2026-10-11T10:36:56"}))
        self.assertFalse(AuthController._user_has_opened_app({}))

    def test_short_codes_must_match_the_derived_hash(self):
        store = _Store()
        controller = _controller(store)
        controller.config = {"SECRET_KEY": "test-secret"}
        ttl = "1234567890"
        email = "ada@example.com"
        code = controller.generate_invite_hash(email, ttl)
        self.assertTrue(controller._invite_code_accepted(email, ttl, code))
        self.assertFalse(controller._invite_code_accepted(email, ttl, "000000"))

    def test_a_long_token_is_accepted_without_the_derived_hash(self):
        controller = _controller(_Store())
        token = "a" * 32
        self.assertTrue(controller._invite_code_accepted("ada@example.com", "123", token))

    def test_invite_response_does_not_say_whether_the_account_exists(self):
        controller = _controller(_Store())
        controller._invite_context = lambda *args, **kwargs: {"success": True}
        controller._issue_team_invite = lambda email, team_id, replace=False: {
            "email": email,
            "code": "b" * 32,
            "ttl": "9999999999",
            "expired": False,
        }
        response = controller.invite_user("Ada@Example.com", "team-1", "port-1", "sender")
        self.assertEqual(response["message"], "Invitation is ready.")
        self.assertNotIn("added", response["message"].lower())
        self.assertNotIn("exist", json_text(response).lower())
        self.assertEqual(response["document"]["email"], "ada@example.com")
        self.assertIn("code=", response["document"]["link"])

    def test_accept_adds_only_the_invited_address(self):
        controller = _controller(_Store())
        live = [{
            "email": "ada@example.com",
            "code": "c" * 32,
            "ttl": "9999999999",
            "team_id": "team-1",
            "expired": False,
        }]
        controller._live_email_invites = lambda email: live if email == "ada@example.com" else []
        controller.list_rel = lambda *args, **kwargs: {"success": True, "document": {"items": []}}
        calls = []
        controller.add_user_to_team_funnel = lambda **kwargs: calls.append(("add", kwargs)) or {
            "success": True
        }
        controller.create_rel = lambda reltype, **kwargs: calls.append((reltype, kwargs)) or {
            "success": True
        }
        controller._delete_team_invite_row = lambda team_id, row: calls.append(("delete", team_id))
        controller.refresh_user_tree = lambda user_id: calls.append(("tree", user_id)) or {
            "success": True
        }
        denied = controller.accept_team_invite("other@example.com", "user-2", "team-1")
        self.assertFalse(denied["success"])
        self.assertEqual(calls, [])
        accepted = controller.accept_team_invite("ada@example.com", "user-1", "team-1")
        self.assertTrue(accepted["success"])
        self.assertEqual(calls[0], ("add", {"user_id": "user-1", "team_id": "team-1"}))
        self.assertEqual(calls[1][0], "team:accept")
        self.assertEqual(calls[1][1]["email"], "ada@example.com")
        self.assertEqual(calls[2], ("delete", "team-1"))
        self.assertEqual(calls[3], ("tree", "user-1"))

    def test_decline_does_not_add_the_person(self):
        controller = _controller(_Store())
        controller._live_email_invites = lambda email: [{
            "email": email,
            "code": "c" * 32,
            "ttl": "9999999999",
            "team_id": "team-1",
            "expired": False,
        }]
        calls = []
        controller.add_user_to_team_funnel = lambda **kwargs: calls.append(("add", kwargs)) or {
            "success": True
        }
        controller._delete_team_invite_row = lambda team_id, row: calls.append(("delete", team_id))
        declined = controller.decline_team_invite("ada@example.com", "team-1")
        self.assertTrue(declined["success"])
        self.assertEqual(calls, [("delete", "team-1")])


def json_text(value):
    import json
    return json.dumps(value)


class InviteUserIdTests(unittest.TestCase):
    def test_google_invite_uses_username_hash_not_sub(self):
        google_id = create_md5_hash(GOOGLE_USERNAME, 9)
        sub_id = create_md5_hash(GOOGLE_SUB, 9)
        store = _Store(
            entities={("irn:entity:user:*", google_id): {"_id": google_id}},
            matches=[{
                "username": GOOGLE_USERNAME,
                "sub": GOOGLE_SUB,
                "status": "EXTERNAL_PROVIDER",
            }],
        )
        response = _controller(store).get_user_id("ada@gmail.com")
        self.assertTrue(response["success"])
        self.assertEqual(response["document"]["user_id"], google_id)
        self.assertNotEqual(google_id, sub_id)
        self.assertEqual(response["document"]["cognito_username"], GOOGLE_USERNAME)

    def test_password_invite_stays_on_the_shared_username_and_sub(self):
        user_id = create_md5_hash(PASSWORD_ID, 9)
        store = _Store(
            entities={("irn:entity:user:*", user_id): {"_id": user_id}},
            matches=[{
                "username": PASSWORD_ID,
                "sub": PASSWORD_ID,
                "status": "CONFIRMED",
            }],
        )
        response = _controller(store).get_user_id("ada@example.com")
        self.assertEqual(response["document"]["user_id"], user_id)

    def test_existing_entity_wins_over_a_second_cognito_profile(self):
        google_id = create_md5_hash(GOOGLE_USERNAME, 9)
        password_id = create_md5_hash(PASSWORD_ID, 9)
        store = _Store(
            entities={("irn:entity:user:*", google_id): {"_id": google_id}},
            matches=[
                {"username": PASSWORD_ID, "sub": PASSWORD_ID, "status": "CONFIRMED"},
                {"username": GOOGLE_USERNAME, "sub": GOOGLE_SUB, "status": "EXTERNAL_PROVIDER"},
            ],
        )
        response = _controller(store).get_user_id("ada@gmail.com")
        self.assertEqual(response["document"]["user_id"], google_id)
        self.assertNotEqual(google_id, password_id)

    def test_google_invite_drops_the_sub_link_and_keeps_a_password_link(self):
        google_id = create_md5_hash(GOOGLE_USERNAME, 9)
        sub_id = create_md5_hash(GOOGLE_SUB, 9)
        store = _Store()
        controller = _controller(store)
        controller._drop_phantom_sub_membership(
            team_id="team-1",
            username=GOOGLE_USERNAME,
            sub=GOOGLE_SUB,
            user_id=google_id,
        )
        controller._drop_phantom_sub_membership(
            team_id="team-1",
            username=PASSWORD_ID,
            sub=PASSWORD_ID,
            user_id=create_md5_hash(PASSWORD_ID, 9),
        )
        self.assertEqual(
            store.deleted,
            [
                {"index": "irn:rel:team:user:team-1:*", "rel": sub_id},
                {"index": "irn:rel:user:team:" + sub_id + ":*", "rel": "team-1"},
            ],
        )
