"""Federated sign-up decisions and identity rows."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.auth.auth_controller import AuthController  # noqa: E402
from renglo.auth.federated_identity import USER_IDENTITY_INDEX  # noqa: E402
from renglo.auth.federated_signup import (  # noqa: E402
    external_signup_error,
    should_deny_external_signup,
)


class PreSignupDecisionTests(unittest.TestCase):
    def test_new_external_email_is_allowed(self):
        self.assertFalse(should_deny_external_signup("PreSignUp_ExternalProvider", False))
        self.assertIsNone(external_signup_error("PreSignUp_ExternalProvider", False))

    def test_existing_email_is_denied_for_any_external_provider(self):
        self.assertTrue(should_deny_external_signup("PreSignUp_ExternalProvider", True))
        self.assertIn("already exists", external_signup_error("PreSignUp_ExternalProvider", True))

    def test_native_paths_stay_open_when_the_email_exists(self):
        self.assertFalse(should_deny_external_signup("PreSignUp_SignUp", True))
        self.assertFalse(should_deny_external_signup("PreSignUp_AdminCreateUser", True))
        self.assertIsNone(external_signup_error("PreSignUp_AdminCreateUser", True))


class _Store:
    def __init__(self):
        self.entities = {}

    def get_entity(self, index, entity_id):
        document = self.entities.get((index, entity_id))
        if not document:
            return {"success": False, "message": "Entity not found", "status": 404}
        return {"success": True, "message": "Entity found", "document": document, "status": 200}

    def put_entity_exclusive(self, data):
        key = (data["index"], data["_id"])
        if key in self.entities:
            return {"success": False, "message": "exists", "status": 409, "document": data}
        self.entities[key] = dict(data)
        return {"success": True, "message": "Entity created", "document": data, "status": 200}

    def update_entity(self, data):
        self.entities[(data["index"], data["_id"])] = dict(data)
        return {"success": True, "message": "Entity updated", "document": data, "status": 200}


def _controller(store):
    with patch("renglo.auth.auth_controller.AuthModel", return_value=store):
        return AuthController(config={})


class IdentityRecordTests(unittest.TestCase):
    def test_username_hash_stays_stable_and_distinct(self):
        email_id = AuthController._user_id_from_claims({"cognito:username": "alice@example.com"})
        google_id = AuthController._user_id_from_claims({"username": "Google_123"})
        okta_id = AuthController._user_id_from_claims({"cognito:username": "Okta_abc"})
        self.assertNotEqual(email_id, google_id)
        self.assertNotEqual(google_id, okta_id)
        self.assertEqual(google_id, AuthController._user_id_from_claims({"username": "Google_123"}))
        self.assertEqual(len(email_id), 9)

    def test_native_login_writes_no_identity_row(self):
        store = _Store()
        controller = _controller(store)
        result = controller.record_federated_identities(
            {"cognito:username": "alice@example.com", "email": "alice@example.com"}
        )
        self.assertTrue(result["success"])
        self.assertEqual(store.entities, {})

    def test_okta_and_google_claims_write_the_same_kind_of_row(self):
        store = _Store()
        controller = _controller(store)
        google = controller.record_federated_identities(
            {
                "username": "Google_123",
                "sub": "cognito-google",
                "email": "ada@gmail.com",
                "identities": [
                    {"providerName": "Google", "userId": "google-subject"},
                ],
            }
        )
        okta = controller.record_federated_identities(
            {
                "cognito:username": "Okta_abc",
                "sub": "cognito-okta",
                "email": "ada@company.com",
                "identities": '[{"providerName": "Okta", "userId": "okta-subject"}]',
            }
        )
        self.assertTrue(google["success"])
        self.assertTrue(okta["success"])
        rows = list(store.entities.values())
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["type"] for row in rows}, {"user_identity"})
        self.assertEqual({row["provider_name"] for row in rows}, {"Google", "Okta"})
        google_row = store.entities[(USER_IDENTITY_INDEX, "Google#google-subject")]
        self.assertEqual(
            google_row["user_id"],
            AuthController._user_id_from_claims({"username": "Google_123"}),
        )

    def test_conflicting_identity_does_not_move_user_id(self):
        store = _Store()
        controller = _controller(store)
        claims = {
            "username": "Google_123",
            "sub": "cognito-google",
            "email": "ada@gmail.com",
            "identities": [{"providerName": "Google", "userId": "google-subject"}],
        }
        self.assertTrue(controller.record_federated_identities(claims)["success"])
        original = store.entities[(USER_IDENTITY_INDEX, "Google#google-subject")]["user_id"]
        conflict = controller.record_federated_identities(
            {
                "username": "Google_999",
                "sub": "other",
                "email": "ada@gmail.com",
                "identities": [{"providerName": "Google", "userId": "google-subject"}],
            }
        )
        self.assertFalse(conflict["success"])
        self.assertEqual(conflict["status"], 409)
        self.assertEqual(
            store.entities[(USER_IDENTITY_INDEX, "Google#google-subject")]["user_id"],
            original,
        )

    def test_repeat_login_refreshes_the_same_row(self):
        store = _Store()
        controller = _controller(store)
        claims = {
            "username": "Okta_abc",
            "sub": "cognito-okta",
            "identities": [{"providerName": "Okta", "userId": "okta-subject"}],
        }
        controller.record_federated_identities(claims)
        controller.record_federated_identities(claims)
        self.assertEqual(len(store.entities), 1)
