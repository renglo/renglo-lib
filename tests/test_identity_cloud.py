"""CLOUD selects the directory and the token checker."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.auth.auth_controller import AuthController  # noqa: E402
from renglo.auth.providers.identity_aws import Identity  # noqa: E402
from renglo.auth.tokens.token_aws import map_claims as map_aws  # noqa: E402
from renglo.auth.tokens.token_azure import map_claims as map_entra  # noqa: E402
from renglo.common import create_md5_hash  # noqa: E402


class ClaimNormalizationTests(unittest.TestCase):
    def test_cognito_and_entra_share_the_user_id(self):
        aws = map_aws({
            "cognito:username": "ada@example.com",
            "email": "ada@example.com",
            "given_name": "Ada",
            "family_name": "Lovelace",
            "sub": "cognito-sub",
        })
        entra = map_entra({
            "preferred_username": "ada@example.com",
            "email": "ada@example.com",
            "given_name": "Ada",
            "family_name": "Lovelace",
            "oid": "entra-oid",
        })
        self.assertEqual(aws["username"], entra["username"])
        self.assertEqual(aws["email"], entra["email"])
        self.assertEqual(
            AuthController._user_id_from_claims(aws),
            AuthController._user_id_from_claims(entra),
        )
        self.assertEqual(len(AuthController._user_id_from_claims(aws)), 9)
        self.assertEqual(
            AuthController._user_id_from_claims(aws),
            create_md5_hash("ada@example.com", 9),
        )
        self.assertEqual(aws["subject"], "cognito-sub")
        self.assertEqual(entra["subject"], "entra-oid")

    def test_access_token_username_and_entra_fallbacks(self):
        self.assertEqual(map_aws({"username": "Google_123"})["username"], "Google_123")
        self.assertEqual(map_entra({"email": "ada@example.com"})["username"], "ada@example.com")
        self.assertEqual(map_entra({"oid": "directory-object"})["username"], "directory-object")
        self.assertEqual(
            map_entra({"signInNames.emailAddress": "ada@example.com", "oid": "customer"})["username"],
            "ada@example.com",
        )
        self.assertEqual(map_entra({"emails": ["ada@example.com"], "oid": "customer"})["username"], "ada@example.com")


class CustomerDirectoryTests(unittest.TestCase):
    def test_create_uses_the_email_identity_of_this_environment(self):
        from renglo.auth.providers.identity_azure import Identity

        identity = Identity.__new__(Identity)
        identity.config = {"ENTRA_TENANT_DOMAIN": "arbitiumpoc1.onmicrosoft.com"}
        identity.calls = []

        def _graph(method, path, **kwargs):
            identity.calls.append((method, path, kwargs.get("body")))
            return {
                "success": True,
                "document": {
                    "id": "oid",
                    "userPrincipalName": "generated@arbitiumpoc1.onmicrosoft.com",
                    "identities": [{
                        "signInType": "emailAddress",
                        "issuer": "arbitiumpoc1.onmicrosoft.com",
                        "issuerAssignedId": "ada@example.com",
                    }],
                },
                "status": 201,
            }

        identity._graph = _graph
        created = identity.create_user_with_password("ada@example.com", "Secret1!", "Ada", "Lovelace")
        self.assertTrue(created["success"])
        self.assertEqual(created["document"]["username"], "ada@example.com")
        body = identity.calls[0][2]
        self.assertNotIn("userPrincipalName", body)
        self.assertEqual(body["identities"][0]["issuerAssignedId"], "ada@example.com")
        self.assertEqual(body["identities"][0]["issuer"], "arbitiumpoc1.onmicrosoft.com")
        self.assertEqual(body["passwordPolicies"], "DisablePasswordExpiration")


class AwsDirectoryDocumentTests(unittest.TestCase):
    def test_create_user_returns_username(self):
        class _Client:
            def admin_create_user(self, **kwargs):
                return {"User": {"Username": kwargs["Username"]}}

            def admin_set_user_password(self, **kwargs):
                return {}

        identity = Identity.__new__(Identity)
        identity.USER_POOL_ID = "pool"
        identity.cognito_client = _Client()
        created = identity.create_user_with_password("ada@example.com", "Secret1!", "Ada", "Lovelace")
        self.assertTrue(created["success"])
        self.assertEqual(created["document"]["username"], "ada@example.com")


class TokenCheckerSelectionTests(unittest.TestCase):
    @patch("renglo.auth.tokens.token_azure.merge_with_metadata", return_value=None)
    def test_azure_init_does_not_import_flask_cognito(self, _merge):
        from flask import Flask

        from renglo.auth.request_identity import init_auth

        sys.modules.pop("flask_cognito", None)
        sys.modules.pop("renglo.auth.tokens.token_aws", None)
        app = Flask("azure-identity")
        app.config["CLOUD"] = "azure"
        app.config["ENTRA_TENANT_ID"] = "tenant"
        app.config["ENTRA_CLIENT_ID"] = "client"
        init_auth(app)
        self.assertNotIn("flask_cognito", sys.modules)
        self.assertEqual(app.extensions["renglo_auth"]["tenant_id"], "tenant")
        self.assertEqual(
            app.extensions["renglo_auth"]["issuer"],
            "https://login.microsoftonline.com/tenant/v2.0",
        )

    @patch(
        "renglo.auth.tokens.token_azure.merge_with_metadata",
        return_value={
            "authority_base": "https://customer-tenant.ciamlogin.com/customer-tenant",
            "issuer": "https://customer-tenant.ciamlogin.com/customer-tenant/v2.0",
            "jwks_uri": "https://customer-tenant.ciamlogin.com/customer-tenant/discovery/v2.0/keys",
            "token_endpoint": "https://customer-tenant.ciamlogin.com/customer-tenant/oauth2/v2.0/token",
        },
    )
    def test_external_directory_uses_its_own_issuer(self, _merge):
        from flask import Flask

        from renglo.auth.request_identity import init_auth

        app = Flask("external-directory")
        app.config["CLOUD"] = "azure"
        app.config["ENTRA_TENANT_ID"] = "customer-tenant"
        app.config["ENTRA_CLIENT_ID"] = "client"
        init_auth(app)
        checker = app.extensions["renglo_auth"]
        self.assertEqual(checker["issuer"], "https://customer-tenant.ciamlogin.com/customer-tenant/v2.0")
        self.assertIn("ciamlogin.com", checker["jwks"].uri)

    def test_aws_init_installs_cognito(self):
        from flask import Flask

        from renglo.auth.request_identity import init_auth

        app = Flask("aws-identity")
        app.config["CLOUD"] = "aws"
        app.config["COGNITO_REGION"] = "us-east-1"
        app.config["COGNITO_USERPOOL_ID"] = "pool"
        app.config["COGNITO_APP_CLIENT_ID"] = "client"
        app.config["COGNITO_JWT_HEADER_NAME"] = "Authorization"
        app.config["COGNITO_JWT_HEADER_PREFIX"] = "Bearer"
        init_auth(app)
        self.assertIn("cognito_auth", app.extensions)
