"""Microsoft Entra directory through Microsoft Graph.

Selected when CLOUD is azure. The method names and result documents match
the AWS identity module. Mail is not part of this module.
"""

from __future__ import annotations

import logging
import secrets
import string
import time
from typing import Any, Optional
from urllib.parse import quote, urlencode

import requests

from renglo.auth.entra_openid import merge_with_metadata

logger = logging.getLogger(__name__)

_GRAPH = "https://graph.microsoft.com/v1.0"


def _temp_password(length: int = 20) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    while True:
        pwd = "".join(secrets.choice(alphabet) for _ in range(length))
        if (
            any(c.islower() for c in pwd)
            and any(c.isupper() for c in pwd)
            and any(c.isdigit() for c in pwd)
            and any(c in "!@#$%^&*" for c in pwd)
        ):
            return pwd


def _odata(value: str) -> str:
    return str(value).replace("'", "''")


def _nickname(email: str) -> str:
    local = str(email or "").split("@", 1)[0]
    cleaned = "".join(char for char in local if char.isalnum())
    return cleaned or "user"


def _sign_in_name(user: dict, fallback: str = "") -> str:
    """Email the person types. External ID stores that on identities, not the UPN."""
    for identity in user.get("identities") or []:
        if not isinstance(identity, dict):
            continue
        assigned = str(identity.get("issuerAssignedId") or "").strip()
        if identity.get("signInType") == "emailAddress" and "@" in assigned:
            return assigned
    mail = str(user.get("mail") or "").strip()
    if "@" in mail:
        return mail
    principal = str(user.get("userPrincipalName") or "").strip()
    if "@" in principal and not principal.endswith(".onmicrosoft.com"):
        return principal
    return fallback or principal


class Identity:

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        self._token = ""
        self._token_exp = 0.0

    def lookup_by_email(self, email):
        if not email:
            return {"success": False, "message": "Email is required", "status": 400}
        escaped = _odata(email)
        query = urlencode({
            "$filter": self._user_filter(escaped),
            "$select": "id,userPrincipalName,mail,accountEnabled,identities",
        })
        response = self._graph("GET", f"/users?{query}")
        if not response.get("success"):
            return response
        users = (response.get("document") or {}).get("value") or []
        matches = []
        for user in users:
            username = _sign_in_name(user, email)
            if not username:
                continue
            matches.append({
                "username": username,
                "subject": user.get("id"),
                "status": "CONFIRMED" if user.get("accountEnabled", True) else "DISABLED",
            })
        if not matches:
            return {"success": False, "message": "User not found", "status": 404}
        return {
            "success": True,
            "message": "User found",
            "document": {
                "email": email,
                "username": matches[0]["username"],
                "subject": matches[0].get("subject"),
                "matches": matches,
            },
            "status": 200,
        }

    def delete_user(self, username):
        user_id = self._directory_id(username)
        if not user_id.get("success"):
            return user_id
        return self._graph("DELETE", f"/users/{quote(user_id['document'])}", ok_status=(204, 200))

    def create_user_with_password(self, email, password, first="FIRST", last="LAST"):
        return self._create(email, password, first, last, force_change=False)

    def set_password(self, email, password):
        user_id = self._directory_id(email)
        if not user_id.get("success"):
            return user_id
        return self._graph(
            "PATCH",
            f"/users/{quote(user_id['document'])}",
            body={
                "passwordProfile": {
                    "password": password,
                    "forceChangePasswordNextSignIn": False,
                }
            },
            ok_status=(204, 200),
            ok_message="Password assigned",
        )

    def create_user(self, email, first="FIRST", last="LAST"):
        return self._create(email, _temp_password(), first, last, force_change=True)

    def update_profile(self, username, *, given_name=None, family_name=None):
        body = {}
        if given_name is not None:
            body["givenName"] = given_name
        if family_name is not None:
            body["surname"] = family_name
        if not body:
            return {"success": True, "message": "Nothing to update", "status": 200}
        user_id = self._directory_id(username)
        if not user_id.get("success"):
            return user_id
        result = self._graph(
            "PATCH",
            f"/users/{quote(user_id['document'])}",
            body=body,
            ok_status=(204, 200),
            ok_message="User updated",
        )
        return result

    def send_email(self, sender, recipient, subject, body_text, body_html, inline_images=None):
        raise NotImplementedError("Azure email is not implemented")

    def _create(self, email, password, first, last, *, force_change: bool):
        if not email:
            return {"success": False, "message": "Email is required", "status": 400}
        display = f"{first} {last}".strip() or email
        domain = self._customer_domain()
        if domain:
            body = {
                "accountEnabled": True,
                "displayName": display,
                "mail": email,
                "givenName": first,
                "surname": last,
                "identities": [{
                    "signInType": "emailAddress",
                    "issuer": domain,
                    "issuerAssignedId": email,
                }],
                "passwordProfile": {
                    "password": password,
                    "forceChangePasswordNextSignIn": False,
                },
                "passwordPolicies": "DisablePasswordExpiration",
            }
        else:
            body = {
                "accountEnabled": True,
                "displayName": display,
                "mailNickname": _nickname(email),
                "userPrincipalName": email,
                "mail": email,
                "givenName": first,
                "surname": last,
                "passwordProfile": {
                    "password": password,
                    "forceChangePasswordNextSignIn": force_change,
                },
            }
        result = self._graph("POST", "/users", body=body)
        if not result.get("success"):
            return result
        created = result.get("document") or {}
        username = _sign_in_name(created, email)
        message = (
            "User created with permanent password"
            if not force_change
            else "User created successfully"
        )
        return {
            "success": True,
            "message": message,
            "document": {"username": username},
            "status": 200,
        }

    def _directory_id(self, username):
        name = str(username or "").strip()
        if not name:
            return {"success": False, "message": "Username is required", "status": 400}
        direct = self._graph("GET", f"/users/{quote(name)}?$select=id")
        if direct.get("success"):
            found = str((direct.get("document") or {}).get("id") or "")
            if found:
                return {"success": True, "document": found, "status": 200}
        escaped = _odata(name)
        listed = self._graph(
            "GET",
            "/users?" + urlencode({
                "$filter": self._user_filter(escaped),
                "$select": "id",
            }),
        )
        if not listed.get("success"):
            return listed
        values = (listed.get("document") or {}).get("value") or []
        if not values:
            return {"success": False, "message": "User not found", "status": 404}
        return {"success": True, "document": values[0].get("id"), "status": 200}

    def _customer_domain(self) -> str:
        return str(self.config.get("ENTRA_TENANT_DOMAIN") or "").strip()

    def _user_filter(self, escaped: str) -> str:
        clauses = [
            f"mail eq '{escaped}'",
            f"userPrincipalName eq '{escaped}'",
        ]
        domain = _odata(self._customer_domain())
        if domain:
            clauses.append(
                f"identities/any(c:c/issuerAssignedId eq '{escaped}' and c/issuer eq '{domain}')"
            )
        return " or ".join(clauses)

    def _token_url(self, tenant: str) -> str:
        merged = merge_with_metadata(
            tenant,
            str(self.config.get("ENTRA_AUTHORITY") or "").strip(),
            str(self.config.get("ENTRA_JWKS_URI") or "").strip(),
        )
        if merged and merged.get("token_endpoint"):
            return merged["token_endpoint"]
        return f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

    def _graph(
        self,
        method: str,
        path: str,
        *,
        body: Optional[dict] = None,
        ok_status: tuple = (200, 201),
        ok_message: str = "OK",
    ) -> dict[str, Any]:
        token = self._access_token()
        if not token.get("success"):
            return token
        try:
            response = requests.request(
                method,
                f"{_GRAPH}{path}",
                headers={
                    "Authorization": f"Bearer {token['document']}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.warning("Graph request failed: %s", exc)
            return {"success": False, "message": str(exc), "status": 400}
        if response.status_code in ok_status:
            document = {}
            if response.content:
                try:
                    document = response.json()
                except ValueError:
                    document = {}
            return {
                "success": True,
                "message": ok_message,
                "document": document,
                "status": response.status_code,
            }
        message = response.text or "Graph request failed"
        try:
            payload = response.json()
            message = ((payload.get("error") or {}).get("message")) or message
        except ValueError:
            pass
        return {
            "success": False,
            "message": message,
            "status": response.status_code or 400,
        }

    def _access_token(self) -> dict[str, Any]:
        now = time.time()
        if self._token and now < self._token_exp - 30:
            return {"success": True, "document": self._token, "status": 200}
        tenant = str(self.config.get("ENTRA_TENANT_ID") or "").strip()
        client_id = str(self.config.get("ENTRA_CLIENT_ID") or "").strip()
        secret = str(self.config.get("ENTRA_CLIENT_SECRET") or "").strip()
        missing = [
            name
            for name, value in (
                ("ENTRA_TENANT_ID", tenant),
                ("ENTRA_CLIENT_ID", client_id),
                ("ENTRA_CLIENT_SECRET", secret),
            )
            if not value
        ]
        if missing:
            return {
                "success": False,
                "message": f"{', '.join(missing)} is required",
                "status": 400,
            }
        try:
            response = requests.post(
                self._token_url(tenant),
                data={
                    "client_id": client_id,
                    "client_secret": secret,
                    "scope": "https://graph.microsoft.com/.default",
                    "grant_type": "client_credentials",
                },
                timeout=20,
            )
        except requests.RequestException as exc:
            return {"success": False, "message": str(exc), "status": 400}
        if response.status_code != 200:
            return {
                "success": False,
                "message": "Could not sign in to Microsoft Graph",
                "status": response.status_code,
            }
        payload = response.json()
        self._token = str(payload.get("access_token") or "")
        self._token_exp = now + int(payload.get("expires_in") or 0)
        if not self._token:
            return {"success": False, "message": "Graph token was empty", "status": 400}
        return {"success": True, "document": self._token, "status": 200}
