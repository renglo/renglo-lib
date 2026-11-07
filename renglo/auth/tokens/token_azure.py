"""Validate an Entra ID bearer token and map it to the shared identity."""

from __future__ import annotations

from typing import Any, Dict, Optional

import jwt
from jwt import PyJWKClient

from renglo.auth.entra_openid import merge_with_metadata


def _sign_in_claim(raw: Dict[str, Any]) -> str:
    """Email the customer typed. External ID may put it on a sign-in claim."""
    for key in ("preferred_username", "email", "signInNames.emailAddress"):
        value = raw.get(key)
        if isinstance(value, str) and "@" in value:
            return value
    emails = raw.get("emails")
    if isinstance(emails, list):
        for value in emails:
            if isinstance(value, str) and "@" in value:
                return value
    for key in ("preferred_username", "email", "oid"):
        value = raw.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def map_claims(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Username is the sign-in email, otherwise preferred_username, otherwise oid."""
    if not raw:
        return {}
    username = _sign_in_claim(raw)
    email = str(raw.get("email") or "")
    if not email and "@" in username:
        email = username
    return {
        "username": username,
        "email": email,
        "given_name": raw.get("given_name") or "",
        "family_name": raw.get("family_name") or "",
        "subject": str(raw.get("oid") or raw.get("sub") or ""),
    }


def init_auth(app):
    tenant = str(app.config.get("ENTRA_TENANT_ID") or "").strip()
    client_id = str(app.config.get("ENTRA_CLIENT_ID") or "").strip()
    if not tenant:
        raise Exception("ENTRA_TENANT_ID not found in app configuration but it is required.")
    if not client_id:
        raise Exception("ENTRA_CLIENT_ID not found in app configuration but it is required.")
    authority_override = str(app.config.get("ENTRA_AUTHORITY") or "").strip()
    jwks_override = str(app.config.get("ENTRA_JWKS_URI") or "").strip()
    merged = merge_with_metadata(tenant, authority_override, jwks_override)
    if merged:
        issuer = merged["issuer"]
        jwks_uri = merged["jwks_uri"]
    else:
        authority = f"https://login.microsoftonline.com/{tenant}"
        issuer = f"{authority}/v2.0"
        jwks_uri = f"{authority}/discovery/v2.0/keys"
    app.extensions["renglo_auth"] = {
        "tenant_id": tenant,
        "client_id": client_id,
        "issuer": issuer,
        "jwks": PyJWKClient(jwks_uri),
    }
    return app.extensions["renglo_auth"]


def require_request():
    from flask import current_app, g, jsonify, request

    checker = current_app.extensions.get("renglo_auth")
    if not checker:
        return jsonify(error="Authorization Required", description="Identity checker is not configured"), 401
    header = request.headers.get("Authorization", "")
    parts = header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1]:
        return jsonify(
            error="Authorization Required",
            description='Request does not contain a well-formed access token in the "Authorization" header beginning with "Bearer"',
        ), 401
    token = parts[1]
    client_id = checker["client_id"]
    try:
        signing_key = checker["jwks"].get_signing_key_from_jwt(token)
        raw = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=[client_id, f"api://{client_id}"],
            issuer=checker["issuer"],
        )
    except Exception as exc:
        return jsonify(error="Invalid authentication token", description=str(exc)), 401
    g.renglo_identity = map_claims(raw)
    return None


def current_claims() -> Dict[str, Any]:
    from flask import g

    claims = getattr(g, "renglo_identity", None)
    if not claims:
        return {}
    return dict(claims)
