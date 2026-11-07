"""Validate a Cognito bearer token and map it to the shared identity."""

from __future__ import annotations

from typing import Any, Dict, Optional


def map_claims(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Username is cognito:username, otherwise the access-token username."""
    if not raw:
        return {}
    username = str(raw.get("cognito:username") or raw.get("username") or "")
    mapped: Dict[str, Any] = {
        "username": username,
        "email": raw.get("email") or "",
        "given_name": raw.get("given_name") or "",
        "family_name": raw.get("family_name") or "",
        "subject": str(raw.get("sub") or ""),
    }
    if "identities" in raw:
        mapped["identities"] = raw.get("identities")
    return mapped


def init_auth(app):
    from flask_cognito import CognitoAuth

    return CognitoAuth(app)


def require_request():
    from flask import g
    from flask_cognito import _cognito_auth_required, current_cognito_jwt

    _cognito_auth_required()
    raw = dict(current_cognito_jwt or {})
    g.renglo_identity = map_claims(raw)


def current_claims() -> Dict[str, Any]:
    from flask import g

    claims = getattr(g, "renglo_identity", None)
    if not claims:
        return {}
    return dict(claims)
