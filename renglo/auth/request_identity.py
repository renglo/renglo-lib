"""The only server-side reader of the identity-provider token."""

from __future__ import annotations

from typing import Any, Dict


def init_auth(app):
    """Install the token checker on the Flask app. Routes never import it."""
    from flask_cognito import CognitoAuth

    return CognitoAuth(app)


def auth_required(view):
    from flask_cognito import cognito_auth_required

    return cognito_auth_required(view)


def current_user():
    from flask_cognito import current_user as cognito_user

    return cognito_user


def current_claims() -> Dict[str, Any]:
    from flask_cognito import current_cognito_jwt

    if not current_cognito_jwt:
        return {}
    return dict(current_cognito_jwt)


def current_identity() -> Dict[str, Any]:
    """Renglo user id is the 9-character hash of the provider username."""
    from renglo.common import create_md5_hash

    claims = current_claims()
    username = str(claims.get("cognito:username") or claims.get("username") or "")
    return {
        "user_id": create_md5_hash(username, 9) if username else "",
        "username": username,
        "email": claims.get("email", ""),
        "given_name": claims.get("given_name", ""),
        "family_name": claims.get("family_name", ""),
        "claims": claims,
    }
