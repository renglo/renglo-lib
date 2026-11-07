"""The only server-side reader of the identity-provider token.

CLOUD selects the checker. Routes call auth_required and current_identity.
"""

from __future__ import annotations

from functools import wraps
from typing import Any, Dict

from renglo.config_names import setting


def _cloud(config) -> str:
    return str(setting(config, "CLOUD", "aws") or "aws").strip().lower() or "aws"


def _module(config):
    cloud = _cloud(config)
    if cloud == "azure":
        from renglo.auth.tokens import token_azure as module
    elif cloud == "aws":
        from renglo.auth.tokens import token_aws as module
    else:
        raise NotImplementedError(f"No identity token checker for cloud {cloud!r}")
    return module


def init_auth(app):
    """Install the token checker on the Flask app. Routes never import it."""
    return _module(app.config).init_auth(app)


def require_auth():
    """Reject the current request when its bearer token is missing or invalid."""
    from flask import current_app

    failure = _module(current_app.config).require_request()
    if failure is not None:
        raise PermissionError("Invalid or expired authentication token")


def auth_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        from flask import current_app

        failure = _module(current_app.config).require_request()
        if failure is not None:
            return failure
        return view(*args, **kwargs)

    return wrapper


def current_claims() -> Dict[str, Any]:
    from flask import current_app

    return _module(current_app.config).current_claims()


def current_identity() -> Dict[str, Any]:
    """Renglo user id is the 9-character hash of the provider username."""
    from renglo.common import create_md5_hash

    claims = current_claims()
    username = str(claims.get("username") or "")
    return {
        "user_id": create_md5_hash(username, 9) if username else "",
        "username": username,
        "email": claims.get("email") or "",
        "given_name": claims.get("given_name") or "",
        "family_name": claims.get("family_name") or "",
        "subject": claims.get("subject") or "",
        "claims": claims,
    }
