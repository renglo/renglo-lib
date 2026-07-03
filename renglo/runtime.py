"""
Runtime helpers that keep renglo-lib framework-neutral.

These helpers avoid hard imports of Flask/Flask-Cognito at module import time.
When running inside Flask, they can still bridge request/session/JWT data.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

# Internal key used to forward Cognito JWT claims to external handlers (Lambda/ECS/Docker).
JWT_CLAIMS_PAYLOAD_KEY = "_jwt_claims"


def get_session_value(key: str, default: Any = None) -> Any:
    try:
        from flask import has_request_context, session  # type: ignore

        if has_request_context():
            return session.get(key, default)
    except Exception:
        pass
    return default


def get_request_json(default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    fallback = default or {}
    try:
        from flask import has_request_context, request  # type: ignore

        if has_request_context():
            payload = request.get_json(silent=True)
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    return fallback


def get_request_args() -> Dict[str, Any]:
    try:
        from flask import has_request_context, request  # type: ignore

        if not has_request_context():
            return {}
        result: Dict[str, Any] = {}
        for key in request.args.keys():
            values = request.args.getlist(key)
            result[key] = values[0] if len(values) == 1 else values
        return result
    except Exception:
        return {}


def get_current_jwt_claims() -> Optional[Dict[str, Any]]:
    try:
        from flask import has_app_context  # type: ignore

        if not has_app_context():
            return None
        from flask_cognito import current_cognito_jwt  # type: ignore

        if current_cognito_jwt:
            return dict(current_cognito_jwt)
    except Exception:
        pass
    return None


def attach_jwt_claims_to_payload(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Copy Cognito JWT claims from the current Flask request into payload for external handlers.
    No-op when not in a Flask app context or when claims are already present.
    """
    if not isinstance(payload, dict):
        payload = {}
    if payload.get(JWT_CLAIMS_PAYLOAD_KEY):
        return payload
    claims = get_current_jwt_claims()
    if claims:
        payload[JWT_CLAIMS_PAYLOAD_KEY] = claims
    return payload


def apply_handler_invocation_context(handler: Any, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Apply forwarded JWT claims to handler controllers before run().
    Removes JWT_CLAIMS_PAYLOAD_KEY from payload so handlers do not persist it.
    Always resets invocation claims (even when absent) so cached handler instances
    on warm Lambda containers do not reuse a previous caller's identity.
    """
    if not isinstance(payload, dict):
        return payload if isinstance(payload, dict) else {}
    claims = payload.pop(JWT_CLAIMS_PAYLOAD_KEY, None)
    for attr in ("AUC", "CHC", "SHC"):
        controller = getattr(handler, attr, None)
        if controller is not None and hasattr(controller, "set_invocation_jwt_claims"):
            controller.set_invocation_jwt_claims(claims)
    return payload
