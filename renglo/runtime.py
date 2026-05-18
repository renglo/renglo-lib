"""
Runtime helpers that keep renglo-lib framework-neutral.

These helpers avoid hard imports of Flask/Flask-Cognito at module import time.
When running inside Flask, they can still bridge request/session/JWT data.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


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
        from flask_cognito import current_cognito_jwt  # type: ignore

        if current_cognito_jwt:
            return dict(current_cognito_jwt)
    except Exception:
        pass
    return None
