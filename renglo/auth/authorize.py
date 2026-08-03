"""
Declarative authorization decorator for renglo controllers.

Usage:
    @authorize()                              # org access (default)
    @authorize(resource="tool", tool_id_param="extension")
    @authorize(resource="org", action="delete")
    @authorize(return_status=True)            # deny as (dict, status) for tuple APIs

Decision logic lives in AuthController.authorize; this decorator only binds
arguments and short-circuits on deny. On success it stores the auth result on
``self._auth_context`` and stamps resolved roles onto a ``payload`` kwarg when
present (``_auth_roles``).
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable

from renglo.runtime import attach_auth_roles_to_payload


def authorize(
    resource: str = "org",
    action: str = "access",
    *,
    portfolio_param: str = "portfolio",
    org_param: str = "org",
    tool_id_param: str = "tool_id",
    return_status: bool = False,
) -> Callable:
    """
    Authorize the current user before entering a controller method.

    Resolves portfolio/org/(tool_id) from the bound method arguments and
    delegates to ``self.AUC.authorize(...)``. On deny, returns the auth
    response dict (or ``(dict, status)`` when ``return_status=True``).
    """

    def decorator(fn: Callable) -> Callable:
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(self, *args: Any, **kwargs: Any):
            try:
                bound = sig.bind(self, *args, **kwargs)
                bound.apply_defaults()
            except TypeError:
                # Let the original method raise its own TypeError for bad calls.
                return fn(self, *args, **kwargs)

            arguments = bound.arguments
            portfolio = arguments.get(portfolio_param)
            org = arguments.get(org_param)
            tool_id = arguments.get(tool_id_param)

            auc = getattr(self, "AUC", None)
            if auc is None or not hasattr(auc, "authorize"):
                denied = {
                    "success": False,
                    "message": "Authorization controller not configured",
                    "status": 500,
                    "roles": [],
                }
                if return_status:
                    return denied, 500
                return denied

            result = auc.authorize(
                portfolio,
                org,
                resource=resource,
                action=action,
                tool_id=tool_id,
            )
            if not result.get("success"):
                self._auth_context = result
                if return_status:
                    return result, result.get("status", 403)
                return result

            self._auth_context = result
            payload = arguments.get("payload")
            if isinstance(payload, dict):
                attach_auth_roles_to_payload(payload, result.get("roles") or [])

            return fn(self, *args, **kwargs)

        return wrapper

    return decorator
