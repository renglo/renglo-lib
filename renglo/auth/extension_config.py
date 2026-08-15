"""
Call an extension's initialize_extension handler when a team is assigned to it.

Onboarding only installs the tool on the portfolio. Per-org setup (config,
jobs, and anything else the extension needs) lives in each extension's
initialize_extension handler.
"""

from __future__ import annotations

from renglo.logger import get_logger

INITIALIZE_EXTENSION_HANDLER = "initialize_extension"


def _portfolio_id_for_team(auc, team_id):
    index = f"irn:rel:team:portfolio:{team_id}:*"
    rels = auc.AUM.list_rel(index)
    items = ((rels or {}).get("document") or {}).get("items") or []
    if not items:
        return None
    return items[0].get("rel")


def _tool_handle(auc, portfolio_id, tool_id):
    response = auc.get_entity("tool", portfolio_id=portfolio_id, tool_id=tool_id)
    if not response.get("success"):
        return None
    handle = str((response.get("document") or {}).get("handle") or "").strip()
    return handle or None


def initialize_assigned_extension(config, team_id, tool_id, org_id):
    """
    After a team/tool:org grant, run {handle}/initialize_extension with org.

    Failures are logged and do not roll back the assignment.
    """
    logger = get_logger()
    from renglo.auth.auth_controller import AuthController

    auc = AuthController(config=config)
    portfolio_id = _portfolio_id_for_team(auc, team_id)
    if not portfolio_id:
        logger.warning(
            "initialize_extension skipped: no portfolio for team %s", team_id
        )
        return {"success": False, "message": "Portfolio not found for team"}

    handle = _tool_handle(auc, portfolio_id, tool_id)
    if not handle:
        logger.warning(
            "initialize_extension skipped: no handle for tool %s in %s",
            tool_id,
            portfolio_id,
        )
        return {"success": False, "message": "Tool handle not found"}

    payload = {
        "portfolio": portfolio_id,
        "org": org_id,
        "tool": handle,
        "tool_id": tool_id,
    }
    logger.info(
        "Calling %s/%s for org %s",
        handle,
        INITIALIZE_EXTENSION_HANDLER,
        org_id,
    )
    try:
        response = _run_initialize_extension(handle, payload)
    except Exception as exc:
        logger.error(
            "initialize_extension failed for %s org %s: %s",
            handle,
            org_id,
            exc,
        )
        return {"success": False, "message": str(exc)}

    if not response.get("success"):
        logger.warning(
            "initialize_extension returned failure for %s org %s: %s",
            handle,
            org_id,
            response,
        )
    return response


def _run_initialize_extension(handle, payload):
    """
    Run {handle}/initialize_extension without @authorize.

    The caller already authorized the team/tool:org assignment.
    """
    from renglo.runtime import attach_jwt_claims_to_payload
    from renglo.schd.external_handler_runner import run_external_handler
    from renglo.schd.external_handlers_config import (
        has_external_handlers,
        is_external_handler_active,
    )
    from renglo.schd.schd_loader import SchdLoader

    if has_external_handlers(handle) and is_external_handler_active(handle):
        attach_jwt_claims_to_payload(payload)
        return run_external_handler(
            extension_name=handle,
            handler_name=INITIALIZE_EXTENSION_HANDLER,
            payload=payload,
        )

    return SchdLoader().load_and_run(
        f"{handle}/{INITIALIZE_EXTENSION_HANDLER}",
        payload=payload,
    )
