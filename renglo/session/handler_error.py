"""Persist and push a handler failure so the chat does not go silent.

The agent usually posts to the socket itself. If it raises, or returns a
failure without reporting, the route that called it uses this helper: write
an ``error`` event on the session turn, and post that same document to the
open WebSocket when ``connectionId`` is present.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional


def error_roll(message: str) -> dict[str, Any]:
    """Session/WebSocket document the chat UI already knows how to render."""
    text = str(message or "").strip() or "The handler failed."
    return {
        "_type": "error",
        "_out": {"role": "assistant", "content": text[:4000]},
        "_meta": {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }


def failure_already_reported(response: Any) -> bool:
    """True when the handler saved and pushed this failure itself."""
    if isinstance(response, dict):
        if response.get("_error_reported") is True:
            return True
        return any(failure_already_reported(value) for value in response.values())
    if isinstance(response, list):
        return any(failure_already_reported(item) for item in response)
    return False


def _preferred_text(obj: Any) -> Optional[str]:
    if isinstance(obj, str):
        text = obj.strip()
        return text[:4000] if text else None
    if isinstance(obj, list):
        for item in obj:
            text = _preferred_text(item)
            if text:
                return text
        return None
    if isinstance(obj, dict):
        for key in ("reply", "error", "message"):
            text = _preferred_text(obj.get(key))
            if text:
                return text
        if "output" in obj:
            return _preferred_text(obj.get("output"))
    return None


def _looks_failed(data: Any, status: Optional[int]) -> bool:
    if status is not None and int(status) >= 400:
        return True
    if isinstance(data, list):
        return any(isinstance(item, dict) and item.get("success") is False for item in data)
    if isinstance(data, dict) and data.get("success") is False:
        return True
    return False


def failure_message(response: Any, status: Optional[int] = None) -> Optional[str]:
    """Human-readable failure, or None when the call succeeded."""
    if isinstance(response, str):
        text = response.strip()
        if status is not None and int(status) < 400:
            return None
        return text[:4000] if text else None
    if not _looks_failed(response, status):
        return None
    return _preferred_text(response) or "The handler failed."


def _payload_ids(payload: dict[str, Any]) -> Optional[dict[str, str]]:
    portfolio = str(payload.get("portfolio") or "").strip()
    org = str(payload.get("org") or "").strip()
    entity_type = str(payload.get("entity_type") or "").strip()
    entity_id = str(payload.get("entity_id") or "").strip()
    thread = str(payload.get("thread") or "").strip()
    if not all([portfolio, org, entity_type, entity_id, thread]):
        return None
    return {
        "portfolio": portfolio,
        "org": org,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "thread": thread,
    }


def persist_handler_error(
    config: dict[str, Any],
    payload: dict[str, Any],
    document: dict[str, Any],
    turn_id: Optional[str] = None,
    sessions: Any = None,
) -> bool:
    """Append ``document`` to ``turn_id``, or to the latest turn when it is omitted."""
    ids = _payload_ids(payload if isinstance(payload, dict) else {})
    if not ids:
        return False

    if sessions is None:
        from renglo.session.session_controller import SessionController

        sessions = SessionController(config=config or {})

    chosen = str(turn_id or "").strip()
    if chosen:
        saved = sessions.update_turn(
            ids["portfolio"],
            ids["org"],
            ids["entity_type"],
            ids["entity_id"],
            ids["thread"],
            chosen,
            document,
        )
        return bool(isinstance(saved, dict) and saved.get("success"))

    listed = sessions.list_turns(
        ids["portfolio"],
        ids["org"],
        ids["entity_type"],
        ids["entity_id"],
        ids["thread"],
        False,
    )
    items = (listed or {}).get("items") if isinstance(listed, dict) else None
    if isinstance(items, list) and items:
        last = items[-1]
        turn_id = str((last or {}).get("_id") or "").strip()
        if not turn_id:
            return False
        saved = sessions.update_turn(
            ids["portfolio"],
            ids["org"],
            ids["entity_type"],
            ids["entity_id"],
            ids["thread"],
            turn_id,
            document,
        )
        return bool(isinstance(saved, dict) and saved.get("success"))

    public_user = payload.get("public_user") or False
    created = sessions.create_turn(
        ids["portfolio"],
        ids["org"],
        ids["entity_type"],
        ids["entity_id"],
        ids["thread"],
        {
            "context": {
                "portfolio": ids["portfolio"],
                "org": ids["org"],
                "entity_type": ids["entity_type"],
                "entity_id": ids["entity_id"],
                "thread": ids["thread"],
                "public_user": public_user,
            },
            "events": [document],
        },
    )
    return bool(isinstance(created, dict) and created.get("success"))


def push_handler_error(config: dict[str, Any], payload: dict[str, Any], document: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    connection_id = str(payload.get("connectionId") or payload.get("connection_id") or "").strip()
    if not connection_id:
        return False
    from renglo.agent.websocket_client import WebSocketClient

    url = str((config or {}).get("WEBSOCKET_CONNECTIONS") or "")
    client = WebSocketClient(url)
    if not client.is_configured():
        return False
    return bool(client.send_message(connection_id, document))


def bubble_handler_error(config: dict[str, Any], payload: dict[str, Any], message: str) -> dict[str, Any]:
    """Save the error on the session and post it to the caller's socket."""
    document = error_roll(message)
    payload = payload if isinstance(payload, dict) else {}
    try:
        persist_handler_error(config or {}, payload, document)
    except Exception as exc:
        print(f"persist_handler_error failed: {exc}")
    try:
        push_handler_error(config or {}, payload, document)
    except Exception as exc:
        print(f"push_handler_error failed: {exc}")
    return document


def surface_handler_failure(
    config: dict[str, Any],
    payload: dict[str, Any],
    response: Any,
    status: Optional[int] = None,
) -> Optional[str]:
    """Bubble a failure the handler did not already report. Return the message."""
    message = failure_message(response, status)
    if not message:
        return None
    if not failure_already_reported(response):
        bubble_handler_error(config or {}, payload if isinstance(payload, dict) else {}, message)
    return message
