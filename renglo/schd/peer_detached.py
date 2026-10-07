"""Finish an Event invoke: write the session, then continue the caller.

A registered callback is asked to continue the agent. The tool result is
already on the session. A call with no callback still pushes the websocket
and sends the channel text from here.
The hub has already returned a running receipt. This runs on the peer.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional


DETACHED_DEADLINE_SECONDS = 14 * 60


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def callback_handler_name(event: dict[str, Any]) -> str:
    raw = _completion(event).get("callback")
    if not isinstance(raw, dict):
        return ""
    return str(raw.get("handler") or "").strip()


def _completion(event: dict[str, Any]) -> dict[str, Any]:
    raw = event.get("completion") if isinstance(event, dict) else None
    return raw if isinstance(raw, dict) else {}


def _payload(event: dict[str, Any]) -> dict[str, Any]:
    raw = event.get("payload") if isinstance(event, dict) else None
    return raw if isinstance(raw, dict) else {}


def _session_id(event: dict[str, Any]) -> str:
    completion = _completion(event)
    existing = str(completion.get("session_id") or "").strip()
    if existing:
        return existing
    payload = _payload(event)
    parts = [
        str(payload.get("entity_type") or "").strip(),
        str(payload.get("entity_id") or "").strip(),
        str(payload.get("thread") or "").strip(),
    ]
    if all(parts):
        return "|".join(parts)
    return ""


def _meta(event: dict[str, Any]) -> dict[str, str]:
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": _session_id(event),
        "timestamp": _now(),
    }


def tool_result_frame(event: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    """Live socket shape used by the hub for a tool result."""
    return {
        "_type": "tool_result",
        "_out": {"role": "system", "content": body},
        "_meta": _meta(event),
    }


def assistant_frame(event: dict[str, Any], text: str) -> dict[str, Any]:
    return {
        "_type": "assistant_message",
        "_out": {"role": "assistant", "content": text},
        "_meta": _meta(event),
    }


def _handler_ok(result: Any) -> bool:
    if not isinstance(result, dict):
        return result is not None
    if result.get("success") is False:
        return False
    body = result.get("body")
    if isinstance(body, dict) and body.get("success") is False:
        return False
    return True


def _handler_error_text(result: Any, error: Optional[BaseException], timed_out: bool) -> str:
    if timed_out:
        return "The report did not finish."
    if error is not None:
        return str(error) or "The handler failed."
    if isinstance(result, dict):
        body = result.get("body") if isinstance(result.get("body"), dict) else result
        if isinstance(body, dict):
            for key in ("message", "error"):
                text = body.get(key)
                if isinstance(text, str) and text.strip():
                    return text.strip()[:4000]
        text = result.get("error")
        if isinstance(text, str) and text.strip():
            return text.strip()[:4000]
    return "The handler failed."


def frames_for_result(
    event: dict[str, Any],
    result: Any,
    *,
    error: Optional[BaseException] = None,
    timed_out: bool = False,
) -> list[dict[str, Any]]:
    completion = _completion(event)
    tool = str(completion.get("tool") or event.get("handler") or "tool")
    call_id = str(completion.get("call_id") or "")
    if timed_out or error is not None or not _handler_ok(result):
        message = _handler_error_text(result, error, timed_out)
        body = {
            "tool": tool,
            "call_id": call_id,
            "success": False,
            "result": result if isinstance(result, dict) else {},
            "error": message,
        }
        return [tool_result_frame(event, body)]
    body = {
        "tool": tool,
        "call_id": call_id,
        "success": True,
        "result": result,
        "error": None,
    }
    ready = "The report is ready."
    when = str(completion.get("when") or "").strip()
    if when:
        ready = f"The report is ready ({when})."
    return [tool_result_frame(event, body), assistant_frame(event, ready)]


def complete_detached(
    event: dict[str, Any],
    result: Any = None,
    *,
    error: Optional[BaseException] = None,
    timed_out: bool = False,
    config: Optional[dict[str, Any]] = None,
    claims: Any = None,
) -> list[dict[str, Any]]:
    """Save the tool result, then continue a registered agent or answer the channel."""
    frames = frames_for_result(event, result, error=error, timed_out=timed_out)
    payload = _payload(event)
    callback = callback_handler_name(event)
    if callback:
        frames = [frame for frame in frames if frame.get("_type") == "tool_result"]
    if config is None:
        try:
            from renglo.common import load_config

            config = load_config()
        except Exception:
            config = {}
    config = config if isinstance(config, dict) else {}
    turn_id = str(_completion(event).get("turn_id") or "").strip()

    from renglo.runtime import stamp_invocation_jwt_claims
    from renglo.session.handler_error import persist_handler_error, push_handler_error
    from renglo.session.session_controller import SessionController

    sessions = SessionController(config=config)
    if claims is None and isinstance(payload, dict):
        claims = payload.get("_jwt_claims")
    route = _completion(event).get("reply")
    route = route if isinstance(route, dict) else {}
    user_id = str(route.get("user_id") or "").strip()
    if not user_id and isinstance(payload, dict):
        user_id = str(payload.get("public_user") or payload.get("user_id") or "").strip()
    if user_id:
        sessions.set_invocation_user(user_id)
    stamp_invocation_jwt_claims(sessions, claims)

    for frame in frames:
        try:
            persist_handler_error(
                config,
                payload,
                frame,
                turn_id=turn_id,
                sessions=sessions,
            )
        except Exception as exc:
            print(f"persist detached frame failed: {exc}")
        try:
            push_handler_error(config, payload, frame)
        except Exception as exc:
            print(f"push detached frame failed: {exc}")

    if callback:
        from renglo.schd.continuation import post_agent_callback

        call_id = str(_completion(event).get("call_id") or "")
        body = {
            "callback": {"handler": callback},
            "call_id": call_id,
            "result": result if isinstance(result, dict) else {},
            "reply": route or None,
            "portfolio": str(payload.get("portfolio") or ""),
            "org": str(payload.get("org") or ""),
            "entity_type": str(payload.get("entity_type") or ""),
            "entity_id": str(payload.get("entity_id") or ""),
            "thread": str(payload.get("thread") or ""),
            "turn_id": turn_id,
            "connectionId": str(payload.get("connectionId") or payload.get("connection_id") or ""),
            "user_id": user_id,
            "public_user": str(payload.get("public_user") or ""),
        }
        try:
            post_agent_callback(config, body)
        except Exception as exc:
            print(f"agent callback failed: {exc}")
        return frames

    handler = str(route.get("handler") or "").strip()
    if handler:
        from renglo.schd.channel_reply import deliver_channel_reply, text_for_channel

        text = text_for_channel(frames)
        if text:
            related = ""
            for frame in frames:
                meta = frame.get("_meta") if isinstance(frame, dict) else None
                if isinstance(meta, dict) and frame.get("_type") == "assistant_message":
                    related = str(meta.get("event_id") or "")
            if not related:
                meta = frames[-1].get("_meta") if frames and isinstance(frames[-1], dict) else None
                related = str((meta or {}).get("event_id") or "") if isinstance(meta, dict) else ""
            body = {
                "reply": route,
                "text": text,
                "portfolio": str(payload.get("portfolio") or ""),
                "org": str(payload.get("org") or ""),
                "entity_type": str(payload.get("entity_type") or ""),
                "entity_id": str(payload.get("entity_id") or ""),
                "thread": str(payload.get("thread") or ""),
                "turn_id": turn_id,
                "related_event_id": related,
            }
            try:
                deliver_channel_reply(config, body)
            except Exception as exc:
                print(f"channel reply failed: {exc}")
    return frames
