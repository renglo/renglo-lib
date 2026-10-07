"""Send a finished live call back to the conversation that started it.

A websocket conversation is pushed by connection id. A webhook conversation
names a channel. The channel picks the outbound handler. The session is
written before this module sends anything.
"""

from __future__ import annotations

import json
from typing import Any, Optional


# channel → outbound handler. ``target`` is the payload field that carries
# the recipient. Extra fields travel in reply_args (thread id, subject, …).
CHANNEL_OUTBOUND: dict[str, dict[str, str]] = {
    "whatsapp": {"handler": "whatsapp/post_message", "target": "target"},
    "gmail": {"handler": "gmail/reply_message", "target": "to"},
}

_TEXT_LIMIT = 4000


def reply_route(payload: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Route for a webhook conversation. None when the call has no channel."""
    if not isinstance(payload, dict):
        return None
    channel = str(payload.get("channel") or "").strip().lower()
    spec = CHANNEL_OUTBOUND.get(channel)
    if not spec:
        return None
    args: dict[str, str] = {}
    raw = payload.get("reply_args")
    if isinstance(raw, dict):
        for key, value in raw.items():
            name = str(key or "").strip()
            text = str(value or "").strip()
            if name and text:
                args[name] = text
    target_field = spec["target"]
    if not args.get(target_field):
        external = str(payload.get("external_id") or "").strip()
        if external:
            args[target_field] = external
    if not args.get(target_field):
        return None
    user_id = str(payload.get("user_id") or payload.get("public_user") or "").strip()
    return {
        "channel": channel,
        "handler": spec["handler"],
        "args": args,
        "external_id": str(payload.get("external_id") or args.get(target_field) or ""),
        "user_id": user_id,
    }


def text_for_channel(frames: list[dict[str, Any]]) -> str:
    """One text message from the frames already saved on the session."""
    assistant: list[str] = []
    error = ""
    detail = ""
    for frame in frames:
        if not isinstance(frame, dict):
            continue
        kind = frame.get("_type")
        content = (frame.get("_out") or {}).get("content") if isinstance(frame.get("_out"), dict) else None
        if kind == "assistant_message" and isinstance(content, str) and content.strip():
            assistant.append(content.strip())
        elif kind == "tool_result" and isinstance(content, dict):
            if content.get("success") is False:
                error = str(content.get("error") or "The handler failed.").strip()
            else:
                detail = _result_text(content.get("result"))
    parts: list[str] = []
    if error and not assistant:
        parts.append(error)
    else:
        parts.extend(assistant)
        if detail:
            parts.append(detail)
        if error:
            parts.append(error)
    return "\n\n".join(part for part in parts if part)[:_TEXT_LIMIT]


def _result_text(result: Any) -> str:
    if isinstance(result, str):
        return result.strip()[:3500]
    if not isinstance(result, dict):
        return ""
    message = ""
    for key in ("reply", "text", "message"):
        value = result.get(key)
        if isinstance(value, str) and value.strip():
            message = value.strip()
            break
    body = result.get("output") if "output" in result else result
    rendered = ""
    if isinstance(body, str):
        rendered = body.strip()
    elif isinstance(body, (dict, list)):
        try:
            rendered = json.dumps(body, default=str, ensure_ascii=False)
        except (TypeError, ValueError):
            rendered = ""
    if message and rendered and message not in rendered:
        text = f"{message}\n\n{rendered}"
    else:
        text = rendered or message
    return text[:3500]


def deliver_channel_reply(config: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    """Send through the outbound handler. Placement picks the process."""
    return run_channel_reply(config, body)


def run_channel_reply(config: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    """Send the saved text through the outbound handler and record delivery."""
    route = body.get("reply") if isinstance(body.get("reply"), dict) else {}
    handler = str(route.get("handler") or "").strip()
    text = str(body.get("text") or "").strip()
    portfolio = str(body.get("portfolio") or "").strip()
    org = str(body.get("org") or "").strip()
    channel = str(route.get("channel") or "").strip()
    if not handler or not text or not portfolio or not org or not channel:
        return {"success": False, "error": "channel reply is missing handler, text, or session"}

    args = route.get("args") if isinstance(route.get("args"), dict) else {}
    payload = {"portfolio": portfolio, "org": org, "message": text, "text": text}
    for key, value in args.items():
        payload[str(key)] = value

    from renglo.schd.schd_controller import call_handler

    user_id = str(route.get("user_id") or body.get("user_id") or "").strip()
    sent = call_handler(config, portfolio, org, handler, payload, user_id=user_id)
    ok, provider_status, provider_error, provider_id = _send_outcome(sent)
    recorded = _record_delivery(
        config,
        body,
        channel=channel,
        external_id=str(route.get("external_id") or args.get("target") or args.get("to") or ""),
        user_id=str(route.get("user_id") or ""),
        ok=ok,
        provider_status=provider_status,
        provider_error=provider_error,
        provider_id=provider_id,
        text=text,
    )
    return {"success": ok, "send": sent, "delivery": recorded}


def _send_outcome(sent: Any) -> tuple[bool, Any, Any, Optional[str]]:
    if not isinstance(sent, dict):
        return False, None, "empty send result", None
    outer = sent.get("output") if isinstance(sent.get("output"), dict) else sent
    ok = bool(sent.get("success")) and outer.get("success") is not False
    provider_id = None
    if isinstance(outer, dict):
        provider_id = outer.get("id") or outer.get("provider_message_id")
        nested = outer.get("output")
        if not provider_id and isinstance(nested, dict):
            provider_id = nested.get("id") or nested.get("provider_message_id")
    error = None if ok else (outer.get("error") if isinstance(outer, dict) else sent.get("error"))
    status = outer.get("status") if isinstance(outer, dict) else None
    return ok, status, error, str(provider_id) if provider_id else None


def _record_delivery(
    config: dict[str, Any],
    body: dict[str, Any],
    *,
    channel: str,
    external_id: str,
    user_id: str,
    ok: bool,
    provider_status: Any,
    provider_error: Any,
    provider_id: Optional[str],
    text: str,
) -> dict[str, Any]:
    portfolio = str(body.get("portfolio") or "").strip()
    org = str(body.get("org") or "").strip()
    entity_type = str(body.get("entity_type") or "").strip()
    entity_id = str(body.get("entity_id") or "").strip()
    thread_id = str(body.get("thread") or body.get("thread_id") or "").strip()
    turn_id = str(body.get("turn_id") or "").strip()
    if not all([portfolio, org, entity_type, entity_id, thread_id, turn_id, user_id]):
        return {"success": False, "message": "missing session refs for channel_delivery"}

    from renglo.session.session_controller import SessionController

    sessions = SessionController(config=config or {})
    sessions.set_invocation_user(user_id)
    return sessions.append_channel_delivery(
        portfolio,
        org,
        entity_type,
        entity_id,
        thread_id,
        turn_id,
        channel=channel,
        status="sent" if ok else "failed",
        external_id=external_id,
        provider_status=provider_status,
        provider_error=provider_error,
        provider_message_id=provider_id,
        related_event_id=str(body.get("related_event_id") or "") or None,
        text_excerpt=(text or "").strip()[:240] or None,
    )
