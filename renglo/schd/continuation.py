"""Run the agent again when a detached handler finishes.

The tool result is already on the session. This asks the callback handler
to continue the turn, then sends whatever reply that handler returns.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Optional


LEASE_SECONDS = 45
LOCK_WAIT_SECONDS = 15
MAX_DRAIN_PASSES = 4


def callback_handler(event: dict[str, Any]) -> str:
    completion = event.get("completion") if isinstance(event.get("completion"), dict) else {}
    raw = completion.get("callback") if isinstance(completion.get("callback"), dict) else {}
    return str(raw.get("handler") or "").strip()


def post_agent_callback(config: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    """Continue the registered handler from this process."""
    return run_agent_callback(config, body)


def run_agent_callback(
    config: dict[str, Any],
    body: dict[str, Any],
    *,
    invoke: Optional[Callable[[dict[str, Any], dict[str, Any]], Any]] = None,
    send: Optional[Callable[[dict[str, Any], dict[str, Any]], Any]] = None,
    load_events: Optional[Callable[[dict[str, Any], dict[str, Any]], list[dict[str, Any]]]] = None,
    acquire: Optional[Callable[[dict[str, Any], dict[str, Any]], Optional[str]]] = None,
    release: Optional[Callable[[dict[str, Any], dict[str, Any], str], None]] = None,
    sleep: Optional[Callable[[float], None]] = None,
) -> dict[str, Any]:
    """Hold the thread, continue the agent, and send its reply."""
    invoke = invoke or _invoke_handler
    send = send or _send_reply
    load_events = load_events or _load_events
    acquire = acquire or _acquire_lock
    release = release or _release_lock
    sleep = sleep or time.sleep

    holder = _wait_for_lock(config, body, acquire, sleep)
    if not holder:
        return {"success": True, "deferred": True}
    sent: list[str] = []
    carry = False
    try:
        for _ in range(MAX_DRAIN_PASSES):
            events = load_events(config, body)
            if not pending_final_call_ids(events) and not carry:
                break
            before = final_event_ids(events)
            known = known_call_ids(events)
            outcome = invoke(config, body)
            reply = extract_reply(outcome)
            if reply:
                send(config, {**body, "text": reply})
                sent.append(reply)
            events = load_events(config, body)
            carry = bool(external_finals(events, known, before))
    finally:
        release(config, body, holder)
    return {"success": True, "replies": sent}


def pending_final_call_ids(events: list[dict[str, Any]]) -> list[str]:
    """Final tool results that no later assistant message has covered."""
    latest: dict[str, dict[str, Any]] = {}
    last_assistant: Optional[datetime] = None
    for event in events:
        if _kind(event) == "assistant_message":
            stamp = _timestamp(event)
            if stamp is not None:
                last_assistant = stamp
            continue
        if _kind(event) != "tool_result":
            continue
        content = _content(event)
        call_id = str(content.get("call_id") or "").strip()
        if not call_id:
            continue
        previous = latest.get(call_id)
        if previous is None or _is_running(previous) or not _is_running(event):
            latest[call_id] = event
    pending: list[str] = []
    for call_id, event in latest.items():
        if _is_running(event):
            continue
        stamp = _timestamp(event)
        if last_assistant is None or (stamp is not None and stamp > last_assistant):
            pending.append(call_id)
    return pending


def final_event_ids(events: list[dict[str, Any]]) -> set[str]:
    """Event ids of the latest non-running tool result for each call."""
    latest: dict[str, dict[str, Any]] = {}
    for event in events:
        if _kind(event) != "tool_result":
            continue
        content = _content(event)
        call_id = str(content.get("call_id") or "").strip()
        if not call_id:
            continue
        previous = latest.get(call_id)
        if previous is None or _is_running(previous) or not _is_running(event):
            latest[call_id] = event
    found: set[str] = set()
    for event in latest.values():
        if _is_running(event):
            continue
        meta = event.get("_meta") if isinstance(event.get("_meta"), dict) else {}
        event_id = str(meta.get("event_id") or "").strip()
        if event_id:
            found.add(event_id)
    return found


def known_call_ids(events: list[dict[str, Any]]) -> set[str]:
    """Call ids already on the thread before this continuation writes its own."""
    found: set[str] = set()
    for event in events:
        if _kind(event) not in ("tool_call", "tool_result"):
            continue
        call_id = str(_content(event).get("call_id") or "").strip()
        if call_id:
            found.add(call_id)
    return found


def external_finals(
    events: list[dict[str, Any]],
    known: set[str],
    before: set[str],
) -> set[str]:
    """Final results for calls that were already open, written during this pass."""
    arrived: set[str] = set()
    for event in events:
        if _kind(event) != "tool_result" or _is_running(event):
            continue
        call_id = str(_content(event).get("call_id") or "").strip()
        meta = event.get("_meta") if isinstance(event.get("_meta"), dict) else {}
        event_id = str(meta.get("event_id") or "").strip()
        if call_id in known and event_id and event_id not in before:
            arrived.add(event_id)
    return arrived


def arrived_after(events: list[dict[str, Any]], started: datetime) -> bool:
    """True when a final tool result was written after this continuation began."""
    began = _as_utc(started)
    for event in events:
        if _kind(event) != "tool_result" or _is_running(event):
            continue
        stamp = _timestamp(event)
        if stamp is not None and stamp > began:
            return True
    return False


def extract_reply(agent_result: Any) -> str:
    """Pull the user-facing text out of a handler or SchdLoader envelope."""
    if not isinstance(agent_result, dict):
        return ""
    outer = agent_result.get("output", agent_result)
    if isinstance(outer, dict) and isinstance(outer.get("output"), dict):
        nested = outer.get("output")
        if isinstance(nested, dict) and any(nested.get(key) for key in ("reply", "message", "text")):
            outer = nested
    if isinstance(outer, dict):
        for key in ("reply", "message", "text", "response"):
            value = outer.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        nested = outer.get("output")
        if isinstance(nested, dict):
            for key in ("reply", "message", "text", "response", "content"):
                value = nested.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return ""


def _wait_for_lock(config, body, acquire, sleep) -> Optional[str]:
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    while True:
        holder = acquire(config, body)
        if holder:
            return holder
        if time.monotonic() >= deadline:
            return None
        sleep(1)


def _lock_key(body: dict[str, Any]) -> tuple[str, str]:
    portfolio = str(body.get("portfolio") or "")
    org = str(body.get("org") or "")
    entity_type = str(body.get("entity_type") or "")
    entity_id = str(body.get("entity_id") or "")
    thread = str(body.get("thread") or "")
    return (
        f"irn:continuation:{portfolio}:{org}",
        f"{entity_type}|{entity_id}|{thread}",
    )


def _table(config: dict[str, Any]):
    import boto3

    region = str((config or {}).get("AWS_REGION") or os.getenv("AWS_REGION") or "us-east-1")
    name = str(
        (config or {}).get("DYNAMODB_SESSION_TABLE")
        or os.getenv("DYNAMODB_SESSION_TABLE")
        or ""
    ).strip()
    if not name:
        return None
    return boto3.resource("dynamodb", region_name=region).Table(name)


def _acquire_lock(config: dict[str, Any], body: dict[str, Any]) -> Optional[str]:
    table = _table(config)
    if table is None:
        return str(uuid.uuid4())
    index, entity_index = _lock_key(body)
    holder = str(uuid.uuid4())
    now = Decimal(str(time.time()))
    lease = Decimal(str(time.time() + LEASE_SECONDS))
    try:
        table.put_item(
            Item={
                "index": index,
                "entity_index": entity_index,
                "holder": holder,
                "lease_until": lease,
            },
            ConditionExpression="attribute_not_exists(#idx) OR lease_until < :now",
            ExpressionAttributeNames={"#idx": "index"},
            ExpressionAttributeValues={":now": now},
        )
    except Exception as exc:
        code = getattr(getattr(exc, "response", {}), "get", lambda *_: {})("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException" or "ConditionalCheckFailed" in str(exc):
            return None
        print(f"continuation lock failed open: {exc}")
        return holder
    return holder


def _release_lock(config: dict[str, Any], body: dict[str, Any], holder: str) -> None:
    table = _table(config)
    if table is None or not holder:
        return
    index, entity_index = _lock_key(body)
    try:
        table.delete_item(
            Key={"index": index, "entity_index": entity_index},
            ConditionExpression="holder = :holder",
            ExpressionAttributeValues={":holder": holder},
        )
    except Exception as exc:
        print(f"continuation unlock failed: {exc}")


def _invoke_handler(config: dict[str, Any], body: dict[str, Any]) -> Any:
    from renglo.schd.schd_controller import call_handler

    handler = str((body.get("callback") or {}).get("handler") or "").strip()
    route = body.get("reply") if isinstance(body.get("reply"), dict) else {}
    user_id = str(body.get("user_id") or route.get("user_id") or body.get("public_user") or "").strip()
    payload = {
        "_continuation": True,
        "portfolio": body.get("portfolio") or "",
        "org": body.get("org") or "",
        "entity_type": body.get("entity_type") or "",
        "entity_id": body.get("entity_id") or "",
        "thread": body.get("thread") or "",
        "turn": body.get("turn_id") or body.get("turn") or "",
        "result": body.get("result") if isinstance(body.get("result"), dict) else {},
        "connectionId": body.get("connectionId") or body.get("connection_id") or "",
        "user_id": user_id,
        "public_user": body.get("public_user") or user_id,
        "channel": route.get("channel") or "",
        "external_id": route.get("external_id") or "",
        "reply_args": route.get("args") if isinstance(route.get("args"), dict) else {},
        "call_id": body.get("call_id") or "",
    }
    return call_handler(
        config,
        str(body.get("portfolio") or ""),
        str(body.get("org") or ""),
        handler,
        payload,
        user_id=user_id,
    )


def _send_reply(config: dict[str, Any], body: dict[str, Any]) -> Any:
    route = body.get("reply") if isinstance(body.get("reply"), dict) else {}
    if not route.get("handler"):
        return {"success": True, "skipped": True}
    from renglo.schd.channel_reply import run_channel_reply

    return run_channel_reply(config, body)


def _load_events(config: dict[str, Any], body: dict[str, Any]) -> list[dict[str, Any]]:
    from renglo.session.session_controller import SessionController

    portfolio = str(body.get("portfolio") or "")
    org = str(body.get("org") or "")
    entity_type = str(body.get("entity_type") or "")
    entity_id = str(body.get("entity_id") or "")
    thread = str(body.get("thread") or "")
    if not all([portfolio, org, entity_type, entity_id, thread]):
        return []
    sessions = SessionController(config=config or {})
    user_id = str(body.get("user_id") or "").strip()
    route = body.get("reply") if isinstance(body.get("reply"), dict) else {}
    if not user_id:
        user_id = str(route.get("user_id") or body.get("public_user") or "").strip()
    if user_id:
        sessions.set_invocation_user(user_id)
    listed = sessions.list_turns(portfolio, org, entity_type, entity_id, thread, False)
    items = (listed or {}).get("items") if isinstance(listed, dict) else None
    events: list[dict[str, Any]] = []
    for turn in items or []:
        if not isinstance(turn, dict):
            continue
        for event in turn.get("events") or []:
            if isinstance(event, dict):
                events.append(event)
    return events


def _kind(event: dict[str, Any]) -> str:
    return str(event.get("_type") or "")


def _content(event: dict[str, Any]) -> dict[str, Any]:
    out = event.get("_out") if isinstance(event.get("_out"), dict) else {}
    content = out.get("content")
    if isinstance(content, dict):
        return content
    return {}


def _is_running(event: dict[str, Any]) -> bool:
    result = _content(event).get("result")
    return isinstance(result, dict) and str(result.get("status") or "") == "running"


def _timestamp(event: dict[str, Any]) -> Optional[datetime]:
    meta = event.get("_meta") if isinstance(event.get("_meta"), dict) else {}
    raw = meta.get("timestamp")
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
