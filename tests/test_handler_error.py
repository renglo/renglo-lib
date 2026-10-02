"""Failure text extraction for handler errors. No AWS."""

from renglo.session.handler_error import (
    error_roll,
    failure_already_reported,
    failure_message,
)


def test_error_roll_is_a_chat_document():
    doc = error_roll("Tool exploded")
    assert doc["_type"] == "error"
    assert doc["_out"]["content"] == "Tool exploded"
    assert doc["_meta"]["event_id"]


def test_success_is_not_a_failure():
    body = [{"success": True, "output": {"reply": "ok"}}]
    assert failure_message(body, 200) is None


def test_load_and_run_error_string_is_the_message():
    body = [
        {
            "success": False,
            "output": {
                "success": False,
                "output": "Error @load_and_run: boom",
            },
        }
    ]
    assert failure_message(body, 400) == "Error @load_and_run: boom"


def test_handler_reply_is_preferred():
    body = {
        "success": False,
        "output": {"reply": "The agent stopped: boom", "error": "boom"},
    }
    assert failure_message(body, 400) == "The agent stopped: boom"


def test_reported_flag_nests_through_the_handler_envelope():
    body = [
        {
            "success": False,
            "output": {
                "success": False,
                "output": {"reply": "saved already", "_error_reported": True},
            },
        }
    ]
    assert failure_already_reported(body) is True
    assert failure_message(body, 400) == "saved already"


def test_plain_exception_text():
    assert failure_message("socket handler crashed", 500) == "socket handler crashed"


def test_persist_appends_to_the_requested_turn():
    from renglo.session.handler_error import persist_handler_error

    class FakeSessions:
        def __init__(self):
            self.calls = []

        def update_turn(self, portfolio, org, entity_type, entity_id, thread, turn_id, document):
            self.calls.append(turn_id)
            return {"success": True}

        def list_turns(self, *args, **kwargs):
            raise AssertionError("a named turn must not fall through to the latest turn")

    sessions = FakeSessions()
    payload = {
        "portfolio": "p",
        "org": "o",
        "entity_type": "dumbo-chat",
        "entity_id": "dumbo-o",
        "thread": "main",
    }
    assert persist_handler_error(
        {},
        payload,
        {"_type": "tool_result"},
        turn_id="turn-9",
        sessions=sessions,
    )
    assert sessions.calls == ["turn-9"]
