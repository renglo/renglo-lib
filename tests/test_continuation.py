"""Continuation drain. No AWS."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.schd.continuation import (  # noqa: E402
    extract_reply,
    pending_final_call_ids,
    run_agent_callback,
    _invoke_handler,
)


def _result(call_id: str, event_id: str, *, running: bool = False, stamp: str = "2026-10-07T12:00:00+00:00") -> dict:
    result = {"status": "running"} if running else {"total": 10}
    return {
        "_type": "tool_result",
        "_out": {"role": "system", "content": {"call_id": call_id, "success": True, "result": result}},
        "_meta": {"event_id": event_id, "timestamp": stamp},
    }


def _assistant(stamp: str) -> dict:
    return {
        "_type": "assistant_message",
        "_out": {"role": "assistant", "content": "waiting"},
        "_meta": {"event_id": "asst", "timestamp": stamp},
    }


class ContinuationTests(unittest.TestCase):
    def test_a_final_result_after_the_waiting_line_is_pending(self) -> None:
        events = [
            _assistant("2026-10-07T12:00:00+00:00"),
            _result("call-1", "run", running=True, stamp="2026-10-07T12:00:01+00:00"),
            _result("call-1", "done", stamp="2026-10-07T12:02:00+00:00"),
        ]
        self.assertEqual(pending_final_call_ids(events), ["call-1"])

    def test_an_answer_after_the_result_is_not_pending(self) -> None:
        events = [
            _result("call-1", "done", stamp="2026-10-07T12:02:00+00:00"),
            _assistant("2026-10-07T12:03:00+00:00"),
        ]
        self.assertEqual(pending_final_call_ids(events), [])

    def test_a_result_that_lands_during_the_run_is_folded_in(self) -> None:
        snapshots = [
            [
                _assistant("2026-10-07T12:00:00+00:00"),
                _result("call-b", "b-run", running=True, stamp="2026-10-07T12:00:01+00:00"),
                _result("call-a", "a-done", stamp="2026-10-07T12:01:00+00:00"),
            ],
            [
                _assistant("2026-10-07T12:00:00+00:00"),
                _result("call-b", "b-run", running=True, stamp="2026-10-07T12:00:01+00:00"),
                _result("call-a", "a-done", stamp="2026-10-07T12:01:00+00:00"),
                _result("call-b", "b-done", stamp="2026-10-07T12:01:30+00:00"),
                _assistant("2026-10-07T12:01:40+00:00"),
            ],
            [
                _assistant("2026-10-07T12:00:00+00:00"),
                _result("call-b", "b-run", running=True, stamp="2026-10-07T12:00:01+00:00"),
                _result("call-a", "a-done", stamp="2026-10-07T12:01:00+00:00"),
                _result("call-b", "b-done", stamp="2026-10-07T12:01:30+00:00"),
                _assistant("2026-10-07T12:02:00+00:00"),
            ],
        ]
        calls = {"n": 0}

        def load_events(_config, _body):
            index = min(calls["n"], len(snapshots) - 1)
            return snapshots[index]

        def invoke(_config, _body):
            calls["n"] += 1
            return {"output": {"reply": f"pass-{calls['n']}"}}

        sent: list[str] = []

        def send(_config, body):
            sent.append(body["text"])
            return {"success": True}

        outcome = run_agent_callback(
            {},
            {"portfolio": "p", "org": "o", "entity_type": "t", "entity_id": "e", "thread": "th"},
            invoke=invoke,
            send=send,
            load_events=load_events,
            acquire=lambda *_: "holder",
            release=lambda *_: None,
            sleep=lambda *_: None,
        )
        self.assertEqual(outcome["replies"], ["pass-1", "pass-2"])
        self.assertEqual(sent, ["pass-1", "pass-2"])

    def test_a_tool_this_turn_wrote_does_not_start_another_pass(self) -> None:
        snapshots = [
            [
                _assistant("2026-10-07T12:00:00+00:00"),
                _result("call-a", "a-done", stamp="2026-10-07T12:01:00+00:00"),
            ],
            [
                _assistant("2026-10-07T12:00:00+00:00"),
                _result("call-a", "a-done", stamp="2026-10-07T12:01:00+00:00"),
                _result("call-sync", "sync-done", stamp="2026-10-07T12:01:20+00:00"),
                _assistant("2026-10-07T12:01:40+00:00"),
            ],
        ]
        calls = {"n": 0}

        def load_events(_config, _body):
            return snapshots[min(calls["n"], len(snapshots) - 1)]

        def invoke(_config, _body):
            calls["n"] += 1
            return {"output": {"reply": "done"}}

        outcome = run_agent_callback(
            {},
            {"portfolio": "p", "org": "o", "entity_type": "t", "entity_id": "e", "thread": "th"},
            invoke=invoke,
            send=lambda *_: {"success": True},
            load_events=load_events,
            acquire=lambda *_: "holder",
            release=lambda *_: None,
            sleep=lambda *_: None,
        )
        self.assertEqual(outcome["replies"], ["done"])
        self.assertEqual(calls["n"], 1)

    def test_the_callback_is_a_handler_call(self) -> None:
        captured: dict = {}

        def fake_call(config, portfolio, org, handler, payload, user_id=""):
            captured["portfolio"] = portfolio
            captured["org"] = org
            captured["handler"] = handler
            captured["payload"] = payload
            captured["user_id"] = user_id
            return {"success": True, "output": {"reply": "The total is 10."}}

        body = {
            "callback": {"handler": "dumbo/generic_agent"},
            "portfolio": "p",
            "org": "o",
            "call_id": "call-1",
            "user_id": "user-1",
            "result": {"total": 10},
        }
        import types

        fake = types.ModuleType("renglo.schd.schd_controller")
        fake.call_handler = fake_call
        previous = sys.modules.get("renglo.schd.schd_controller")
        sys.modules["renglo.schd.schd_controller"] = fake
        try:
            outcome = _invoke_handler({}, body)
        finally:
            if previous is None:
                sys.modules.pop("renglo.schd.schd_controller", None)
            else:
                sys.modules["renglo.schd.schd_controller"] = previous
        self.assertEqual(captured["handler"], "dumbo/generic_agent")
        self.assertEqual(captured["portfolio"], "p")
        self.assertTrue(captured["payload"]["_continuation"])
        self.assertEqual(captured["payload"]["call_id"], "call-1")
        self.assertEqual(captured["user_id"], "user-1")
        self.assertEqual(extract_reply(outcome), "The total is 10.")

    def test_extract_reply_reads_the_agent_envelope(self) -> None:
        self.assertEqual(
            extract_reply({"success": True, "output": {"output": {"reply": "The total is 10."}}}),
            "The total is 10.",
        )


if __name__ == "__main__":
    unittest.main()
