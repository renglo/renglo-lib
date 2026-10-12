"""Detached peer completion frames. No AWS."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import renglo.runtime  # noqa: E402
import renglo.session.handler_error  # noqa: E402
from renglo.schd.peer_detached import complete_detached, frames_for_result  # noqa: E402


EVENT = {
    "handler": "agencies_info_reports",
    "payload": {
        "portfolio": "p",
        "org": "o",
        "entity_type": "dumbo-chat",
        "entity_id": "dumbo-o",
        "thread": "main",
        "connectionId": "conn",
    },
    "completion": {
        "call_id": "call-1",
        "tool": "tourbotlink/agencies_info_reports",
        "session_id": "dumbo-chat|dumbo-o|main",
        "when": "Open search (name only)",
    },
    "detached": True,
}


class DetachedFrameTests(unittest.TestCase):
    def test_success_writes_result_then_assistant(self) -> None:
        frames = frames_for_result(EVENT, {"success": True, "output": {"match_count": 1}})
        self.assertEqual([frame["_type"] for frame in frames], ["tool_result", "assistant_message"])
        self.assertTrue(frames[0]["_out"]["content"]["success"])
        self.assertEqual(frames[0]["_out"]["content"]["call_id"], "call-1")
        self.assertIn("ready", frames[1]["_out"]["content"])
        self.assertIn("Open search", frames[1]["_out"]["content"])

    def test_failure_is_a_tool_result(self) -> None:
        frames = frames_for_result(EVENT, {"success": False, "message": "catalog down"})
        self.assertEqual(len(frames), 1)
        self.assertFalse(frames[0]["_out"]["content"]["success"])
        self.assertEqual(frames[0]["_out"]["content"]["error"], "catalog down")

    def test_timeout_says_it_did_not_finish(self) -> None:
        frames = frames_for_result(EVENT, None, timed_out=True)
        self.assertEqual(frames[0]["_out"]["content"]["error"], "The report did not finish.")

    def test_completion_writes_the_turn_that_started_the_call(self) -> None:
        import sys
        import types

        event = {
            **EVENT,
            "completion": {**EVENT["completion"], "turn_id": "turn-9"},
        }
        fake = types.ModuleType("renglo.session.session_controller")

        class FakeController:
            def __init__(self, config=None):
                self.config = config

        fake.SessionController = FakeController
        previous = sys.modules.get("renglo.session.session_controller")
        sys.modules["renglo.session.session_controller"] = fake
        try:
            with patch("renglo.runtime.stamp_invocation_jwt_claims") as stamp, patch(
                "renglo.session.handler_error.persist_handler_error", return_value=True
            ) as persist, patch("renglo.session.handler_error.push_handler_error", return_value=True):
                complete_detached(event, {"success": True}, config={}, claims={"sub": "user"})
        finally:
            if previous is None:
                sys.modules.pop("renglo.session.session_controller", None)
            else:
                sys.modules["renglo.session.session_controller"] = previous
        self.assertGreaterEqual(persist.call_count, 1)
        for call in persist.call_args_list:
            self.assertEqual(call.kwargs["turn_id"], "turn-9")
        stamp.assert_called_once()
        self.assertEqual(stamp.call_args.args[1], {"sub": "user"})

    def test_webhook_reply_is_sent_after_the_session_write(self) -> None:
        import sys
        import types

        event = {
            **EVENT,
            "payload": {
                **EVENT["payload"],
                "connectionId": "",
                "public_user": "user-1",
            },
            "completion": {
                **EVENT["completion"],
                "turn_id": "turn-9",
                "reply": {
                    "channel": "whatsapp",
                    "handler": "whatsapp/post_message",
                    "args": {"target": "15551212"},
                    "external_id": "15551212",
                    "user_id": "user-1",
                },
            },
        }
        order: list[str] = []
        fake = types.ModuleType("renglo.session.session_controller")

        class FakeController:
            def __init__(self, config=None):
                self.config = config
                self.user_id = ""

            def set_invocation_user(self, user_id):
                self.user_id = user_id

        fake.SessionController = FakeController
        previous = sys.modules.get("renglo.session.session_controller")
        sys.modules["renglo.session.session_controller"] = fake

        def persist(*_args, **_kwargs):
            order.append("persist")
            return True

        def push(*_args, **_kwargs):
            order.append("push")
            return False

        def deliver(_config, body):
            order.append("deliver")
            self.assertEqual(body["entity_id"], "dumbo-o")
            self.assertEqual(body["turn_id"], "turn-9")
            self.assertEqual(body["reply"]["channel"], "whatsapp")
            self.assertIn("ready", body["text"])
            return {"success": True}

        try:
            with patch("renglo.runtime.stamp_invocation_jwt_claims"), patch(
                "renglo.session.handler_error.persist_handler_error", side_effect=persist
            ), patch("renglo.session.handler_error.push_handler_error", side_effect=push), patch(
                "renglo.schd.channel_reply.deliver_channel_reply", side_effect=deliver
            ):
                complete_detached(event, {"success": True, "output": {"total": 10}}, config={})
        finally:
            if previous is None:
                sys.modules.pop("renglo.session.session_controller", None)
            else:
                sys.modules["renglo.session.session_controller"] = previous
        self.assertEqual([step for step in order if step == "persist"], ["persist", "persist"])
        self.assertEqual(order[-1], "deliver")
        self.assertLess(order.index("persist"), order.index("deliver"))

    def test_callback_saves_the_tool_result_and_continues_the_agent(self) -> None:
        import sys
        import types

        event = {
            **EVENT,
            "completion": {
                **EVENT["completion"],
                "turn_id": "turn-9",
                "callback": {"handler": "dumbo/generic_agent"},
                "reply": {
                    "channel": "whatsapp",
                    "handler": "whatsapp/post_message",
                    "args": {"target": "15551212"},
                    "user_id": "user-1",
                },
            },
        }
        persisted: list[str] = []
        fake = types.ModuleType("renglo.session.session_controller")

        class FakeController:
            def __init__(self, config=None):
                pass

            def set_invocation_user(self, user_id):
                return None

        fake.SessionController = FakeController
        previous = sys.modules.get("renglo.session.session_controller")
        sys.modules["renglo.session.session_controller"] = fake

        def persist(_config, _payload, frame, **_kwargs):
            persisted.append(frame["_type"])
            return True

        def callback(_config, body):
            self.assertEqual(body["callback"]["handler"], "dumbo/generic_agent")
            self.assertEqual(body["call_id"], "call-1")
            self.assertEqual(body["reply"]["channel"], "whatsapp")
            return {"success": True}

        try:
            with patch("renglo.runtime.stamp_invocation_jwt_claims"), patch(
                "renglo.session.handler_error.persist_handler_error", side_effect=persist
            ), patch("renglo.session.handler_error.push_handler_error", return_value=True), patch(
                "renglo.schd.continuation.post_agent_callback", side_effect=callback
            ), patch("renglo.schd.channel_reply.deliver_channel_reply") as raw_send:
                complete_detached(event, {"success": True, "output": {"total": 10}}, config={})
        finally:
            if previous is None:
                sys.modules.pop("renglo.session.session_controller", None)
            else:
                sys.modules["renglo.session.session_controller"] = previous
        self.assertEqual(persisted, ["tool_result"])
        raw_send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
