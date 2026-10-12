"""Channel reply route for a finished live call. No AWS."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.schd.channel_reply import reply_route, text_for_channel  # noqa: E402


class ReplyRouteTests(unittest.TestCase):
    def test_whatsapp_uses_the_external_id_as_the_recipient(self) -> None:
        route = reply_route(
            {
                "channel": "whatsapp",
                "external_id": "15551212",
                "user_id": "user-1",
            }
        )
        self.assertEqual(route["handler"], "whatsapp/post_message")
        self.assertEqual(route["args"]["target"], "15551212")
        self.assertEqual(route["user_id"], "user-1")

    def test_gmail_keeps_the_thread_fields_on_the_outbound_call(self) -> None:
        route = reply_route(
            {
                "channel": "gmail",
                "external_id": "a@example.com",
                "reply_args": {
                    "to": "a@example.com",
                    "thread_id": "thr-1",
                    "subject": "Re: totals",
                    "message_id_header": "<m@mail>",
                },
            }
        )
        self.assertEqual(route["handler"], "gmail/reply_message")
        self.assertEqual(route["args"]["thread_id"], "thr-1")
        self.assertEqual(route["args"]["message_id_header"], "<m@mail>")

    def test_a_console_turn_has_no_channel_route(self) -> None:
        self.assertIsNone(reply_route({"connectionId": "conn"}))
        self.assertIsNone(reply_route({"channel": "slack"}))

    def test_channel_text_includes_the_saved_result(self) -> None:
        frames = [
            {
                "_type": "tool_result",
                "_out": {
                    "role": "system",
                    "content": {
                        "success": True,
                        "result": {"message": "Report generated", "output": {"total": 42}},
                    },
                },
            },
            {
                "_type": "assistant_message",
                "_out": {"role": "assistant", "content": "The report is ready."},
            },
        ]
        text = text_for_channel(frames)
        self.assertIn("The report is ready.", text)
        self.assertIn("42", text)


if __name__ == "__main__":
    unittest.main()
