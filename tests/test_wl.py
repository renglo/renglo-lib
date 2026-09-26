#!/usr/bin/env python3
from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.wl import (  # noqa: E402
    DEFAULT_APP_NAME,
    LOGO_CID,
    app_name,
    build_raw_email,
    invite_inline_images,
    invite_strings,
    render_invite_email,
    small_logo_path,
)


def _fake_wl(*, name: str = "Arbitium", logo: Path | None = None):
    module = types.ModuleType("wl")
    module.app_name = name
    module.locales = {
        "en": {
            "appName": name,
            "email": {
                "invite": {
                    "subject": "You have been invited to {team} on {appName}",
                    "heading": "Hello from {appName}",
                    "intro": "You have been invited by {inviter} to team {team}.",
                    "code": "Your invite code is: {code}",
                    "link": "Follow this link:",
                }
            },
        }
    }
    module.small_logo_path = logo
    return module


class WlFallbackTests(unittest.TestCase):
    def test_missing_pack_uses_renglo_not_env_name(self) -> None:
        with patch.dict(sys.modules, {"wl": None}):
            with patch("renglo.wl.load_wl_module", return_value=None):
                self.assertEqual(app_name(), DEFAULT_APP_NAME)
                self.assertIsNone(small_logo_path())
                email = render_invite_email(
                    inviter="Ada",
                    team="Ops/Admin",
                    code="123456",
                    link="http://127.0.0.1:5174/invite?code=123456",
                )
                self.assertIn("Renglo", email.subject)
                self.assertNotIn("arbitium0813", email.subject)
                self.assertNotIn("arbitium0813", email.body_html)


class WlPackTests(unittest.TestCase):
    def test_invite_uses_product_name_and_escapes_html(self) -> None:
        pack = _fake_wl(name="Arbitium")
        with patch("renglo.wl.load_wl_module", return_value=pack):
            email = render_invite_email(
                inviter='Ada <script>alert(1)</script>',
                team="Data Centers/Admin",
                code="354876",
                link="http://127.0.0.1:5174/invite?code=354876&email=a@b.com",
            )
            self.assertEqual(
                email.subject,
                "You have been invited to Data Centers/Admin on Arbitium",
            )
            self.assertIn("Hello from Arbitium", email.body_html)
            self.assertNotIn("arbitium0813", email.body_html)
            self.assertNotIn("<script>", email.body_html)
            self.assertIn("&lt;script&gt;", email.body_html)
            self.assertIsNone(email.logo_path)

    def test_logo_is_attached_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            logo = Path(raw) / "small_logo.png"
            logo.write_bytes(b"\x89PNG\r\n\x1a\n")
            pack = _fake_wl(logo=logo)
            with patch("renglo.wl.load_wl_module", return_value=pack):
                email = render_invite_email(
                    inviter="Ada",
                    team="Ops/Admin",
                    code="123456",
                    link="http://example.test/invite",
                )
                self.assertEqual(email.logo_cid, LOGO_CID)
                self.assertIn(f'cid:{LOGO_CID}', email.body_html)
                self.assertEqual(invite_inline_images(email), [(logo, LOGO_CID)])

    def test_invite_strings_merge_defaults(self) -> None:
        pack = _fake_wl()
        with patch("renglo.wl.load_wl_module", return_value=pack):
            strings = invite_strings()
            self.assertIn("{appName}", strings["subjectHint"])


class RawEmailTests(unittest.TestCase):
    def test_build_raw_email_embeds_cid(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            logo = Path(raw) / "small_logo.png"
            logo.write_bytes(b"\x89PNG\r\n\x1a\n")
            message = build_raw_email(
                sender="from@example.com",
                recipient="to@example.com",
                subject="Invite",
                body_text="hello",
                body_html='<img src="cid:wl-logo" />',
                inline_images=[(logo, "wl-logo")],
            )
            text = message.decode("utf-8", errors="replace")
            self.assertIn("Content-ID: <wl-logo>", text)
            self.assertIn("image/png", text)


if __name__ == "__main__":
    unittest.main()
