#!/usr/bin/env python3
"""embed_handler ref parse and result extract."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from unittest.mock import MagicMock, patch

from renglo.vector.embed_handler import (
    extract_embed_payload,
    invoke_embed_handler,
    parse_embed_handler_ref,
)


class TestParseEmbedHandlerRef(unittest.TestCase):
    def test_extension_handler_defaults_prepare_embed(self):
        parsed = parse_embed_handler_ref("arbitiumtriage/threat_fingerprint_builder")
        self.assertEqual(
            parsed,
            {
                "extension": "arbitiumtriage",
                "handler": "threat_fingerprint_builder",
                "subhandler": "prepare_embed",
            },
        )

    def test_schd_path_third_segment(self):
        parsed = parse_embed_handler_ref(
            "arbitiumtriage/threat_fingerprint_builder/prepare_embed"
        )
        self.assertEqual(parsed["subhandler"], "prepare_embed")
        self.assertEqual(parsed["handler"], "threat_fingerprint_builder")

    def test_legacy_gt_alias(self):
        parsed = parse_embed_handler_ref(
            "arbitiumtriage/threat_fingerprint_builder>prepare_embed"
        )
        self.assertEqual(parsed["subhandler"], "prepare_embed")
        self.assertEqual(parsed["handler"], "threat_fingerprint_builder")

    def test_empty_is_none(self):
        self.assertIsNone(parse_embed_handler_ref(""))
        self.assertIsNone(parse_embed_handler_ref(None))

    def test_invoke_always_calls_run_with_subhandler(self):
        instance = MagicMock()
        instance.run.return_value = {"success": True, "text": "ok"}
        with patch(
            "renglo.vector.embed_handler._load_handler_instance",
            return_value=instance,
        ):
            result = invoke_embed_handler(
                "arbitiumtriage/threat_fingerprint_builder/prepare_embed",
                {"portfolio": "p", "org": "o", "_id": "te-1"},
            )
        self.assertEqual(result["text"], "ok")
        instance.run.assert_called_once()
        instance.prepare_embed.assert_not_called()
        passed = instance.run.call_args.args[0]
        self.assertEqual(passed["subhandler"], "prepare_embed")
        self.assertEqual(passed["_id"], "te-1")


class TestExtractEmbedPayload(unittest.TestCase):
    def test_text_and_attrs(self):
        text, attrs, error, skip = extract_embed_payload(
            {"success": True, "text": "  hello  ", "attrs": {"kind": "catalog"}}
        )
        self.assertEqual(text, "hello")
        self.assertEqual(attrs, {"kind": "catalog"})
        self.assertIsNone(error)
        self.assertFalse(skip)

    def test_legacy_fingerprint(self):
        text, attrs, error, skip = extract_embed_payload(
            {"success": True, "fingerprint": "[provider=aws]"}
        )
        self.assertEqual(text, "[provider=aws]")
        self.assertEqual(attrs, {})
        self.assertIsNone(error)
        self.assertFalse(skip)

    def test_skip_and_empty(self):
        self.assertEqual(extract_embed_payload({"skip": True})[3], True)
        self.assertEqual(extract_embed_payload({"success": True, "text": ""})[3], True)

    def test_failure(self):
        text, attrs, error, skip = extract_embed_payload(
            {"success": False, "error": "boom"}
        )
        self.assertEqual(text, "")
        self.assertEqual(error, "boom")
        self.assertFalse(skip)

    def test_unwraps_output_list(self):
        text, attrs, error, skip = extract_embed_payload(
            {"success": True, "output": [{"fingerprint": "wrapped"}]}
        )
        self.assertEqual(text, "wrapped")
        self.assertIsNone(error)
        self.assertFalse(skip)


if __name__ == "__main__":
    unittest.main()
