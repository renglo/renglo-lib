"""Entra OpenID merge logic (no network)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.auth.entra_openid import merge_with_metadata  # noqa: E402


class MergeMetadataTests(unittest.TestCase):
    def test_explicit_authority_and_jwks_skip_discovery(self):
        merged = merge_with_metadata(
            "any-tenant-id",
            "https://sub.ciamlogin.com/tenant-guid",
            "https://sub.ciamlogin.com/tenant-guid/discovery/v2.0/keys",
        )
        self.assertIsNotNone(merged)
        assert merged is not None
        self.assertEqual(
            merged["jwks_uri"],
            "https://sub.ciamlogin.com/tenant-guid/discovery/v2.0/keys",
        )
        self.assertEqual(merged["issuer"], "https://sub.ciamlogin.com/tenant-guid/v2.0")
