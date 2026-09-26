#!/usr/bin/env python3
"""Resolve extension package dirs without assuming folder name == handle."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from renglo.schd.external_handlers_config import (  # noqa: E402
    resolve_extension_package_dir,
    resolve_extension_package_path,
)


class ExtensionPackageDirTests(unittest.TestCase):
    def test_direct_folder_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pkg = root / "extensions" / "arbitiumlab" / "package"
            pkg.mkdir(parents=True)
            (pkg / "handlers_config.json").write_text('{"handlers": {}}', encoding="utf-8")
            with unittest.mock.patch(
                "renglo.schd.external_handlers_config._get_workspace_root",
                return_value=root,
            ):
                found = resolve_extension_package_dir("arbitiumlab")
            self.assertEqual(found, pkg)

    def test_extension_handle_marker_when_folder_differs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pkg = root / "extensions" / "my-local-clone" / "package"
            pkg.mkdir(parents=True)
            (pkg / "extension_handle").write_text("arbitiumlab\n", encoding="utf-8")
            (pkg / "handlers_config.json").write_text('{"handlers": {}}', encoding="utf-8")
            with unittest.mock.patch(
                "renglo.schd.external_handlers_config._get_workspace_root",
                return_value=root,
            ):
                found = resolve_extension_package_dir("arbitiumlab")
                self.assertEqual(found, pkg)
                rel = resolve_extension_package_path("arbitiumlab")
            self.assertTrue(rel.endswith("my-local-clone/package"))

    def test_env_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pkg = Path(tmp) / "custom" / "package"
            pkg.mkdir(parents=True)
            key = "EXTERNAL_HANDLERS_PACKAGE_ARBITIUMLAB"
            old = os.environ.get(key)
            os.environ[key] = str(pkg)
            try:
                found = resolve_extension_package_dir("arbitiumlab")
            finally:
                if old is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old
            self.assertEqual(found, pkg)


if __name__ == "__main__":
    unittest.main()
