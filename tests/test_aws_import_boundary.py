"""Request-path AWS SDK imports stay inside the store classes."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path


LIB_ROOT = Path(__file__).resolve().parents[1] / "renglo"
API_ROOT = Path(__file__).resolve().parents[2] / "renglo-api" / "renglo_api"

# Paths are relative to each walked tree (renglo/ or renglo_api/).
ALLOWLIST = {
    "timeseries/timeseries_model.py",
    "auth/auth_model.py",
    "session/session_model.py",
    "blueprint/blueprint_model.py",
    "vector/vector_model.py",
    "data/data_model.py",
    "files/files_model.py",
    "schd/schd_model.py",
    "state/state_model.py",
    "search/search_model.py",
    "graph/graph_model.py",
    "chat/chat_model.py",
    "llm/providers/bedrock_converse.py",
    "chat/connection_model.py",
    "schd/peer_model.py",
    "rag/rag_model.py",
}


def _imports_boto3(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "boto3" or alias.name.startswith("boto3."):
                    return True
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "boto3" or node.module.startswith("boto3."):
                return True
    return False


def _offenders(root: Path) -> list[str]:
    found = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        relative = path.relative_to(root).as_posix()
        if relative in ALLOWLIST:
            continue
        if _imports_boto3(path):
            found.append(f"{root.name}/{relative}")
    return found


class AwsImportBoundaryTest(unittest.TestCase):
    def test_boto3_imports_stay_on_the_allowlist(self):
        offenders = _offenders(LIB_ROOT) + _offenders(API_ROOT)
        self.assertEqual(
            offenders,
            [],
            "boto3 import outside the store allowlist:\n" + "\n".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
