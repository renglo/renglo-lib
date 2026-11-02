"""Request-path AWS SDK imports stay inside the store classes."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path


LIB_ROOT = Path(__file__).resolve().parents[1] / "renglo"
API_ROOT = Path(__file__).resolve().parents[2] / "renglo-api" / "renglo_api"

# Paths are relative to each walked tree (renglo/ or renglo_api/).
ALLOWLIST = {
    "timeseries/submodels/timeseries_model_aws.py",
    "auth/submodels/auth_model_aws.py",
    "session/submodels/session_model_aws.py",
    "blueprint/submodels/blueprint_model_aws.py",
    "vector/submodels/vector_model_aws.py",
    "data/submodels/data_model_aws.py",
    "files/submodels/files_model_aws.py",
    "schd/submodels/schd_model_aws.py",
    "state/submodels/state_model_aws.py",
    "search/submodels/search_model_aws.py",
    "graph/submodels/graph_model_aws.py",
    "chat/submodels/chat_model_aws.py",
    "llm/providers/bedrock_converse.py",
    "chat/submodels/connection_model_aws.py",
    "schd/submodels/peer_model_aws.py",
    "rag/submodels/rag_model_aws.py",
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
