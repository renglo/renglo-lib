"""Request-path AWS SDK imports stay inside the store classes."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path


LIB_ROOT = Path(__file__).resolve().parents[1] / "renglo"
API_ROOT = Path(__file__).resolve().parents[2] / "renglo-api" / "renglo_api"

# Paths are relative to each walked tree (renglo/ or renglo_api/).
ALLOWLIST = {
    "document_stores/document_store_aws.py",
    "auth/identity.py",
    "vector/submodels/vector_model_aws.py",
    "files/submodels/files_model_aws.py",
    "schd/submodels/schd_model_aws.py",
    "llm/providers/bedrock_converse.py",
    "chat/submodels/connection_model_aws.py",
    "schd/submodels/peer_model_aws.py",
    "rag/submodels/rag_model_aws.py",
}


AZURE_ALLOWLIST = {
    "document_stores/document_store_azure.py",
    "files/submodels/files_model_azure.py",
}


def _imports_named(path: Path, names) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if any(alias.name == name or alias.name.startswith(name + ".") for name in names):
                    return True
        elif isinstance(node, ast.ImportFrom) and node.module:
            if any(node.module == name or node.module.startswith(name + ".") for name in names):
                return True
    return False


def _imports_boto3(path: Path) -> bool:
    return _imports_named(path, ("boto3",))


def _imports_azure_sdk(path: Path) -> bool:
    return _imports_named(path, ("azure.cosmos", "azure.identity", "azure.storage"))


def _offenders(root: Path, detect, allowlist) -> list[str]:
    found = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        relative = path.relative_to(root).as_posix()
        if relative in allowlist:
            continue
        if detect(path):
            found.append(f"{root.name}/{relative}")
    return found


class AwsImportBoundaryTest(unittest.TestCase):
    def test_boto3_imports_stay_on_the_allowlist(self):
        offenders = _offenders(LIB_ROOT, _imports_boto3, ALLOWLIST) + _offenders(
            API_ROOT, _imports_boto3, ALLOWLIST
        )
        self.assertEqual(
            offenders,
            [],
            "boto3 import outside the store allowlist:\n" + "\n".join(offenders),
        )

    def test_azure_sdk_imports_stay_in_the_document_store(self):
        offenders = _offenders(LIB_ROOT, _imports_azure_sdk, AZURE_ALLOWLIST) + _offenders(
            API_ROOT, _imports_azure_sdk, AZURE_ALLOWLIST
        )
        self.assertEqual(
            offenders,
            [],
            "Azure SDK import outside document_store_azure.py:\n" + "\n".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
