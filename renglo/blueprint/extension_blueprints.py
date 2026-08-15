"""
Install an extension's blueprint JSON files when they are missing from DynamoDB.

Looks for a blueprints/ folder next to the extension (same files upload_blueprints.py
uses) and puts any document that get_blueprint cannot find.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from renglo.blueprint.blueprint_controller import BlueprintController
from renglo.logger import get_logger


def find_blueprints_dir(module_file: str) -> Path | None:
    current = Path(module_file).resolve().parent
    for _ in range(6):
        candidate = current / "blueprints"
        if candidate.is_dir() and any(candidate.glob("*.json")):
            return candidate
        if current.parent == current:
            break
        current = current.parent
    return None


def load_blueprint_files(blueprints_dir: Path) -> list[tuple[Path, dict]]:
    loaded = []
    for json_file in sorted(blueprints_dir.glob("*.json")):
        try:
            with open(json_file, encoding="utf-8") as handle:
                document = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(document, dict):
            loaded.append((json_file, document))
    return loaded


def _blueprint_name(document: dict, path: Path) -> str:
    name = str(document.get("name") or "").strip()
    if name:
        return name
    irn = str(document.get("irn") or "")
    parts = irn.split(":")
    if len(parts) >= 4:
        return parts[-1]
    return path.stem


def _blueprint_handle(document: dict) -> str:
    return str(document.get("handle") or "irma").strip() or "irma"


def _blueprint_exists(bpc: BlueprintController, document: dict, path: Path) -> bool:
    existing = bpc.get_blueprint(
        _blueprint_handle(document),
        _blueprint_name(document, path),
        "last",
    )
    if not isinstance(existing, dict) or existing.get("success") is False:
        return False
    return bool(existing.get("irn") or existing.get("fields") or existing.get("name"))


def _prepare_blueprint(document: dict, path: Path) -> dict:
    item = dict(document)
    handle = _blueprint_handle(item)
    name = _blueprint_name(item, path)
    item.setdefault("handle", handle)
    item.setdefault("name", name)
    item.setdefault("irn", f"irn:blueprint:{handle}:{name}")
    item.setdefault("version", "latest")
    if not item.get("_id"):
        item["_id"] = str(uuid.uuid4())
    return item


def ensure_extension_blueprints(config, *, module_file: str) -> dict:
    """
    Idempotent: install each local blueprint JSON that is not already in DynamoDB.
    """
    action = "ensure_blueprints"
    logger = get_logger()
    blueprints_dir = find_blueprints_dir(module_file)
    if not blueprints_dir:
        return {
            "success": False,
            "action": action,
            "message": "Extension blueprints directory not found",
            "output": {"source": None, "installed": [], "already_present": [], "failed": []},
        }

    files = load_blueprint_files(blueprints_dir)
    if not files:
        return {
            "success": False,
            "action": action,
            "message": f"No blueprint JSON files in {blueprints_dir}",
            "output": {
                "source": str(blueprints_dir),
                "installed": [],
                "already_present": [],
                "failed": [],
            },
        }

    bpc = BlueprintController(config=config)
    installed = []
    already_present = []
    failed = []

    for path, document in files:
        name = _blueprint_name(document, path)
        try:
            if _blueprint_exists(bpc, document, path):
                already_present.append(name)
                continue
            item = _prepare_blueprint(document, path)
            response = bpc.BPM.put_blueprint(item)
            if response.get("success"):
                installed.append(f"{item['irn']}@{item.get('version')}")
                logger.info("Installed missing blueprint %s", item["irn"])
            else:
                failed.append(f"{name}: {response.get('error') or response.get('message')}")
        except Exception as exc:
            failed.append(f"{name}: {exc}")
            logger.warning("Failed to install blueprint %s: %s", name, exc)

    success = not failed
    if installed and already_present:
        message = f"Installed {len(installed)} missing blueprint(s); {len(already_present)} already present"
    elif installed:
        message = f"Installed {len(installed)} missing blueprint(s)"
    elif already_present:
        message = "All extension blueprints are already installed"
    else:
        message = "Could not install extension blueprints"

    return {
        "success": success,
        "action": action,
        "message": message,
        "output": {
            "source": str(blueprints_dir),
            "installed": installed,
            "already_present": already_present,
            "failed": failed,
        },
    }
