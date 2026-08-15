"""
Install an extension's blueprint JSON files when they are missing from DynamoDB.

Looks for a blueprints/ folder next to the extension (same files upload_blueprints.py
uses) and puts any document that get_blueprint cannot find.

Declared pip dependencies that are themselves Renglo extensions (gro, arbitiumlab,
…) are resolved the same way so activating one extension also installs the
rings it writes to (e.g. arbitiumtriage → arbitium_threat_events).
"""

from __future__ import annotations

import importlib
import importlib.metadata
import importlib.util
import json
import re
import tomllib
import uuid
from pathlib import Path
from typing import Any, Iterable

from renglo.blueprint.blueprint_controller import BlueprintController
from renglo.logger import get_logger

_REQ_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


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


def _normalize_dist_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", str(name or "").strip()).lower()


def _requirement_name(requirement: str) -> str:
    match = _REQ_NAME.match(str(requirement or ""))
    return match.group(1) if match else ""


def _is_extra_requirement(requirement: str) -> bool:
    marker = str(requirement or "").replace(" ", "").lower()
    return "extra==" in marker


def _find_pyproject(module_file: str) -> Path | None:
    current = Path(module_file).resolve().parent
    for _ in range(8):
        candidate = current / "pyproject.toml"
        if candidate.is_file():
            return candidate
        if current.parent == current:
            break
        current = current.parent
    return None


def _load_pyproject(path: Path) -> dict[str, Any]:
    try:
        with open(path, "rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _workspace_root_from(start: Path) -> Path | None:
    current = start.resolve()
    if current.is_file():
        current = current.parent
    for _ in range(10):
        if (current / "extensions").is_dir() and (current / "dev").is_dir():
            return current
        if current.parent == current:
            break
        current = current.parent
    return None


def _declared_dependencies(module_file: str) -> tuple[str, list[str]]:
    """Return (this distribution name, direct dependency distribution names)."""
    names: list[str] = []
    this_name = ""
    pyproject_path = _find_pyproject(module_file)
    if pyproject_path:
        project = _load_pyproject(pyproject_path).get("project") or {}
        this_name = str(project.get("name") or "").strip()
        for req in project.get("dependencies") or []:
            name = _requirement_name(req)
            if name:
                names.append(name)

    dist_name = this_name
    if dist_name:
        try:
            for req in importlib.metadata.requires(dist_name) or []:
                if _is_extra_requirement(req):
                    continue
                name = _requirement_name(req)
                if name:
                    names.append(name)
        except importlib.metadata.PackageNotFoundError:
            pass

    seen: set[str] = set()
    unique: list[str] = []
    self_key = _normalize_dist_name(this_name)
    for name in names:
        key = _normalize_dist_name(name)
        if not key or key == self_key or key in seen:
            continue
        seen.add(key)
        unique.append(name)
    return this_name, unique


def _iter_workspace_extensions(module_file: str) -> Iterable[tuple[str, Path]]:
    root = _workspace_root_from(Path(module_file))
    if root is None:
        return
    extensions_root = root / "extensions"
    if not extensions_root.is_dir():
        return
    for ext_dir in sorted(extensions_root.iterdir()):
        if not ext_dir.is_dir():
            continue
        pyproject_path = ext_dir / "package" / "pyproject.toml"
        if not pyproject_path.is_file():
            continue
        project = _load_pyproject(pyproject_path).get("project") or {}
        dist_name = str(project.get("name") or "").strip()
        if dist_name:
            yield dist_name, ext_dir


def _workspace_extension_dir(module_file: str, dist_name: str) -> Path | None:
    want = _normalize_dist_name(dist_name)
    for name, ext_dir in _iter_workspace_extensions(module_file):
        if _normalize_dist_name(name) == want:
            return ext_dir
    return None


def _import_names_for_dist(dist_name: str) -> list[str]:
    candidates = [dist_name.replace("-", "_")]
    if dist_name not in candidates:
        candidates.append(dist_name)
    try:
        mapping = importlib.metadata.packages_distributions()
    except Exception:
        mapping = {}
    want = _normalize_dist_name(dist_name)
    for package, dists in mapping.items():
        if any(_normalize_dist_name(item) == want for item in dists or []):
            candidates.append(package)
    seen: set[str] = set()
    unique: list[str] = []
    for name in candidates:
        key = name.lower()
        if not name or key in seen:
            continue
        seen.add(key)
        unique.append(name)
    return unique


def _try_import(dist_name: str):
    for name in _import_names_for_dist(dist_name):
        try:
            return importlib.import_module(name)
        except Exception:
            continue
    return None


def _is_extension_module(module) -> bool:
    top = str(getattr(module, "__name__", "") or "").split(".", 1)[0]
    if not top:
        return False
    try:
        if importlib.util.find_spec(f"{top}.handlers.initialize_extension"):
            return True
    except ModuleNotFoundError:
        pass
    module_file = getattr(module, "__file__", None)
    if not module_file:
        return False
    package_root = Path(module_file).resolve().parent
    return (package_root / "handlers").is_dir()


def _blueprints_dir_for_dependency(module_file: str, dist_name: str) -> tuple[Path | None, str | None]:
    """
    Locate a dependency's blueprints folder.

    Returns (dir, error). error is set when the dep is an extension but
    blueprints cannot be found. (None, None) means it is not an extension.
    """
    workspace_dir = _workspace_extension_dir(module_file, dist_name)
    if workspace_dir is not None:
        candidate = workspace_dir / "blueprints"
        if candidate.is_dir() and any(candidate.glob("*.json")):
            return candidate, None
        module = _try_import(dist_name)
        module_path = getattr(module, "__file__", None) if module is not None else None
        if module_path:
            found = find_blueprints_dir(module_path)
            if found:
                return found, None
        return None, (
            f"{dist_name}: extension dependency has no blueprints/ JSON "
            f"(looked in {candidate})"
        )

    module = _try_import(dist_name)
    if module is None or not _is_extension_module(module):
        return None, None
    module_path = getattr(module, "__file__", None)
    if not module_path:
        return None, f"{dist_name}: extension package has no __file__; cannot locate blueprints"
    found = find_blueprints_dir(module_path)
    if found:
        return found, None
    return None, f"{dist_name}: extension is installed but its blueprints/ folder was not found"


def _install_from_dir(
    bpc: BlueprintController,
    blueprints_dir: Path,
    *,
    label: str = "",
) -> dict[str, list[str]]:
    logger = get_logger()
    installed: list[str] = []
    already_present: list[str] = []
    failed: list[str] = []
    prefix = f"{label}/" if label else ""

    files = load_blueprint_files(blueprints_dir)
    if not files:
        failed.append(f"{prefix}no blueprint JSON files in {blueprints_dir}")
        return {
            "installed": installed,
            "already_present": already_present,
            "failed": failed,
        }

    for path, document in files:
        name = _blueprint_name(document, path)
        tagged = f"{prefix}{name}"
        try:
            if _blueprint_exists(bpc, document, path):
                already_present.append(tagged)
                continue
            item = _prepare_blueprint(document, path)
            response = bpc.BPM.put_blueprint(item)
            if response.get("success"):
                installed.append(f"{item['irn']}@{item.get('version')}")
                logger.info("Installed missing blueprint %s", item["irn"])
            else:
                failed.append(f"{tagged}: {response.get('error') or response.get('message')}")
        except Exception as exc:
            failed.append(f"{tagged}: {exc}")
            logger.warning("Failed to install blueprint %s: %s", tagged, exc)

    return {
        "installed": installed,
        "already_present": already_present,
        "failed": failed,
    }


def _summarize(installed: list[str], already_present: list[str], failed: list[str], dep_count: int) -> str:
    dep_note = f" (including {dep_count} dependency extension(s))" if dep_count else ""
    if failed and not installed and not already_present:
        return "Could not install extension blueprints"
    if installed and already_present:
        return (
            f"Installed {len(installed)} missing blueprint(s); "
            f"{len(already_present)} already present{dep_note}"
        )
    if installed:
        return f"Installed {len(installed)} missing blueprint(s){dep_note}"
    if already_present:
        return f"All extension blueprints are already installed{dep_note}"
    return "Could not install extension blueprints"


def ensure_extension_blueprints(config, *, module_file: str) -> dict:
    """
    Idempotent: install each local blueprint JSON that is not already in DynamoDB.

    Also installs blueprints for pip dependencies that are Renglo extensions
    (in-repo via extensions/*/package/pyproject.toml, or imported packages
    that expose handlers.initialize_extension).
    """
    action = "ensure_blueprints"
    blueprints_dir = find_blueprints_dir(module_file)
    if not blueprints_dir:
        return {
            "success": False,
            "action": action,
            "message": "Extension blueprints directory not found",
            "output": {
                "source": None,
                "installed": [],
                "already_present": [],
                "failed": [],
                "dependencies": [],
            },
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
                "dependencies": [],
            },
        }

    bpc = BlueprintController(config=config)
    own = _install_from_dir(bpc, blueprints_dir)
    installed = list(own["installed"])
    already_present = list(own["already_present"])
    failed = list(own["failed"])
    seen_dirs = {blueprints_dir.resolve()}
    dependency_results: list[dict[str, Any]] = []

    _this_name, dependencies = _declared_dependencies(module_file)
    for dist_name in dependencies:
        dep_dir, error = _blueprints_dir_for_dependency(module_file, dist_name)
        if error:
            failed.append(error)
            dependency_results.append(
                {
                    "name": dist_name,
                    "source": None,
                    "installed": [],
                    "already_present": [],
                    "failed": [error],
                }
            )
            continue
        if dep_dir is None:
            continue
        resolved = dep_dir.resolve()
        if resolved in seen_dirs:
            continue
        seen_dirs.add(resolved)
        result = _install_from_dir(bpc, dep_dir, label=dist_name)
        installed.extend(result["installed"])
        already_present.extend(result["already_present"])
        failed.extend(result["failed"])
        dependency_results.append(
            {
                "name": dist_name,
                "source": str(dep_dir),
                "installed": result["installed"],
                "already_present": result["already_present"],
                "failed": result["failed"],
            }
        )

    success = not failed
    return {
        "success": success,
        "action": action,
        "message": _summarize(installed, already_present, failed, len(dependency_results)),
        "output": {
            "source": str(blueprints_dir),
            "installed": installed,
            "already_present": already_present,
            "failed": failed,
            "dependencies": dependency_results,
        },
    }
