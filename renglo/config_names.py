"""Generic config names.

Deployed processes still export the previous names. `setting` reads the
generic name first, then that previous name, so either form works.

Table names that are left blank are `{environment}_{suffix}`. The environment
is `WL_NAME` on the config or in the process environment, then the `name`
field of the workspace `renglo.yaml`. An explicit table name still wins.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping, Optional


LEGACY = {
    "FILES_BUCKET": "S3_BUCKET_NAME",
    "DATA_TABLE": "DYNAMODB_RINGDATA_TABLE",
    "ENTITY_TABLE": "DYNAMODB_ENTITY_TABLE",
    "BLUEPRINT_TABLE": "DYNAMODB_BLUEPRINT_TABLE",
    "REL_TABLE": "DYNAMODB_REL_TABLE",
    "CHAT_TABLE": "DYNAMODB_CHAT_TABLE",
    "GRAPH_TABLE": "DYNAMODB_GRAPH_TABLE",
    "SEARCH_TABLE": "DYNAMODB_SEARCH_TABLE",
    "SESSION_TABLE": "DYNAMODB_SESSION_TABLE",
    "TIMESERIES_TABLE": "DYNAMODB_TIMESERIES_TABLE",
    "REGION": "AWS_REGION",
    "PROFILE": "AWS_PROFILE",
}

# Same suffixes as the bootstrap and Lambda env builders.
TABLE_SUFFIX = {
    "DATA_TABLE": "data",
    "ENTITY_TABLE": "entities",
    "BLUEPRINT_TABLE": "blueprints",
    "REL_TABLE": "rel",
    "CHAT_TABLE": "chat",
    "GRAPH_TABLE": "graph",
    "SEARCH_TABLE": "search",
    "SESSION_TABLE": "session",
    "TIMESERIES_TABLE": "timeseries",
}

_PLACEHOLDER_ENV = {"x"}
_OWNER_REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_bom_name_cached: Optional[str] = None


def _present(value: Any) -> bool:
    return value is not None and value != ""


def setting(
    config: Optional[Mapping[str, Any]],
    name: str,
    default: Any = None,
    *,
    environ: bool = False,
):
    """Return a config value by its generic name."""
    source = config or {}
    if _present(source.get(name)):
        return source.get(name)
    legacy = LEGACY.get(name)
    if legacy and _present(source.get(legacy)):
        return source.get(legacy)
    if environ:
        env_value = os.getenv(name)
        if _present(env_value):
            return env_value
        if legacy:
            env_value = os.getenv(legacy)
            if _present(env_value):
                return env_value
    return default


def environment_name(config: Optional[Mapping[str, Any]] = None) -> str:
    """Environment id used to name tables."""
    source = config or {}
    named = _clean_env(source.get("WL_NAME"))
    if named:
        return named
    named = _clean_env(os.getenv("WL_NAME"))
    if named:
        return named
    return _bom_name()


def table_name(
    config: Optional[Mapping[str, Any]],
    name: str,
    default: Any = None,
    *,
    environ: bool = False,
):
    """Return a table name, deriving `{environment}_{suffix}` when it is blank."""
    found = setting(config, name, environ=environ)
    if _present(found):
        return found
    suffix = TABLE_SUFFIX.get(name)
    env_name = environment_name(config) if suffix else ""
    if suffix and env_name:
        return f"{env_name}_{suffix}"
    return default


def _clean_env(value: Any) -> str:
    if not _present(value):
        return ""
    text = str(value).strip()
    if text.lower() in _PLACEHOLDER_ENV:
        return ""
    return text


def _bom_name() -> str:
    global _bom_name_cached
    if _bom_name_cached is not None:
        return _bom_name_cached
    _bom_name_cached = _discover_bom_name()
    return _bom_name_cached


def _discover_bom_name() -> str:
    seen: set[Path] = set()
    for root in _search_roots():
        try:
            resolved = root.resolve()
        except OSError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        name = _name_at(resolved)
        if name:
            return name
    return ""


def _search_roots():
    workspace = os.getenv("RENGLO_WORKSPACE_ROOT", "").strip()
    if workspace:
        yield Path(workspace).expanduser()
    cwd = Path.cwd()
    yield cwd
    yield from cwd.parents
    yield from Path(__file__).resolve().parents


def _name_at(root: Path) -> str:
    state = root / "renglo-dev" / ".renglo-dev.yaml"
    if state.is_file():
        spec = _root_scalar(_read(state), "bom")
        yaml_path = _bom_yaml(root, spec) if spec else None
        if yaml_path is not None and yaml_path.is_file():
            return _root_scalar(_read(yaml_path), "name")
    direct = root / "renglo.yaml"
    if direct.is_file():
        return _root_scalar(_read(direct), "name")
    return ""


def _bom_yaml(workspace: Path, spec: str) -> Optional[Path]:
    text = spec.strip()
    candidate = Path(text).expanduser()
    found = _yaml_file(candidate)
    if found is not None:
        return found
    if not candidate.is_absolute():
        found = _yaml_file((workspace / candidate).resolve())
        if found is not None:
            return found
    if _OWNER_REPO.fullmatch(text):
        repo = text.split("/", 1)[1]
        return workspace / "ops" / repo / "renglo.yaml"
    return None


def _yaml_file(path: Path) -> Optional[Path]:
    if path.is_file() and path.name == "renglo.yaml":
        return path
    nested = path / "renglo.yaml"
    if path.is_dir() and nested.is_file():
        return nested
    return None


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _root_scalar(text: str, key: str) -> str:
    pattern = re.compile(
        rf"^{re.escape(key)}:\s*(?:(?P<q>['\"])(?P<quoted>.*?)(?P=q)|(?P<bare>[^#]*?))\s*(?:#.*)?$"
    )
    for line in text.splitlines():
        if not line.strip() or line[0] in " \t#":
            continue
        match = pattern.match(line)
        if match:
            return (match.group("quoted") or match.group("bare") or "").strip()
    return ""
