"""Immutable blueprint versions.

POST and PUT ignore a version in the body. They compare the saved blueprint
with the merged one and write the next version. The previous version stays.
"""

from __future__ import annotations

import copy


def parse_version(version):
    parts = str(version or "").split(".")
    if len(parts) != 3:
        return None
    try:
        return tuple(int(part) for part in parts)
    except ValueError:
        return None


def structural_fields(blueprint):
    fields = (blueprint or {}).get("fields") or []
    if not isinstance(fields, list):
        return []
    signature = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        signature.append((str(field.get("name") or ""), str(field.get("type") or "")))
    return signature


def _as_names(value):
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def structural_indexes(blueprint):
    indexes = (blueprint or {}).get("indexes") or {}
    if not isinstance(indexes, dict):
        return {"path": [], "time": [], "geo": []}
    return {
        "path": _as_names(indexes.get("path")),
        "time": _as_names(indexes.get("time")),
        "geo": _as_names(indexes.get("geo")),
    }


def _list_change(previous, nxt):
    """Return major, minor, or None for one ordered list of names or (name, type)."""
    if previous == nxt:
        return None
    named = bool(previous) and isinstance(previous[0], tuple)
    if named or (nxt and isinstance(nxt[0], tuple)):
        prev_map = {item[0]: item[1] for item in previous}
        next_map = {item[0]: item[1] for item in nxt}
        prev_names = [item[0] for item in previous]
        next_names = [item[0] for item in nxt]
        for name, field_type in prev_map.items():
            if name in next_map and next_map[name] != field_type:
                return "major"
    else:
        prev_map = {item: item for item in previous}
        next_map = {item: item for item in nxt}
        prev_names = list(previous)
        next_names = list(nxt)
    if any(name not in next_map for name in prev_map):
        return "major"
    surviving_prev = [name for name in prev_names if name in next_map]
    surviving_next = [name for name in next_names if name in prev_map]
    if surviving_prev != surviving_next:
        return "major"
    if len(next_map) > len(prev_map):
        return "minor"
    return "major"


def classify_change(previous, merged):
    """Major removes or rewrites structure. Minor adds it. Patch does neither."""
    changes = [
        _list_change(structural_fields(previous), structural_fields(merged)),
        _list_change(
            structural_indexes(previous)["path"],
            structural_indexes(merged)["path"],
        ),
        _list_change(
            structural_indexes(previous)["time"],
            structural_indexes(merged)["time"],
        ),
        _list_change(
            structural_indexes(previous)["geo"],
            structural_indexes(merged)["geo"],
        ),
        _set_change(_dictionary_refs(previous), _dictionary_refs(merged)),
    ]
    if "major" in changes:
        return "major"
    if "minor" in changes:
        return "minor"
    return "patch"


def _dictionary_refs(blueprint):
    value = (blueprint or {}).get("dictionaries") or []
    if not isinstance(value, list):
        return set()
    return {str(item) for item in value if str(item).strip()}


def _set_change(previous, nxt):
    """Adding a dictionary reference is minor. Removing one is major."""
    if previous - nxt:
        return "major"
    if nxt - previous:
        return "minor"
    return None


def next_version(current, kind):
    parsed = parse_version(current) or (1, 0, 0)
    major, minor, patch = parsed
    if kind == "major":
        return f"{major + 1}.0.0"
    if kind == "minor":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


_KEEP = ("version", "_id", "irn", "added", "handle", "name")


def merge_blueprint(previous, incoming):
    """Overlay the caller's fields onto the saved blueprint and keep its identity."""
    merged = copy.deepcopy(previous or {})
    for key, value in (incoming or {}).items():
        if key in _KEEP:
            continue
        merged[key] = copy.deepcopy(value)
    for key in ("handle", "name", "_id", "added", "irn"):
        if previous and key in previous:
            merged[key] = previous[key]
    return merged
