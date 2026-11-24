"""How a dictionary draft becomes the next semantic version.

Adding a term or an external mapping is a minor change. Removing one, or
changing a term's inverse or status, is a major change. Labels and
descriptions are a patch.
"""

from __future__ import annotations


def classify_dictionary_change(previous, merged):
    """Major removes or rewrites terms. Minor adds them. Patch does neither."""
    kinds = [
        _keyed(_structure(previous), _structure(merged)),
        _set(_mappings(previous), _mappings(merged)),
    ]
    if "major" in kinds:
        return "major"
    if "minor" in kinds:
        return "minor"
    return "patch"


def _structure(document):
    found = {}
    for key, term in _terms(document):
        found[key] = (
            str(term.get("inverse") or ""),
            str(term.get("status") or ""),
        )
    return found


def _mappings(document):
    found = set()
    for key, term in _terms(document):
        mappings = term.get("external_mappings") or []
        if not isinstance(mappings, list):
            continue
        for item in mappings:
            found.add((key, str(item)))
    return found


def _terms(document):
    terms = (document or {}).get("terms") or {}
    if not isinstance(terms, dict):
        return []
    return [
        (str(key), term)
        for key, term in terms.items()
        if isinstance(term, dict)
    ]


def _keyed(previous, nxt):
    if any(key not in nxt or nxt[key] != previous[key] for key in previous):
        return "major"
    if any(key not in previous for key in nxt):
        return "minor"
    return None


def _set(previous, nxt):
    if previous - nxt:
        return "major"
    if nxt - previous:
        return "minor"
    return None
