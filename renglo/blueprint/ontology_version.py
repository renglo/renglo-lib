"""How an ontology draft becomes the next semantic version.

Adding a type, domain, relationship, rule, or taxonomy is a minor change.
Removing one, or changing a type's parent, semantic, relationship class, or
domain membership, is a major change. Description, examples, and labels are
a patch.
"""

from __future__ import annotations


def classify_ontology_change(previous, merged):
    """Major removes or rewrites structure. Minor adds it. Patch does neither."""
    kinds = [
        _keyed(_nodes(previous), _nodes(merged)),
        _keyed(_domains(previous), _domains(merged)),
        _keyed(_membership(previous), _membership(merged)),
        _keyed(_relationships(previous), _relationships(merged)),
        _keyed(_taxonomies(previous), _taxonomies(merged)),
        _set(_ids(previous, "relationship_classes"), _ids(merged, "relationship_classes")),
        _set(_rules(previous), _rules(merged)),
        _set(_alignments(previous), _alignments(merged)),
        _set(_dictionaries(previous), _dictionaries(merged)),
    ]
    if "major" in kinds:
        return "major"
    if "minor" in kinds:
        return "minor"
    return "patch"


def _nodes(document):
    found = {}
    for item in _list(document, "node_types"):
        name = item.get("type")
        if not name:
            continue
        found[str(name)] = (
            str(item.get("parent_type") or ""),
            item.get("abstract") is True,
            str(item.get("semantic") or ""),
        )
    return found


def _domains(document):
    found = {}
    for item in _list(document, "functional_domains"):
        domain_id = item.get("id")
        if not domain_id:
            continue
        found[str(domain_id)] = (
            str(item.get("semantic") or ""),
            str(item.get("root_type") or ""),
        )
    return found


def _membership(document):
    found = {}
    for item in _list(document, "functional_domains"):
        domain_id = str(item.get("id") or "")
        for type_name in item.get("types") or []:
            found[str(type_name)] = domain_id
    return found


def _relationships(document):
    found = {}
    for item in _list(document, "relationship_types"):
        label = item.get("label")
        if not label:
            continue
        found[str(label)] = (
            str(item.get("semantic") or ""),
            str(item.get("reverse_label") or ""),
            str(item.get("reverse_semantic") or ""),
            str(item.get("relationship_class") or ""),
            str(item.get("edge_taxonomy") or ""),
        )
    return found


def _taxonomies(document):
    found = {}
    for item in _list(document, "edge_taxonomies"):
        taxonomy_id = item.get("id")
        if not taxonomy_id:
            continue
        found[str(taxonomy_id)] = item.get("default_weight")
    return found


def _ids(document, key):
    found = set()
    for item in _list(document, key):
        item_id = item.get("id")
        if item_id:
            found.add(str(item_id))
    return found


def _rules(document):
    found = set()
    for item in _list(document, "rules"):
        found.add((
            str(item.get("source_type") or ""),
            str(item.get("relationship") or ""),
            str(item.get("target_type") or ""),
        ))
    return found


def _alignments(document):
    found = set()
    for item in _list(document, "canonical_alignment"):
        found.add((str(item.get("label") or ""), str(item.get("canonical_label") or "")))
    return found


def _dictionaries(document):
    value = (document or {}).get("dictionaries") or []
    if not isinstance(value, list):
        return set()
    return {str(item) for item in value}


def _list(document, key):
    value = (document or {}).get(key) or []
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


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
