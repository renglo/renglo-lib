"""Shape of an ontology document.

An ontology names types, groups the instantiable ones into functional
domains, and declares the relationships that may connect them. The fields
come from the ontology document shape. Nothing in here is specific to cloud
infrastructure: the same document can describe food groups, a business
process, or a social structure.
"""

from __future__ import annotations

import re


_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.]*$")
_TOKEN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_SEMANTIC = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*)*$")

_TOP_LEVEL = {
    "name",
    "description",
    "dictionaries",
    "classification_policy",
    "relationship_classes",
    "node_types",
    "functional_domains",
    "relationship_types",
    "canonical_alignment",
    "rules",
    "edge_taxonomies",
    "version",
    "status",
    "origin",
    "handle",
    "irn",
    "_id",
    "added",
    "modified",
}

_STATUS = {"draft", "final", "deprecated"}
_POLICY = {"description", "derivation"}
_CLASS = {"id", "description"}
_NODE = {"type", "semantic", "parent_type", "abstract", "description", "examples"}
_DOMAIN = {"id", "label", "semantic", "root_type", "types"}
_RELATIONSHIP = {
    "label",
    "semantic",
    "reverse_label",
    "reverse_semantic",
    "description",
    "edge_taxonomy",
    "relationship_class",
}
_ALIGNMENT = {"label", "canonical_label", "notes"}
_RULE = {"source_type", "relationship", "target_type"}
_TAXONOMY = {"id", "description", "default_weight"}
_ENVELOPE = {"version", "status", "origin", "handle", "irn", "_id", "added", "modified"}


def ontology_skeleton(name):
    """The document every ontology starts from. Lists are empty and the version is a draft."""
    return {
        "name": name,
        "description": "",
        "dictionaries": [],
        "classification_policy": {
            "description": "",
            "derivation": "",
        },
        "relationship_classes": [],
        "node_types": [],
        "functional_domains": [],
        "relationship_types": [],
        "canonical_alignment": [],
        "rules": [],
        "edge_taxonomies": [],
        "version": "x.y.z",
        "status": "draft",
        "origin": "",
    }


def ontology_errors(document):
    """Return a message when the document is not an ontology, else None."""
    errors = []
    if not isinstance(document, dict):
        return "Ontology must be an object"
    _document(document, errors)
    if len(errors) > 12:
        extra = len(errors) - 12
        errors = errors[:12]
        errors.append(f"and {extra} more")
    if not errors:
        return None
    return "; ".join(errors)


def _document(document, errors):
    for key in document:
        if key not in _TOP_LEVEL:
            errors.append(f'unknown property "{key}"')
    name = document.get("name")
    if not isinstance(name, str) or not _NAME.match(name):
        errors.append("name must be a letter followed by letters, digits, underscores, or dots")
    if "description" in document and not isinstance(document.get("description"), str):
        errors.append("description must be a string")
    elif "description" not in document:
        errors.append("description is required")
    for key in _ENVELOPE:
        if key in document and key != "status" and not isinstance(document[key], str):
            errors.append(f"{key} must be a string")
    if "status" in document and document["status"] not in _STATUS:
        errors.append('status must be "draft", "final", or "deprecated"')
    _string_list("dictionaries", document.get("dictionaries"), errors, required=True)
    policy = _policy(document.get("classification_policy"), errors)
    classes = _classes(document.get("relationship_classes"), errors)
    nodes = _nodes(document.get("node_types"), errors)
    domains = _domains(document.get("functional_domains"), nodes, errors)
    taxonomies = _taxonomies(document.get("edge_taxonomies"), errors)
    relationships = _relationships(document.get("relationship_types"), classes, taxonomies, errors)
    _alignments(document.get("canonical_alignment"), relationships, errors)
    _rules(document.get("rules"), nodes, relationships, errors)
    if policy is None or classes is None or nodes is None or domains is None:
        return
    if relationships is None or taxonomies is None:
        return


def _policy(policy, errors):
    if not isinstance(policy, dict):
        errors.append("classification_policy must be an object")
        return None
    for key in policy:
        if key not in _POLICY:
            errors.append(f'classification_policy has unknown property "{key}"')
    for key in _POLICY:
        if key not in policy:
            errors.append(f"classification_policy.{key} is required")
        elif not isinstance(policy[key], str):
            errors.append(f"classification_policy.{key} must be a string")
    return policy


def _classes(classes, errors):
    if not isinstance(classes, list):
        errors.append("relationship_classes must be a list")
        return None
    ids = []
    for index, item in enumerate(classes):
        where = f"relationship_classes[{index}]"
        if not isinstance(item, dict):
            errors.append(f"{where} must be an object")
            ids.append("")
            continue
        for key in item:
            if key not in _CLASS:
                errors.append(f'{where} has unknown property "{key}"')
        ids.append(_token(where, "id", item.get("id"), errors))
        _required_text(where, "description", item.get("description"), errors)
    _duplicates("relationship_classes", "id", ids, errors)
    return {item for item in ids if item}


def _nodes(nodes, errors):
    if not isinstance(nodes, list):
        errors.append("node_types must be a list")
        return None
    parsed = []
    names = []
    semantics = []
    for index, item in enumerate(nodes):
        where = f"node_types[{index}]"
        if not isinstance(item, dict):
            errors.append(f"{where} must be an object")
            parsed.append(None)
            names.append("")
            semantics.append("")
            continue
        for key in item:
            if key not in _NODE:
                errors.append(f'{where} has unknown property "{key}"')
        type_name = _token(where, "type", item.get("type"), errors)
        semantic = _semantic(where, item.get("semantic"), errors)
        _required_text(where, "description", item.get("description"), errors)
        _examples(where, item.get("examples"), errors)
        parent = item.get("parent_type")
        if parent is not None and (not isinstance(parent, str) or not _TOKEN.match(parent)):
            errors.append(f"{where}.parent_type must name a node type")
            parent = None
        if "abstract" in item and not isinstance(item.get("abstract"), bool):
            errors.append(f"{where}.abstract must be true or false")
        names.append(type_name)
        semantics.append(semantic)
        parsed.append({
            "type": type_name,
            "semantic": semantic,
            "parent_type": parent,
            "abstract": item.get("abstract") is True,
        })
    _duplicates("node_types", "type", names, errors)
    _duplicates("node_types", "semantic", semantics, errors)
    known = {item["type"] for item in parsed if item and item["type"]}
    roots = []
    for index, item in enumerate(parsed):
        if not item or not item["type"]:
            continue
        parent = item["parent_type"]
        if parent is None:
            roots.append(item["type"])
            continue
        if parent not in known:
            errors.append(f'node_types[{index}].parent_type "{parent}" is not a node type')
        elif parent == item["type"]:
            errors.append(f'node_types[{index}].parent_type cannot be "{parent}"')
    if parsed and len(roots) != 1:
        errors.append("node_types needs one type with no parent_type")
    _cycles(parsed, errors)
    return [item for item in parsed if item and item["type"]]


def _cycles(nodes, errors):
    by_name = {item["type"]: item for item in nodes if item and item["type"]}
    reported = set()
    for name in by_name:
        seen = []
        current = name
        while current:
            if current in seen:
                start = seen.index(current)
                cycle = seen[start:]
                key = tuple(sorted(cycle))
                if key not in reported:
                    reported.add(key)
                    errors.append("node_types parent_type cycles through " + ", ".join(cycle))
                break
            seen.append(current)
            parent = (by_name.get(current) or {}).get("parent_type")
            if not parent or parent not in by_name:
                break
            current = parent


def _domains(domains, nodes, errors):
    if not isinstance(domains, list):
        errors.append("functional_domains must be a list")
        return None
    if nodes is None:
        return None
    known = {item["type"]: item for item in nodes}
    instantiable = {name for name, item in known.items() if not item["abstract"]}
    ids = []
    semantics = []
    assigned = []
    for index, item in enumerate(domains):
        where = f"functional_domains[{index}]"
        if not isinstance(item, dict):
            errors.append(f"{where} must be an object")
            ids.append("")
            semantics.append("")
            continue
        for key in item:
            if key not in _DOMAIN:
                errors.append(f'{where} has unknown property "{key}"')
        domain_id = _token(where, "id", item.get("id"), errors)
        _required_text(where, "label", item.get("label"), errors)
        semantic = _semantic(where, item.get("semantic"), errors)
        root = item.get("root_type")
        if not isinstance(root, str) or not _TOKEN.match(root) or root not in known:
            errors.append(f"{where}.root_type must name a node type")
        types = item.get("types")
        if not isinstance(types, list):
            errors.append(f"{where}.types must be a list")
            types = []
        seen = set()
        for type_name in types:
            if not isinstance(type_name, str) or not _TOKEN.match(type_name):
                errors.append(f"{where}.types entries must name node types")
                continue
            if type_name in seen:
                errors.append(f'{where}.types lists "{type_name}" more than once')
            seen.add(type_name)
            if type_name not in known:
                errors.append(f'{where}.types "{type_name}" is not a node type')
            elif known[type_name]["abstract"]:
                errors.append(f'{where}.types "{type_name}" is abstract')
            assigned.append(type_name)
        ids.append(domain_id)
        semantics.append(semantic)
    _duplicates("functional_domains", "id", ids, errors)
    _duplicates("functional_domains", "semantic", semantics, errors)
    counts = {}
    for type_name in assigned:
        counts[type_name] = counts.get(type_name, 0) + 1
    for type_name, count in counts.items():
        if count > 1:
            errors.append(f'node type "{type_name}" is listed in more than one functional domain')
    for type_name in sorted(instantiable):
        if type_name not in counts:
            errors.append(f'node type "{type_name}" is not listed in a functional domain')
    return ids


def _taxonomies(taxonomies, errors):
    if not isinstance(taxonomies, list):
        errors.append("edge_taxonomies must be a list")
        return None
    ids = []
    for index, item in enumerate(taxonomies):
        where = f"edge_taxonomies[{index}]"
        if not isinstance(item, dict):
            errors.append(f"{where} must be an object")
            ids.append("")
            continue
        for key in item:
            if key not in _TAXONOMY:
                errors.append(f'{where} has unknown property "{key}"')
        ids.append(_token(where, "id", item.get("id"), errors))
        _required_text(where, "description", item.get("description"), errors)
        weight = item.get("default_weight")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or weight < 0:
            errors.append(f"{where}.default_weight must be a number zero or greater")
    _duplicates("edge_taxonomies", "id", ids, errors)
    return {item for item in ids if item}


def _relationships(relationships, classes, taxonomies, errors):
    if not isinstance(relationships, list):
        errors.append("relationship_types must be a list")
        return None
    parsed = []
    labels = []
    semantics = []
    for index, item in enumerate(relationships):
        where = f"relationship_types[{index}]"
        if not isinstance(item, dict):
            errors.append(f"{where} must be an object")
            parsed.append(None)
            labels.append("")
            semantics.append("")
            continue
        for key in item:
            if key not in _RELATIONSHIP:
                errors.append(f'{where} has unknown property "{key}"')
        label = _token(where, "label", item.get("label"), errors)
        semantic = _semantic(where, item.get("semantic"), errors)
        reverse_label = _token(where, "reverse_label", item.get("reverse_label"), errors)
        reverse_semantic = _semantic_field(where, "reverse_semantic", item.get("reverse_semantic"), errors)
        _required_text(where, "description", item.get("description"), errors)
        edge = item.get("edge_taxonomy")
        if not isinstance(edge, str) or not _TOKEN.match(edge):
            errors.append(f"{where}.edge_taxonomy must name an edge taxonomy")
            edge = ""
        elif taxonomies is not None and edge not in taxonomies:
            errors.append(f'{where}.edge_taxonomy "{edge}" is not an edge taxonomy')
        kind = item.get("relationship_class")
        if not isinstance(kind, str) or not _TOKEN.match(kind):
            errors.append(f"{where}.relationship_class must name a relationship class")
            kind = ""
        elif classes is not None and kind not in classes:
            errors.append(f'{where}.relationship_class "{kind}" is not a relationship class')
        labels.append(label)
        semantics.append(semantic)
        parsed.append({
            "label": label,
            "semantic": semantic,
            "reverse_label": reverse_label,
            "reverse_semantic": reverse_semantic,
        })
    _duplicates("relationship_types", "label", labels, errors)
    _duplicates("relationship_types", "semantic", semantics, errors)
    by_label = {item["label"]: item for item in parsed if item and item["label"]}
    reported = set()
    for item in parsed:
        if not item or not item["label"] or not item["reverse_label"]:
            continue
        other = by_label.get(item["reverse_label"])
        if other is None:
            continue
        pair = tuple(sorted((item["label"], item["reverse_label"])))
        if pair in reported:
            continue
        if other["reverse_label"] != item["label"] or other["semantic"] != item["reverse_semantic"] or other["reverse_semantic"] != item["semantic"]:
            reported.add(pair)
            errors.append(
                f'relationship_types "{item["label"]}" and "{item["reverse_label"]}" do not reverse each other'
            )
    return by_label


def _alignments(alignments, relationships, errors):
    if not isinstance(alignments, list):
        errors.append("canonical_alignment must be a list")
        return
    labels = []
    for index, item in enumerate(alignments):
        where = f"canonical_alignment[{index}]"
        if not isinstance(item, dict):
            errors.append(f"{where} must be an object")
            labels.append("")
            continue
        for key in item:
            if key not in _ALIGNMENT:
                errors.append(f'{where} has unknown property "{key}"')
        label = _token(where, "label", item.get("label"), errors)
        canonical = _token(where, "canonical_label", item.get("canonical_label"), errors)
        if "notes" in item and not isinstance(item.get("notes"), str):
            errors.append(f"{where}.notes must be a string")
        if relationships is not None and label and label not in relationships:
            errors.append(f'{where}.label "{label}" is not a relationship type')
        if relationships is not None and canonical and canonical not in relationships:
            errors.append(f'{where}.canonical_label "{canonical}" is not a relationship type')
        labels.append(label)
    _duplicates("canonical_alignment", "label", labels, errors)


def _rules(rules, nodes, relationships, errors):
    if not isinstance(rules, list):
        errors.append("rules must be a list")
        return
    known = {item["type"] for item in nodes} if nodes is not None else None
    seen = set()
    for index, item in enumerate(rules):
        where = f"rules[{index}]"
        if not isinstance(item, dict):
            errors.append(f"{where} must be an object")
            continue
        for key in item:
            if key not in _RULE:
                errors.append(f'{where} has unknown property "{key}"')
        source = _token(where, "source_type", item.get("source_type"), errors)
        relationship = _token(where, "relationship", item.get("relationship"), errors)
        target = _token(where, "target_type", item.get("target_type"), errors)
        if known is not None and source and source not in known:
            errors.append(f'{where}.source_type "{source}" is not a node type')
        if known is not None and target and target not in known:
            errors.append(f'{where}.target_type "{target}" is not a node type')
        if relationships is not None and relationship and relationship not in relationships:
            errors.append(f'{where}.relationship "{relationship}" is not a relationship type')
        key = (source, relationship, target)
        if all(key) and key in seen:
            errors.append(f"{where} repeats {source} {relationship} {target}")
        if all(key):
            seen.add(key)


def _token(where, key, value, errors):
    if not isinstance(value, str) or not _TOKEN.match(value):
        errors.append(f"{where}.{key} must be a letter followed by letters, digits, or underscores")
        return ""
    return value


def _semantic(where, value, errors):
    return _semantic_field(where, "semantic", value, errors)


def _semantic_field(where, key, value, errors):
    if not isinstance(value, str) or not _SEMANTIC.match(value):
        errors.append(f"{where}.{key} must be a dotted name")
        return ""
    return value


def _required_text(where, key, value, errors):
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{where}.{key} is required")


def _examples(where, examples, errors):
    if not isinstance(examples, list):
        errors.append(f"{where}.examples must be a list")
        return
    for item in examples:
        if not isinstance(item, str) or not item.strip():
            errors.append(f"{where}.examples must be a list of strings")
            return


def _string_list(key, value, errors, required=False):
    if value is None and not required:
        return
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        errors.append(f"{key} must be a list of strings")


def _duplicates(section, key, values, errors):
    seen = set()
    for index, value in enumerate(values):
        if not value:
            continue
        if value in seen:
            errors.append(f'{section}[{index}].{key} "{value}" is already used')
        seen.add(value)
