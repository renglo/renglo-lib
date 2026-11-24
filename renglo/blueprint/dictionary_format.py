"""Shape of a semantic dictionary.

A dictionary is the shared vocabulary an ontology or a schema cites. Each
term has a stable id, a label, and a description. A term that names an
inverse is a relationship, and the two sides cite each other.
``external_mappings`` lists the provider-native names that mean this term.
"""

from __future__ import annotations

import re


_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.]*$")
_TERM_KEY = re.compile(r"^[a-z][a-z0-9_]*$")

_TOP_LEVEL = {
    "id",
    "name",
    "description",
    "terms",
    "version",
    "status",
    "origin",
    "handle",
    "irn",
    "_id",
    "added",
    "modified",
}

_TERM = {"id", "label", "description", "inverse", "external_mappings", "status"}
_TERM_STATUS = {"active", "deprecated"}
_DOCUMENT_STATUS = {"draft", "final", "deprecated"}
_ENVELOPE = {"origin", "handle", "irn", "_id", "added", "modified"}


def dictionary_skeleton(name):
    """The document every dictionary starts from. Terms are empty and the version is a draft."""
    return {
        "id": f"{name}.x.y.z",
        "name": name,
        "description": "",
        "terms": {},
        "version": "x.y.z",
        "status": "draft",
        "origin": "",
    }


def dictionary_errors(document):
    """Return a message when the document is not a dictionary, else None."""
    errors = []
    if not isinstance(document, dict):
        return "Dictionary must be an object"
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
        name = ""
    if "description" in document and not isinstance(document.get("description"), str):
        errors.append("description must be a string")
    elif "description" not in document:
        errors.append("description is required")
    version = document.get("version")
    if not isinstance(version, str) or not version.strip():
        errors.append("version is required")
        version = ""
    expected_id = f"{name}.{version}" if name and version else ""
    document_id = document.get("id")
    if "id" not in document:
        errors.append("id is required")
    elif not isinstance(document_id, str):
        errors.append("id must be a string")
    elif expected_id and document_id != expected_id:
        errors.append(f'id must be "{expected_id}"')
    for key in _ENVELOPE:
        if key in document and not isinstance(document[key], str):
            errors.append(f"{key} must be a string")
    if "status" in document and document["status"] not in _DOCUMENT_STATUS:
        errors.append('status must be "draft", "final", or "deprecated"')
    _terms(name, document.get("terms"), errors)


def _terms(name, terms, errors):
    if not isinstance(terms, dict):
        errors.append("terms must be an object")
        return
    parsed = {}
    for key, term in terms.items():
        where = f"terms.{key}"
        if not isinstance(key, str) or not _TERM_KEY.match(key):
            errors.append(f"{where} must use a lowercase term name")
            continue
        if not isinstance(term, dict):
            errors.append(f"{where} must be an object")
            continue
        for field in term:
            if field not in _TERM:
                errors.append(f'{where} has unknown property "{field}"')
        term_id = term.get("id")
        expected = f"{name}.{key}" if name else ""
        if not isinstance(term_id, str) or not term_id.strip():
            errors.append(f"{where}.id is required")
            term_id = ""
        elif expected and term_id != expected:
            errors.append(f'{where}.id must be "{expected}"')
        _required_text(where, "label", term.get("label"), errors)
        _required_text(where, "description", term.get("description"), errors)
        status = term.get("status")
        if status not in _TERM_STATUS:
            errors.append(f'{where}.status must be "active" or "deprecated"')
        _mappings(where, term.get("external_mappings"), errors)
        inverse = term.get("inverse")
        if "inverse" in term and (not isinstance(inverse, str) or not inverse.strip()):
            errors.append(f"{where}.inverse must name a term id")
            inverse = ""
        parsed[key] = {"id": term_id, "inverse": inverse if isinstance(inverse, str) else ""}
    _inverses(parsed, errors)


def _inverses(terms, errors):
    by_id = {}
    for key, term in terms.items():
        term_id = term.get("id") or ""
        if not term_id:
            continue
        if term_id in by_id:
            errors.append(f'terms.{key}.id "{term_id}" is already used')
            continue
        by_id[term_id] = key
    reported = set()
    for key, term in terms.items():
        inverse = term.get("inverse") or ""
        if not inverse or not term.get("id"):
            continue
        other_key = by_id.get(inverse)
        if other_key is None:
            errors.append(f'terms.{key}.inverse "{inverse}" is not a term in this dictionary')
            continue
        pair = tuple(sorted((term["id"], inverse)))
        if pair in reported:
            continue
        reported.add(pair)
        other = terms[other_key]
        if other.get("inverse") != term["id"]:
            errors.append(f'terms "{key}" and "{other_key}" do not reverse each other')


def _mappings(where, mappings, errors):
    if not isinstance(mappings, list):
        errors.append(f"{where}.external_mappings must be a list of strings")
        return
    seen = set()
    for item in mappings:
        if not isinstance(item, str) or not item.strip():
            errors.append(f"{where}.external_mappings must be a list of strings")
            return
        if item in seen:
            errors.append(f'{where}.external_mappings lists "{item}" more than once')
        seen.add(item)


def _required_text(where, key, value, errors):
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{where}.{key} is required")
