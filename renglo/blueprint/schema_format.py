"""Shape of a blueprint document.

Extension blueprints share one document. A schema that leaves that shape is
refused, so document writes, indexes, and graph edges are not handed a
corrupt definition.
"""

from __future__ import annotations

import re


_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

_TOP_LEVEL = {
    "name",
    "label",
    "description",
    "fields",
    "indexes",
    "public",
    "singleton",
    "license",
    "dictionaries",
    "status",
    "version",
    "origin",
    "blueprint_origin",
    "uri",
    "is_legacy",
    "handle",
    "irn",
    "_id",
    "added",
    "modified",
}

_FIELD = {
    "name",
    "label",
    "type",
    "widget",
    "cardinality",
    "layer",
    "required",
    "default",
    "hint",
    "multilingual",
    "order",
    "semantic",
    "source",
    "search",
    "options",
}

_TYPES = ("string", "number", "boolean", "object", "json", "file")

_WIDGETS = {
    "string": {"text", "textarea", "select", "list", "document"},
    "number": {"number"},
    "boolean": {"checkbox"},
    "object": {"textarea", "json", "list"},
    "json": {"textarea"},
    "file": {"document"},
}

_CARDINALITY = {"single", "multiple"}
_LAYERS = {"0", "1", "2"}
_STATUS = {"draft", "final", "deprecated"}
_SOURCE_KEYS = {"target", "target_key", "preview", "label", "projection"}
_INDEX_KEYS = ("path", "time", "geo")
_STRING_KEYS = {
    "description",
    "license",
    "version",
    "origin",
    "blueprint_origin",
    "uri",
    "handle",
    "irn",
    "_id",
    "added",
    "modified",
}


def schema_errors(document):
    """Return a message when the document is not a blueprint, else None."""
    errors = []
    if not isinstance(document, dict):
        return "Schema must be an object"
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
        errors.append("name must be a letter followed by letters, digits, or underscores")
    label = document.get("label")
    if not isinstance(label, str) or not label.strip():
        errors.append("label is required")
    for key in _STRING_KEYS:
        if key in document and not isinstance(document[key], str):
            errors.append(f"{key} must be a string")
    if "status" in document and document["status"] not in _STATUS:
        errors.append('status must be "draft", "final", or "deprecated"')
    _dictionaries(document.get("dictionaries"), errors)
    for key in ("public", "singleton", "is_legacy"):
        if key in document and not isinstance(document[key], bool):
            errors.append(f"{key} must be true or false")
    fields = document.get("fields")
    if not isinstance(fields, list):
        errors.append("fields must be a list")
        fields = []
    names = []
    for index, field in enumerate(fields):
        names.append(_field(index, field, errors))
    _duplicate_names(names, errors)
    _indexes(document.get("indexes"), names, fields, errors)


def _field(index, field, errors):
    where = f"fields[{index}]"
    if not isinstance(field, dict):
        errors.append(f"{where} must be an object")
        return ""
    for key in field:
        if key not in _FIELD:
            errors.append(f'{where} has unknown property "{key}"')
    name = field.get("name")
    if not isinstance(name, str) or not _NAME.match(name):
        errors.append(f"{where}.name must be a letter followed by letters, digits, or underscores")
        name = ""
    label = field.get("label")
    if not isinstance(label, str) or not label.strip():
        errors.append(f"{where}.label is required")
    field_type = field.get("type")
    if field_type not in _TYPES:
        listed = ", ".join(_TYPES)
        errors.append(f"{where}.type must be one of {listed}")
        field_type = None
    widget = field.get("widget")
    if field_type and widget not in _WIDGETS[field_type]:
        allowed = ", ".join(sorted(_WIDGETS[field_type]))
        errors.append(f'{where}.widget must be one of {allowed} for type "{field_type}"')
    cardinality = field.get("cardinality")
    if cardinality not in _CARDINALITY:
        errors.append(f'{where}.cardinality must be "single" or "multiple"')
    layer = field.get("layer")
    if layer not in _LAYERS:
        errors.append(f'{where}.layer must be "0", "1", or "2"')
    if not isinstance(field.get("required"), bool):
        errors.append(f"{where}.required must be true or false")
    if cardinality in _CARDINALITY and field_type:
        _default(where, field_type, cardinality, field.get("default"), errors)
    _optional_field(where, field, errors)
    return name


_DEFAULT = {
    "string": ("a string", "a list of strings"),
    "number": ("a number", "a list of numbers"),
    "boolean": ("true or false", "a list of true or false values"),
    "object": ("an object", "a list of objects"),
    "json": ("a string", "a list of strings"),
    "file": ("a string", "a list of strings"),
}


def _default(where, field_type, cardinality, default, errors):
    single, multiple = _DEFAULT[field_type]
    if cardinality == "multiple":
        if not isinstance(default, list) or not all(_default_item(field_type, item) for item in default):
            errors.append(f"{where}.default must be {multiple}")
        return
    if not _default_item(field_type, default, blank_number=True):
        errors.append(f"{where}.default must be {single}")


def _default_item(field_type, value, blank_number=False):
    if field_type in ("string", "json", "file"):
        return isinstance(value, str)
    if field_type == "number":
        return _number(value) or (blank_number and value == "")
    if field_type == "boolean":
        return isinstance(value, bool)
    if field_type == "object":
        return isinstance(value, dict)
    return False


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _optional_field(where, field, errors):
    if "hint" in field and not isinstance(field["hint"], str):
        errors.append(f"{where}.hint must be a string")
    if "semantic" in field and not isinstance(field["semantic"], str):
        errors.append(f"{where}.semantic must be a string")
    if "multilingual" in field and not isinstance(field["multilingual"], bool):
        errors.append(f"{where}.multilingual must be true or false")
    if "order" in field and not _order(field["order"]):
        errors.append(f"{where}.order must be a non-negative integer")
    if "search" in field and (isinstance(field["search"], bool) or field["search"] not in (1, 2, 3)):
        errors.append(f"{where}.search must be 1, 2, or 3")
    _source(where, field.get("source"), errors)
    _options(where, field, errors)


def _order(value):
    if isinstance(value, str) and value.isdigit():
        return True
    return _number(value) and value >= 0 and float(value).is_integer()


def _source(where, source, errors):
    if source is None or source == "":
        return
    if not isinstance(source, dict):
        errors.append(f"{where}.source must be an object or an empty string")
        return
    for key in source:
        if key not in _SOURCE_KEYS:
            errors.append(f'{where}.source has unknown property "{key}"')
    for key in _SOURCE_KEYS:
        if key not in source:
            errors.append(f"{where}.source.{key} is required")
    target = source.get("target")
    if "target" in source and (not isinstance(target, str) or not _NAME.match(target)):
        errors.append(f"{where}.source.target must name a schema")
    if "target_key" in source and source.get("target_key") != "_id":
        errors.append(f'{where}.source.target_key must be "_id"')
    if "preview" in source and not _string_list(source.get("preview")):
        errors.append(f"{where}.source.preview must be a list of strings")
    if "projection" in source and not _string_list(source.get("projection")):
        errors.append(f"{where}.source.projection must be a list of strings")
    label = source.get("label")
    if "label" in source and (
        not isinstance(label, list)
        or len(label) != 2
        or not all(isinstance(item, str) and item.strip() for item in label)
    ):
        errors.append(f"{where}.source.label must be two strings")


def _options(where, field, errors):
    options = field.get("options")
    source = field.get("source")
    has_source = isinstance(source, dict)
    if options is None:
        if field.get("widget") == "select" and not has_source:
            errors.append(f"{where} is a select and needs options")
        return
    if field.get("widget") != "select" or has_source:
        errors.append(f"{where}.options is only valid on a select without a source")
        return
    if (
        not isinstance(options, dict)
        or not options
        or not all(isinstance(key, str) and key and isinstance(value, str) for key, value in options.items())
    ):
        errors.append(f"{where}.options must map values to labels")


def _string_list(value):
    return isinstance(value, list) and bool(value) and all(isinstance(item, str) and item.strip() for item in value)


def _indexes(indexes, names, fields, errors):
    if indexes is None:
        return
    if not isinstance(indexes, dict) or not indexes:
        errors.append("indexes needs a path, time, or geo list")
        return
    known = set(names)
    sourced = set()
    for field in fields:
        if isinstance(field, dict) and isinstance(field.get("source"), dict):
            sourced.add(field.get("name"))
    for key, value in indexes.items():
        if key not in _INDEX_KEYS:
            errors.append(f'indexes has unknown property "{key}"')
            continue
        if not isinstance(value, list) or not value or not all(isinstance(item, str) and item.strip() for item in value):
            errors.append(f"indexes.{key} must be a list of field names")
            continue
        seen = set()
        for item in value:
            if item in seen:
                errors.append(f'indexes.{key} lists "{item}" more than once')
            seen.add(item)
            if item not in known:
                errors.append(f'indexes.{key} "{item}" is not a field')
            elif item in sourced:
                errors.append(f'indexes.{key} "{item}" is a source field')


def _dictionaries(value, errors):
    """Optional list of dictionary ids this schema cites. Absent on older blueprints."""
    if value is None:
        return
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        errors.append("dictionaries must be a list of strings")
        return
    seen = set()
    for item in value:
        if item in seen:
            errors.append(f'dictionaries lists "{item}" more than once')
        seen.add(item)


def _duplicate_names(names, errors):
    seen = set()
    for index, name in enumerate(names):
        if not name:
            continue
        if name in seen:
            errors.append(f'fields[{index}].name "{name}" is already used')
        seen.add(name)
