"""In-process embed_handler lookup: prepare text only, never write vectors."""

from __future__ import annotations

import importlib
import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

log = logging.getLogger(__name__)

EmbedHandlerInvoker = Callable[[str, Dict[str, Any]], Dict[str, Any]]


def parse_embed_handler_ref(raw: Any) -> Optional[Dict[str, str]]:
    """
    Parse blueprint embed_handler as a SCHD path.

    Supported:
      - extension/handler
      - extension/handler/subhandler
      - extension/handler>subhandler  (legacy alias for the third segment)
    """
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    subhandler = "prepare_embed"
    if ">" in text:
        text, subhandler = [part.strip() for part in text.split(">", 1)]
        subhandler = subhandler or "prepare_embed"
    if not text:
        return None
    parts = [part.strip() for part in text.split("/") if part.strip()]
    if len(parts) >= 3:
        extension, handler, extra = parts[0], parts[1], parts[2]
        if extra:
            subhandler = extra
    elif len(parts) == 2:
        extension, handler = parts
    elif len(parts) == 1:
        extension, handler = "", parts[0]
    else:
        return None
    if not handler:
        return None
    return {"extension": extension, "handler": handler, "subhandler": subhandler}


def _class_name_from_handler(handler: str) -> str:
    return "".join(part[:1].upper() + part[1:] for part in handler.split("_") if part)


def _handlers_config_path(extension: str) -> Optional[Path]:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "extensions" / extension / "package" / "handlers_config.json"
        if candidate.is_file():
            return candidate
    return None


def _dotted_from_handlers_config(extension: str, handler: str) -> Optional[str]:
    path = _handlers_config_path(extension)
    if path is None:
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    mapping = data.get("handlers") if isinstance(data, dict) else None
    if not isinstance(mapping, dict):
        return None
    dotted = mapping.get(handler)
    return str(dotted).strip() if dotted else None


def _load_handler_instance(extension: str, handler: str) -> Any:
    dotted = _dotted_from_handlers_config(extension, handler) if extension else None
    if dotted and "." in dotted:
        module_path, class_name = dotted.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)()
    if not extension:
        raise ValueError("embed_handler must be extension/handler or extension/handler/subhandler")
    module = importlib.import_module(f"{extension}.handlers.{handler}")
    class_name = _class_name_from_handler(handler)
    return getattr(module, class_name)()


def invoke_embed_handler(ref: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    parsed = parse_embed_handler_ref(ref)
    if not parsed:
        return {"success": False, "error": "invalid embed_handler"}
    instance = _load_handler_instance(parsed["extension"], parsed["handler"])
    fn = getattr(instance, "run", None)
    if not callable(fn):
        return {
            "success": False,
            "error": f"handler {parsed['handler']} has no run",
        }
    body = dict(payload) if isinstance(payload, dict) else {}
    if not str(body.get("subhandler") or "").strip():
        body["subhandler"] = parsed["subhandler"]
    result = fn(body)
    if not isinstance(result, dict):
        return {"success": False, "error": "embed handler returned a non-object"}
    return result


def extract_embed_payload(result: Any) -> Tuple[str, Dict[str, Any], Optional[str], bool]:
    """
    Normalize a handler result to (text, attrs, error, skip).

    Accepts prepare_embed shape or a wrapped {output:[...]} / {output:{...}} handler result.
    """
    if not isinstance(result, dict):
        return "", {}, "embed handler returned a non-object", False
    if result.get("skip") is True:
        return "", {}, None, True
    if result.get("success") is False:
        return "", {}, str(result.get("error") or result.get("message") or "embed handler failed"), False

    inner: Any = result
    output = result.get("output")
    if isinstance(output, list) and output and isinstance(output[0], dict):
        inner = output[0]
    elif isinstance(output, dict):
        nested = output.get("output")
        if isinstance(nested, list) and nested and isinstance(nested[0], dict):
            inner = nested[0]
        elif isinstance(nested, dict):
            inner = nested
        else:
            inner = output

    if isinstance(inner, dict) and inner.get("skip") is True:
        return "", {}, None, True
    if isinstance(inner, dict) and inner.get("success") is False:
        return "", {}, str(inner.get("error") or inner.get("message") or "embed handler failed"), False

    text = ""
    attrs: Dict[str, Any] = {}
    if isinstance(inner, dict):
        raw_text = inner.get("text")
        if raw_text is None:
            raw_text = inner.get("fingerprint")
        text = str(raw_text or "").strip()
        raw_attrs = inner.get("attrs")
        if isinstance(raw_attrs, dict):
            attrs = dict(raw_attrs)
    if not text:
        return "", attrs, None, True
    return text, attrs, None, False
