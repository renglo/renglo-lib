"""Peer Lambda entry. The zip root shim imports ``lambda_handler`` from here.

``handlers_config.json`` sits next to that shim (the Lambda task root).
An event with ``detached: true`` runs the handler and then writes the session.
"""

from __future__ import annotations

import importlib
import json
import os
import threading
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from renglo.runtime import apply_handler_invocation_context
from renglo.schd.peer_detached import DETACHED_DEADLINE_SECONDS, complete_detached

_handler_cache: Dict[str, Any] = {}
_handler_modules_cache: Optional[Dict[str, str]] = None


def normalize_for_json(obj: Any) -> Any:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: normalize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [normalize_for_json(item) for item in obj]
    if hasattr(obj, "__dict__") and not isinstance(obj, type):
        return normalize_for_json(obj.__dict__)
    return obj


def _config_paths() -> list[Path]:
    paths: list[Path] = []
    override = (os.getenv("HANDLERS_CONFIG") or "").strip()
    if override:
        paths.append(Path(override))
    paths.append(Path.cwd() / "handlers_config.json")
    try:
        import lambda_router

        paths.append(Path(lambda_router.__file__).resolve().parent / "handlers_config.json")
    except Exception:
        pass
    paths.append(Path(__file__).resolve().parent / "handlers_config.json")
    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def load_handler_config() -> Dict[str, str]:
    global _handler_modules_cache
    if _handler_modules_cache is not None:
        return _handler_modules_cache
    config_data = None
    tried: list[str] = []
    for config_path in _config_paths():
        tried.append(str(config_path))
        if not config_path.is_file():
            continue
        try:
            config_data = json.loads(config_path.read_text(encoding="utf-8"))
            break
        except (json.JSONDecodeError, OSError):
            continue
    if not isinstance(config_data, dict) or "handlers" not in config_data:
        raise FileNotFoundError(
            "handlers_config.json was not found. Tried: " + ", ".join(tried)
        )
    handlers = config_data.get("handlers")
    if not isinstance(handlers, dict):
        raise KeyError("handlers_config.json must contain a handlers object")
    _handler_modules_cache = {str(k): str(v) for k, v in handlers.items()}
    return _handler_modules_cache


def get_handler(handler_name: str) -> Any:
    if handler_name in _handler_cache:
        return _handler_cache[handler_name]
    handler_modules = load_handler_config()
    if handler_name not in handler_modules:
        available = ", ".join(sorted(handler_modules)) or "none"
        raise ValueError(f"Handler '{handler_name}' not found. Available handlers: {available}")
    module_path = handler_modules[handler_name]
    module_name, class_name = module_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    handler_class = getattr(module, class_name)
    handler_instance = handler_class()
    _handler_cache[handler_name] = handler_instance
    return handler_instance


def _stamp_invocation_jwt(target: Any, claims: Any) -> None:
    if target is None:
        return
    if hasattr(target, "set_invocation_jwt_claims"):
        target.set_invocation_jwt_claims(claims)
        return
    auc = getattr(target, "AUC", None)
    if auc is not None and hasattr(auc, "set_invocation_jwt_claims"):
        auc.set_invocation_jwt_claims(claims)


def _apply_invocation_context(handler: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    claims = payload.get("_jwt_claims") if isinstance(payload, dict) else None
    try:
        payload = apply_handler_invocation_context(handler, payload)
    except Exception:
        if isinstance(payload, dict):
            payload.pop("_jwt_claims", None)
    for attr in ("AUC", "CHC", "SHC", "DAC", "BPC", "DCC", "GRC"):
        _stamp_invocation_jwt(getattr(handler, attr, None), claims)
    return payload if isinstance(payload, dict) else {}


def _deadline_seconds(context: Any) -> int:
    remaining = None
    getter = getattr(context, "get_remaining_time_in_millis", None)
    if callable(getter):
        try:
            remaining = int(getter()) / 1000.0
        except Exception:
            remaining = None
    if remaining is None:
        return DETACHED_DEADLINE_SECONDS
    # Leave time to persist "did not finish" before the function is killed.
    return max(1, min(DETACHED_DEADLINE_SECONDS, int(remaining) - 45))


def _reserved_flag(value: Any) -> bool:
    """Same reserved-flag rules as SchdController._pop_reserved_flag."""
    if value is True or value == 1:
        return True
    if isinstance(value, str) and value.strip().lower() in ("1", "true", "yes"):
        return True
    return False


def _describe_fallback(handler_name: str) -> Dict[str, Any]:
    return {
        "success": True,
        "action": "describe",
        "output": {
            "described": False,
            "handler": handler_name,
            "input_schema": {
                "type": "object",
                "additionalProperties": True,
            },
            "output_schema": {"type": "object"},
        },
    }


def _run_handler(handler_name: str, payload: Dict[str, Any]) -> Any:
    subhandler = None
    base_handler_name = handler_name
    if "/" in handler_name:
        base_handler_name, subhandler = handler_name.split("/", 1)
        if subhandler and "subhandler" not in payload:
            payload["subhandler"] = subhandler
    handler = get_handler(base_handler_name)
    payload = _apply_invocation_context(handler, payload)
    # Peer calls skip SchdLoader. Honor the same _describe switch here so a
    # schema request does not execute run() with default inputs.
    describe = _reserved_flag(payload.pop("_describe", False))
    payload.pop("_stack", None)
    if describe:
        describe_method = getattr(handler, "describe", None)
        if callable(describe_method):
            return describe_method(payload)
        return _describe_fallback(base_handler_name)
    return handler.run(payload)


def _run_bounded(handler_name: str, payload: Dict[str, Any], seconds: int) -> tuple[Any, Optional[BaseException], bool]:
    box: Dict[str, Any] = {}

    def target() -> None:
        try:
            box["result"] = _run_handler(handler_name, payload)
        except Exception as exc:
            box["error"] = exc

    worker = threading.Thread(target=target, daemon=True)
    worker.start()
    worker.join(seconds)
    if worker.is_alive():
        return None, None, True
    if "error" in box:
        return None, box["error"], False
    return box.get("result"), None, False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        if isinstance(event, str):
            event = json.loads(event)
        if not isinstance(event, dict):
            event = {}
        handler_name = event.get("handler")
        if not handler_name:
            return {"statusCode": 400, "success": False, "error": "Missing required field: handler"}
        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        detached = bool(event.get("detached"))
        if detached:
            claims = payload.get("_jwt_claims") if isinstance(payload, dict) else None
            result, error, timed_out = _run_bounded(
                str(handler_name),
                payload,
                _deadline_seconds(context),
            )
            complete_detached(
                event,
                result,
                error=error,
                timed_out=timed_out,
                claims=claims,
            )
            return {"statusCode": 202, "success": True, "body": {"status": "recorded"}}

        result = _run_handler(str(handler_name), payload)
        return {
            "statusCode": 200,
            "success": True,
            "body": normalize_for_json(result),
        }
    except ValueError as exc:
        return {"statusCode": 400, "success": False, "error": str(exc)}
    except Exception as exc:
        print(f"[peer_lambda] ERROR: {exc}")
        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "success": False,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
