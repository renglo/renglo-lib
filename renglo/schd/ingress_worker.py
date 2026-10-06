"""Background execution for EventBridge webhook ingress (fire-and-forget)."""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

from renglo.schd.schd_loader import SchdLoader

_logger = logging.getLogger(__name__)

INGRESS_WORKER_FLAG = "renglo_ingress_worker"


def try_invoke_async_ingress_worker(handler_route: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Queue ``handler_route`` on a new Lambda invocation (same function, Event)."""
    function_name = (os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or "").strip()
    if not function_name:
        return {"success": False, "error": "not running on Lambda"}

    try:
        import boto3
    except ImportError:
        return {"success": False, "error": "boto3 unavailable"}

    request_id = str(uuid.uuid4())
    event = {
        INGRESS_WORKER_FLAG: True,
        "request_id": request_id,
        "handler": handler_route,
        "payload": dict(payload or {}),
    }
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    try:
        client = boto3.client("lambda", region_name=region)
        response = client.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(event).encode("utf-8"),
        )
    except Exception as exc:
        _logger.exception("Async ingress self-invoke failed")
        return {"success": False, "error": f"Lambda invoke failed: {exc}"}

    status = int(response.get("StatusCode") or 0)
    if status not in (200, 202):
        return {
            "success": False,
            "error": f"Lambda invoke not accepted (status {status})",
        }
    return {
        "success": True,
        "action": "ingress_webhook_async",
        "request_id": request_id,
        "task_id": None,
        "worker": "lambda_event",
    }


def run_ingress_webhook_worker(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Entry for async Lambda worker invocations (not API Gateway)."""
    handler_route = str(event.get("handler") or "").strip()
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    request_id = str(event.get("request_id") or "")
    if not handler_route:
        return {"statusCode": 400, "success": False, "error": "handler required"}

    _logger.info(
        "Ingress worker start handler=%s request_id=%s",
        handler_route,
        request_id,
    )
    try:
        result = SchdLoader().load_and_run(handler_route, payload=payload)
        success = bool((result or {}).get("success", True))
        _logger.info(
            "Ingress worker done handler=%s request_id=%s success=%s action=%s",
            handler_route,
            request_id,
            success,
            (result or {}).get("action"),
        )
        return {
            "statusCode": 200,
            "success": success,
            "request_id": request_id,
            "output": result,
        }
    except Exception as exc:
        _logger.exception("Ingress worker failed handler=%s", handler_route)
        return {
            "statusCode": 500,
            "success": False,
            "request_id": request_id,
            "error": str(exc),
        }
