"""Map provider exceptions onto plane error types."""

from __future__ import annotations

from renglo.llm.types import (
    ERROR_INVALID,
    ERROR_PROVIDER,
    ERROR_RATE_LIMIT,
    ERROR_TIMEOUT,
)

_THROTTLE_CODES = {
    "ThrottlingException",
    "TooManyRequestsException",
    "ServiceQuotaExceededException",
}


def classify_provider_error(exc) -> str:
    status = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    code = ""
    if isinstance(response, dict):
        meta = response.get("ResponseMetadata") or {}
        status = status or meta.get("HTTPStatusCode")
        code = ((response.get("Error") or {}).get("Code")) or ""
    if code in _THROTTLE_CODES or status == 429:
        return ERROR_RATE_LIMIT
    name = type(exc).__name__.lower()
    if "timeout" in name or status in (408, 504):
        return ERROR_TIMEOUT
    if status is not None and status >= 500:
        return ERROR_PROVIDER
    if status is not None and 400 <= status < 500:
        return ERROR_INVALID
    if "connection" in name:
        return ERROR_PROVIDER
    return ERROR_PROVIDER


def provider_error_message(exc) -> str:
    message = getattr(exc, "message", None)
    if isinstance(message, str) and message.strip():
        return message.strip()
    text = str(exc).strip()
    return text or type(exc).__name__
