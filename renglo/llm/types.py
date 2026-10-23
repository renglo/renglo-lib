"""Canonical LLM plane shapes. schema_version 1. Later fields are additive."""

from __future__ import annotations

SCHEMA_VERSION = 1

ROLE_SYSTEM = "system"
ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"
ROLE_TOOL = "tool"
TEXT_ROLES = (ROLE_SYSTEM, ROLE_USER, ROLE_ASSISTANT)

BLOCK_TEXT = "text"
BLOCK_TOOL_CALL = "tool_call"
BLOCK_TOOL_RESULT = "tool_result"

CAPABILITY_TEXT = "text"
CAPABILITY_STREAMING = "streaming"
CAPABILITY_TOOL_CALLING = "tool_calling"
CAPABILITY_STRUCTURED_OUTPUT = "structured_output"

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"
STATUS_TIMED_OUT = "timed_out"

ERROR_CONFIG = "config_error"
ERROR_INVALID = "invalid_request"
ERROR_UNSUPPORTED = "unsupported"
ERROR_RATE_LIMIT = "rate_limit"
ERROR_TIMEOUT = "timeout"
ERROR_PROVIDER = "provider_error"

RETRYABLE_ERRORS = (ERROR_RATE_LIMIT, ERROR_TIMEOUT, ERROR_PROVIDER)
STOP_ERRORS = (ERROR_INVALID, ERROR_CONFIG, ERROR_UNSUPPORTED)

CONTEXT_KEYS = (
    "portfolio_id",
    "org_id",
    "extension_id",
    "user_id",
    "agent_id",
    "session_id",
    "parent_execution_id",
)


class LlmConfigError(Exception):
    """Registry or credential configuration is missing."""


def empty_usage():
    return {
        "input_tokens": None,
        "output_tokens": None,
        "reasoning_tokens": None,
        "cached_tokens": None,
    }


def empty_cost(currency="USD"):
    return {
        "estimated": None,
        "provider_reported": None,
        "reconciled": None,
        "currency": currency,
    }


def copy_context(context):
    """Copy caller context onto the execution. This round does not authorize it."""
    if not context:
        return {}
    copied = {}
    for key in CONTEXT_KEYS:
        if key in context:
            copied[key] = context[key]
    return copied
