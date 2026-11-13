"""OpenAI chat-completions translator. The deployment endpoint is the base URL."""

from __future__ import annotations

import json

from renglo.llm.providers.errors import classify_provider_error, provider_error_message
from renglo.llm.registry import (
    CHAT_CAPABILITIES,
    CallTarget,
    CatalogEntry,
    ProviderContribution,
    configured_models,
    text_model,
)
from renglo.llm.types import (
    BLOCK_TEXT,
    BLOCK_TOOL_CALL,
    BLOCK_TOOL_RESULT,
    ERROR_CONFIG,
    ROLE_TOOL,
    empty_usage,
)

PUBLIC_ENDPOINT = "https://api.openai.com/v1"
PROVIDER_ID = "openai"
_MODELS_KEY = "OPENAI_MODELS"
_ENDPOINT_KEY = "OPENAI_BASE_URL"
_CREDENTIAL_KEY = "OPENAI_API_KEY"
_SOURCE = "OpenAI API"

_PROTECTED = {"model", "messages"}


def _block_text(message) -> str:
    parts = []
    for block in message.get("content") or []:
        if isinstance(block, dict) and block.get("type") == BLOCK_TEXT:
            parts.append(str(block.get("text") or ""))
    return "\n".join(parts)


def _setting(config, key):
    return str((config or {}).get(key) or "").strip()


def create_adapter(client_factory=None, **_ignored):
    return OpenAIChatAdapter(client_factory=client_factory)


def prepare(config, target):
    """Hand the adapter its credential. Callers never see how this provider authenticates."""
    secret = _setting(config, _CREDENTIAL_KEY)
    if not secret:
        return None, f"{_CREDENTIAL_KEY} is not set"
    return secret, None


def target_for(config, model_id):
    model_id = str(model_id or "").strip()
    if not model_id:
        return None, "model is required"
    return _target(config, model_id), None


def contribute(config):
    """One catalog row per id in OPENAI_MODELS. Nothing is selected for the caller."""
    config = config or {}
    model_ids = configured_models(config, _MODELS_KEY)
    if not model_ids:
        return ProviderContribution(
            entries=[
                CatalogEntry(
                    provider_id=PROVIDER_ID,
                    model_id=None,
                    available=False,
                    reason=f"{_MODELS_KEY} is not set",
                    source_label=_SOURCE,
                )
            ],
            targets={},
            models={},
        )
    secret = _setting(config, _CREDENTIAL_KEY)
    available = bool(secret)
    entries = []
    targets = {}
    models = {}
    for model_id in model_ids:
        entries.append(
            CatalogEntry(
                provider_id=PROVIDER_ID,
                model_id=model_id,
                available=available,
                reason=None if available else f"{_CREDENTIAL_KEY} is not set",
                source_label=_SOURCE,
            )
        )
        targets[model_id] = _target(config, model_id)
        models[model_id] = text_model(
            model_id, max_output_tokens=16384, capabilities=CHAT_CAPABILITIES
        )
    return ProviderContribution(entries=entries, targets=targets, models=models)


def _target(config, model_id):
    return CallTarget(
        provider_id=PROVIDER_ID,
        model_id=model_id,
        target_id=f"{PROVIDER_ID}:{model_id}",
        endpoint=_setting(config, _ENDPOINT_KEY) or PUBLIC_ENDPOINT,
        region=None,
    )


def _read(obj, key, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class OpenAIChatAdapter:
    api_family = "openai_chat"
    provider_id = PROVIDER_ID

    def __init__(self, client_factory=None):
        self.client_factory = client_factory
        self.last_credential = None

    def to_provider(self, request, deployment):
        """Canonical messages become chat-completions role + string content."""
        messages = [_openai_message(message) for message in (request.get("messages") or [])]
        native = {
            "model": deployment.model_id,
            "messages": messages,
        }
        max_tokens = request.get("max_output_tokens")
        if max_tokens is not None:
            native["max_tokens"] = max_tokens
        specs = _openai_tools(request.get("tools"))
        if specs:
            native["tools"] = specs
        for key, value in (request.get("provider_options") or {}).items():
            if key not in _PROTECTED:
                if key == "tool_choice" and not specs:
                    continue
                native[key] = value
        native["model"] = deployment.model_id
        native["messages"] = messages
        return native

    def from_provider(self, native):
        usage = empty_usage()
        raw_usage = _read(native, "usage") or {}
        usage["input_tokens"] = _read(raw_usage, "prompt_tokens")
        usage["output_tokens"] = _read(raw_usage, "completion_tokens")
        prompt_details = _read(raw_usage, "prompt_tokens_details") or {}
        completion_details = _read(raw_usage, "completion_tokens_details") or {}
        usage["cached_tokens"] = _read(prompt_details, "cached_tokens")
        usage["reasoning_tokens"] = _read(completion_details, "reasoning_tokens")

        text = ""
        content = []
        choices = _read(native, "choices") or []
        if choices:
            message = _read(choices[0], "message") or {}
            raw_content = _read(message, "content")
            if isinstance(raw_content, str) and raw_content:
                text = raw_content
                content.append({"type": BLOCK_TEXT, "text": raw_content})
            elif isinstance(raw_content, list):
                text = "".join(
                    str(part.get("text") or "")
                    for part in raw_content
                    if isinstance(part, dict)
                )
                if text:
                    content.append({"type": BLOCK_TEXT, "text": text})
            for tool_call in _read(message, "tool_calls") or []:
                fn = _read(tool_call, "function") or {}
                content.append(
                    {
                        "type": BLOCK_TOOL_CALL,
                        "id": str(_read(tool_call, "id") or ""),
                        "name": str(_read(fn, "name") or ""),
                        "arguments": _parse_arguments(_read(fn, "arguments")),
                    }
                )
        if hasattr(native, "model_dump"):
            provider_output = native.model_dump()
        elif isinstance(native, dict):
            provider_output = native
        else:
            provider_output = {"id": _read(native, "id")}
        return {
            "text": text,
            "content": content,
            "usage": usage,
            "provider_output": provider_output,
        }

    def invoke(self, request, deployment, credential, timeout_seconds):
        self.last_credential = credential
        if not credential or credential == "aws_iam":
            return _failure(ERROR_CONFIG, "OpenAI chat requires a resolved API key")
        native = self.to_provider(request, deployment)
        try:
            client = self._client(credential, deployment.endpoint, timeout_seconds)
            response = client.chat.completions.create(
                **native,
                timeout=timeout_seconds,
            )
        except Exception as exc:
            return _failure(classify_provider_error(exc), provider_error_message(exc))
        parsed = self.from_provider(response)
        return {"ok": True, "error_type": None, "error": None, **parsed}

    def _client(self, credential, endpoint, timeout_seconds):
        if self.client_factory is not None:
            return self.client_factory(
                api_key=credential,
                base_url=endpoint,
                timeout_seconds=timeout_seconds,
            )
        from openai import OpenAI

        return OpenAI(
            api_key=credential,
            base_url=endpoint,
            timeout=timeout_seconds,
            max_retries=0,
        )


def _openai_message(message):
    role = message.get("role")
    if role == ROLE_TOOL:
        text = ""
        tool_call_id = str(message.get("tool_call_id") or "")
        for block in message.get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == BLOCK_TOOL_RESULT:
                tool_call_id = str(block.get("tool_call_id") or tool_call_id)
                text = str(block.get("text") or "")
            elif block.get("type") == BLOCK_TEXT:
                text = str(block.get("text") or text)
        return {"role": ROLE_TOOL, "tool_call_id": tool_call_id, "content": text}

    text = _block_text(message)
    row = {"role": role, "content": text or None}
    tool_calls = []
    for block in message.get("content") or []:
        if not isinstance(block, dict) or block.get("type") != BLOCK_TOOL_CALL:
            continue
        args = block.get("arguments")
        if isinstance(args, dict):
            arg_str = json.dumps(args)
        elif isinstance(args, str) and args:
            arg_str = args
        else:
            arg_str = "{}"
        tool_calls.append(
            {
                "id": block.get("id"),
                "type": "function",
                "function": {
                    "name": str(block.get("name") or ""),
                    "arguments": arg_str,
                },
            }
        )
    if tool_calls:
        row["tool_calls"] = tool_calls
        if not text:
            row["content"] = None
    else:
        row["content"] = text
    return row


def _openai_tools(tools):
    specs = []
    for tool in tools or []:
        if not isinstance(tool, dict):
            continue
        name = str(tool.get("name") or "").strip()
        if not name:
            continue
        parameters = tool.get("parameters")
        if not isinstance(parameters, dict):
            parameters = {"type": "object", "properties": {}}
        fn = {
            "name": name,
            "parameters": parameters,
        }
        description = str(tool.get("description") or "")
        if description:
            fn["description"] = description
        specs.append({"type": "function", "function": fn})
    return specs


def _parse_arguments(raw):
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _failure(error_type, message):
    return {
        "ok": False,
        "error_type": error_type,
        "error": message,
        "text": "",
        "content": [],
        "usage": empty_usage(),
        "provider_output": {},
    }
