"""OpenAI chat-completions translator. The deployment endpoint is the base URL."""

from __future__ import annotations

from renglo.llm.providers.errors import classify_provider_error, provider_error_message
from renglo.llm.types import BLOCK_TEXT, ERROR_CONFIG, empty_usage

_PROTECTED = {"model", "messages"}


def _block_text(message) -> str:
    parts = []
    for block in message.get("content") or []:
        if isinstance(block, dict) and block.get("type") == BLOCK_TEXT:
            parts.append(str(block.get("text") or ""))
    return "\n".join(parts)


def _read(obj, key, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class OpenAIChatAdapter:
    api_family = "openai_chat"

    def __init__(self, client_factory=None):
        self.client_factory = client_factory
        self.last_credential = None

    def to_provider(self, request, deployment):
        """Canonical messages become chat-completions role + string content."""
        messages = []
        for message in request.get("messages") or []:
            messages.append(
                {"role": message.get("role"), "content": _block_text(message)}
            )
        native = {
            "model": deployment.model_id,
            "messages": messages,
        }
        max_tokens = request.get("max_output_tokens")
        if max_tokens is not None:
            native["max_tokens"] = max_tokens
        for key, value in (request.get("provider_options") or {}).items():
            if key not in _PROTECTED:
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
        choices = _read(native, "choices") or []
        if choices:
            message = _read(choices[0], "message") or {}
            content = _read(message, "content")
            if isinstance(content, str):
                text = content
            elif isinstance(content, list):
                text = "".join(
                    str(part.get("text") or "")
                    for part in content
                    if isinstance(part, dict)
                )
        if hasattr(native, "model_dump"):
            provider_output = native.model_dump()
        elif isinstance(native, dict):
            provider_output = native
        else:
            provider_output = {"id": _read(native, "id")}
        return {
            "text": text,
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


def _failure(error_type, message):
    return {
        "ok": False,
        "error_type": error_type,
        "error": message,
        "text": "",
        "usage": empty_usage(),
        "provider_output": {},
    }
