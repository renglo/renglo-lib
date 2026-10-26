"""Bedrock Runtime Converse translator. One function for every Converse model id."""

from __future__ import annotations

from renglo.llm.providers.errors import classify_provider_error, provider_error_message
from renglo.llm.registry import CallTarget, CatalogEntry, ProviderContribution, text_model
from renglo.llm.types import (
    BLOCK_TEXT,
    ERROR_CONFIG,
    ROLE_ASSISTANT,
    ROLE_SYSTEM,
    ROLE_USER,
    empty_usage,
)

# Default model id for this provider. Empty until namespaced config supplies one.
DEFAULT_MODEL_ID = ""
SECONDARY_MODEL_ID = ""
PROVIDER_ID = "bedrock"
AMBIENT_CREDENTIAL = "aws_iam"
_PRIMARY_KEY = "BEDROCK_MODEL"
_SECONDARY_KEY = "BEDROCK_CONVERSE_MODEL"
_SOURCE = "Bedrock"

_INFERENCE_KEYS = {
    "temperature": "temperature",
    "topP": "topP",
    "top_p": "topP",
    "stopSequences": "stopSequences",
}


def _is_openai_gpt(model_id: str) -> bool:
    folded = (model_id or "").lower()
    return "openai" in folded and "gpt" in folded


def _block_text(block) -> str:
    if not isinstance(block, dict) or block.get("type") != BLOCK_TEXT:
        return ""
    return str(block.get("text") or "")


class BedrockConverseAdapter:
    api_family = "bedrock_converse"
    provider_id = PROVIDER_ID

    def __init__(self, client=None):
        self.client = client
        self.last_credential = None

    def to_provider(self, request, deployment):
        """Canonical messages become a Converse request. System text becomes system."""
        system = []
        messages = []
        for message in request.get("messages") or []:
            text = "\n".join(
                _block_text(block) for block in (message.get("content") or [])
            )
            role = message.get("role")
            if role == ROLE_SYSTEM:
                system.append({"text": text})
                continue
            if role in (ROLE_USER, ROLE_ASSISTANT):
                messages.append({"role": role, "content": [{"text": text}]})

        native = {
            "modelId": deployment.model_id,
            "messages": messages,
        }
        if system:
            native["system"] = system

        inference = {}
        max_tokens = request.get("max_output_tokens")
        if max_tokens is not None:
            inference["maxTokens"] = max_tokens

        additional = {}
        effort = request.get("reasoning_effort")
        if effort and _is_openai_gpt(deployment.model_id):
            additional["reasoning"] = {"effort": effort}

        for key, value in (request.get("provider_options") or {}).items():
            mapped = _INFERENCE_KEYS.get(key)
            if mapped:
                inference[mapped] = value
                continue
            if key == "reasoning" and isinstance(value, dict):
                additional["reasoning"] = value
                continue
            additional[key] = value

        if inference:
            native["inferenceConfig"] = inference
        if additional:
            native["additionalModelRequestFields"] = additional
        return native

    def from_provider(self, native):
        usage = empty_usage()
        raw_usage = {}
        text_parts = []
        provider_output = native if isinstance(native, dict) else {"raw": str(native)}
        if isinstance(native, dict):
            raw_usage = native.get("usage") or {}
            message = ((native.get("output") or {}).get("message") or {})
            for block in message.get("content") or []:
                if isinstance(block, dict) and block.get("text"):
                    text_parts.append(block["text"])
        usage["input_tokens"] = _first(raw_usage, "inputTokens", "input_tokens")
        usage["output_tokens"] = _first(raw_usage, "outputTokens", "output_tokens")
        usage["reasoning_tokens"] = _first(raw_usage, "reasoningTokens", "reasoning_tokens")
        usage["cached_tokens"] = _first(
            raw_usage, "cacheReadInputTokens", "cached_tokens"
        )
        return {
            "text": "".join(text_parts),
            "usage": usage,
            "provider_output": provider_output,
        }

    def invoke(self, request, deployment, credential, timeout_seconds):
        self.last_credential = credential
        if credential != AMBIENT_CREDENTIAL:
            return _failure(ERROR_CONFIG, "Bedrock Converse requires aws_iam")
        native = self.to_provider(request, deployment)
        client = self.client or self._runtime_client(deployment, timeout_seconds)
        try:
            response = client.converse(**native)
        except Exception as exc:
            return _failure(classify_provider_error(exc), provider_error_message(exc))
        parsed = self.from_provider(response)
        return {"ok": True, "error_type": None, "error": None, **parsed}

    def _runtime_client(self, deployment, timeout_seconds):
        import boto3
        from botocore.config import Config

        timeout = timeout_seconds if timeout_seconds and timeout_seconds > 0 else 120
        return boto3.client(
            "bedrock-runtime",
            region_name=deployment.region or "us-east-1",
            config=Config(
                read_timeout=timeout,
                connect_timeout=min(timeout, 10),
                retries={"max_attempts": 1},
            ),
        )


def _setting(config, key):
    return str((config or {}).get(key) or "").strip()


def _region(config):
    return (
        _setting(config, "AWS_REGION")
        or _setting(config, "AWS_DEFAULT_REGION")
        or "us-east-1"
    )


def create_adapter(client=None, **_ignored):
    return BedrockConverseAdapter(client=client)


def prepare(config, target):
    """Hand the adapter its credential. Callers never see how this provider authenticates."""
    return AMBIENT_CREDENTIAL, None


def target_for(config, model_id):
    model_id = str(model_id or "").strip()
    if not model_id:
        return None, "model is required"
    return _target(config, model_id), None


def contribute(config):
    """Catalog rows for this provider. Namespaced config overrides the defaults."""
    config = config or {}
    primary = _setting(config, _PRIMARY_KEY) or DEFAULT_MODEL_ID
    secondary = _setting(config, _SECONDARY_KEY) or SECONDARY_MODEL_ID
    entries = [_entry(primary, _PRIMARY_KEY, is_default=True)]
    targets = {}
    models = {}
    if primary:
        targets[primary] = _target(config, primary)
        models[primary] = text_model(primary)
    if secondary and secondary != primary:
        entries.append(_entry(secondary, _SECONDARY_KEY, is_default=False))
        targets[secondary] = _target(config, secondary)
        models[secondary] = text_model(secondary)
    return ProviderContribution(entries=entries, targets=targets, models=models)


def _entry(model_id, config_key, is_default):
    available = bool(model_id)
    return CatalogEntry(
        provider_id=PROVIDER_ID,
        model_id=model_id or None,
        is_default=is_default,
        available=available,
        reason=None if available else f"{config_key} is not set",
        source_label=_SOURCE,
    )


def _target(config, model_id):
    return CallTarget(
        provider_id=PROVIDER_ID,
        model_id=model_id,
        target_id=f"{PROVIDER_ID}:{model_id}",
        endpoint=None,
        region=_region(config),
    )


def _first(mapping, *keys):
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _failure(error_type, message):
    return {
        "ok": False,
        "error_type": error_type,
        "error": message,
        "text": "",
        "usage": empty_usage(),
        "provider_output": {},
    }
