"""Microsoft Foundry translator.

One account hosts deployments from any publisher in the Foundry catalog.
The deployment name is the model id callers pass, the same way a Bedrock
model id is just a name on the Converse API. Claude deployments use the
Anthropic Messages API. Every other chat deployment uses the Foundry
chat-completions route. Authentication is the ambient Azure identity unless
AZURE_AI_API_KEY is set.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request

from renglo.llm.providers.errors import classify_provider_error, provider_error_message
from renglo.llm.registry import (
    CallTarget,
    CatalogEntry,
    ProviderContribution,
    configured_models,
    text_model,
)
from renglo.llm.types import (
    BLOCK_TEXT,
    ERROR_CONFIG,
    ROLE_ASSISTANT,
    ROLE_SYSTEM,
    ROLE_USER,
    empty_usage,
)

PROVIDER_ID = "azure"
_MODELS_KEY = "AZURE_AI_MODELS"
_ENDPOINT_KEY = "AZURE_AI_ENDPOINT"
_CREDENTIAL_KEY = "AZURE_AI_API_KEY"
_API_VERSION_KEY = "AZURE_AI_API_VERSION"
_LOCATION_KEY = "AZURE_AI_LOCATION"
_SOURCE = "Azure AI"
DEFAULT_API_VERSION = "2024-05-01-preview"
_ANTHROPIC_VERSION = "2023-06-01"
_TOKEN_SCOPE = "https://ai.azure.com/.default"
_DEFAULT_MAX_TOKENS = 4096

_PROTECTED = {"model", "messages"}
# Claude 4.6 and every 5+ generation reject temperature, top_p, and top_k.
_CLAUDE_GENERATION = re.compile(
    r"claude-(?:opus|sonnet|haiku)-(\d+)(?:-(\d+))?",
    re.IGNORECASE,
)


def _drops_sampling(model_id: str) -> bool:
    match = _CLAUDE_GENERATION.search(model_id or "")
    if not match:
        return False
    major = int(match.group(1))
    minor = int(match.group(2) or 0)
    return major > 4 or (major == 4 and minor >= 6)


def _uses_messages(model_id: str) -> bool:
    return "claude" in (model_id or "").lower()


def _completion_token_limit(model_id: str) -> bool:
    """gpt-5 and o-series deployments reject max_tokens."""
    folded = (model_id or "").lower()
    return folded.startswith(("gpt-5", "o1", "o3", "o4"))


def _block_text(message) -> str:
    parts = []
    for block in message.get("content") or []:
        if isinstance(block, dict) and block.get("type") == BLOCK_TEXT:
            parts.append(str(block.get("text") or ""))
    return "\n".join(parts)


def _setting(config, key):
    return str((config or {}).get(key) or "").strip()


def _endpoint(config):
    return _setting(config, _ENDPOINT_KEY).rstrip("/")


def _api_version(config):
    return _setting(config, _API_VERSION_KEY) or DEFAULT_API_VERSION


def create_adapter(client_factory=None, **_ignored):
    return AzureAdapter(client_factory=client_factory)


def prepare(config, target):
    """Hand the adapter its credential. Callers never see how this provider authenticates."""
    if not getattr(target, "endpoint", None):
        return None, f"{_ENDPOINT_KEY} is not set"
    secret = _setting(config, _CREDENTIAL_KEY)
    if secret:
        return {"api_key": secret}, None
    return {"identity": True, "client_id": _setting(config, "AZURE_CLIENT_ID")}, None


def target_for(config, model_id):
    model_id = str(model_id or "").strip()
    if not model_id:
        return None, "model is required"
    if not _endpoint(config):
        return None, f"{_ENDPOINT_KEY} is not set"
    return _target(config, model_id), None


def contribute(config):
    """One catalog row per deployment name in AZURE_AI_MODELS."""
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
    endpoint = _endpoint(config)
    available = bool(endpoint)
    entries = []
    targets = {}
    models = {}
    for model_id in model_ids:
        entries.append(
            CatalogEntry(
                provider_id=PROVIDER_ID,
                model_id=model_id,
                available=available,
                reason=None if available else f"{_ENDPOINT_KEY} is not set",
                source_label=_SOURCE,
            )
        )
        targets[model_id] = _target(config, model_id)
        models[model_id] = text_model(model_id, max_output_tokens=16384)
    return ProviderContribution(entries=entries, targets=targets, models=models)


def _target(config, model_id):
    return CallTarget(
        provider_id=PROVIDER_ID,
        model_id=model_id,
        target_id=f"{PROVIDER_ID}:{model_id}",
        endpoint=_endpoint(config) or None,
        region=_setting(config, _LOCATION_KEY) or None,
        api_version=_api_version(config),
    )


def _identity_token(client_id):
    """Token for Foundry. Same identity the data store uses for Cosmos."""
    from renglo.data.data_stores.data_store_azure import _credential

    config = {"AZURE_CLIENT_ID": client_id} if client_id else {}
    return _credential(config).get_token(_TOKEN_SCOPE).token


def _read(obj, key, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class _StatusError(Exception):
    def __init__(self, status_code, message):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class _FoundryClient:
    """Posts to the Foundry account. Tests inject their own client instead."""

    def __init__(self, credential, endpoint, api_version, timeout_seconds):
        self.credential = credential
        self.endpoint = str(endpoint or "").rstrip("/")
        self.api_version = api_version
        self.timeout_seconds = timeout_seconds
        self.chat = _Chat(self)
        self.messages = _Messages(self)

    def _auth(self):
        if self.credential.get("api_key"):
            return {"api-key": self.credential["api_key"]}
        token = _identity_token(self.credential.get("client_id") or "")
        return {"Authorization": f"Bearer {token}"}

    def post(self, url, body, extra_headers, timeout):
        payload = json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
        headers.update(extra_headers)
        headers.update(self._auth())
        request = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode(errors="replace")
            message = raw.strip() or str(exc.reason)
            try:
                parsed = json.loads(raw)
                error = parsed.get("error") if isinstance(parsed, dict) else None
                if isinstance(error, dict) and error.get("message"):
                    message = str(error["message"])
                elif isinstance(error, str) and error:
                    message = error
            except json.JSONDecodeError:
                pass
            raise _StatusError(exc.code, message) from exc


class _Chat:
    def __init__(self, owner):
        self.completions = _Completions(owner)


class _Completions:
    def __init__(self, owner):
        self.owner = owner

    def create(self, **body):
        timeout = body.pop("timeout", self.owner.timeout_seconds)
        version = urllib.request.quote(str(self.owner.api_version), safe="")
        url = f"{self.owner.endpoint}/models/chat/completions?api-version={version}"
        return self.owner.post(url, body, {}, timeout)


class _Messages:
    def __init__(self, owner):
        self.owner = owner

    def create(self, **body):
        timeout = body.pop("timeout", self.owner.timeout_seconds)
        url = f"{self.owner.endpoint}/anthropic/v1/messages"
        return self.owner.post(
            url,
            body,
            {"anthropic-version": _ANTHROPIC_VERSION},
            timeout,
        )


class AzureAdapter:
    api_family = "azure"
    provider_id = PROVIDER_ID

    def __init__(self, client_factory=None):
        self.client_factory = client_factory
        self.last_credential = None

    def to_provider(self, request, deployment):
        """Canonical messages become the body for this deployment's API.

        Claude deployments take the Anthropic Messages shape. Other
        deployments take chat completions. The model field is the deployment name.
        """
        if _uses_messages(deployment.model_id):
            return self._to_messages(request, deployment)
        return self._to_chat(request, deployment)

    def _to_chat(self, request, deployment):
        messages = []
        for message in request.get("messages") or []:
            messages.append(
                {"role": message.get("role"), "content": _block_text(message)}
            )
        native = {"model": deployment.model_id, "messages": messages}
        max_tokens = request.get("max_output_tokens")
        if max_tokens is not None:
            token_key = (
                "max_completion_tokens"
                if _completion_token_limit(deployment.model_id)
                else "max_tokens"
            )
            native[token_key] = max_tokens
        self._apply_options(native, request, deployment)
        native["model"] = deployment.model_id
        native["messages"] = messages
        return native

    def _to_messages(self, request, deployment):
        system = []
        messages = []
        for message in request.get("messages") or []:
            text = _block_text(message)
            role = message.get("role")
            if role == ROLE_SYSTEM:
                if text:
                    system.append(text)
                continue
            if role in (ROLE_USER, ROLE_ASSISTANT):
                messages.append({"role": role, "content": text})
        native = {
            "model": deployment.model_id,
            "max_tokens": request.get("max_output_tokens") or _DEFAULT_MAX_TOKENS,
            "messages": messages,
        }
        if system:
            native["system"] = "\n".join(system)
        self._apply_options(native, request, deployment)
        native["model"] = deployment.model_id
        native["messages"] = messages
        if system:
            native["system"] = "\n".join(system)
        return native

    def _apply_options(self, native, request, deployment):
        for key, value in (request.get("provider_options") or {}).items():
            if key in _PROTECTED:
                continue
            if _drops_sampling(deployment.model_id) and key.lower().replace("_", "") in {
                "temperature",
                "topp",
                "topk",
            }:
                continue
            native[key] = value

    def from_provider(self, native):
        if _read(native, "choices") is None and isinstance(_read(native, "content"), list):
            return self._from_messages(native)
        return self._from_chat(native)

    def _from_chat(self, native):
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
        return {
            "text": text,
            "usage": usage,
            "provider_output": _provider_output(native),
        }

    def _from_messages(self, native):
        usage = empty_usage()
        raw_usage = _read(native, "usage") or {}
        usage["input_tokens"] = _read(raw_usage, "input_tokens")
        usage["output_tokens"] = _read(raw_usage, "output_tokens")
        usage["cached_tokens"] = _read(raw_usage, "cache_read_input_tokens")
        text = ""
        for block in _read(native, "content") or []:
            if _read(block, "type") == "text":
                text += str(_read(block, "text") or "")
        return {
            "text": text,
            "usage": usage,
            "provider_output": _provider_output(native),
        }

    def invoke(self, request, deployment, credential, timeout_seconds):
        self.last_credential = credential
        if not isinstance(credential, dict) or not (
            credential.get("api_key") or credential.get("identity")
        ):
            return _failure(ERROR_CONFIG, "Azure AI requires an API key or an Azure identity")
        if not deployment.endpoint:
            return _failure(ERROR_CONFIG, f"{_ENDPOINT_KEY} is not set")
        native = self.to_provider(request, deployment)
        try:
            client = self._client(credential, deployment, timeout_seconds)
        except Exception as exc:
            return _failure(ERROR_CONFIG, provider_error_message(exc))
        try:
            if _uses_messages(deployment.model_id):
                response = client.messages.create(**native, timeout=timeout_seconds)
            else:
                response = client.chat.completions.create(**native, timeout=timeout_seconds)
        except Exception as exc:
            return _failure(classify_provider_error(exc), provider_error_message(exc))
        parsed = self.from_provider(response)
        return {"ok": True, "error_type": None, "error": None, **parsed}

    def _client(self, credential, deployment, timeout_seconds):
        api_version = deployment.api_version or DEFAULT_API_VERSION
        if self.client_factory is not None:
            return self.client_factory(
                credential=credential,
                endpoint=deployment.endpoint,
                api_version=api_version,
                timeout_seconds=timeout_seconds,
            )
        return _FoundryClient(credential, deployment.endpoint, api_version, timeout_seconds)


def _provider_output(native):
    if hasattr(native, "model_dump"):
        return native.model_dump()
    if isinstance(native, dict):
        return native
    return {"id": _read(native, "id")}


def _failure(error_type, message):
    return {
        "ok": False,
        "error_type": error_type,
        "error": message,
        "text": "",
        "usage": empty_usage(),
        "provider_output": {},
    }
