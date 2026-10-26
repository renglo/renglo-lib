"""Provider package. A module with contribute() is a provider. The registry discovers it."""

from __future__ import annotations

from renglo.llm.providers.bedrock_converse import BedrockConverseAdapter
from renglo.llm.providers.errors import classify_provider_error, provider_error_message
from renglo.llm.providers.openai_chat import OpenAIChatAdapter
from renglo.llm.registry import provider_modules


def default_adapters():
    """One adapter per provider module, keyed by provider id."""
    adapters = {}
    for module in provider_modules():
        provider_id = getattr(module, "PROVIDER_ID", None)
        create = getattr(module, "create_adapter", None)
        if not provider_id or not callable(create):
            continue
        adapters[provider_id] = create()
    return adapters


__all__ = [
    "BedrockConverseAdapter",
    "OpenAIChatAdapter",
    "classify_provider_error",
    "default_adapters",
    "provider_error_message",
]
