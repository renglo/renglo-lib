"""api_family registry. Translators register here. generate does not branch on provider names."""

from __future__ import annotations

from renglo.llm.providers.bedrock_converse import BedrockConverseAdapter
from renglo.llm.providers.errors import classify_provider_error, provider_error_message
from renglo.llm.providers.openai_chat import OpenAIChatAdapter


def default_adapters(bedrock_client=None, openai_client_factory=None):
    return {
        "bedrock_converse": BedrockConverseAdapter(client=bedrock_client),
        "openai_chat": OpenAIChatAdapter(client_factory=openai_client_factory),
    }


__all__ = [
    "BedrockConverseAdapter",
    "OpenAIChatAdapter",
    "classify_provider_error",
    "default_adapters",
    "provider_error_message",
]
