"""Handler-facing LLM interface. generate is the only public call."""

from __future__ import annotations

from renglo.llm.llm_model import LlmModel
from renglo.llm.types import BLOCK_TEXT


class LlmController:
    def __init__(self, config=None, adapters=None, sleep=None, timeseries=None):
        self.config = config or {}
        self.LMM = LlmModel(
            self.config, adapters=adapters, sleep=sleep, timeseries=timeseries
        )

    def generate(
        self,
        messages,
        model="default",
        provider=None,
        max_output_tokens=None,
        timeout_seconds=120,
        fallback=None,
        context=None,
        provider_options=None,
        stream=None,
        tools=None,
        output_schema=None,
    ):
        """Generate text from canonical messages. A string content value becomes one text block."""
        return self.LMM.generate(
            messages=_normalize_messages(messages),
            model=model,
            provider=provider,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
            fallback=fallback,
            context=context,
            provider_options=provider_options,
            stream=stream,
            tools=tools,
            output_schema=output_schema,
        )


def _normalize_messages(messages):
    if not isinstance(messages, list):
        return messages
    normalized = []
    for message in messages:
        if not isinstance(message, dict):
            normalized.append(message)
            continue
        content = message.get("content")
        if isinstance(content, str):
            copied = dict(message)
            copied["content"] = [{"type": BLOCK_TEXT, "text": content}]
            normalized.append(copied)
        else:
            normalized.append(message)
    return normalized
