"""Handler-facing LLM interface. generate is the only public call."""

from __future__ import annotations

from renglo.llm.llm_model import LlmModel
from renglo.llm.types import BLOCK_TEXT, STATUS_COMPLETED
from renglo.logger import get_logger

_TOKEN_SERIES = (
    ("input_tokens", "llm.input_tokens"),
    ("output_tokens", "llm.output_tokens"),
    ("reasoning_tokens", "llm.reasoning_tokens"),
    ("cached_tokens", "llm.cached_tokens"),
    ("cache_write_5m_tokens", "llm.cache_write_5m_tokens"),
    ("cache_write_1h_tokens", "llm.cache_write_1h_tokens"),
)


class LlmController:
    def __init__(self, config=None, adapters=None, sleep=None, timeseries=None):
        self.config = config or {}
        self.logger = get_logger()
        self._timeseries = timeseries
        self.LMM = LlmModel(self.config, adapters=adapters, sleep=sleep)

    def generate(
        self,
        messages,
        model=None,
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
        result = self.LMM.generate(
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
        self._record_usage(result)
        return result

    def _record_usage(self, result):
        recorder = self._usage_recorder()
        if recorder is None:
            return
        output = (result or {}).get("output") or {}
        execution = output.get("execution") or {}
        context = execution.get("context") or {}
        portfolio = context.get("portfolio_id")
        org = context.get("org_id")
        if not portfolio or not org:
            return
        extension = str(context.get("extension_id") or "unknown")
        for attempt in output.get("attempts") or []:
            if isinstance(attempt, dict):
                self._record_attempt(recorder, portfolio, org, extension, attempt)

    def _record_attempt(self, recorder, portfolio, org, extension, attempt):
        dimensions = [
            str(attempt.get("provider_id") or "unknown"),
            str(attempt.get("model_id") or "unknown"),
            str(attempt.get("status") or "unknown"),
            extension,
        ]
        measures = {"llm.requests": 1}
        if attempt.get("status") != STATUS_COMPLETED:
            measures["llm.errors"] = 1
        if _is_number(attempt.get("duration_ms")):
            measures["llm.latency_ms"] = attempt.get("duration_ms")
        usage = attempt.get("usage") or {}
        for field, series_key in _TOKEN_SERIES:
            value = usage.get(field)
            if _is_number(value):
                measures[series_key] = value
        estimated = (attempt.get("cost") or {}).get("estimated")
        if _is_number(estimated):
            measures["llm.cost_usd"] = estimated
        attempt_id = attempt.get("attempt_id")
        for series_key, value in measures.items():
            try:
                recorder.put(
                    portfolio,
                    org,
                    key=series_key,
                    value=value,
                    dimensions=dimensions,
                    resolution="day",
                    idempotency_key=f"{attempt_id}:{series_key}",
                )
            except Exception as exc:
                self.logger.warning("timeseries put failed for %s: %s", series_key, exc)

    def _usage_recorder(self):
        if self._timeseries is not None:
            return self._timeseries
        if not self.config.get("DYNAMODB_TIMESERIES_TABLE"):
            return None
        try:
            from renglo.timeseries.timeseries_model import TimeSeriesModel

            self._timeseries = TimeSeriesModel(config=self.config)
        except Exception as exc:
            self.logger.warning("timeseries store unavailable: %s", exc)
            return None
        return self._timeseries


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


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
