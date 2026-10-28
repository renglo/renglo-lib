"""Published USD prices per 1,000,000 tokens, grouped by provider.

Bedrock does not return a price. A normal call uses the card for its provider
and region. Batch rates are stored and are not applied here. Reasoning stays
unpriced when the published output price already includes it.

Amazon Nova cache reads are 75 percent less than the on-demand input price.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ModelRates:
    """One model's prices. None means that price is not published."""

    provider: str
    region: str | None
    name: str
    input_per_million: float
    output_per_million: float
    batch_input_per_million: float | None = None
    batch_output_per_million: float | None = None
    cache_write_5m_per_million: float | None = None
    cache_write_1h_per_million: float | None = None
    cache_read_per_million: float | None = None
    reasoning_per_million: float | None = None
    input_image_per_million: float | None = None
    input_video_per_million: float | None = None
    input_audio_per_million: float | None = None
    output_image_per_million: float | None = None
    output_video_per_million: float | None = None
    output_audio_per_million: float | None = None
    cache_write_30m_per_million: float | None = None
    cache_write_12h_per_million: float | None = None
    long_input_per_million: float | None = None
    long_output_per_million: float | None = None
    long_cache_read_per_million: float | None = None
    long_cache_write_30m_per_million: float | None = None
    long_cache_write_12h_per_million: float | None = None
    cache_read_image_per_million: float | None = None
    cache_read_audio_per_million: float | None = None

    def to_dict(self):
        payload = asdict(self)
        payload.pop("name", None)
        return payload


def _anthropic(name, input_price, output_price, batch_input, batch_output, cache_write_5m, cache_write_1h, cache_read, region=None):
    return ModelRates(
        provider="bedrock",
        region=region,
        name=name,
        input_per_million=input_price,
        output_per_million=output_price,
        batch_input_per_million=batch_input,
        batch_output_per_million=batch_output,
        cache_write_5m_per_million=cache_write_5m,
        cache_write_1h_per_million=cache_write_1h,
        cache_read_per_million=cache_read,
    )


def _deepseek(name, input_price, output_price, region):
    return _text(name, input_price, output_price, region)


def _google(name, input_price, output_price, region):
    return _text(name, input_price, output_price, region)


def _meta(name, input_price, output_price, region, batch_input=None, batch_output=None):
    return _text(name, input_price, output_price, region, batch_input=batch_input, batch_output=batch_output)


def _nvidia(name, input_price, output_price, region):
    return _text(name, input_price, output_price, region)


def _grok(name, input_price, output_price, cache_read, region):
    return _text(name, input_price, output_price, region, cache_read=cache_read)


def _text(name, input_price, output_price, region, batch_input=None, batch_output=None, cache_read=None):
    return ModelRates(
        provider="bedrock",
        region=region,
        name=name,
        input_per_million=input_price,
        output_per_million=output_price,
        batch_input_per_million=batch_input,
        batch_output_per_million=batch_output,
        cache_read_per_million=cache_read,
    )


def _rows(key, regions, build):
    return tuple((key, build(region)) for region in regions)


_US = ("us-east-1", "us-east-2", "us-west-2")
_GOV = ("us-gov-east-1", "us-gov-west-1")
_MUMBAI_IRELAND_MILAN = ("ap-south-1", "eu-west-1", "eu-south-1")
_SAO_TOKYO = ("sa-east-1", "ap-northeast-1")


def _nova(name, text_in, text_out, image_in=None, video_in=None, audio_in=None, image_out=None, video_out=None, audio_out=None):
    """US East (N. Virginia) standard prices for Global cross-region inference."""
    return ModelRates(
        provider="bedrock",
        region="us-east-1",
        name=name,
        input_per_million=text_in,
        output_per_million=text_out,
        input_image_per_million=image_in,
        input_video_per_million=video_in,
        input_audio_per_million=audio_in,
        output_image_per_million=image_out,
        output_video_per_million=video_out,
        output_audio_per_million=audio_out,
        cache_read_per_million=text_in * 0.25,
    )


# Longer keys are matched first so claude-sonnet-5-5 does not use the Sonnet 5 card.
BEDROCK_RATES = (
    ("claude-haiku-5-5-long", _anthropic("Claude Haiku 5.5 - Long Context", 0.50, 2.50, None, None, 0.625, 1.00, 0.05)),
    ("claude-haiku-5-5", _anthropic("Claude Haiku 5.5", 0.10, 0.50, None, None, 0.125, 0.20, 0.01)),
    ("claude-sonnet-5-5", _anthropic("Claude Sonnet 5.5", 2.00, 10.00, None, None, 2.50, 4.00, 0.10)),
    ("claude-opus-5-5", _anthropic("Claude Opus 5.5", 4.00, 20.00, None, None, 5.00, 8.00, 0.20)),
    ("claude-fable-5-1", _anthropic("Claude Fable 5.1", 10.00, 50.00, None, None, 12.50, 20.00, 0.25)),
    ("claude-mythos-5-1", _anthropic("Claude Mythos 5.1", 10.00, 50.00, None, None, 12.50, 20.00, 0.25)),
    ("claude-opus-4-8", _anthropic("Claude Opus 4.8", 5.00, 25.00, None, None, 6.25, 10.00, 0.50)),
    ("claude-opus-4-7", _anthropic("Claude Opus 4.7", 5.00, 25.00, None, None, 6.25, 10.00, 0.50)),
    ("claude-opus-4-6", _anthropic("Claude Opus 4.6", 5.00, 25.00, 2.50, 12.50, 6.25, 10.00, 0.50)),
    ("claude-opus-4-5", _anthropic("Claude Opus 4.5", 5.00, 25.00, 2.50, 12.50, 6.25, 10.00, 0.50)),
    ("claude-sonnet-4-6", _anthropic("Claude Sonnet 4.6", 3.00, 15.00, 1.50, 7.50, 3.75, 6.00, 0.30)),
    ("claude-sonnet-4-5", _anthropic("Claude Sonnet 4.5", 3.00, 15.00, 1.50, 7.50, 3.75, 6.00, 0.30)),
    ("claude-haiku-4-5", _anthropic("Claude Haiku 4.5", 1.00, 5.00, 0.50, 2.50, 1.25, 2.00, 0.10)),
    ("claude-opus-5", _anthropic("Claude Opus 5", 5.00, 25.00, 2.50, 12.50, 6.25, 10.00, 0.50)),
    ("claude-sonnet-5", _anthropic("Claude Sonnet 5", 2.00, 10.00, None, None, 2.50, 4.00, 0.20)),
    ("claude-fable-5", _anthropic("Claude Fable 5", 10.00, 50.00, None, None, 12.50, 20.00, 1.00)),
    ("claude-sonnet-4", _anthropic("Claude Sonnet 4", 3.00, 15.00, 1.50, 7.50, 3.75, None, 0.30)),
    ("nova-2-lite", _nova("Amazon Nova 2 Lite", 0.30, 2.50, image_in=0.30, video_in=0.30, image_out=2.50, video_out=2.50)),
    ("nova-2-omni", _nova("Amazon Nova 2 Omni", 0.30, 2.50, image_in=0.30, video_in=0.30, audio_in=1.00, image_out=40.00)),
    ("nova-2-pro", _nova("Amazon Nova 2 Pro", 1.25, 10.00, image_in=1.25, video_in=1.25, audio_in=1.25)),
    *_rows("deepseek-r1", ("us-east-1",), lambda region: _deepseek("DeepSeek-R1", 1.35, 5.40, region)),
    *_rows("deepseek-v3-2", _US, lambda region: _deepseek("DeepSeek V3.2", 0.62, 1.85, region)),
    *_rows("deepseek-v3-2", ("ap-south-1", "sa-east-1", "ap-southeast-3", "ap-northeast-1", "eu-north-1"), lambda region: _deepseek("DeepSeek V3.2", 0.74, 2.22, region)),
    *_rows("deepseek-v3-2", ("ap-southeast-2",), lambda region: _deepseek("DeepSeek V3.2", 0.6386, 1.9055, region)),
    *_rows("deepseek-v3-1", ("ap-southeast-2",), lambda region: _deepseek("DeepSeek V3.1", 0.5974, 1.7304, region)),
    *_rows("gemma-4-31b", _US, lambda region: _google("Gemma 4 31B", 0.14, 0.40, region)),
    *_rows("gemma-4-26b-a4b", _US, lambda region: _google("Gemma 4 26B A4B", 0.13, 0.40, region)),
    *_rows("gemma-4-e2b", _US, lambda region: _google("Gemma 4 E2B", 0.04, 0.08, region)),
    *_rows("gemma-3-4b", _US, lambda region: _google("Gemma 3 4B", 0.04, 0.08, region)),
    *_rows("gemma-3-12b", _US, lambda region: _google("Gemma 3 12B", 0.09, 0.29, region)),
    *_rows("gemma-3-27b", _US, lambda region: _google("Gemma 3 27B", 0.23, 0.38, region)),
    *_rows("gemma-4-31b", ("eu-central-1",), lambda region: _google("Gemma 4 31B", 0.17, 0.48, region)),
    *_rows("gemma-4-26b-a4b", ("eu-central-1",), lambda region: _google("Gemma 4 26B A4B", 0.16, 0.48, region)),
    *_rows("gemma-4-e2b", ("eu-central-1",), lambda region: _google("Gemma 4 E2B", 0.05, 0.10, region)),
    *_rows("gemma-3-4b", _MUMBAI_IRELAND_MILAN, lambda region: _google("Gemma 3 4B", 0.05, 0.09, region)),
    *_rows("gemma-3-12b", _MUMBAI_IRELAND_MILAN, lambda region: _google("Gemma 3 12B", 0.11, 0.34, region)),
    *_rows("gemma-3-27b", _MUMBAI_IRELAND_MILAN, lambda region: _google("Gemma 3 27B", 0.27, 0.45, region)),
    *_rows("gemma-3-4b", _SAO_TOKYO, lambda region: _google("Gemma 3 4B", 0.05, 0.10, region)),
    *_rows("gemma-3-12b", _SAO_TOKYO, lambda region: _google("Gemma 3 12B", 0.11, 0.35, region)),
    *_rows("gemma-3-27b", _SAO_TOKYO, lambda region: _google("Gemma 3 27B", 0.28, 0.46, region)),
    *_rows("gemma-3-4b", ("eu-west-2",), lambda region: _google("Gemma 3 4B", 0.06, 0.12, region)),
    *_rows("gemma-3-12b", ("eu-west-2",), lambda region: _google("Gemma 3 12B", 0.14, 0.45, region)),
    *_rows("gemma-3-27b", ("eu-west-2",), lambda region: _google("Gemma 3 27B", 0.36, 0.59, region)),
    *_rows("gemma-3-4b", ("ap-southeast-2",), lambda region: _google("Gemma 3 4B", 0.0412, 0.0824, region)),
    *_rows("gemma-3-12b", ("ap-southeast-2",), lambda region: _google("Gemma 3 12B", 0.0927, 0.2987, region)),
    *_rows("gemma-3-27b", ("ap-southeast-2",), lambda region: _google("Gemma 3 27B", 0.2369, 0.3914, region)),
    ("llama4-maverick", _meta("Llama 4 Maverick 17B", 0.24, 0.97, "us-east-1", batch_input=0.12, batch_output=0.485)),
    ("llama4-scout", _meta("Llama 4 Scout 17B", 0.17, 0.66, "us-east-1", batch_input=0.085, batch_output=0.33)),
    ("llama3-3-70b", _meta("Llama 3.3 Instruct 70B", 0.72, 0.72, "us-east-1", batch_input=0.36, batch_output=0.36)),
    ("llama3-2-1b", _meta("Llama 3.2 Instruct 1B", 0.10, 0.10, "us-east-1")),
    ("llama3-2-3b", _meta("Llama 3.2 Instruct 3B", 0.15, 0.15, "us-east-1")),
    ("llama3-2-11b", _meta("Llama 3.2 Instruct 11B", 0.16, 0.16, "us-east-1")),
    ("llama3-2-90b", _meta("Llama 3.2 Instruct 90B", 0.72, 0.72, "us-east-1")),
    *_rows("nemotron-nano-2-vl", _US, lambda region: _nvidia("Nemotron Nano 2 VL", 0.20, 0.60, region)),
    *_rows("nemotron-nano-2", _US, lambda region: _nvidia("Nemotron Nano 2", 0.06, 0.23, region)),
    *_rows("nemotron-3-nano", _US, lambda region: _nvidia("Nemotron 3 Nano 30B A3B", 0.06, 0.24, region)),
    *_rows("nemotron-3-super", _US, lambda region: _nvidia("Nemotron 3 Super 120B A12B", 0.15, 0.65, region)),
    *_rows("nemotron-nano-2-vl", _GOV, lambda region: _nvidia("Nemotron Nano 2 VL", 0.240, 0.720, region)),
    *_rows("nemotron-nano-2", _GOV, lambda region: _nvidia("Nemotron Nano 2", 0.072, 0.276, region)),
    *_rows("nemotron-3-nano", _GOV, lambda region: _nvidia("Nemotron 3 Nano 30B A3B", 0.072, 0.288, region)),
    *_rows("nemotron-3-super", _GOV, lambda region: _nvidia("Nemotron 3 Super 120B A12B", 0.180, 0.780, region)),
    *_rows("nemotron-nano-2-vl", _MUMBAI_IRELAND_MILAN, lambda region: _nvidia("Nemotron Nano 2 VL", 0.24, 0.71, region)),
    *_rows("nemotron-nano-2", _MUMBAI_IRELAND_MILAN, lambda region: _nvidia("Nemotron Nano 2", 0.07, 0.27, region)),
    *_rows("nemotron-3-nano", _MUMBAI_IRELAND_MILAN, lambda region: _nvidia("Nemotron 3 Nano 30B A3B", 0.07, 0.28, region)),
    *_rows("nemotron-3-super", _MUMBAI_IRELAND_MILAN, lambda region: _nvidia("Nemotron 3 Super 120B A12B", 0.18, 0.78, region)),
    *_rows("nemotron-nano-2-vl", _SAO_TOKYO, lambda region: _nvidia("Nemotron Nano 2 VL", 0.24, 0.73, region)),
    *_rows("nemotron-nano-2", _SAO_TOKYO, lambda region: _nvidia("Nemotron Nano 2", 0.07, 0.28, region)),
    *_rows("nemotron-3-nano", _SAO_TOKYO, lambda region: _nvidia("Nemotron 3 Nano 30B A3B", 0.07, 0.29, region)),
    *_rows("nemotron-3-super", _SAO_TOKYO, lambda region: _nvidia("Nemotron 3 Super 120B A12B", 0.18, 0.78, region)),
    *_rows("nemotron-nano-2-vl", ("eu-west-2",), lambda region: _nvidia("Nemotron Nano 2 VL", 0.31, 0.93, region)),
    *_rows("nemotron-nano-2", ("eu-west-2",), lambda region: _nvidia("Nemotron Nano 2", 0.09, 0.36, region)),
    *_rows("nemotron-3-nano", ("eu-west-2",), lambda region: _nvidia("Nemotron 3 Nano 30B A3B", 0.09, 0.37, region)),
    *_rows("nemotron-3-super", ("eu-west-2",), lambda region: _nvidia("Nemotron 3 Super 120B A12B", 0.23, 1.01, region)),
    *_rows("nemotron-nano-2-vl", ("ap-southeast-2",), lambda region: _nvidia("Nemotron Nano 2 VL", 0.2060, 0.6180, region)),
    *_rows("nemotron-nano-2", ("ap-southeast-2",), lambda region: _nvidia("Nemotron Nano 2", 0.0618, 0.2369, region)),
    *_rows("nemotron-3-nano", ("ap-southeast-2",), lambda region: _nvidia("Nemotron 3 Nano 30B A3B", 0.0618, 0.2472, region)),
    *_rows("nemotron-3-super", ("ap-southeast-2",), lambda region: _nvidia("Nemotron 3 Super 120B A12B", 0.15, 0.67, region)),
    ("grok-4-7", _grok("Grok 4.7 Geo", 2.20, 6.60, 0.55, None)),
    ("grok-4-7", _grok("Grok 4.7 Global", 2.00, 6.00, 0.50, "global")),
)

def _openai(
    name,
    input_price,
    output_price,
    cache_read=None,
    batch_input=None,
    batch_output=None,
    write_30m=None,
    write_12h=None,
    long_input=None,
    long_output=None,
    long_cache_read=None,
    long_write_30m=None,
    long_write_12h=None,
    input_image=None,
    output_image=None,
    input_audio=None,
    output_audio=None,
    cache_read_image=None,
    cache_read_audio=None,
):
    """Standard OpenAI prices. Input and output are the short-context text rates."""
    return ModelRates(
        provider="openai",
        region=None,
        name=name,
        input_per_million=input_price,
        output_per_million=output_price,
        cache_read_per_million=cache_read,
        batch_input_per_million=batch_input,
        batch_output_per_million=batch_output,
        cache_write_30m_per_million=write_30m,
        cache_write_12h_per_million=write_12h,
        long_input_per_million=long_input,
        long_output_per_million=long_output,
        long_cache_read_per_million=long_cache_read,
        long_cache_write_30m_per_million=long_write_30m,
        long_cache_write_12h_per_million=long_write_12h,
        input_image_per_million=input_image,
        output_image_per_million=output_image,
        input_audio_per_million=input_audio,
        output_audio_per_million=output_audio,
        cache_read_image_per_million=cache_read_image,
        cache_read_audio_per_million=cache_read_audio,
    )


# Published standard rates. https://developers.openai.com/api/docs/pricing
# Long-context rates apply above 272K input tokens and are stored, not used for a short call.
OPENAI_RATES = (
    ("gpt-6-astra", _openai("gpt-6-astra", 10, 50, 1, 5, 25, 12.50, 25, 20, 75, 2, 25, 50)),
    ("gpt-6.1-sol", _openai("gpt-6.1-sol", 2, 10, 0.10, 1, 5, 2.50, 5, 4, 15, 0.20, 5, 10)),
    ("gpt-6.1-terra", _openai("gpt-6.1-terra", 1, 5, 0.05, 0.50, 2.50, 1.25, 2.50, 2, 7.50, 0.10, 2.50, 5)),
    ("gpt-6-luna", _openai("gpt-6-luna", 0.10, 0.50, 0.01, 0.05, 0.25, 0.125, 0.25, 0.20, 0.75, 0.02, 0.25, 0.50)),
    ("gpt-6-sol", _openai("gpt-6-sol", 2, 10, 0.20, 1, 5, 2.50, 5, 4, 15, 0.40, 5, 10)),
    ("gpt-5.6-cyber", _openai("gpt-5.6-cyber", 12.50, 75, 1.25, write_30m=15.625, write_12h=31.25)),
    ("gpt-5.6-sol", _openai("gpt-5.6-sol", 4, 20, 0.40, 2, 10, 5, 10, 8, 30, 0.80, 10, 20)),
    ("gpt-5.6-terra", _openai("gpt-5.6-terra", 2, 12, 0.20, 1, 6, 2.50, 5, 4, 18, 0.40, 5, 10)),
    ("gpt-5.6-luna", _openai("gpt-5.6-luna", 0.20, 1.20, 0.02, 0.10, 0.60, 0.25, 0.50, 0.40, 1.80, 0.04, 0.50, 1)),
    ("gpt-5.5-cyber", _openai("gpt-5.5-cyber", 12.50, 75, 1.25)),
    ("gpt-5.5-pro", _openai("gpt-5.5-pro", 30, 180, batch_input=15, batch_output=90, long_input=60, long_output=270)),
    ("gpt-5.5", _openai("gpt-5.5", 5, 30, 0.50, 2.50, 15, long_input=10, long_output=45, long_cache_read=1)),
    ("gpt-5.4-pro", _openai("gpt-5.4-pro", 30, 180, batch_input=15, batch_output=90, long_input=60, long_output=270)),
    ("gpt-5.4-mini", _openai("gpt-5.4-mini", 0.75, 4.50, 0.075, 0.375, 2.25)),
    ("gpt-5.4-nano", _openai("gpt-5.4-nano", 0.20, 1.25, 0.02, 0.10, 0.625)),
    ("gpt-5.4", _openai("gpt-5.4", 2.50, 15, 0.25, 1.25, 7.50, long_input=5, long_output=22.50, long_cache_read=0.50)),
    ("gpt-5.3-codex", _openai("gpt-5.3-codex", 1.75, 14, 0.175)),
    ("gpt-5.2-pro", _openai("gpt-5.2-pro", 21, 168, batch_input=10.50, batch_output=84)),
    ("gpt-5.2", _openai("gpt-5.2", 1.75, 14, 0.175, 0.875, 7)),
    ("gpt-5.1", _openai("gpt-5.1", 1.25, 10, 0.125, 0.625, 5)),
    ("gpt-5-search-api", _openai("gpt-5-search-api", 1.25, 10, 0.125)),
    ("gpt-5-pro", _openai("gpt-5-pro", 15, 120, batch_input=7.50, batch_output=60)),
    ("gpt-5-mini", _openai("gpt-5-mini", 0.25, 2, 0.025, 0.125, 1)),
    ("gpt-5-nano", _openai("gpt-5-nano", 0.05, 0.40, 0.005, 0.025, 0.20)),
    ("gpt-5", _openai("gpt-5", 1.25, 10, 0.125, 0.625, 5)),
    ("gpt-4.1-mini", _openai("gpt-4.1-mini", 0.40, 1.60, 0.10, 0.20, 0.80)),
    ("gpt-4.1-nano", _openai("gpt-4.1-nano", 0.10, 0.40, 0.025, 0.05, 0.20)),
    ("gpt-4.1", _openai("gpt-4.1", 2, 8, 0.50, 1, 4)),
    ("gpt-4o-mini-transcribe", _openai("gpt-4o-mini-transcribe", 1.25, 5)),
    ("gpt-4o-mini-tts", _openai("gpt-4o-mini-tts", 0.60, 0, output_audio=12)),
    ("gpt-4o-mini", _openai("gpt-4o-mini", 0.15, 0.60, 0.075, 0.075, 0.30)),
    ("gpt-4o-transcribe-diarize", _openai("gpt-4o-transcribe-diarize", 2.50, 10)),
    ("gpt-4o-transcribe", _openai("gpt-4o-transcribe", 2.50, 10)),
    ("gpt-4o-2024-05-13", _openai("gpt-4o-2024-05-13", 5, 15, batch_input=2.50, batch_output=7.50)),
    ("gpt-4o", _openai("gpt-4o", 2.50, 10, 1.25, 1.25, 5)),
    ("gpt-4-turbo-2024-04-09", _openai("gpt-4-turbo-2024-04-09", 10, 30, batch_input=5, batch_output=15)),
    ("gpt-4-0613", _openai("gpt-4-0613", 30, 60, batch_input=15, batch_output=30)),
    ("gpt-3.5-turbo-0125", _openai("gpt-3.5-turbo-0125", 0.50, 1.50, batch_input=0.25, batch_output=0.75)),
    ("gpt-3.5-turbo", _openai("gpt-3.5-turbo", 0.50, 1.50)),
    ("o1-pro", _openai("o1-pro", 150, 600, batch_input=75, batch_output=300)),
    ("o1", _openai("o1", 15, 60, 7.50, 7.50, 30)),
    ("o3-pro", _openai("o3-pro", 20, 80, batch_input=10, batch_output=40)),
    ("o3-mini", _openai("o3-mini", 1.10, 4.40, 0.55, 0.55, 2.20)),
    ("o3", _openai("o3", 2, 8, 0.50, 1, 4)),
    ("o4-mini", _openai("o4-mini", 1.10, 4.40, 0.275, 0.55, 2.20)),
    ("gpt-rosalind-research", _openai("gpt-rosalind-research", 5, 25, 0.50)),
    ("gpt-rosalind-discovery", _openai("gpt-rosalind-discovery", 5, 25, 0.50)),
    ("chat-latest", _openai("chat-latest", 5, 30, 0.50)),
    ("text-embedding-3-small", _openai("text-embedding-3-small", 0.02, 0)),
    ("text-embedding-3-large", _openai("text-embedding-3-large", 0.13, 0)),
    ("text-embedding-ada-002", _openai("text-embedding-ada-002", 0.10, 0)),
    ("omni-moderation-latest", _openai("omni-moderation-latest", 0, 0)),
    ("gpt-realtime-2.1-mini", _openai("gpt-realtime-2.1-mini", 0.60, 2.40, 0.06, input_audio=10, output_audio=20, cache_read_audio=0.30, input_image=0.80, cache_read_image=0.08)),
    ("gpt-realtime-2.1", _openai("gpt-realtime-2.1", 4, 24, 0.40, input_audio=32, output_audio=64, cache_read_audio=0.40, input_image=5, cache_read_image=0.50)),
    ("gpt-realtime-2", _openai("gpt-realtime-2", 4, 24, 0.40, input_audio=32, output_audio=64, cache_read_audio=0.40, input_image=5, cache_read_image=0.50)),
    ("gpt-realtime-1.5", _openai("gpt-realtime-1.5", 4, 16, 0.40, input_audio=32, output_audio=64, cache_read_audio=0.40, input_image=5, cache_read_image=0.50)),
    ("gpt-realtime-mini", _openai("gpt-realtime-mini", 0.60, 2.40, 0.06, input_audio=10, output_audio=20, cache_read_audio=0.30, input_image=0.80, cache_read_image=0.08)),
    ("gpt-realtime", _openai("gpt-realtime", 4, 16, 0.40, input_audio=32, output_audio=64, cache_read_audio=0.40, input_image=5, cache_read_image=0.50)),
    ("gpt-audio-1.5", _openai("gpt-audio-1.5", 2.50, 10, input_audio=32, output_audio=64)),
    ("gpt-audio-mini", _openai("gpt-audio-mini", 0.60, 2.40, input_audio=10, output_audio=20)),
    ("gpt-audio", _openai("gpt-audio", 2.50, 10, input_audio=32, output_audio=64)),
    ("gpt-image-2.5-sunburst", _openai("gpt-image-2.5-sunburst", 5, 0, 1.25, 2.50, 0, input_image=8, output_image=30, cache_read_image=2)),
    ("gpt-image-2.5-flare", _openai("gpt-image-2.5-flare", 5, 0, 1.25, 2.50, 0, input_image=8, output_image=30, cache_read_image=2)),
    ("gpt-image-2", _openai("gpt-image-2", 5, 0, 1.25, 2.50, 0, input_image=8, output_image=30, cache_read_image=2)),
    ("gpt-image-1.5", _openai("gpt-image-1.5", 5, 10, 1.25, 2.50, 5, input_image=8, output_image=32, cache_read_image=2)),
    ("gpt-image-1-mini", _openai("gpt-image-1-mini", 2, 0, 0.20, 1, 0, input_image=2.50, output_image=8, cache_read_image=0.25)),
    ("chatgpt-image-latest", _openai("chatgpt-image-latest", 5, 10, 1.25, 2.50, 5, input_image=8, output_image=32, cache_read_image=2)),
    ("ft:o4-mini-2025-04-16", _openai("ft:o4-mini-2025-04-16", 4, 16, 1, 2, 8)),
    ("ft:gpt-4.1-mini-2025-04-14", _openai("ft:gpt-4.1-mini-2025-04-14", 0.80, 3.20, 0.20, 0.40, 1.60)),
    ("ft:gpt-4.1-nano-2025-04-14", _openai("ft:gpt-4.1-nano-2025-04-14", 0.20, 0.80, 0.05, 0.10, 0.40)),
    ("ft:gpt-4.1-2025-04-14", _openai("ft:gpt-4.1-2025-04-14", 3, 12, 0.75, 1.50, 6)),
    ("ft:gpt-4o-mini-2024-07-18", _openai("ft:gpt-4o-mini-2024-07-18", 0.30, 1.20, 0.15, 0.15, 0.60)),
    ("ft:gpt-4o-2024-08-06", _openai("ft:gpt-4o-2024-08-06", 3.75, 15, 1.875, 2.225, 12.50)),
    ("ft:gpt-3.5-turbo", _openai("ft:gpt-3.5-turbo", 3, 6, batch_input=1.50, batch_output=3)),
    ("ft:davinci-002", _openai("ft:davinci-002", 12, 12, batch_input=6, batch_output=6)),
    ("ft:babbage-002", _openai("ft:babbage-002", 1.60, 1.60, batch_input=0.80, batch_output=0.90)),
)
AZURE_RATES = ()

_BY_PROVIDER = {
    "bedrock": BEDROCK_RATES,
    "openai": OPENAI_RATES,
    "azure": AZURE_RATES,
}


def rates_for(model_id, region=None, provider=None):
    """Longest model-id match, then the card for this region."""
    normalized = str(model_id or "").lower().replace("_", "-").replace(".", "-")
    best_key = ""
    matches = []
    for key, card in _cards(provider):
        normalized_key = key.lower().replace("_", "-").replace(".", "-")
        if normalized_key not in normalized or len(normalized_key) < len(best_key):
            continue
        if len(normalized_key) > len(best_key):
            best_key = normalized_key
            matches = [card]
        else:
            matches.append(card)
    if not matches:
        return None
    return _for_region(matches, region, normalized)


def estimate_cost(model_id, usage, region=None, provider=None):
    """USD for one call. None when this model has no card for the region."""
    card = rates_for(model_id, region=region, provider=provider)
    if card is None or not isinstance(usage, dict):
        return None
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    if input_tokens is None and output_tokens is None:
        return None
    return (
        _portion(input_tokens, card.input_per_million)
        + _portion(output_tokens, card.output_per_million)
        + _portion(usage.get("cached_tokens"), card.cache_read_per_million)
        + _portion(usage.get("cache_write_5m_tokens"), card.cache_write_5m_per_million)
        + _portion(usage.get("cache_write_1h_tokens"), card.cache_write_1h_per_million)
        + _portion(usage.get("reasoning_tokens"), card.reasoning_per_million)
    )


def _cards(provider):
    if provider is None:
        for pool in _BY_PROVIDER.values():
            yield from pool
        return
    yield from _BY_PROVIDER.get(provider, ())


def _for_region(cards, region, normalized):
    if normalized.startswith("global-"):
        for card in cards:
            if (card.region or "").lower() == "global":
                return card
    wanted = str(region or "").strip().lower()
    if wanted:
        for card in cards:
            if (card.region or "").lower() == wanted:
                return card
    for card in cards:
        if not card.region:
            return card
    return None


def _portion(tokens, price_per_million):
    if not tokens or price_per_million is None:
        return 0.0
    return (tokens / 1_000_000) * price_per_million
