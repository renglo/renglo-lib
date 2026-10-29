"""Published model rates price token counts without a provider price."""

from renglo.llm.providers.rates import estimate_cost, rates_for


def test_model_ids_match_the_longest_card():
    haiku = rates_for("us.anthropic.claude-haiku-5-5")
    sonnet = rates_for("us.anthropic.claude-sonnet-5-5")
    long_context = rates_for("us.anthropic.claude-haiku-5-5-long-context")
    sonnet_45 = rates_for("us.anthropic.claude-sonnet-4-5-20250929-v1:0")
    assert haiku.input_per_million == 0.10
    assert haiku.output_per_million == 0.50
    assert sonnet.input_per_million == 2.00
    assert sonnet.output_per_million == 10.00
    assert sonnet.input_per_million == haiku.input_per_million * 20
    assert sonnet.output_per_million == haiku.output_per_million * 20
    assert long_context.input_per_million == 0.50
    assert sonnet_45.input_per_million == 3.00
    assert rates_for("anthropic.claude-sonnet-4").cache_write_1h_per_million is None
    assert rates_for("us.anthropic.claude-haiku-5-5", region="us-east-1", provider="bedrock") is haiku
    mini = rates_for("gpt-4o-mini", provider="openai")
    assert mini.input_per_million == 0.15
    assert mini.output_per_million == 0.60
    assert mini.cache_read_per_million == 0.075
    assert rates_for("gpt-4o", provider="openai").input_per_million == 2.50
    assert rates_for("gpt-4o-mini-tts", provider="openai").output_audio_per_million == 12
    assert rates_for("gpt-6-astra", provider="openai").long_input_per_million == 20
    assert rates_for("gpt-6-astra", provider="openai").cache_write_30m_per_million == 12.50


def test_nova_prices_are_for_us_east_1():
    lite = rates_for("us.amazon.nova-2-lite-v1:0", region="us-east-1", provider="bedrock")
    omni = rates_for("amazon.nova-2-omni", region="us-east-1")
    pro = rates_for("amazon.nova-2-pro", region="us-east-1")
    assert lite.provider == "bedrock"
    assert lite.region == "us-east-1"
    assert lite.input_per_million == 0.30
    assert lite.output_per_million == 2.50
    assert lite.input_video_per_million == 0.30
    assert lite.cache_read_per_million == lite.input_per_million * 0.25
    assert omni.input_audio_per_million == 1.00
    assert omni.output_image_per_million == 40.00
    assert pro.input_audio_per_million == 1.25
    assert pro.output_per_million == 10.00
    assert pro.output_image_per_million is None
    assert rates_for("us.amazon.nova-2-lite-v1:0", region="us-west-2") is None
    assert rates_for("us.amazon.nova-2-lite-v1:0") is None
    usage = {"input_tokens": 1_000_000, "output_tokens": 1_000_000, "cached_tokens": 1_000_000}
    assert estimate_cost("us.amazon.nova-2-lite-v1:0", usage, region="us-east-1", provider="bedrock") == (
        lite.input_per_million + lite.output_per_million + lite.cache_read_per_million
    )


def test_published_family_prices_follow_the_region():
    assert rates_for("us.deepseek.r1-v1:0", region="us-east-1").input_per_million == 1.35
    assert rates_for("us.deepseek.r1-v1:0", region="us-west-2") is None
    assert rates_for("deepseek.v3.2", region="us-east-2").output_per_million == 1.85
    assert rates_for("deepseek.v3.2", region="eu-north-1").input_per_million == 0.74
    assert rates_for("deepseek.v3.2", region="ap-southeast-2").output_per_million == 1.9055
    assert rates_for("deepseek.v3.1", region="ap-southeast-2").input_per_million == 0.5974
    assert rates_for("google.gemma-3-27b-it", region="ap-southeast-2").output_per_million == 0.3914
    assert rates_for("google.gemma-4-31b", region="eu-central-1").input_per_million == 0.17
    assert rates_for("google.gemma-4-31b", region="eu-west-2") is None
    assert rates_for("meta.llama4-maverick-17b-instruct-v1:0", region="us-east-1").batch_output_per_million == 0.485
    assert rates_for("meta.llama3-2-1b-instruct-v1:0", region="us-east-1").input_per_million == 0.10
    assert rates_for("meta.llama3-2-11b-instruct-v1:0", region="us-east-1").input_per_million == 0.16
    assert rates_for("nvidia.nemotron-nano-2-vl", region="us-gov-east-1").output_per_million == 0.720
    assert rates_for("nvidia.nemotron-nano-2", region="eu-west-2").input_per_million == 0.09
    assert rates_for("us.xai.grok-4-7", region="us-east-1").cache_read_per_million == 0.55
    assert rates_for("global.xai.grok-4-7", region="us-east-1").input_per_million == 2.00


def test_estimate_prices_input_output_cache_and_skips_unpriced_reasoning():
    usage = {
        "input_tokens": 1_000_000,
        "output_tokens": 2_000_000,
        "cached_tokens": 4_000_000,
        "cache_write_5m_tokens": 1_000_000,
        "cache_write_1h_tokens": 1_000_000,
        "reasoning_tokens": 9_000_000,
    }
    # Haiku 5.5: 0.10 + 1.00 + 0.04 + 0.125 + 0.20. Reasoning has no separate price.
    assert estimate_cost("us.anthropic.claude-haiku-5-5", usage) == 1.465
    assert estimate_cost("gpt-4o-mini", usage, provider="openai") == 0.15 + 1.20 + 0.30
    assert estimate_cost("us.anthropic.claude-haiku-5-5", {}) is None
