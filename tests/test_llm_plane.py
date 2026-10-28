"""LLM plane: Converse and OpenAI chat share one canonical generate()."""

import importlib
import logging

import pytest

from renglo.llm import LlmController
from renglo.llm.providers.bedrock_converse import BedrockConverseAdapter
from renglo.llm.providers.openai_chat import OpenAIChatAdapter
from renglo.llm.types import BLOCK_TOOL_CALL, SCHEMA_VERSION


class StatusError(Exception):
    def __init__(self, status_code, message="error"):
        super().__init__(message)
        self.status_code = status_code


class FakeConverse:
    def __init__(self, responses):
        self.calls = []
        self.responses = list(responses)

    def converse(self, **kwargs):
        self.calls.append(kwargs)
        if not self.responses:
            raise StatusError(500, "unexpected converse call")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _Completions:
    def __init__(self, owner):
        self.owner = owner

    def create(self, **kwargs):
        self.owner.calls.append(kwargs)
        if not self.owner.responses:
            raise StatusError(500, "unexpected openai call")
        item = self.owner.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeOpenAI:
    def __init__(self, responses):
        self.calls = []
        self.responses = list(responses)
        self.chat = type("Chat", (), {})()
        self.chat.completions = _Completions(self)


def converse_ok(text="hello", input_tokens=10, output_tokens=4):
    return {
        "output": {"message": {"role": "assistant", "content": [{"text": text}]}},
        "usage": {"inputTokens": input_tokens, "outputTokens": output_tokens},
        "ResponseMetadata": {"RequestId": "req-1"},
    }


def openai_ok(text="hello", prompt_tokens=11, completion_tokens=6, cached=2, reasoning=3):
    return {
        "id": "chatcmpl-1",
        "choices": [{"message": {"role": "assistant", "content": text}}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "prompt_tokens_details": {"cached_tokens": cached},
            "completion_tokens_details": {"reasoning_tokens": reasoning},
        },
    }


def plane_config(**overrides):
    config = {
        "BEDROCK_MODELS": "openai.gpt-5.5,anthropic.claude-sonnet-4",
        "OPENAI_MODELS": "gpt-4o-mini",
        "OPENAI_API_KEY": "sk-test-key",
        "AWS_REGION": "us-east-1",
    }
    config.update(overrides)
    return config


def make_controller(converse, openai_client, config=None, factory_log=None):
    def factory(api_key, base_url, timeout_seconds=None):
        if factory_log is not None:
            factory_log.append(
                {
                    "api_key": api_key,
                    "base_url": base_url,
                    "timeout_seconds": timeout_seconds,
                }
            )
        return openai_client

    return LlmController(
        config if config is not None else plane_config(),
        adapters={
            "bedrock": BedrockConverseAdapter(client=converse),
            "openai": OpenAIChatAdapter(client_factory=factory),
        },
        sleep=lambda _seconds: None,
    )


def test_named_openai_model_is_the_one_called():
    openai_client = FakeOpenAI([openai_ok("chosen")])
    controller = make_controller(
        FakeConverse([]),
        openai_client,
        config=plane_config(OPENAI_MODELS="gpt-4.1-mini,gpt-4o-mini"),
    )
    result = controller.generate(
        messages=[{"role": "user", "content": "hi"}],
        model="gpt-4.1-mini",
        provider="openai",
    )
    assert result["success"]
    assert result["output"]["model"] == "gpt-4.1-mini"
    assert openai_client.calls[0]["model"] == "gpt-4.1-mini"


def test_provider_catalog_lists_every_configured_model():
    from renglo.llm.registry import build_registry

    registry = build_registry(
        {
            "BEDROCK_MODELS": "haiku, sonnet-4, sonnet-5",
            "OPENAI_MODELS": "gpt-4o, gpt-4o-mini",
            "OPENAI_API_KEY": "sk-test",
        }
    )
    bedrock = [entry for entry in registry.catalog if entry.provider_id == "bedrock"]
    openai = [entry for entry in registry.catalog if entry.provider_id == "openai"]
    assert [entry.model_id for entry in bedrock] == ["haiku", "sonnet-4", "sonnet-5"]
    assert all(entry.is_default is False for entry in bedrock)
    assert all(entry.available for entry in bedrock)
    assert [entry.model_id for entry in openai] == ["gpt-4o", "gpt-4o-mini"]
    assert all(entry.is_default is False for entry in openai)


def test_registry_module_does_not_name_a_provider():
    from pathlib import Path
    import renglo.llm.registry as registry

    text = Path(registry.__file__).read_text(encoding="utf-8").lower()
    assert "bedrock" not in text
    assert "openai" not in text


def test_aliases_route_to_converse_effort_and_openai_fast():
    converse = FakeConverse([converse_ok("low"), converse_ok("high"), converse_ok("claude")])
    openai_client = FakeOpenAI([openai_ok("fast")])
    controller = make_controller(converse, openai_client)

    low = controller.generate(
        messages=[{"role": "user", "content": "default please"}],
        model="openai.gpt-5.5",
        provider_options={"reasoning": {"effort": "low"}},
    )
    high = controller.generate(
        messages=[{"role": "user", "content": "think"}],
        model="openai.gpt-5.5",
        provider_options={"reasoning": {"effort": "high"}},
    )
    claude = controller.generate(messages=[{"role": "user", "content": "claude"}], model="anthropic.claude-sonnet-4")
    fast = controller.generate(messages=[{"role": "user", "content": "quick"}], model="gpt-4o-mini")

    assert low["success"] and low["output"]["text"] == "low"
    assert converse.calls[0]["modelId"] == "openai.gpt-5.5"
    assert converse.calls[0]["additionalModelRequestFields"]["reasoning"]["effort"] == "low"
    assert converse.calls[1]["additionalModelRequestFields"]["reasoning"]["effort"] == "high"
    assert converse.calls[2]["modelId"] == "anthropic.claude-sonnet-4"
    assert "additionalModelRequestFields" not in converse.calls[2]
    assert fast["output"]["text"] == "fast"
    assert openai_client.calls[0]["model"] == "gpt-4o-mini"
    assert fast["output"]["provider"] == "openai"


def test_two_converse_model_ids_share_message_shape():
    converse = FakeConverse([converse_ok("a"), converse_ok("b")])
    controller = make_controller(converse, FakeOpenAI([]))
    messages = [{"role": "user", "content": "same shape"}]
    controller.generate(messages=messages, model="openai.gpt-5.5")
    controller.generate(messages=messages, model="anthropic.claude-sonnet-4")
    assert converse.calls[0]["modelId"] != converse.calls[1]["modelId"]
    assert converse.calls[0]["messages"] == converse.calls[1]["messages"]


def test_self_hosted_openai_base_url_is_the_endpoint():
    openai_client = FakeOpenAI([openai_ok("local")])
    seen = []
    controller = make_controller(
        FakeConverse([]),
        openai_client,
        config=plane_config(OPENAI_BASE_URL="http://127.0.0.1:8080/v1"),
        factory_log=seen,
    )
    result = controller.generate(messages=[{"role": "user", "content": "hi"}], model="gpt-4o-mini")
    assert result["output"]["text"] == "local"
    assert seen[0]["base_url"] == "http://127.0.0.1:8080/v1"


def test_explicit_provider_and_provider_options_stay_on_that_adapter():
    converse = FakeConverse([])
    openai_client = FakeOpenAI([openai_ok("routed")])
    controller = make_controller(converse, openai_client)
    result = controller.generate(
        messages=[{"role": "user", "content": "hi"}],
        model="gpt-4o",
        provider="openai",
        provider_options={"temperature": 0.2},
    )
    assert result["success"]
    assert result["output"]["model"] == "gpt-4o"
    assert openai_client.calls[0]["temperature"] == 0.2
    assert openai_client.calls[0]["messages"][0]["content"] == "hi"
    assert converse.calls == []


def test_same_canonical_messages_translate_differently():
    converse = FakeConverse([converse_ok("from-converse")])
    openai_client = FakeOpenAI([openai_ok("from-openai")])
    controller = make_controller(converse, openai_client)
    messages = [
        {"role": "system", "content": [{"type": "text", "text": "Be brief."}]},
        {"role": "user", "content": [{"type": "text", "text": "Question"}]},
        {"role": "assistant", "content": [{"type": "text", "text": "Earlier"}]},
    ]
    converse_result = controller.generate(messages=messages, model="openai.gpt-5.5")
    openai_result = controller.generate(messages=messages, model="gpt-4o-mini")

    native = converse.calls[0]
    assert native["system"] == [{"text": "Be brief."}]
    assert native["messages"] == [
        {"role": "user", "content": [{"text": "Question"}]},
        {"role": "assistant", "content": [{"text": "Earlier"}]},
    ]
    assert openai_client.calls[0]["messages"] == [
        {"role": "system", "content": "Be brief."},
        {"role": "user", "content": "Question"},
        {"role": "assistant", "content": "Earlier"},
    ]
    assert converse_result["output"]["text"] == "from-converse"
    assert openai_result["output"]["text"] == "from-openai"


def test_tool_call_block_is_unsupported_on_text_models():
    converse = FakeConverse([converse_ok()])
    controller = make_controller(converse, FakeOpenAI([]))
    result = controller.generate(
        messages=[
            {
                "role": "assistant",
                "content": [
                    {
                        "type": BLOCK_TOOL_CALL,
                        "id": "call_1",
                        "name": "lookup",
                        "arguments": {},
                    }
                ],
            }
        ],
        model="openai.gpt-5.5",
    )
    assert result["success"] is False
    assert result["error_type"] == "unsupported"
    assert converse.calls == []


def test_adapters_receive_resolved_credentials_not_the_config_dict():
    converse = FakeConverse([converse_ok()])
    openai_client = FakeOpenAI([openai_ok()])
    seen = []
    config = plane_config(OPENAI_API_KEY="sk-resolved")
    controller = make_controller(converse, openai_client, config=config, factory_log=seen)
    controller.generate(messages=[{"role": "user", "content": "iam"}], model="openai.gpt-5.5")
    controller.generate(messages=[{"role": "user", "content": "key"}], model="gpt-4o-mini")
    bedrock = controller.LMM.adapters["bedrock"]
    openai = controller.LMM.adapters["openai"]
    assert bedrock.last_credential == "aws_iam"
    assert seen[0]["api_key"] == "sk-resolved"
    assert openai.last_credential == "sk-resolved"
    assert not hasattr(bedrock, "config")
    assert not hasattr(openai, "config")


def test_retry_429_then_200_is_two_attempts_and_one_execution():
    converse = FakeConverse(
        [StatusError(429, "slow down"), converse_ok("ok", input_tokens=9, output_tokens=2)]
    )
    controller = make_controller(converse, FakeOpenAI([]))
    result = controller.generate(messages=[{"role": "user", "content": "retry"}], model="openai.gpt-5.5")
    output = result["output"]
    assert result["success"]
    assert len(output["attempts"]) == 2
    assert output["attempts"][0]["error_type"] == "rate_limit"
    assert output["attempts"][0]["execution_id"] == output["attempts"][1]["execution_id"]
    assert output["attempts"][1]["execution_id"] == output["execution"]["execution_id"]
    assert output["execution"]["usage"]["input_tokens"] == 9
    assert output["execution"]["usage"]["output_tokens"] == 2
    assert output["execution"]["usage"] == output["attempts"][1]["usage"]


def test_400_does_not_retry_or_fall_back():
    converse = FakeConverse([StatusError(400, "bad request"), converse_ok("nope")])
    openai_client = FakeOpenAI([openai_ok("should-not-run")])
    controller = make_controller(converse, openai_client)
    result = controller.generate(
        messages=[{"role": "user", "content": "bad"}],
        model="openai.gpt-5.5",
        fallback=["gpt-4o-mini"],
    )
    assert result["success"] is False
    assert result["error_type"] == "invalid_request"
    assert len(converse.calls) == 1
    assert openai_client.calls == []
    assert len(result["output"]["attempts"]) == 1


def test_fallback_economy_runs_after_converse_retries_are_exhausted():
    converse = FakeConverse(
        [StatusError(500, "down"), StatusError(500, "down"), StatusError(503, "down")]
    )
    openai_client = FakeOpenAI([openai_ok("saved", prompt_tokens=4, completion_tokens=5)])
    controller = make_controller(converse, openai_client)
    result = controller.generate(
        messages=[{"role": "user", "content": "please"}],
        model="openai.gpt-5.5",
        fallback=["gpt-4o-mini"],
    )
    assert result["success"]
    assert result["output"]["text"] == "saved"
    assert result["output"]["model"] == "gpt-4o-mini"
    assert len(converse.calls) == 3
    assert len(openai_client.calls) == 1
    assert len(result["output"]["attempts"]) == 4
    assert len({row["execution_id"] for row in result["output"]["attempts"]}) == 1
    assert result["output"]["execution"]["usage"]["input_tokens"] == 4


def test_stream_tools_and_output_schema_are_unsupported():
    converse = FakeConverse([converse_ok(), converse_ok(), converse_ok()])
    controller = make_controller(converse, FakeOpenAI([]))
    messages = [{"role": "user", "content": "hi"}]
    for kwargs in (
        {"stream": True},
        {"tools": [{"name": "lookup"}]},
        {"output_schema": {"type": "object"}},
    ):
        result = controller.generate(messages=messages, model="openai.gpt-5.5", **kwargs)
        assert result["error_type"] == "unsupported"
    assert converse.calls == []


def test_context_is_copied_and_not_checked():
    controller = make_controller(FakeConverse([converse_ok()]), FakeOpenAI([]))
    result = controller.generate(
        messages=[{"role": "user", "content": "hi"}],
        model="openai.gpt-5.5",
        context={"org_id": "org_1", "extension_id": "lab", "not_a_field": "ignored"},
    )
    assert result["output"]["execution"]["context"] == {
        "org_id": "org_1",
        "extension_id": "lab",
    }


def test_usage_tokens_come_from_the_provider_and_cost_uses_the_rate_card():
    controller = make_controller(
        FakeConverse([
            converse_ok("unpriced", input_tokens=1_000_000, output_tokens=500_000),
            converse_ok("priced", input_tokens=1_000_000, output_tokens=500_000),
        ]),
        FakeOpenAI([]),
    )
    unpriced = controller.generate(messages=[{"role": "user", "content": "cost"}], model="openai.gpt-5.5")
    assert unpriced["output"]["execution"]["cost"]["estimated"] is None
    result = controller.generate(messages=[{"role": "user", "content": "cost"}], model="anthropic.claude-sonnet-4")
    usage = result["output"]["execution"]["usage"]
    cost = result["output"]["execution"]["cost"]
    assert usage["input_tokens"] == 1_000_000
    assert usage["output_tokens"] == 500_000
    assert cost["estimated"] == 3.0 + (500_000 / 1_000_000) * 15.0
    assert cost["provider_reported"] is None
    assert cost["reconciled"] is None
    assert result["schema_version"] == SCHEMA_VERSION == 1


def test_openai_usage_tokens_are_copied():
    controller = make_controller(FakeConverse([]), FakeOpenAI([openai_ok("u")]))
    result = controller.generate(messages=[{"role": "user", "content": "u"}], model="gpt-4o-mini")
    usage = result["output"]["execution"]["usage"]
    assert usage["input_tokens"] == 11
    assert usage["output_tokens"] == 6
    assert usage["cached_tokens"] == 2
    assert usage["reasoning_tokens"] == 3
    assert result["output"]["provider_output"]["id"] == "chatcmpl-1"


def test_logs_omit_api_key_and_prompt(caplog):
    prompt = "PROMPT-SECRET-do-not-log"
    config = plane_config(OPENAI_API_KEY="sk-log-secret")
    controller = make_controller(
        FakeConverse([converse_ok()]),
        FakeOpenAI([openai_ok()]),
        config=config,
    )
    with caplog.at_level(logging.INFO, logger="renglo"):
        controller.generate(messages=[{"role": "user", "content": prompt}], model="openai.gpt-5.5")
        controller.generate(messages=[{"role": "user", "content": prompt}], model="gpt-4o-mini")
    assert "llm_attempt" in caplog.text
    assert prompt not in caplog.text
    assert "sk-log-secret" not in caplog.text


def test_removed_modules_are_not_importable():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("renglo.llm.bedrock_openai")
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("renglo.agent.llm")
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("renglo.agent.llm.llm_openai")


def test_controller_has_no_openai_call():
    controller = LlmController(plane_config())
    assert not hasattr(controller, "openai_call")
    assert not hasattr(LlmController, "openai_call")
