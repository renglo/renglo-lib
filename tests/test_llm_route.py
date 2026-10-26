"""The Console generate route stays on aliases and returns the call record."""

import importlib.util
import json
from pathlib import Path

import pytest


class RecordingController:
    def __init__(self):
        self.calls = []

    def generate(self, messages, model="default", **kwargs):
        self.calls.append({"messages": messages, "model": model, **kwargs})
        return {
            "success": True,
            "action": "generate",
            "output": {
                "text": "hello",
                "provider": "bedrock",
                "requested_model": model,
                "model": "openai.gpt-5.5",
                "execution": {
                    "execution_id": "llm_exec_1",
                    "status": "completed",
                    "context": kwargs.get("context"),
                    "usage": {"input_tokens": 4, "output_tokens": 2, "reasoning_tokens": 1, "cached_tokens": 0},
                    "cost": {"estimated": 0.02, "provider_reported": None, "reconciled": None, "currency": "USD"},
                },
                "attempts": [{
                    "attempt_id": "llm_attempt_1",
                    "execution_id": "llm_exec_1",
                    "deployment_id": "bedrock_default",
                    "provider_id": "bedrock",
                    "model_id": "openai.gpt-5.5",
                    "status": "completed",
                    "error_type": None,
                    "duration_ms": 840,
                    "usage": {"input_tokens": 4, "output_tokens": 2, "reasoning_tokens": 1, "cached_tokens": 0},
                    "cost": {"estimated": 0.02, "currency": "USD"},
                    "credential": "hidden",
                }],
                "provider_output": {
                    "api_key": "hidden",
                    "stopReason": "end_turn",
                    "ResponseMetadata": {"RequestId": "req-1"},
                    "output": {"message": {"content": [{"text": "prompt echo"}]}},
                },
            },
        }


def load_routes():
    flask = pytest.importorskip("flask")
    flask_cognito = pytest.importorskip("flask_cognito")
    route_path = Path(__file__).resolve().parents[2] / "renglo-api" / "renglo_api" / "routes" / "llm_routes.py"
    spec = importlib.util.spec_from_file_location("llm_routes_under_test", route_path)
    routes = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(routes)
    app = flask.Flask(__name__)
    app.renglo_config = {}
    app.config["COGNITO_REGION"] = "us-east-1"
    app.config["COGNITO_USERPOOL_ID"] = "us-east-1_test"
    app.config["COGNITO_APP_CLIENT_ID"] = "client"
    flask_cognito.CognitoAuth(app)
    app.register_blueprint(routes.app_llm)
    controller = RecordingController()
    routes.LLC = controller
    return app, routes, controller


def test_models_route_lists_enabled_models_and_blocks_a_missing_alias():
    from renglo.llm import LlmController

    app, routes, _controller = load_routes()
    routes.LLC = LlmController(config={"OPENAI_API_KEY": "sk-secret"})
    with app.test_request_context("/_llm/portfolio_y/org_x/models"):
        body, status = routes.route_models.__wrapped__("portfolio_y", "org_x")
    assert status == 200
    payload = body.get_json()
    by_key = {(item["provider"], item["model"]): item for item in payload["output"]["models"]}
    assert by_key[("bedrock", None)]["enabled"] is False
    assert by_key[("bedrock", None)]["reason"] == "BEDROCK_MODEL is not set"
    assert by_key[("openai", "gpt-4o-mini")]["enabled"] is True
    assert by_key[("openai", "gpt-4o-mini")]["source"] == "OpenAI API"
    assert "sk-secret" not in json.dumps(payload)

    status, denied = post(
        app,
        routes,
        "portfolio_y",
        "org_x",
        {"messages": [{"role": "user", "content": "hi"}], "model": "default", "provider": "bedrock"},
    )
    assert status == 400
    assert denied["error"] == "BEDROCK_MODEL is not set"


def post(app, routes, portfolio, org, payload):
    with app.test_request_context(
        f"/_llm/{portfolio}/{org}/generate",
        method="POST",
        data=json.dumps(payload),
        content_type="application/json",
    ):
        body, status = routes.route_generate.__wrapped__(portfolio, org)
    return status, body.get_json()


def test_generate_route_uses_the_alias_and_hides_provider_payload():
    app, routes, controller = load_routes()
    client = app.test_client()
    denied = client.post("/_llm/portfolio_y/org_x/generate", json={"messages": [{"role": "user", "content": "hi"}]})
    assert denied.status_code == 401

    status, payload = post(
        app,
        routes,
        "portfolio_y",
        "org_x",
        {"messages": [{"role": "user", "content": "Say hello"}], "model": "fast"},
    )
    assert status == 200
    assert payload["output"]["text"] == "hello"
    assert payload["output"]["source"] == "bedrock"
    assert payload["output"]["model"] == "openai.gpt-5.5"
    assert payload["output"]["response_time_ms"] == 840
    assert payload["output"]["stop_reason"] == "end_turn"
    assert payload["output"]["request_id"] == "req-1"
    assert payload["output"]["usage"]["input_tokens"] == 4
    assert payload["output"]["usage"]["output_tokens"] == 2
    assert payload["output"]["cost"]["estimated"] == 0.02
    assert payload["output"]["limits"]["max_output_tokens"] == 256
    assert payload["output"]["attempts"][0]["deployment_id"] == "bedrock_default"
    assert payload["output"]["attempts"][0]["source"] == "bedrock"
    assert "provider_output" not in payload["output"]
    encoded = json.dumps(payload)
    assert "credential" not in encoded
    assert "hidden" not in encoded
    assert "prompt echo" not in encoded
    call = controller.calls[0]
    assert call["model"] == "fast"
    assert call["max_output_tokens"] == 256
    assert call["context"] == {
        "portfolio_id": "portfolio_y",
        "org_id": "org_x",
        "extension_id": "data",
    }
    assert call.get("provider") in (None, "")


def test_generate_route_rejects_provider_controls_and_a_long_prompt():
    app, routes, controller = load_routes()
    status, payload = post(
        app,
        routes,
        "portfolio_y",
        "org_x",
        {"messages": [{"role": "user", "content": "hi"}], "provider": "openai"},
    )
    assert status == 400
    assert payload["error"] == "provider is not accepted"
    assert controller.calls == []

    status, payload = post(app, routes, "portfolio_y", " ", {"messages": [{"role": "user", "content": "hi"}]})
    assert status == 400

    status, payload = post(
        app,
        routes,
        "portfolio_y",
        "org_x",
        {"messages": [{"role": "user", "content": "x" * 8001}]},
    )
    assert status == 400
    assert controller.calls == []
