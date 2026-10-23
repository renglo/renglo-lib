"""In-code Provider, Model, Deployment, and Alias registry. Built from config."""

from __future__ import annotations

from dataclasses import dataclass

from renglo.llm.types import CAPABILITY_TEXT

OPENAI_PUBLIC_ENDPOINT = "https://api.openai.com/v1"
FAST_MODEL_ID = "gpt-4o-mini"
PRICE_VERSION = "1"
PRICE_EFFECTIVE_FROM = "2026-01-01"
INPUT_PER_MILLION = 2.0
OUTPUT_PER_MILLION = 8.0

ALIAS_DEFAULT = "default"
ALIAS_REASONING = "reasoning"
ALIAS_FAST = "fast"
ALIAS_ECONOMY = "economy"
ALIAS_CLAUDE = "claude"
KNOWN_ALIASES = (
    ALIAS_DEFAULT,
    ALIAS_REASONING,
    ALIAS_FAST,
    ALIAS_ECONOMY,
    ALIAS_CLAUDE,
)

PROVIDER_BEDROCK = "bedrock"
PROVIDER_OPENAI = "openai"
FAMILY_CONVERSE = "bedrock_converse"
FAMILY_OPENAI_CHAT = "openai_chat"
CREDENTIAL_AWS_IAM = "aws_iam"
CREDENTIAL_OPENAI = "OPENAI_API_KEY"

DEPLOYMENT_BEDROCK = "bedrock_default"
DEPLOYMENT_OPENAI = "openai_fast"
DEPLOYMENT_CLAUDE = "bedrock_claude"


@dataclass
class Pricing:
    input_per_million: float
    output_per_million: float
    currency: str = "USD"
    version: str = PRICE_VERSION
    effective_from: str = PRICE_EFFECTIVE_FROM


@dataclass
class ProviderRecord:
    provider_id: str
    name: str
    adapter: str
    enabled: bool = True


@dataclass
class ModelRecord:
    model_id: str
    capabilities: list
    context_window: int
    max_output_tokens: int
    pricing: Pricing


@dataclass
class DeploymentRecord:
    deployment_id: str
    provider_id: str
    model_id: str
    api_family: str
    endpoint: str | None
    credential_ref: str
    enabled: bool = True
    region: str | None = None


@dataclass
class AliasRecord:
    alias: str
    deployments: list
    routing_policy: str = "priority"
    reasoning_effort: str | None = None
    enabled: bool = True


@dataclass
class Registry:
    providers: dict
    models: dict
    deployments: dict
    aliases: dict
    content_logging: bool = False
    region: str = "us-east-1"


def _pricing():
    return Pricing(
        input_per_million=INPUT_PER_MILLION,
        output_per_million=OUTPUT_PER_MILLION,
    )


def _text_model(model_id, context_window=128000, max_output_tokens=8192):
    return ModelRecord(
        model_id=model_id,
        capabilities=[CAPABILITY_TEXT],
        context_window=context_window,
        max_output_tokens=max_output_tokens,
        pricing=_pricing(),
    )


def build_registry(config) -> Registry:
    """Build the v1 registry. A later control plane can replace this source."""
    config = config or {}
    region = (
        config.get("AWS_REGION")
        or config.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    bedrock_model = (config.get("BEDROCK_MODEL") or "").strip()
    claude_model = (config.get("BEDROCK_CONVERSE_MODEL") or "").strip()
    openai_base = (config.get("OPENAI_BASE_URL") or "").strip() or OPENAI_PUBLIC_ENDPOINT

    providers = {
        PROVIDER_BEDROCK: ProviderRecord(
            provider_id=PROVIDER_BEDROCK,
            name="Amazon Bedrock",
            adapter=FAMILY_CONVERSE,
        ),
        PROVIDER_OPENAI: ProviderRecord(
            provider_id=PROVIDER_OPENAI,
            name="OpenAI",
            adapter=FAMILY_OPENAI_CHAT,
        ),
    }

    models = {}
    if bedrock_model:
        models[bedrock_model] = _text_model(bedrock_model)
    if claude_model and claude_model not in models:
        models[claude_model] = _text_model(claude_model)
    models[FAST_MODEL_ID] = _text_model(FAST_MODEL_ID, max_output_tokens=16384)

    deployments = {}
    if bedrock_model:
        deployments[DEPLOYMENT_BEDROCK] = DeploymentRecord(
            deployment_id=DEPLOYMENT_BEDROCK,
            provider_id=PROVIDER_BEDROCK,
            model_id=bedrock_model,
            api_family=FAMILY_CONVERSE,
            endpoint=None,
            credential_ref=CREDENTIAL_AWS_IAM,
            region=region,
        )
    deployments[DEPLOYMENT_OPENAI] = DeploymentRecord(
        deployment_id=DEPLOYMENT_OPENAI,
        provider_id=PROVIDER_OPENAI,
        model_id=FAST_MODEL_ID,
        api_family=FAMILY_OPENAI_CHAT,
        endpoint=openai_base,
        credential_ref=CREDENTIAL_OPENAI,
        region=None,
    )
    if claude_model:
        deployments[DEPLOYMENT_CLAUDE] = DeploymentRecord(
            deployment_id=DEPLOYMENT_CLAUDE,
            provider_id=PROVIDER_BEDROCK,
            model_id=claude_model,
            api_family=FAMILY_CONVERSE,
            endpoint=None,
            credential_ref=CREDENTIAL_AWS_IAM,
            region=region,
        )

    bedrock_ids = [DEPLOYMENT_BEDROCK] if bedrock_model else []
    aliases = {
        ALIAS_DEFAULT: AliasRecord(
            alias=ALIAS_DEFAULT,
            deployments=list(bedrock_ids),
            reasoning_effort="low",
            enabled=bool(bedrock_model),
        ),
        ALIAS_REASONING: AliasRecord(
            alias=ALIAS_REASONING,
            deployments=list(bedrock_ids),
            reasoning_effort="high",
            enabled=bool(bedrock_model),
        ),
        ALIAS_FAST: AliasRecord(
            alias=ALIAS_FAST,
            deployments=[DEPLOYMENT_OPENAI],
        ),
        ALIAS_ECONOMY: AliasRecord(
            alias=ALIAS_ECONOMY,
            deployments=[DEPLOYMENT_OPENAI],
        ),
        ALIAS_CLAUDE: AliasRecord(
            alias=ALIAS_CLAUDE,
            deployments=[DEPLOYMENT_CLAUDE] if claude_model else [],
            enabled=bool(claude_model),
        ),
    }

    return Registry(
        providers=providers,
        models=models,
        deployments=deployments,
        aliases=aliases,
        content_logging=False,
        region=region,
    )


def estimate_cost(usage, pricing):
    """Fill estimated cost from reported input and output tokens and the registry price."""
    if usage is None or pricing is None:
        return None
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    if input_tokens is None and output_tokens is None:
        return None
    input_tokens = input_tokens or 0
    output_tokens = output_tokens or 0
    return (
        (input_tokens / 1_000_000) * pricing.input_per_million
        + (output_tokens / 1_000_000) * pricing.output_per_million
    )
