"""CLOUD chooses a provider when one model id is listed more than once.

A model that only one provider offers still uses that provider. Callers can
name a provider and keep using Bedrock, Microsoft Foundry, the OpenAI API, or a
self-hosted endpoint on any cloud.
"""

from __future__ import annotations


_PROVIDER_FOR_CLOUD = {
    "aws": "bedrock",
    "azure": "azure",
}


def preferred_provider(config) -> str | None:
    """Provider id for this installation, or None when CLOUD has no default."""
    cloud = str((config or {}).get("CLOUD") or "aws").strip().lower() or "aws"
    return _PROVIDER_FOR_CLOUD.get(cloud)


def narrow_matches(matches, provider, config):
    """Prefer the cloud's provider when several catalog rows share a model id."""
    if provider or len(matches) <= 1:
        return matches
    preferred = preferred_provider(config)
    chosen = [entry for entry in matches if entry.provider_id == preferred]
    if len(chosen) == 1:
        return chosen
    return matches
