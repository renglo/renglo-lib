"""Provider-agnostic registry. Callers see a catalog of models, not provider mechanics."""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass, field

from renglo.llm.types import CAPABILITY_TEXT


@dataclass
class CatalogEntry:
    """One model a provider offers. Availability is already decided by that provider."""

    provider_id: str
    model_id: str | None
    is_default: bool = False
    available: bool = True
    reason: str | None = None
    source_label: str | None = None
    capabilities: list = field(default_factory=lambda: [CAPABILITY_TEXT])


@dataclass
class CallTarget:
    """Passed through to the provider adapter. Callers do not interpret it."""

    provider_id: str
    model_id: str
    target_id: str
    endpoint: str | None = None
    region: str | None = None


@dataclass
class ModelRecord:
    model_id: str
    capabilities: list
    context_window: int
    max_output_tokens: int


@dataclass
class ProviderContribution:
    entries: list
    targets: dict
    models: dict


@dataclass
class Registry:
    catalog: list
    targets: dict
    models: dict
    modules: dict


def _split_model_ids(raw):
    seen = []
    for part in str(raw or "").replace("\n", ",").split(","):
        model_id = part.strip()
        if model_id and model_id not in seen:
            seen.append(model_id)
    return seen


def configured_models(config, models_key):
    """Model ids from one comma-separated config value. Callers name the id on each call."""
    config = config or {}
    return _split_model_ids(config.get(models_key))


def text_model(model_id, context_window=128000, max_output_tokens=8192, capabilities=None):
    return ModelRecord(
        model_id=model_id,
        capabilities=list(capabilities or [CAPABILITY_TEXT]),
        context_window=context_window,
        max_output_tokens=max_output_tokens,
    )


def provider_modules():
    """Provider modules are the ones under providers that export contribute()."""
    import renglo.llm.providers as package

    modules = []
    for info in pkgutil.iter_modules(package.__path__):
        if info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{package.__name__}.{info.name}")
        if callable(getattr(module, "contribute", None)):
            modules.append(module)
    modules.sort(key=lambda module: module.__name__)
    return modules


def build_registry(config) -> Registry:
    """Merge every provider catalog. A later control plane can replace this source."""
    config = config or {}
    catalog = []
    targets = {}
    models = {}
    modules = {}
    for module in provider_modules():
        provider_id = getattr(module, "PROVIDER_ID", None)
        if not provider_id:
            continue
        modules[provider_id] = module
        contribution = module.contribute(config)
        catalog.extend(contribution.entries or [])
        for model_id, target in (contribution.targets or {}).items():
            targets[(target.provider_id, model_id)] = target
        for model_id, model in (contribution.models or {}).items():
            models.setdefault(model_id, model)
    return Registry(catalog=catalog, targets=targets, models=models, modules=modules)
