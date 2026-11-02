"""LlmModel interface.

Callers construct this class. CLOUD in config selects
renglo.llm.submodels.llm_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.llm.submodels.llm_model_aws import (
    _MAX_EXTRA_RETRIES,
    _BACKOFF_SECONDS,
    _numeric,
)

def _submodel(config):
    return load_submodel("renglo.llm.submodels", "llm_model", "LlmModel", config)


class LlmModel:
    def __init__(self, config=None, adapters=None, sleep=None):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, adapters, sleep))

    def generate(self, messages, model=None, provider=None, max_output_tokens=None, timeout_seconds=120, fallback=None, context=None, provider_options=None, stream=None, tools=None, output_schema=None):
        return self._impl.generate(messages, model, provider, max_output_tokens, timeout_seconds, fallback, context, provider_options, stream, tools, output_schema)

    def enforce_policies(self, execution, request):
        """Pass-through. Phase 2 evaluates policy without a new call path."""
        return self._impl.enforce_policies(execution, request)

    def reserve_budget(self, execution, request):
        """Pass-through. Phase 2 reserves budget without a new call path."""
        return self._impl.reserve_budget(execution, request)

    def finalize_budget(self, execution):
        """Pass-through. Phase 2 settles the reservation without a new call path."""
        return self._impl.finalize_budget(execution)

    def _validate(self, request):
        return self._impl._validate(request)

    def _required_capabilities(self, request):
        return self._impl._required_capabilities(request)

    def _resolve_targets(self, request):
        return self._impl._resolve_targets(request)

    def _one_target(self, model_name, provider):
        return self._impl._one_target(model_name, provider)

    def _target_for_entry(self, entry):
        return self._impl._target_for_entry(entry)

    def _off_catalog(self, provider, model_name):
        return self._impl._off_catalog(provider, model_name)

    def _run_deployment(self, execution, request, deployment, attempts):
        return self._impl._run_deployment(execution, request, deployment, attempts)

    def _resolve_credential(self, deployment):
        return self._impl._resolve_credential(deployment)

    def _new_execution(self, context):
        return self._impl._new_execution(context)

    def _attempt(self, execution, deployment, outcome, duration_ms):
        return self._impl._attempt(execution, deployment, outcome, duration_ms)

    def _cost(self, outcome, deployment):
        """Price the call from the rate card for this provider and region."""
        return self._impl._cost(outcome, deployment)

    def _note_attempt(self, execution, attempt):
        return self._impl._note_attempt(execution, attempt)

    def _log_attempt(self, attempt):
        return self._impl._log_attempt(attempt)

    def _success(self, execution, attempts, outcome, deployment, requested_model):
        return self._impl._success(execution, attempts, outcome, deployment, requested_model)

    def _failure(self, execution, attempts, error, error_type, requested_model, deployment, provider_output=None):
        return self._impl._failure(execution, attempts, error, error_type, requested_model, deployment, provider_output)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
