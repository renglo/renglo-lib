"""Provider selection and the generate lifecycle. Policy and budget are pass-throughs."""

from __future__ import annotations

import time
import uuid

from renglo.llm.providers import default_adapters
from renglo.llm.registry import (
    build_registry,
    estimate_cost,
    text_model,
)
from renglo.llm.types import (
    BLOCK_TEXT,
    BLOCK_TOOL_CALL,
    BLOCK_TOOL_RESULT,
    CAPABILITY_STREAMING,
    CAPABILITY_STRUCTURED_OUTPUT,
    CAPABILITY_TEXT,
    CAPABILITY_TOOL_CALLING,
    ERROR_CONFIG,
    ERROR_INVALID,
    ERROR_PROVIDER,
    ERROR_TIMEOUT,
    ERROR_UNSUPPORTED,
    RETRYABLE_ERRORS,
    SCHEMA_VERSION,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_TIMED_OUT,
    STOP_ERRORS,
    TEXT_ROLES,
    copy_context,
    empty_cost,
    empty_usage,
)
from renglo.logger import get_logger

_MAX_EXTRA_RETRIES = 2
_BACKOFF_SECONDS = (0.05, 0.1)


class LlmModel:
    def __init__(self, config=None, adapters=None, sleep=None):
        self.config = config or {}
        self.registry = build_registry(self.config)
        self.adapters = adapters or default_adapters()
        self.sleep = sleep or time.sleep
        self.logger = get_logger()

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
        request = {
            "messages": messages,
            "model": model,
            "provider": provider,
            "max_output_tokens": max_output_tokens,
            "timeout_seconds": timeout_seconds,
            "fallback": fallback,
            "context": context,
            "provider_options": provider_options,
            "stream": stream,
            "tools": tools,
            "output_schema": output_schema,
        }
        valid, error = self._validate(request)
        if not valid:
            return self._failure(None, [], error, ERROR_INVALID, model, None)

        execution = self._new_execution(request["context"])
        self.enforce_policies(execution, request)

        resolved, error_type, error = self._resolve_targets(request)
        if error_type:
            execution["status"] = STATUS_FAILED
            return self._failure(
                execution, [], error, error_type, model, None
            )

        required = self._required_capabilities(request)
        eligible = []
        for target in resolved:
            model_record = self.registry.models.get(target.model_id)
            capabilities = list(model_record.capabilities) if model_record else [CAPABILITY_TEXT]
            if all(name in capabilities for name in required):
                eligible.append((target, model_record))
        if not eligible:
            execution["status"] = STATUS_FAILED
            return self._failure(
                execution,
                [],
                "The resolved model does not declare the required capabilities",
                ERROR_UNSUPPORTED,
                model,
                None,
            )

        self.reserve_budget(execution, request)
        execution["status"] = STATUS_RUNNING

        attempts = []
        last = None
        for target, model_record in eligible:
            outcome = self._run_deployment(
                execution, request, target, attempts
            )
            last = outcome
            if outcome["ok"]:
                usage = outcome["usage"]
                cost = self._cost(usage, model_record)
                execution["usage"] = usage
                execution["cost"] = cost
                self.finalize_budget(execution)
                execution["status"] = STATUS_COMPLETED
                return self._success(
                    execution,
                    attempts,
                    outcome,
                    target,
                    model,
                )
            error_type = outcome["error_type"]
            if error_type in STOP_ERRORS:
                self.finalize_budget(execution)
                execution["status"] = (
                    STATUS_TIMED_OUT if error_type == ERROR_TIMEOUT else STATUS_FAILED
                )
                return self._failure(
                    execution,
                    attempts,
                    outcome["error"],
                    error_type,
                    model,
                    target,
                    outcome.get("provider_output"),
                )

        error_type = (last or {}).get("error_type") or ERROR_PROVIDER
        self.finalize_budget(execution)
        execution["status"] = (
            STATUS_TIMED_OUT if error_type == ERROR_TIMEOUT else STATUS_FAILED
        )
        return self._failure(
            execution,
            attempts,
            (last or {}).get("error") or "Generation failed",
            error_type,
            model,
            eligible[-1][0] if eligible else None,
            (last or {}).get("provider_output"),
        )

    def enforce_policies(self, execution, request):
        """Pass-through. Phase 2 evaluates policy without a new call path."""
        return None

    def reserve_budget(self, execution, request):
        """Pass-through. Phase 2 reserves budget without a new call path."""
        return None

    def finalize_budget(self, execution):
        """Pass-through. Phase 2 settles the reservation without a new call path."""
        return None

    def _validate(self, request):
        messages = request.get("messages")
        if not isinstance(messages, list) or not messages:
            return False, "messages must be a non-empty list"
        normalized = []
        for message in messages:
            if not isinstance(message, dict):
                return False, "each message must be an object"
            role = message.get("role")
            if role not in TEXT_ROLES and role != "tool":
                return False, "message role must be system, user, assistant, or tool"
            content = message.get("content")
            if isinstance(content, str):
                blocks = [{"type": BLOCK_TEXT, "text": content}]
            elif isinstance(content, list) and content:
                blocks = content
            else:
                return False, "message content must be a string or a list of blocks"
            for block in blocks:
                if not isinstance(block, dict) or not block.get("type"):
                    return False, "each content block must include a type"
            normalized.append({"role": role, "content": blocks})
        request["messages"] = normalized

        if request.get("provider_options") is not None and not isinstance(
            request.get("provider_options"), dict
        ):
            return False, "provider_options must be an object"
        if request.get("context") is not None and not isinstance(request.get("context"), dict):
            return False, "context must be an object"
        fallback = request.get("fallback")
        if fallback is not None and (
            not isinstance(fallback, list)
            or any(not isinstance(item, str) for item in fallback)
        ):
            return False, "fallback must be a list of alias names"
        max_tokens = request.get("max_output_tokens")
        if max_tokens is not None and (not isinstance(max_tokens, int) or max_tokens < 1):
            return False, "max_output_tokens must be a positive integer"
        timeout = request.get("timeout_seconds")
        if timeout is None:
            request["timeout_seconds"] = 120
        elif not isinstance(timeout, (int, float)) or timeout <= 0:
            return False, "timeout_seconds must be a positive number"
        if not isinstance(request.get("model"), str) or not request["model"].strip():
            return False, "model is required"
        request["model"] = request["model"].strip()
        return True, None

    def _required_capabilities(self, request):
        required = [CAPABILITY_TEXT]
        if request.get("stream"):
            required.append(CAPABILITY_STREAMING)
        if request.get("tools"):
            required.append(CAPABILITY_TOOL_CALLING)
        if request.get("output_schema"):
            required.append(CAPABILITY_STRUCTURED_OUTPUT)
        for message in request["messages"]:
            if message.get("role") == "tool":
                required.append(CAPABILITY_TOOL_CALLING)
            for block in message.get("content") or []:
                if block.get("type") in (BLOCK_TOOL_CALL, BLOCK_TOOL_RESULT):
                    required.append(CAPABILITY_TOOL_CALLING)
        return list(dict.fromkeys(required))

    def _resolve_targets(self, request):
        provider = request.get("provider")
        if provider is not None and not isinstance(provider, str):
            return None, ERROR_INVALID, "provider must be a string"
        if isinstance(provider, str):
            provider = provider.strip() or None
            request["provider"] = provider
        names = [request["model"]]
        for name in request.get("fallback") or []:
            names.append(name)
        selected = []
        for index, name in enumerate(names):
            target, error_type, error = self._one_target(name, provider if index == 0 else None)
            if error_type:
                return None, error_type, error
            selected.append(target)
        return selected, None, None

    def _one_target(self, model_name, provider):
        if model_name in (None, "", "default"):
            return None, ERROR_INVALID, "model is required"
        matches = [
            entry for entry in self.registry.catalog
            if entry.model_id == model_name and (not provider or entry.provider_id == provider)
        ]
        if not matches:
            if not provider:
                return None, ERROR_INVALID, f"Unknown model {model_name}"
            return self._off_catalog(provider, model_name)
        if len(matches) > 1:
            return None, ERROR_INVALID, f"Model {model_name} is offered by more than one provider"
        entry = matches[0]
        if not entry.available:
            return None, ERROR_CONFIG, entry.reason or "The model is not available"
        return self._target_for_entry(entry)

    def _target_for_entry(self, entry):
        target = self.registry.targets.get((entry.provider_id, entry.model_id))
        if target is None:
            return None, ERROR_CONFIG, "The model is not available"
        return target, None, None

    def _off_catalog(self, provider, model_name):
        module = self.registry.modules.get(provider)
        if module is None or not hasattr(module, "target_for"):
            return None, ERROR_INVALID, f"Unknown provider {provider}"
        target, error = module.target_for(self.config, model_name)
        if error:
            return None, ERROR_INVALID, error
        if model_name not in self.registry.models:
            self.registry.models[model_name] = text_model(model_name)
        return target, None, None

    def _run_deployment(self, execution, request, deployment, attempts):
        adapter = self.adapters.get(deployment.provider_id)
        if adapter is None:
            outcome = {
                "ok": False,
                "error_type": ERROR_CONFIG,
                "error": f"No translator for {deployment.provider_id}",
                "text": "",
                "usage": empty_usage(),
                "provider_output": {},
            }
            attempts.append(self._attempt(execution, deployment, outcome, 0))
            self._note_attempt(execution, attempts[-1])
            return outcome

        credential, error = self._resolve_credential(deployment)
        if error:
            outcome = {
                "ok": False,
                "error_type": ERROR_CONFIG,
                "error": error,
                "text": "",
                "usage": empty_usage(),
                "provider_output": {},
            }
            attempts.append(self._attempt(execution, deployment, outcome, 0))
            self._note_attempt(execution, attempts[-1])
            return outcome

        options = request.get("provider_options") or {}
        call = {
            "messages": request["messages"],
            "max_output_tokens": request.get("max_output_tokens"),
            "provider_options": options,
        }
        if isinstance(options.get("reasoning"), dict) and options["reasoning"].get("effort"):
            call["reasoning_effort"] = options["reasoning"]["effort"]

        last = None
        for retry_index in range(_MAX_EXTRA_RETRIES + 1):
            started = time.perf_counter()
            outcome = adapter.invoke(
                call,
                deployment,
                credential,
                request.get("timeout_seconds"),
            )
            duration_ms = int((time.perf_counter() - started) * 1000)
            attempts.append(self._attempt(execution, deployment, outcome, duration_ms))
            self._note_attempt(execution, attempts[-1])
            last = outcome
            if outcome["ok"] or outcome["error_type"] not in RETRYABLE_ERRORS:
                return outcome
            if retry_index < _MAX_EXTRA_RETRIES:
                self.sleep(_BACKOFF_SECONDS[retry_index])
        return last

    def _resolve_credential(self, deployment):
        module = self.registry.modules.get(deployment.provider_id)
        if module is None or not hasattr(module, "prepare"):
            return None, "The model is not available"
        return module.prepare(self.config, deployment)

    def _new_execution(self, context):
        return {
            "execution_id": f"llm_exec_{uuid.uuid4().hex}",
            "status": STATUS_PENDING,
            "context": copy_context(context),
            "usage": empty_usage(),
            "cost": empty_cost(),
        }

    def _attempt(self, execution, deployment, outcome, duration_ms):
        usage = outcome.get("usage") or empty_usage()
        model_record = self.registry.models.get(deployment.model_id)
        status = STATUS_COMPLETED if outcome.get("ok") else (
            STATUS_TIMED_OUT if outcome.get("error_type") == ERROR_TIMEOUT else STATUS_FAILED
        )
        return {
            "attempt_id": f"llm_attempt_{uuid.uuid4().hex}",
            "execution_id": execution["execution_id"],
            "deployment_id": deployment.target_id,
            "provider_id": deployment.provider_id,
            "model_id": deployment.model_id,
            "status": status,
            "error_type": outcome.get("error_type"),
            "usage": usage,
            "cost": self._cost(usage, model_record) if outcome.get("ok") else empty_cost(),
            "duration_ms": duration_ms,
        }

    def _cost(self, usage, model_record):
        pricing = model_record.pricing if model_record else None
        cost = empty_cost(pricing.currency if pricing else "USD")
        cost["estimated"] = estimate_cost(usage, pricing)
        return cost

    def _note_attempt(self, execution, attempt):
        self._log_attempt(attempt)

    def _log_attempt(self, attempt):
        usage = attempt.get("usage") or {}
        self.logger.info(
            "llm_attempt execution_id=%s attempt_id=%s deployment_id=%s provider_id=%s model_id=%s status=%s error_type=%s duration_ms=%s input_tokens=%s output_tokens=%s",
            attempt.get("execution_id"),
            attempt.get("attempt_id"),
            attempt.get("deployment_id"),
            attempt.get("provider_id"),
            attempt.get("model_id"),
            attempt.get("status"),
            attempt.get("error_type"),
            attempt.get("duration_ms"),
            usage.get("input_tokens"),
            usage.get("output_tokens"),
        )

    def _success(self, execution, attempts, outcome, deployment, requested_model):
        return {
            "success": True,
            "action": "generate",
            "schema_version": SCHEMA_VERSION,
            "output": {
                "text": outcome.get("text") or "",
                "provider": deployment.provider_id,
                "requested_model": requested_model,
                "model": deployment.model_id,
                "execution": execution,
                "attempts": attempts,
                "provider_output": outcome.get("provider_output") or {},
            },
        }

    def _failure(
        self,
        execution,
        attempts,
        error,
        error_type,
        requested_model,
        deployment,
        provider_output=None,
    ):
        output = {
            "text": "",
            "provider": deployment.provider_id if deployment else None,
            "requested_model": requested_model,
            "model": deployment.model_id if deployment else None,
            "execution": execution,
            "attempts": attempts,
            "provider_output": provider_output or {},
        }
        return {
            "success": False,
            "action": "generate",
            "schema_version": SCHEMA_VERSION,
            "error": error,
            "error_type": error_type,
            "output": output,
        }
