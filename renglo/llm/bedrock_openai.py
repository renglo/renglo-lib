from typing import Any, Dict, Optional, Union

import requests
from renglo.logger import get_logger


class LlmConfigError(Exception):
    pass


class LlmController:
    """Bedrock Mantle OpenAI-compatible Responses API client (HTTP)."""

    DEFAULT_MODEL = "openai.gpt-5.5"
    DEFAULT_REASONING = {"effort": "low"}
    DEFAULT_TIMEOUT_SECONDS = 120

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        session: Optional[Any] = None,
    ):
        self.config = config or {}
        self.logger = get_logger()
        self.region = (self.config.get("AWS_REGION") or "us-east-1").strip()
        configured_base = (
            self.config.get("OPENAI_BASE_URL")
            or self.config.get("BEDROCK_BASE_URL")
            or ""
        ).strip()
        self.base_url = (
            configured_base
            or f"https://bedrock-mantle.{self.region}.api.aws/openai/v1"
        ).rstrip("/")
        self.api_key = (self.config.get("BEDROCK_API_KEY") or "").strip()
        self.model = (self.config.get("BEDROCK_MODEL") or self.DEFAULT_MODEL).strip()
        self.session = session or requests.Session()
        if not self.api_key:
            self.logger.error("BEDROCK_API_KEY is required")

    def _require_api_key(self) -> str:
        if not self.api_key:
            raise LlmConfigError("BEDROCK_API_KEY is required")
        return self.api_key

    def _extract_output_text(self, response: Any) -> str:
        if not isinstance(response, dict):
            text = getattr(response, "output_text", None)
            if isinstance(text, str) and text:
                return text
            output_items = getattr(response, "output", None) or []
        else:
            text = response.get("output_text")
            if isinstance(text, str) and text:
                return text
            output_items = response.get("output") or []

        chunks: list[str] = []
        for item in output_items:
            item_type = getattr(item, "type", None) or (item.get("type") if isinstance(item, dict) else None)
            if item_type != "message":
                continue
            content = getattr(item, "content", None)
            if content is None and isinstance(item, dict):
                content = item.get("content")
            for part in content or []:
                part_text = getattr(part, "text", None)
                if part_text is None and isinstance(part, dict):
                    part_text = part.get("text")
                if isinstance(part_text, str) and part_text:
                    chunks.append(part_text)
        return "".join(chunks)

    def openai_call(
        self,
        prompt: Union[str, Dict[str, Any]],
        *,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Call Bedrock Mantle via HTTP POST ``{base_url}/responses``.

        Matches the working Mantle request shape:
        ``{model, input, reasoning: {effort}}`` plus any extra Responses kwargs
        (e.g. ``instructions``).

        Args:
            prompt: Plain text prompt, or a dict with at least an ``input`` key
                (and optionally ``model`` / ``reasoning`` / other Responses API kwargs).
            model: Optional model override (defaults to configured Bedrock model).
        """
        action = "openai_call"

        if isinstance(prompt, str):
            input_payload: Any = prompt
            request: Dict[str, Any] = {}
        elif isinstance(prompt, dict):
            request = dict(prompt)
            input_payload = request.pop("input", None)
            if input_payload is None:
                return {
                    "success": False,
                    "action": action,
                    "input": prompt,
                    "error": "prompt dict must include an 'input' key",
                }
        else:
            return {
                "success": False,
                "action": action,
                "input": prompt,
                "error": "prompt must be a string or dict",
            }

        if isinstance(input_payload, str) and not input_payload.strip():
            return {
                "success": False,
                "action": action,
                "input": prompt,
                "error": "prompt must be a non-empty string",
            }

        resolved_model = (model or request.pop("model", None) or self.model).strip()
        params: Dict[str, Any] = {
            "model": resolved_model,
            "input": input_payload,
            **request,
        }
        if "reasoning" not in params:
            params["reasoning"] = dict(self.DEFAULT_REASONING)

        try:
            api_key = self._require_api_key()
            http_response = self.session.post(
                f"{self.base_url}/responses",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=params,
                timeout=self.DEFAULT_TIMEOUT_SECONDS,
            )
            try:
                data = http_response.json()
            except Exception:
                data = {"raw_text": http_response.text}

            if not http_response.ok:
                error_detail = data if isinstance(data, dict) else {"raw_text": str(data)}
                error_msg = (
                    error_detail.get("error", {}).get("message")
                    if isinstance(error_detail.get("error"), dict)
                    else None
                ) or error_detail.get("message") or http_response.text or f"HTTP {http_response.status_code}"
                self.logger.error(f"openai_call failed ({http_response.status_code}): {error_msg}")
                return {
                    "success": False,
                    "action": action,
                    "input": prompt,
                    "error": str(error_msg),
                    "status_code": http_response.status_code,
                    "raw": data,
                }

            output_text = self._extract_output_text(data)
            return {
                "success": True,
                "action": action,
                "input": prompt,
                "output": {
                    "text": output_text,
                    "model": resolved_model,
                    "response_id": data.get("id") if isinstance(data, dict) else None,
                    "raw": data,
                },
            }
        except Exception as e:
            self.logger.error(f"openai_call failed: {e}")
            return {
                "success": False,
                "action": action,
                "input": prompt,
                "error": str(e),
            }
