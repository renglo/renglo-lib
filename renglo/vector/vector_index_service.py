"""Blueprint-driven Vector DB plan: field concat or embed_handler text."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

from renglo.logger import get_logger
from renglo.vector.embed_handler import (
    EmbedHandlerInvoker,
    extract_embed_payload,
    invoke_embed_handler,
    parse_embed_handler_ref,
)


class VectorIndexService:
    """Resolve embed plan: field concat, or embed_handler text, then platform write."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        blueprint_handle: Optional[str] = None,
        dynamodb_resource: Optional[Any] = None,
        region_name: Optional[str] = None,
        embed_handler_invoker: Optional[EmbedHandlerInvoker] = None,
    ) -> None:
        self.config = config or {}
        self.logger = get_logger()
        self.blueprint_handle = blueprint_handle
        self.dynamodb_resource = dynamodb_resource
        self.region_name = region_name
        self._invoke_embed_handler = embed_handler_invoker
        self._bpc = None
        self._plan_cache: Dict[str, Dict[str, Any]] = {}

    def _get_blueprint_controller(self):
        if self._bpc is None:
            from renglo.blueprint.blueprint_controller import BlueprintController

            self._bpc = BlueprintController(
                config=self.config,
                dynamodb_resource=self.dynamodb_resource,
                region_name=self.region_name,
            )
        return self._bpc

    @staticmethod
    def _is_valid_blueprint(blueprint: Any) -> bool:
        return isinstance(blueprint, dict) and isinstance(blueprint.get("fields"), list)

    @staticmethod
    def _parse_embed_level(value: Any) -> int:
        """
        Field embed flag:
          - missing / false / 0 -> not embedded
          - true                -> embedded (weight 1)
          - N>0                 -> embedded with weight N
        """
        if value is None:
            return 0
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int):
            return value if value > 0 else 0
        if isinstance(value, Decimal):
            if value % 1 != 0:
                return 0
            as_int = int(value)
            return as_int if as_int > 0 else 0
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"true", "yes", "y", "on", "1"}:
                return 1
            if text.isdigit():
                number = int(text)
                return number if number > 0 else 0
        return 0

    @staticmethod
    def _enable_embed(blueprint: Dict[str, Any]) -> bool:
        if not isinstance(blueprint, dict):
            return True
        if "enable_embed" not in blueprint:
            return True
        raw = blueprint.get("enable_embed")
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            return raw.strip().lower() not in {"false", "0", "no", "off"}
        return bool(raw)

    @staticmethod
    def _is_reference_field(field: Dict[str, Any]) -> bool:
        source = field.get("source")
        return isinstance(source, dict) and bool(source.get("target"))

    def _resolve_blueprint(self, ring: str, *, blueprint_handle: Optional[str] = None) -> Dict[str, Any]:
        bpc = self._get_blueprint_controller()
        handles: List[str] = []
        if blueprint_handle:
            handles.append(str(blueprint_handle))
        if self.blueprint_handle:
            handles.append(str(self.blueprint_handle))
        if self.config.get("BLUEPRINT_HANDLE"):
            handles.append(str(self.config["BLUEPRINT_HANDLE"]))
        handles.append("irma")
        seen = set()
        for handle in handles:
            if handle in seen:
                continue
            seen.add(handle)
            try:
                blueprint = bpc.get_blueprint(handle, ring, "last")
            except Exception as exc:
                self.logger.warning(
                    "VectorIndexService blueprint lookup failed for %s/%s: %s",
                    handle,
                    ring,
                    exc,
                )
                continue
            if self._is_valid_blueprint(blueprint):
                return blueprint
        return {}

    def get_index_plan(
        self,
        ring: str,
        *,
        force_refresh: bool = False,
        blueprint_handle: Optional[str] = None,
        blueprint: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        cache_key = f"{blueprint_handle or 'default'}::{ring}"
        if blueprint is None and not force_refresh and cache_key in self._plan_cache:
            return self._plan_cache[cache_key]

        resolved = blueprint if self._is_valid_blueprint(blueprint) else self._resolve_blueprint(
            ring, blueprint_handle=blueprint_handle
        )
        embed_fields: List[str] = []
        field_weights: Dict[str, float] = {}
        enabled = self._enable_embed(resolved)

        if enabled:
            for field in resolved.get("fields") or []:
                if not isinstance(field, dict):
                    continue
                name = str(field.get("name") or "").strip()
                if not name:
                    continue
                if self._is_reference_field(field):
                    continue
                if str(field.get("type") or "").strip().lower() == "object":
                    continue
                level = self._parse_embed_level(field.get("embed"))
                if level <= 0:
                    continue
                embed_fields.append(name)
                field_weights[name] = float(level)

        handler_ref = ""
        if enabled:
            parsed = parse_embed_handler_ref(resolved.get("embed_handler"))
            if parsed:
                handler_ref = str(resolved.get("embed_handler") or "").strip()

        plan = {
            "ring": ring,
            "entity_type": ring,
            "enabled": enabled,
            "embed_handler": handler_ref,
            "embed_fields": embed_fields,
            "field_weights": field_weights,
            "blueprint": resolved.get("uri") or resolved.get("irn") or "",
            "blueprint_version": resolved.get("version") or "",
        }
        if blueprint is None:
            self._plan_cache[cache_key] = plan
        return plan

    @staticmethod
    def _stringify_value(raw: Any) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, bool):
            return [str(raw).lower()]
        if isinstance(raw, (int, float, Decimal)):
            return [str(raw)]
        if isinstance(raw, str):
            text = raw.strip()
            return [text] if text else []
        if isinstance(raw, list):
            values: List[str] = []
            for item in raw:
                for part in VectorIndexService._stringify_value(item):
                    if part not in values:
                        values.append(part)
            return values
        if isinstance(raw, dict):
            if raw.get("value") is not None:
                return VectorIndexService._stringify_value(raw.get("value"))
            return []
        text = str(raw).strip()
        return [text] if text else []

    def build_fingerprint(self, plan: Dict[str, Any], attributes: Optional[Dict[str, Any]]) -> str:
        attrs = attributes if isinstance(attributes, dict) else {}
        parts: List[str] = []
        used: List[str] = []
        for name in plan.get("embed_fields") or []:
            values = self._stringify_value(attrs.get(name))
            if not values:
                continue
            parts.append(f"[{name}={','.join(values)}]")
            used.append(name)
        plan["used_fields"] = used
        return " ".join(parts)

    def _text_from_embed_handler(
        self,
        handler_ref: str,
        *,
        portfolio: str,
        org: str,
        ring: str,
        doc_id: str,
        attributes: Optional[Dict[str, Any]],
        verb: str,
    ) -> tuple:
        payload = {
            "portfolio": portfolio,
            "org": org,
            "ring": ring,
            "entity_type": ring,
            "_id": doc_id,
            "doc_id": doc_id,
            "entity_id": doc_id,
            "verb": str(verb or "PUT"),
            "attributes": attributes if isinstance(attributes, dict) else {},
        }
        invoker = self._invoke_embed_handler or invoke_embed_handler
        try:
            raw = invoker(handler_ref, payload)
        except Exception as exc:
            self.logger.error("embed_handler %s failed: %s", handler_ref, exc)
            return "", {}, str(exc), False
        return extract_embed_payload(raw)

    def sync_document(
        self,
        vectors: Any,
        *,
        portfolio: str,
        org: str,
        ring: str,
        doc_id: str,
        attributes: Optional[Dict[str, Any]] = None,
        verb: str = "PUT",
        blueprint_handle: Optional[str] = None,
        blueprint: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Best-effort put/delete of the canonical CRUD vector for one document."""
        entity_type = str(ring or "").strip()
        entity_id = str(doc_id or "").strip()
        if not portfolio or not org or not entity_type or not entity_id:
            return {
                "success": True,
                "skipped": True,
                "reason": "portfolio, org, ring, and doc_id are required",
            }
        plan = self.get_index_plan(
            entity_type,
            blueprint_handle=blueprint_handle,
            blueprint=blueprint,
        )
        if not plan.get("enabled"):
            return {"success": True, "skipped": True, "reason": "enable_embed is false"}
        handler_ref = str(plan.get("embed_handler") or "").strip()
        if not handler_ref and not plan.get("embed_fields"):
            return {"success": True, "skipped": True, "reason": "no embed fields on blueprint"}

        if str(verb or "").upper() == "DELETE":
            result = vectors.delete_vector(
                portfolio=portfolio,
                org=org,
                entity_type=entity_type,
                entity_id=entity_id,
            )
            if isinstance(result, dict):
                return {**result, "entity_type": entity_type, "entity_id": entity_id}
            return {"success": True, "entity_type": entity_type, "entity_id": entity_id}

        if handler_ref:
            text, extra_attrs, handler_error, skip = self._text_from_embed_handler(
                handler_ref,
                portfolio=portfolio,
                org=org,
                ring=entity_type,
                doc_id=entity_id,
                attributes=attributes,
                verb=verb,
            )
            if handler_error:
                return {
                    "success": False,
                    "skipped": True,
                    "reason": handler_error,
                    "error": handler_error,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "embed_handler": handler_ref,
                }
            if skip or not text:
                return {
                    "success": True,
                    "skipped": True,
                    "reason": "embed handler skipped or empty text",
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "embed_handler": handler_ref,
                }
            attrs = {
                **extra_attrs,
                "source": "embed_handler",
                "embed_handler": handler_ref,
                "blueprint": plan.get("blueprint") or "",
                "blueprint_version": plan.get("blueprint_version") or "",
            }
        else:
            text = self.build_fingerprint(plan, attributes)
            if not text:
                return {"success": True, "skipped": True, "reason": "empty fingerprint"}
            attrs = {
                "source": "blueprint",
                "blueprint": plan.get("blueprint") or "",
                "blueprint_version": plan.get("blueprint_version") or "",
                "fields": plan.get("used_fields") or [],
            }

        result = vectors.put_vector(
            portfolio=portfolio,
            org=org,
            entity_type=entity_type,
            entity_id=entity_id,
            text=text,
            attrs=attrs,
        )
        if isinstance(result, dict):
            return {
                **result,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "fingerprint_length": len(text),
            }
        return {"success": True, "entity_type": entity_type, "entity_id": entity_id}
