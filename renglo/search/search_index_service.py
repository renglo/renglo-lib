"""Search indexing planning service based on Blueprint metadata."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

from renglo.logger import get_logger


class SearchIndexService:
    """
    Resolves search indexing rules for a ring from Blueprint metadata.

    This service decides which fields are indexable and optional field weights,
    so callers do not need to pass search field definitions.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        blueprint_handle: Optional[str] = None,
        dynamodb_resource: Optional[Any] = None,
        region_name: Optional[str] = None,
    ) -> None:
        self.config = config or {}
        self.logger = get_logger()
        self.blueprint_handle = blueprint_handle
        self.dynamodb_resource = dynamodb_resource
        self.region_name = region_name
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
    def _parse_search_level(value: Any) -> int:
        """
        Canonical search configuration:
          - search: 0   -> not searchable
          - search: N>0 -> searchable with weight N

        Missing/invalid values are treated as 0.
        """
        if isinstance(value, bool):
            return 0
        if isinstance(value, int):
            return value if value > 0 else 0
        if isinstance(value, Decimal):
            if value % 1 != 0:
                return 0
            as_int = int(value)
            return as_int if as_int > 0 else 0
        return 0

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
                self.logger.warning(f"SearchIndexService blueprint lookup failed for {handle}/{ring}: {exc}")
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
    ) -> Dict[str, Any]:
        """
        Return indexing plan for a ring:
          - searchable_fields
          - field_weights
        """
        cache_key = f"{blueprint_handle or 'default'}::{ring}"
        if not force_refresh and cache_key in self._plan_cache:
            return self._plan_cache[cache_key]

        blueprint = self._resolve_blueprint(ring, blueprint_handle=blueprint_handle)
        searchable_fields: List[str] = []
        field_weights: Dict[str, float] = {}
        field_modes: Dict[str, str] = {}

        for field in blueprint.get("fields", []):
            if not isinstance(field, dict):
                continue
            field_name = field.get("name")
            if not isinstance(field_name, str) or not field_name.strip():
                continue
            search_level = self._parse_search_level(field.get("search", 0))
            if search_level <= 0:
                continue

            name = field_name.strip()
            searchable_fields.append(name)
            field_weights[name] = float(search_level)
            mode = str(field.get("search_mode", "text")).strip().lower()
            field_modes[name] = "exact" if mode == "exact" else "text"

        plan = {
            "ring": ring,
            "searchable_fields": searchable_fields,
            "field_weights": field_weights,
            "field_modes": field_modes,
        }
        self._plan_cache[cache_key] = plan
        return plan

