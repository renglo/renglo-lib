"""
Graph controller interface layer.

The controller exposes the public graph API to the application and delegates
data/persistence behavior to GraphModel.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from renglo.graph.graph_model import (
    GraphEdge,
    GraphModel,
    GraphQueryCancelled,
    GraphQueryTimeout,
    GraphTraversalBudgetExceeded,
    PageResult,
    TraversalResult,
    TraversalStep,
)


class GraphController:
    """
    Application-facing interface for graph operations.

    Mirrors DataController-style composition by delegating to GraphModel while
    exposing all public graph functionality through controller methods.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        region_name: Optional[str] = None,
        dynamodb_resource: Optional[Any] = None,
        reverse_index_name: str = "backward_index",
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self.config = config or {}
        self.GRM = GraphModel(
            config=config,
            region_name=region_name,
            dynamodb_resource=dynamodb_resource,
            reverse_index_name=reverse_index_name,
            clock=clock or time.time,
        )
        # Cache blueprint lookups by (handle, ring) to avoid repeated DynamoDB
        # reads when processing many documents from the same rings.
        self._blueprint_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

    @property
    def model(self) -> GraphModel:
        return self.GRM

    @property
    def dynamodb(self) -> Any:
        return self.GRM.dynamodb

    @property
    def table(self) -> Any:
        return self.GRM.table

    @property
    def reverse_index_name(self) -> str:
        return self.GRM.reverse_index_name

    @property
    def clock(self) -> Callable[[], float]:
        return self.GRM.clock

    @staticmethod
    def make_pk(portfolio: str, org: str) -> str:
        return GraphModel.make_pk(portfolio, org)

    @staticmethod
    def make_node_id(ring: str, node_id: str) -> str:
        return GraphModel.make_node_id(ring, node_id)

    @staticmethod
    def make_forward_sk(edge_type: str, from_node_id: str, to_node_id: str) -> str:
        return GraphModel.make_forward_sk(edge_type, from_node_id, to_node_id)

    @staticmethod
    def make_reverse_sk(edge_type: str, to_node_id: str, from_node_id: str) -> str:
        return GraphModel.make_reverse_sk(edge_type, to_node_id, from_node_id)

    def put_edge(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        from_node_id: str,
        to_node_id: str,
        *,
        properties: Optional[Dict[str, Any]] = None,
    ) -> GraphEdge:
        return self.GRM.put_edge(
            portfolio,
            org,
            edge_type,
            from_node_id,
            to_node_id,
            properties=properties,
        )

    def remove_edge(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        from_node_id: str,
        to_node_id: str,
    ) -> bool:
        return self.GRM.remove_edge(portfolio, org, edge_type, from_node_id, to_node_id)

    def get_edge(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        from_node_id: str,
        to_node_id: str,
    ) -> Optional[GraphEdge]:
        return self.GRM.get_edge(portfolio, org, edge_type, from_node_id, to_node_id)

    def list_edges_by_type(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        *,
        limit: int = 100,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
    ) -> PageResult:
        return self.GRM.list_edges_by_type(
            portfolio,
            org,
            edge_type,
            limit=limit,
            exclusive_start_key=exclusive_start_key,
        )

    def list_outgoing_edges(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        from_node_id: str,
        *,
        limit: int = 100,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
    ) -> PageResult:
        return self.GRM.list_outgoing_edges(
            portfolio,
            org,
            edge_type,
            from_node_id,
            limit=limit,
            exclusive_start_key=exclusive_start_key,
        )

    def list_incoming_edges(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        to_node_id: str,
        *,
        limit: int = 100,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
    ) -> PageResult:
        return self.GRM.list_incoming_edges(
            portfolio,
            org,
            edge_type,
            to_node_id,
            limit=limit,
            exclusive_start_key=exclusive_start_key,
        )

    def list_incoming_edges_any_type(
        self,
        portfolio: str,
        org: str,
        to_node_id: str,
        *,
        limit: int = 100,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
    ) -> PageResult:
        return self.GRM.list_incoming_edges_any_type(
            portfolio,
            org,
            to_node_id,
            limit=limit,
            exclusive_start_key=exclusive_start_key,
        )

    def list_edges_between_nodes(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        from_node_id: str,
        to_node_id: str,
    ) -> List[GraphEdge]:
        return self.GRM.list_edges_between_nodes(
            portfolio,
            org,
            edge_type,
            from_node_id,
            to_node_id,
        )

    def traverse(
        self,
        portfolio: str,
        org: str,
        start_node_id: str,
        edge_types: Sequence[str],
        *,
        direction: str = "forward",
        max_depth: int = 3,
        per_query_limit: int = 100,
        max_nodes: int = 1_000,
        max_edges: int = 5_000,
        max_neighbors_per_node: int = 100,
        timeout_seconds: float = 10.0,
        cancel_check: Optional[Callable[[], bool]] = None,
        score_edge: Optional[Callable[[GraphEdge], float]] = None,
        min_score: Optional[float] = None,
        include_duplicate_steps: bool = True,
        return_frontier_on_stop: bool = False,
    ) -> TraversalResult:
        return self.GRM.traverse(
            portfolio,
            org,
            start_node_id,
            edge_types,
            direction=direction,
            max_depth=max_depth,
            per_query_limit=per_query_limit,
            max_nodes=max_nodes,
            max_edges=max_edges,
            max_neighbors_per_node=max_neighbors_per_node,
            timeout_seconds=timeout_seconds,
            cancel_check=cancel_check,
            score_edge=score_edge,
            min_score=min_score,
            include_duplicate_steps=include_duplicate_steps,
            return_frontier_on_stop=return_frontier_on_stop,
        )

    def remove_node_edges(
        self,
        portfolio: str,
        org: str,
        node_id: str,
        edge_types: Sequence[str],
        *,
        batch_size: int = 25,
    ) -> Dict[str, int]:
        return self.GRM.remove_node_edges(
            portfolio,
            org,
            node_id,
            edge_types,
            batch_size=batch_size,
        )

    def verify_node_edges_removed(
        self,
        portfolio: str,
        org: str,
        node_id: str,
        edge_types: Sequence[str],
    ) -> Dict[str, int]:
        return self.GRM.verify_node_edges_removed(portfolio, org, node_id, edge_types)

    def find_orphan_edges_for_node(
        self,
        portfolio: str,
        org: str,
        node_id: str,
        edge_types: Sequence[str],
        node_exists: Callable[[str], bool],
    ) -> List[GraphEdge]:
        return self.GRM.find_orphan_edges_for_node(portfolio, org, node_id, edge_types, node_exists)

    def scan_orphan_edges_by_type(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        node_exists: Callable[[str], bool],
        *,
        limit_pages: Optional[int] = None,
    ) -> List[GraphEdge]:
        return self.GRM.scan_orphan_edges_by_type(
            portfolio,
            org,
            edge_type,
            node_exists,
            limit_pages=limit_pages,
        )

    def remove_edges(self, edges: Iterable[GraphEdge]) -> int:
        return self.GRM.remove_edges(edges)

    def sync_node_edges(
        self,
        portfolio: str,
        org: str,
        from_node_id: str,
        desired_edges: Sequence[Tuple[str, str, Optional[Dict[str, Any]]]],
        *,
        managed_edge_types: Optional[Sequence[str]] = None,
    ) -> Dict[str, int]:
        return self.GRM.sync_node_edges(
            portfolio,
            org,
            from_node_id,
            desired_edges,
            managed_edge_types=managed_edge_types,
        )

    def traverse_dynamic_forward(
        self,
        portfolio: str,
        org: str,
        start_node_id: str,
        *,
        max_depth: int = 3,
        per_query_limit: int = 100,
        max_nodes: int = 1_000,
        max_edges: int = 5_000,
        max_neighbors_per_node: int = 100,
        timeout_seconds: float = 10.0,
        cancel_check: Optional[Callable[[], bool]] = None,
        include_duplicate_steps: bool = True,
        return_frontier_on_stop: bool = False,
    ) -> TraversalResult:
        """
        Forward traversal that infers edge types per node from each node ring blueprint.

        This is intended for explorer/debug scenarios where hop N may involve edge
        types that cannot be known from the start node blueprint alone.
        """
        if max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        started_at = self.clock()
        visited_nodes: Set[str] = {start_node_id}
        visited_edges: Set[str] = set()
        duplicate_visits: Dict[str, int] = {}
        cycles_detected: List[List[str]] = []
        steps: List[TraversalStep] = []
        frontier: List[Tuple[str, int, List[str]]] = [(start_node_id, 0, [start_node_id])]
        stopped_reason: Optional[str] = None

        def check_cancel_or_timeout() -> None:
            if cancel_check and cancel_check():
                raise GraphQueryCancelled("Traversal cancelled")
            if timeout_seconds is not None and self.clock() - started_at > timeout_seconds:
                raise GraphQueryTimeout(f"Traversal exceeded {timeout_seconds} seconds")

        while frontier:
            check_cancel_or_timeout()
            current_node_id, depth, path = frontier.pop(0)
            if depth >= max_depth:
                continue

            neighbors_seen_for_node = 0
            try:
                ring, _ = GraphModel.split_node_id(current_node_id)
            except ValueError:
                continue

            blueprint = self._get_blueprint_for_ring(ring)
            edge_specs = self._get_edge_specs_from_blueprint(blueprint, ring)
            node_edge_types = sorted({spec["edge_type"] for spec in edge_specs})
            if not node_edge_types:
                continue

            for edge_type in node_edge_types:
                page_key = None
                while True:
                    check_cancel_or_timeout()
                    page = self.list_outgoing_edges(
                        portfolio,
                        org,
                        edge_type,
                        current_node_id,
                        limit=per_query_limit,
                        exclusive_start_key=page_key,
                    )
                    for edge in page.items:
                        check_cancel_or_timeout()
                        edge_id = edge.sk
                        if edge_id in visited_edges:
                            continue
                        visited_edges.add(edge_id)
                        next_node_id = edge.to_node_id
                        next_path = path + [next_node_id]

                        cycle_detected = next_node_id in path
                        duplicate_visit = next_node_id in visited_nodes
                        if cycle_detected:
                            cycles_detected.append(next_path)
                        if duplicate_visit:
                            duplicate_visits[next_node_id] = duplicate_visits.get(next_node_id, 0) + 1

                        if include_duplicate_steps or not duplicate_visit:
                            steps.append(
                                TraversalStep(
                                    depth=depth + 1,
                                    edge=edge,
                                    path=next_path,
                                    duplicate_visit=duplicate_visit,
                                    cycle_detected=cycle_detected,
                                )
                            )

                        if not duplicate_visit and not cycle_detected:
                            visited_nodes.add(next_node_id)
                            frontier.append((next_node_id, depth + 1, next_path))

                        neighbors_seen_for_node += 1
                        if len(visited_nodes) > max_nodes:
                            stopped_reason = "max_nodes_exceeded"
                            raise GraphTraversalBudgetExceeded(stopped_reason)
                        if len(visited_edges) > max_edges:
                            stopped_reason = "max_edges_exceeded"
                            raise GraphTraversalBudgetExceeded(stopped_reason)
                        if neighbors_seen_for_node >= max_neighbors_per_node:
                            stopped_reason = "max_neighbors_per_node_reached"
                            break

                    if stopped_reason == "max_neighbors_per_node_reached":
                        break
                    page_key = page.last_evaluated_key
                    if not page_key:
                        break

                if stopped_reason == "max_neighbors_per_node_reached":
                    break

        return TraversalResult(
            start_node_id=start_node_id,
            direction="forward",
            visited_nodes=visited_nodes,
            visited_edges=visited_edges,
            steps=steps,
            cycles_detected=cycles_detected,
            duplicate_visits=duplicate_visits,
            stopped_reason=stopped_reason,
            next_frontier=frontier if return_frontier_on_stop else None,
        )

    def traverse_dynamic_backward(
        self,
        portfolio: str,
        org: str,
        start_node_id: str,
        *,
        max_depth: int = 3,
        per_query_limit: int = 100,
        max_nodes: int = 1_000,
        max_edges: int = 5_000,
        max_neighbors_per_node: int = 100,
        timeout_seconds: float = 10.0,
        cancel_check: Optional[Callable[[], bool]] = None,
        include_duplicate_steps: bool = True,
        return_frontier_on_stop: bool = False,
    ) -> TraversalResult:
        """
        Backward traversal that discovers incoming edges per node.

        Incoming edge types are declared on source blueprints, not the current
        node's blueprint, so each hop uses incoming-edge discovery rather than a
        fixed edge_types list from the start node.
        """
        if max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        started_at = self.clock()
        visited_nodes: Set[str] = {start_node_id}
        visited_edges: Set[str] = set()
        duplicate_visits: Dict[str, int] = {}
        cycles_detected: List[List[str]] = []
        steps: List[TraversalStep] = []
        frontier: List[Tuple[str, int, List[str]]] = [(start_node_id, 0, [start_node_id])]
        stopped_reason: Optional[str] = None

        def check_cancel_or_timeout() -> None:
            if cancel_check and cancel_check():
                raise GraphQueryCancelled("Traversal cancelled")
            if timeout_seconds is not None and self.clock() - started_at > timeout_seconds:
                raise GraphQueryTimeout(f"Traversal exceeded {timeout_seconds} seconds")

        while frontier:
            check_cancel_or_timeout()
            current_node_id, depth, path = frontier.pop(0)
            if depth >= max_depth:
                continue

            neighbors_seen_for_node = 0
            page_key = None
            while True:
                check_cancel_or_timeout()
                page = self.list_incoming_edges_any_type(
                    portfolio,
                    org,
                    current_node_id,
                    limit=per_query_limit,
                    exclusive_start_key=page_key,
                )
                for edge in page.items:
                    check_cancel_or_timeout()
                    edge_id = edge.sk
                    if edge_id in visited_edges:
                        continue
                    visited_edges.add(edge_id)
                    next_node_id = edge.from_node_id
                    next_path = path + [next_node_id]

                    cycle_detected = next_node_id in path
                    duplicate_visit = next_node_id in visited_nodes
                    if cycle_detected:
                        cycles_detected.append(next_path)
                    if duplicate_visit:
                        duplicate_visits[next_node_id] = duplicate_visits.get(next_node_id, 0) + 1

                    if include_duplicate_steps or not duplicate_visit:
                        steps.append(
                            TraversalStep(
                                depth=depth + 1,
                                edge=edge,
                                path=next_path,
                                duplicate_visit=duplicate_visit,
                                cycle_detected=cycle_detected,
                            )
                        )

                    if not duplicate_visit and not cycle_detected:
                        visited_nodes.add(next_node_id)
                        frontier.append((next_node_id, depth + 1, next_path))

                    neighbors_seen_for_node += 1
                    if len(visited_nodes) > max_nodes:
                        stopped_reason = "max_nodes_exceeded"
                        raise GraphTraversalBudgetExceeded(stopped_reason)
                    if len(visited_edges) > max_edges:
                        stopped_reason = "max_edges_exceeded"
                        raise GraphTraversalBudgetExceeded(stopped_reason)
                    if neighbors_seen_for_node >= max_neighbors_per_node:
                        stopped_reason = "max_neighbors_per_node_reached"
                        break

                if stopped_reason == "max_neighbors_per_node_reached":
                    break
                page_key = page.last_evaluated_key
                if not page_key:
                    break

            if stopped_reason == "max_neighbors_per_node_reached":
                break

        return TraversalResult(
            start_node_id=start_node_id,
            direction="backward",
            visited_nodes=visited_nodes,
            visited_edges=visited_edges,
            steps=steps,
            cycles_detected=cycles_detected,
            duplicate_visits=duplicate_visits,
            stopped_reason=stopped_reason,
            next_frontier=frontier if return_frontier_on_stop else None,
        )

    # -----------------------------------------------------------------
    # Graph integration helpers
    # -----------------------------------------------------------------

    def _get_blueprint_controller(self):
        bpc = getattr(self, "_bpc", None)
        if bpc is None:
            from renglo.blueprint.blueprint_controller import BlueprintController
            self._bpc = BlueprintController(
                config=self.config,
                dynamodb_resource=self.GRM.dynamodb,
            )
            bpc = self._bpc
        return bpc

    def _parse_edge_source(self, source: Any):
        # Legacy format: "<target_blueprint>:<target_key>:<preview>"
        if isinstance(source, str):
            parts = [p.strip() for p in source.split(":")]
            if len(parts) != 3:
                return None
            to_ring, id_token, label_field = parts
            if not to_ring or not id_token:
                return None
            label_fields = [token.strip() for token in str(label_field).split(",") if token and token.strip()]
            return {
                "to_ring": to_ring,
                "id_token": id_token,
                "label_fields": label_fields,
                "edge_type": None,
                "qualifier_keys": [],
                "dynamic": False,
                "source_raw": source,
            }

        # New format:
        # {
        #   "target": "knowledge_concept",
        #   "target_key": "_id",
        #   "preview": ["name"],
        #   "label": ["DELEGATES_TO", "DELEGATED_BY"],
        #   "qualifiers": ["since", "domain"],
        #   "dynamic": true
        # }
        if not isinstance(source, dict):
            return None

        to_ring = source.get("target")
        id_token = source.get("target_key", "_id")
        preview = source.get("preview")
        if not isinstance(to_ring, str) or not to_ring.strip():
            return None
        if not isinstance(id_token, str) or not id_token.strip():
            return None

        label_fields: List[str] = []
        if isinstance(preview, list):
            label_fields = [str(token).strip() for token in preview if str(token).strip()]
        elif isinstance(preview, str) and preview.strip():
            label_fields = [token.strip() for token in preview.split(",") if token and token.strip()]

        qualifier_keys = []
        if isinstance(source.get("qualifiers"), list):
            qualifier_keys = [str(token).strip() for token in source.get("qualifiers", []) if str(token).strip()]

        label_pair: List[str] = []
        raw_label = source.get("label")
        if isinstance(raw_label, list):
            label_pair = [str(token).strip() for token in raw_label if str(token).strip()]
        elif isinstance(raw_label, str) and raw_label.strip():
            label_pair = [token.strip() for token in raw_label.split(",") if token and token.strip()]

        return {
            "to_ring": to_ring.strip(),
            "id_token": id_token.strip(),
            "label_fields": label_fields,
            "edge_labels": label_pair[:2],
            "qualifier_keys": qualifier_keys,
            "dynamic": bool(source.get("dynamic")),
            "source_raw": source,
        }

    def _implicit_edge_type(
        self,
        from_blueprint: str,
        from_field: str,
        to_blueprint: str,
        to_field: str,
    ) -> Optional[str]:
        if not from_blueprint or not from_field or not to_blueprint or not to_field:
            return None
        return f"{from_blueprint}:{from_field}:{to_blueprint}:{to_field}"

    def _is_graph_enabled(self, blueprint: Dict[str, Any]) -> bool:
        # Backward-compatible default: graphing is enabled unless explicitly disabled.
        if not isinstance(blueprint, dict):
            return True
        enabled = blueprint.get("enable_graph", True)
        return bool(enabled)

    @staticmethod
    def _is_literal_edge_enabled(field: Dict[str, Any]) -> bool:
        raw = field.get("literal_edge")
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return raw != 0
        if isinstance(raw, str):
            return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
        return False

    @staticmethod
    def _to_upper_snake(raw: str) -> str:
        candidate = re.sub(r"[^A-Za-z0-9]+", "_", str(raw or "").strip())
        candidate = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", candidate)
        candidate = re.sub(r"_+", "_", candidate).strip("_")
        return candidate.upper()

    @staticmethod
    def _resolve_primary_id_field(blueprint: Dict[str, Any]) -> str:
        indexes = blueprint.get("indexes") if isinstance(blueprint, dict) else None
        if isinstance(indexes, dict):
            path = indexes.get("path")
            if isinstance(path, list):
                for entry in path:
                    entry_str = str(entry).strip()
                    if entry_str:
                        return entry_str
        return "_id"

    @staticmethod
    def _normalize_literal_scalar(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (int, float, bool)):
            return value
        if isinstance(value, dict):
            # Stable map ordering keeps canonical value deterministic.
            return {str(k): GraphController._normalize_literal_scalar(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
        if isinstance(value, list):
            return [GraphController._normalize_literal_scalar(v) for v in value]
        return str(value).strip()

    @staticmethod
    def _is_empty_literal_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, (list, dict)):
            return len(value) == 0
        return False

    def _literal_value_token(self, value: Any) -> str:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha1(encoded.encode("utf-8")).hexdigest()

    def _extract_literal_edge_declarations(self, raw_value: Any, spec: Dict[str, Any]) -> List[Tuple[str, str, Dict[str, Any]]]:
        values = raw_value if isinstance(raw_value, list) else [raw_value]
        declarations: List[Tuple[str, str, Dict[str, Any]]] = []
        edge_type = str(spec.get("edge_type") or "").strip()
        label = str(spec.get("literal_edge_label") or edge_type).strip() or edge_type
        field_name = str(spec.get("field_name") or "").strip()
        if not edge_type or not field_name:
            return declarations

        seen_tokens: Set[str] = set()
        for value in values:
            normalized = self._normalize_literal_scalar(value)
            if self._is_empty_literal_value(normalized):
                continue
            value_token = self._literal_value_token(normalized)
            if value_token in seen_tokens:
                continue
            seen_tokens.add(value_token)
            to_node_id = f"_literal/{field_name}/{value_token}"
            declarations.append(
                (
                    edge_type,
                    to_node_id,
                    {
                        "value": normalized,
                        "label_forward": label,
                        "qualifiers": {},
                    },
                )
            )
        return declarations

    def _get_edge_specs_from_blueprint(self, blueprint, ring: str):
        if not isinstance(blueprint, dict):
            return []
        if not self._is_graph_enabled(blueprint):
            return []

        from_blueprint = blueprint.get("name") if isinstance(blueprint.get("name"), str) else ring
        primary_id_field = self._resolve_primary_id_field(blueprint)
        specs = []
        for field in blueprint.get("fields", []):
            if not isinstance(field, dict):
                continue
            field_name = field.get("name")
            if field_name is None:
                continue
            field_name_value = str(field_name)
            source = field.get("source")

            if source:
                source_parts = self._parse_edge_source(source)
                if source_parts:
                    edge_type = self._implicit_edge_type(
                        from_blueprint,
                        field_name_value,
                        source_parts["to_ring"],
                        source_parts["id_token"],
                    )
                    if edge_type:
                        specs.append(
                            {
                                "kind": "source",
                                "field_name": field_name_value,
                                "edge_type": edge_type,
                                "to_ring": source_parts["to_ring"],
                                "id_token": source_parts["id_token"],
                                "label_fields": source_parts.get("label_fields", []),
                                "edge_labels": source_parts.get("edge_labels", []),
                                "qualifier_keys": source_parts.get("qualifier_keys", []),
                                "source": source_parts["source_raw"],
                            }
                        )

            if self._is_literal_edge_enabled(field):
                literal_label = f"HAS_{self._to_upper_snake(field_name_value)}"
                literal_edge_type = f"{from_blueprint}:{primary_id_field}:_literal:{field_name_value}"
                specs.append(
                    {
                        "kind": "literal",
                        "field_name": field_name_value,
                        "edge_type": literal_edge_type,
                        "literal_edge_label": literal_label,
                    }
                )
        return specs

    def _merge_edge_properties(self, current_props, next_props):
        if not isinstance(current_props, dict):
            return next_props if isinstance(next_props, dict) else None
        if not isinstance(next_props, dict):
            return current_props
        merged = dict(current_props)
        for key, value in next_props.items():
            if key == "qualifiers" and isinstance(value, dict):
                prior = merged.get("qualifiers") if isinstance(merged.get("qualifiers"), dict) else {}
                merged["qualifiers"] = {**prior, **value}
            else:
                merged[key] = value
        return merged

    def _extract_edge_declarations(self, raw_value, spec):
        values = raw_value if isinstance(raw_value, list) else [raw_value]
        declarations = []

        for value in values:
            if value is None:
                continue

            value_obj = value if isinstance(value, dict) else {}
            target_obj = value_obj.get("target") if isinstance(value_obj.get("target"), dict) else {}

            candidate_ids = []
            id_token = spec.get("id_token")
            if isinstance(id_token, str) and id_token:
                candidate_ids.append(target_obj.get(id_token))
                candidate_ids.append(value_obj.get(id_token))
            candidate_ids.extend(
                [
                    target_obj.get("id"),
                    target_obj.get("_id"),
                    target_obj.get("value"),
                    value_obj.get("_id"),
                    value_obj.get("id"),
                    value_obj.get("to_id"),
                    value_obj.get("value"),
                    value,
                ]
            )

            to_id = None
            for candidate in candidate_ids:
                if candidate is None or isinstance(candidate, (dict, list)):
                    continue
                candidate_str = str(candidate).strip()
                if candidate_str:
                    to_id = candidate_str
                    break
            if not to_id:
                continue

            edge_type = spec.get("edge_type")
            if not isinstance(edge_type, str) or not edge_type.strip():
                continue

            edge_props = {}
            qualifiers = {}
            declared_qualifiers = spec.get("qualifier_keys") or []
            raw_qualifiers = value_obj.get("qualifiers")
            if isinstance(raw_qualifiers, dict):
                if declared_qualifiers:
                    for qualifier_key in declared_qualifiers:
                        if qualifier_key in raw_qualifiers:
                            qualifiers[qualifier_key] = raw_qualifiers[qualifier_key]
                else:
                    qualifiers.update(raw_qualifiers)

            if declared_qualifiers:
                for qualifier_key in declared_qualifiers:
                    if qualifier_key in value_obj and qualifier_key not in qualifiers:
                        qualifiers[qualifier_key] = value_obj[qualifier_key]

            spec_edge_labels = spec.get("edge_labels") if isinstance(spec.get("edge_labels"), list) else []
            raw_value_label = value_obj.get("label")
            value_edge_labels = []
            if isinstance(raw_value_label, list):
                value_edge_labels = [str(token).strip() for token in raw_value_label if str(token).strip()]
            elif isinstance(raw_value_label, str) and raw_value_label.strip():
                value_edge_labels = [token.strip() for token in raw_value_label.split(",") if token and token.strip()]

            resolved_edge_labels = value_edge_labels if value_edge_labels else spec_edge_labels
            forward_edge_label = resolved_edge_labels[0] if len(resolved_edge_labels) > 0 else edge_type.strip()
            backward_edge_label = resolved_edge_labels[1] if len(resolved_edge_labels) > 1 else edge_type.strip()

            if qualifiers:
                edge_props["qualifiers"] = qualifiers
            edge_props["label_forward"] = forward_edge_label
            edge_props["label_backward"] = backward_edge_label

            declarations.append((edge_type.strip(), to_id, edge_props))

        return declarations

    def _build_desired_edges(self, edge_specs, attributes):
        if not isinstance(attributes, dict):
            return []

        desired_by_key = {}
        for spec in edge_specs:
            field_name = spec["field_name"]
            if field_name not in attributes:
                continue
            if spec.get("kind") == "literal":
                declarations = self._extract_literal_edge_declarations(attributes.get(field_name), spec)
            else:
                declarations = self._extract_edge_declarations(attributes.get(field_name), spec)
            for edge_type, to_id, edge_props in declarations:
                if spec.get("kind") == "literal":
                    to_node_id = to_id
                else:
                    to_node_id = self.make_node_id(spec["to_ring"], to_id)
                key = (edge_type, to_node_id)
                desired_by_key[key] = self._merge_edge_properties(desired_by_key.get(key), edge_props)

        return [(edge_type, to_node_id, props) for (edge_type, to_node_id), props in desired_by_key.items()]

    def upsert_edge_and_verify(self, portfolio, org, edge_type, from_node_id, to_node_id, properties=None):
        before = self.get_edge(portfolio, org, edge_type, from_node_id, to_node_id)
        self.put_edge(
            portfolio,
            org,
            edge_type,
            from_node_id,
            to_node_id,
            properties=properties or {},
        )
        after = self.get_edge(portfolio, org, edge_type, from_node_id, to_node_id)
        return {
            'success': after is not None,
            'existed_before': before is not None,
            'exists_after': after is not None,
        }

    def remove_edge_and_verify(self, portfolio, org, edge_type, from_node_id, to_node_id):
        existed_before = self.get_edge(portfolio, org, edge_type, from_node_id, to_node_id) is not None
        removed = self.remove_edge(portfolio, org, edge_type, from_node_id, to_node_id)
        exists_after = self.get_edge(portfolio, org, edge_type, from_node_id, to_node_id) is not None

        return {
            'success': not exists_after,
            'existed_before': existed_before,
            'removed': bool(removed),
            'exists_after': exists_after,
        }

    def _get_blueprint_for_ring(self, ring: str, blueprint_handle: Optional[str] = None):
        bpc = self._get_blueprint_controller()
        if not bpc:
            return {}

        handles: List[str] = []
        if blueprint_handle:
            handles.append(str(blueprint_handle))
        cfg_handle = self.config.get("BLUEPRINT_HANDLE")
        if cfg_handle:
            handles.append(str(cfg_handle))
        handles.append("irma")

        seen = set()
        for handle in handles:
            if not handle or handle in seen:
                continue
            seen.add(handle)

            cache_key = (handle, ring)
            cached = self._blueprint_cache.get(cache_key)
            if cached is not None:
                blueprint = cached
            else:
                blueprint = bpc.get_blueprint(handle, ring, "last")
                if isinstance(blueprint, dict):
                    self._blueprint_cache[cache_key] = blueprint

            if isinstance(blueprint, dict) and isinstance(blueprint.get("fields"), list):
                return blueprint

        # Return the last attempted shape for diagnostics compatibility.
        return blueprint if "blueprint" in locals() else {}

    def sync_document_graph_edges(self, portfolio, org, ring, idx, attributes, blueprint_handle: Optional[str] = None):
        blueprint = self._get_blueprint_for_ring(ring, blueprint_handle=blueprint_handle)
        edge_specs = self._get_edge_specs_from_blueprint(blueprint, ring)
        if not edge_specs:
            return {'success': True, 'skipped': True, 'reason': 'No valid blueprint source relationships found'}

        desired_edges = self._build_desired_edges(edge_specs, attributes)
        managed_edge_types = sorted({spec["edge_type"] for spec in edge_specs})

        from_node_id = GraphController.make_node_id(ring, idx)
        sync_result = self.sync_node_edges(
            portfolio,
            org,
            from_node_id,
            desired_edges=desired_edges,
            managed_edge_types=managed_edge_types or None,
        )

        missing = 0
        for edge_type, to_node_id, _ in desired_edges:
            if self.get_edge(portfolio, org, edge_type, from_node_id, to_node_id) is None:
                missing += 1

        return {
            'success': missing == 0,
            'node_id': from_node_id,
            'managed_edge_types': managed_edge_types,
            'edge_specs': len(edge_specs),
            'sync': sync_result,
            'missing_desired_edges': missing,
        }

    def remove_document_graph_edges(self, portfolio, org, ring, idx, attributes, blueprint_handle: Optional[str] = None):
        blueprint = self._get_blueprint_for_ring(ring, blueprint_handle=blueprint_handle)
        edge_specs = self._get_edge_specs_from_blueprint(blueprint, ring)
        managed_edge_types = sorted({spec["edge_type"] for spec in edge_specs})
        if not managed_edge_types:
            return {'success': True, 'skipped': True, 'reason': 'No valid blueprint source relationships found'}

        node_id = GraphController.make_node_id(ring, idx)
        remove_result = self.remove_node_edges(portfolio, org, node_id, managed_edge_types)
        verification = self.verify_node_edges_removed(portfolio, org, node_id, managed_edge_types)

        return {
            'success': verification.get('remaining', 0) == 0,
            'node_id': node_id,
            'managed_edge_types': managed_edge_types,
            'remove': remove_result,
            'verification': verification,
        }


if __name__ == "__main__":
    graph = GraphController(config={"DYNAMODB_GRAPH_TABLE": "renglo_graph", "AWS_REGION": "us-east-1"})

    portfolio = "p_acme"
    org = "o_hotelco"

    reservation = GraphController.make_node_id("Reservation", "resv_123")
    user = GraphController.make_node_id("User", "u_001")
    hotel = GraphController.make_node_id("Hotel", "h_999")

    graph.put_edge(portfolio, org, "BOOKED_BY", reservation, user)
    graph.put_edge(portfolio, org, "FOR_HOTEL", reservation, hotel)

    outgoing = graph.list_outgoing_edges(portfolio, org, "BOOKED_BY", reservation)
    print(outgoing.items)

    result = graph.traverse(
        portfolio,
        org,
        start_node_id=reservation,
        edge_types=["BOOKED_BY", "FOR_HOTEL"],
        max_depth=2,
        timeout_seconds=5,
    )
    print(result.visited_nodes)
