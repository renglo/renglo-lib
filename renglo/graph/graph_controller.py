"""
Graph controller interface layer.

The controller exposes the public graph API to the application and delegates
data/persistence behavior to GraphModel.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

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
        reverse_index_name: str = "backward_label",
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

    def _parse_edge_source(self, source: str):
        if not isinstance(source, str):
            return None
        parts = [p.strip() for p in source.split(":")]
        if len(parts) != 3:
            return None
        to_ring, id_token, label_field = parts
        if not to_ring or id_token != "_id":
            return None
        return {
            "to_ring": to_ring,
            "label_field": label_field,
            "id_token": id_token,
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

    def _get_edge_specs_from_blueprint(self, blueprint, ring: str):
        if not isinstance(blueprint, dict):
            return []
        if not self._is_graph_enabled(blueprint):
            return []

        from_blueprint = blueprint.get("name") if isinstance(blueprint.get("name"), str) else ring
        specs = []
        for field in blueprint.get("fields", []):
            if not isinstance(field, dict):
                continue
            source = field.get("source")
            field_name = field.get("name")
            if not source or field_name is None:
                continue
            source_parts = self._parse_edge_source(source)
            if not source_parts:
                continue
            field_name_value = str(field_name)
            edge_type = self._implicit_edge_type(
                from_blueprint,
                field_name_value,
                source_parts["to_ring"],
                source_parts["id_token"],
            )
            if not edge_type:
                continue
            specs.append(
                {
                    "field_name": field_name_value,
                    "edge_type": edge_type,
                    "to_ring": source_parts["to_ring"],
                    "label_field": source_parts["label_field"],
                    "source": source_parts["source_raw"],
                }
            )
        return specs

    def _extract_to_ids(self, raw_value):
        values = raw_value if isinstance(raw_value, list) else [raw_value]
        result = []
        for value in values:
            if value is None:
                continue
            if isinstance(value, dict):
                candidate = value.get("_id") or value.get("id") or value.get("to_id")
                if candidate:
                    result.append(str(candidate))
                continue
            value_str = str(value).strip()
            if value_str:
                result.append(value_str)
        return result

    def _build_desired_edges(self, edge_specs, attributes):
        if not isinstance(attributes, dict):
            return []

        desired_by_key = {}
        for spec in edge_specs:
            field_name = spec["field_name"]
            if field_name not in attributes:
                continue
            to_ids = self._extract_to_ids(attributes.get(field_name))
            for to_id in to_ids:
                to_node_id = self.make_node_id(spec["to_ring"], to_id)
                desired_by_key[(spec["edge_type"], to_node_id)] = None

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
