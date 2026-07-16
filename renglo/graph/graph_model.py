"""
Renglo Graph DB model layer.

This module owns graph data models and DynamoDB persistence/query behavior.
Controller-level callers can compose it as needed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import boto3
from boto3.dynamodb.conditions import Key


class GraphQueryCancelled(Exception):
    """Raised when a traversal is actively cancelled."""


class GraphQueryTimeout(Exception):
    """Raised when a traversal exceeds its timeout budget."""


class GraphTraversalBudgetExceeded(Exception):
    """Raised when traversal exceeds configured node/edge/fan-out budgets."""


@dataclass(frozen=True)
class GraphEdge:
    portfolio: str
    org: str
    edge_type: str
    from_node_id: str
    to_node_id: str
    properties: Dict[str, Any] = field(default_factory=dict)

    @property
    def pk(self) -> str:
        return GraphModel.make_pk(self.portfolio, self.org)

    @property
    def sk(self) -> str:
        return GraphModel.make_forward_sk(
            self.edge_type,
            self.from_node_id,
            self.to_node_id,
        )

    @property
    def backward_index(self) -> str:
        return GraphModel.make_reverse_sk(
            self.edge_type,
            self.to_node_id,
            self.from_node_id,
        )


@dataclass
class TraversalStep:
    depth: int
    edge: GraphEdge
    path: List[str]
    duplicate_visit: bool = False
    cycle_detected: bool = False


@dataclass
class TraversalResult:
    start_node_id: str
    direction: str
    visited_nodes: Set[str]
    visited_edges: Set[str]
    steps: List[TraversalStep]
    cycles_detected: List[List[str]]
    duplicate_visits: Dict[str, int]
    stopped_reason: Optional[str] = None
    next_frontier: Optional[List[Tuple[str, int, List[str]]]] = None


@dataclass
class PageResult:
    items: List[GraphEdge]
    last_evaluated_key: Optional[Dict[str, Any]] = None


class GraphModel:
    """
    DynamoDB-backed graph edge model.

    This class intentionally does not own canonical documents/nodes. The document
    table remains the source of truth. This class stores and traverses relationships.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        region_name: Optional[str] = None,
        dynamodb_resource: Optional[Any] = None,
        reverse_index_name: str = "backward_index",
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.config = config or {}
        resolved_region = region_name or self.config.get("AWS_REGION", "us-east-1")
        resolved_table_name = self.config.get("DYNAMODB_GRAPH_TABLE")
        if not resolved_table_name:
            raise ValueError("DYNAMODB_GRAPH_TABLE configuration is required but not found")

        self.dynamodb = dynamodb_resource or boto3.resource("dynamodb", region_name=resolved_region)
        self.table = self.dynamodb.Table(resolved_table_name)
        self.DYNAMODB_GRAPH_TABLE = resolved_table_name
        self.reverse_index_name = reverse_index_name
        self.clock = clock

    # -------------------------------------------------------------------------
    # Key helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def make_pk(portfolio: str, org: str) -> str:
        GraphModel._require_safe_graph_index_part("portfolio", portfolio)
        GraphModel._require_safe_graph_index_part("org", org)
        return f"irn:edge:{portfolio}:{org}"

    @staticmethod
    def make_node_id(ring: str, node_id: str) -> str:
        """
        Recommended normalized node id inside one portfolio#org.

        Example:
            User/u123
            Reservation/resv_123
            Application/booking-api
        """
        GraphModel._require_safe_key_part("ring", ring)
        GraphModel._require_safe_key_part("node_id", node_id)
        return f"{ring}/{node_id}"

    @staticmethod
    def split_node_id(node_id: str) -> Tuple[str, str]:
        if not isinstance(node_id, str):
            raise ValueError("node_id must be a string")
        parts = node_id.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"Invalid node_id format: {node_id!r}")
        return parts[0], parts[1]

    @staticmethod
    def make_forward_sk(edge_type: str, from_node_id: str, to_node_id: str) -> str:
        GraphModel._require_safe_key_part("edge_type", edge_type)
        GraphModel._require_safe_key_part("from_node_id", from_node_id)
        GraphModel._require_safe_key_part("to_node_id", to_node_id)
        return f"{edge_type}#{from_node_id}#{to_node_id}"

    @staticmethod
    def make_reverse_edge_type(edge_type: str, to_node_id: Optional[str] = None) -> str:
        GraphModel._require_safe_key_part("edge_type", edge_type)
        parts = edge_type.split(":")
        # Canonical implicit type: <from_blueprint>:<from_field>:<to_blueprint>:<to_field>
        # Backward wildcard label: <to_blueprint>:<to_field>:*:*
        if len(parts) != 4:
            # New explicit edge labels (for example "DELEGATES_TO") do not encode
            # target blueprint metadata. Derive reverse partition from destination.
            if not to_node_id:
                raise ValueError(
                    f"Invalid edge_type format (expected 4 colon-separated parts): {edge_type!r}"
                )
            to_ring, _ = GraphModel.split_node_id(to_node_id)
            return f"{to_ring}:_id:{edge_type}:*"
        return f"{parts[2]}:{parts[3]}:*:*"

    @staticmethod
    def make_reverse_sk(edge_type: str, to_node_id: str, from_node_id: str) -> str:
        reverse_edge_type = GraphModel.make_reverse_edge_type(edge_type, to_node_id=to_node_id)
        GraphModel._require_safe_key_part("to_node_id", to_node_id)
        GraphModel._require_safe_key_part("from_node_id", from_node_id)
        return f"{reverse_edge_type}#{to_node_id}#{from_node_id}"

    @staticmethod
    def _require_safe_key_part(name: str, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string")
        if "#" in value:
            raise ValueError(f"{name} cannot contain '#': {value!r}")

    @staticmethod
    def _require_safe_graph_index_part(name: str, value: str) -> None:
        GraphModel._require_safe_key_part(name, value)
        if ":" in value:
            raise ValueError(f"{name} cannot contain ':': {value!r}")

    @staticmethod
    def _now_iso() -> str:
        # Keep this dependency-free. Store sortable epoch string.
        return str(int(time.time()))

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def _edge_to_item(self, edge: GraphEdge) -> Dict[str, Any]:
        now = self._now_iso()
        item: Dict[str, Any] = {
            "graph_index": edge.pk,
            "forward_index": edge.sk,
            "backward_index": edge.backward_index,
            "created_at": now,
            "updated_at": now,
        }
        for key, value in edge.properties.items():
            if key in item or key in {"graph_index", "forward_index", "backward_index"}:
                raise ValueError(f"edge property uses reserved name: {key}")
            item[key] = self._to_dynamo_value(value)
        return item

    @staticmethod
    def _parse_graph_index(graph_index: str) -> Tuple[str, str]:
        if not isinstance(graph_index, str) or not graph_index.startswith("irn:edge:"):
            raise ValueError(f"Invalid graph_index format: {graph_index!r}")
        parts = graph_index.split(":", 3)
        if len(parts) != 4:
            raise ValueError(f"Invalid graph_index format: {graph_index!r}")
        return parts[2], parts[3]

    @staticmethod
    def _parse_forward_index(forward_index: str) -> Tuple[str, str, str]:
        if not isinstance(forward_index, str):
            raise ValueError(f"Invalid forward_index format: {forward_index!r}")
        parts = forward_index.split("#", 2)
        if len(parts) != 3:
            raise ValueError(f"Invalid forward_index format: {forward_index!r}")
        return parts[0], parts[1], parts[2]

    def _item_to_edge(self, item: Dict[str, Any]) -> GraphEdge:
        reserved = {
            "graph_index",
            "forward_index",
            "backward_index",
            "created_at",
            "updated_at",
        }
        props = {k: v for k, v in item.items() if k not in reserved}
        portfolio, org = self._parse_graph_index(item["graph_index"])
        edge_type, from_node_id, to_node_id = self._parse_forward_index(item["forward_index"])
        return GraphEdge(
            portfolio=portfolio,
            org=org,
            edge_type=edge_type,
            from_node_id=from_node_id,
            to_node_id=to_node_id,
            properties=props,
        )

    def _to_dynamo_value(self, value: Any) -> Any:
        # DynamoDB does not accept float. Convert recursively to Decimal.
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, dict):
            return {k: self._to_dynamo_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._to_dynamo_value(v) for v in value]
        return value

    # -------------------------------------------------------------------------
    # Basic edge writes
    # -------------------------------------------------------------------------

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
        """
        Idempotently insert/update an edge.

        Calling this multiple times for the same edge creates one edge only because
        graph_index+forward_index is deterministic.
        """
        edge = GraphEdge(
            portfolio=portfolio,
            org=org,
            edge_type=edge_type,
            from_node_id=from_node_id,
            to_node_id=to_node_id,
            properties=properties or {},
        )
        item = self._edge_to_item(edge)

        # Preserve created_at if edge already exists; update all other fields.
        update_expr_parts = []
        expr_names: Dict[str, str] = {}
        expr_values: Dict[str, Any] = {}

        for key, value in item.items():
            if key in {"graph_index", "forward_index", "created_at"}:
                continue
            name_key = f"#{key}"
            value_key = f":{key}"
            expr_names[name_key] = key
            expr_values[value_key] = value
            update_expr_parts.append(f"{name_key} = {value_key}")

        expr_names["#created_at"] = "created_at"
        expr_values[":created_at"] = item["created_at"]
        update_expression = (
            "SET "
            + ", ".join(update_expr_parts)
            + ", #created_at = if_not_exists(#created_at, :created_at)"
        )

        self.table.update_item(
            Key={"graph_index": edge.pk, "forward_index": edge.sk},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
        )
        return edge

    def remove_edge(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        from_node_id: str,
        to_node_id: str,
    ) -> bool:
        """
        Idempotently remove an edge.

        Returns True if the edge existed before deletion, False otherwise.
        """
        pk = self.make_pk(portfolio, org)
        sk = self.make_forward_sk(edge_type, from_node_id, to_node_id)
        response = self.table.delete_item(
            Key={"graph_index": pk, "forward_index": sk},
            ReturnValues="ALL_OLD",
        )
        return "Attributes" in response

    def get_edge(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        from_node_id: str,
        to_node_id: str,
    ) -> Optional[GraphEdge]:
        pk = self.make_pk(portfolio, org)
        sk = self.make_forward_sk(edge_type, from_node_id, to_node_id)
        response = self.table.get_item(Key={"graph_index": pk, "forward_index": sk})
        item = response.get("Item")
        return self._item_to_edge(item) if item else None

    # -------------------------------------------------------------------------
    # Discovery and node-specific searches
    # -------------------------------------------------------------------------

    def list_edges_by_type(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        *,
        limit: int = 100,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
    ) -> PageResult:
        """Discovery: who is using an edge type inside portfolio#org?"""
        return self._query_forward(
            portfolio,
            org,
            begins_with_value=f"{edge_type}#",
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
        """Find all edges of one type going out from a node."""
        return self._query_forward(
            portfolio,
            org,
            begins_with_value=f"{edge_type}#{from_node_id}#",
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
        """
        Find all edges of one type pointing into a node using the reverse LSI.

        Since backward_index uses wildcard source slots, query by destination node
        prefix and filter by exact edge_type.
        """
        reverse_edge_type = self.make_reverse_edge_type(edge_type, to_node_id=to_node_id)
        begins_with_value = f"{reverse_edge_type}#{to_node_id}#"
        matched: List[GraphEdge] = []
        cursor = exclusive_start_key
        last_key: Optional[Dict[str, Any]] = None

        while len(matched) < limit:
            page = self._query_reverse(
                portfolio,
                org,
                begins_with_value=begins_with_value,
                limit=limit,
                exclusive_start_key=cursor,
            )
            for edge in page.items:
                if edge.edge_type == edge_type and edge.to_node_id == to_node_id:
                    matched.append(edge)
                    if len(matched) >= limit:
                        break
            last_key = page.last_evaluated_key
            if not last_key or len(matched) >= limit:
                break
            cursor = last_key

        return PageResult(items=matched, last_evaluated_key=last_key)

    def list_incoming_edges_any_type(
        self,
        portfolio: str,
        org: str,
        to_node_id: str,
        *,
        limit: int = 100,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
    ) -> PageResult:
        """
        Discovery fallback: find incoming edges for one node without known edge types.

        Uses wildcard backward_index prefix by exact destination node id.
        """
        to_ring, _ = self.split_node_id(to_node_id)
        begins_with_value = f"{to_ring}:_id:*:*#{to_node_id}#"
        return self._query_reverse(
            portfolio,
            org,
            begins_with_value=begins_with_value,
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
        """Node-specific search: who is going from A to B?"""
        edge = self.get_edge(portfolio, org, edge_type, from_node_id, to_node_id)
        return [edge] if edge else []

    def _query_forward(
        self,
        portfolio: str,
        org: str,
        *,
        begins_with_value: str,
        limit: int,
        exclusive_start_key: Optional[Dict[str, Any]],
    ) -> PageResult:
        query_kwargs = {
            "KeyConditionExpression": Key("graph_index").eq(self.make_pk(portfolio, org))
            & Key("forward_index").begins_with(begins_with_value),
            "Limit": limit,
        }
        if exclusive_start_key:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key
        response = self.table.query(**query_kwargs)
        return PageResult(
            items=[self._item_to_edge(item) for item in response.get("Items", [])],
            last_evaluated_key=response.get("LastEvaluatedKey"),
        )

    def _query_reverse(
        self,
        portfolio: str,
        org: str,
        *,
        begins_with_value: str,
        limit: int,
        exclusive_start_key: Optional[Dict[str, Any]],
    ) -> PageResult:
        query_kwargs = {
            "IndexName": self.reverse_index_name,
            "KeyConditionExpression": Key("graph_index").eq(self.make_pk(portfolio, org))
            & Key("backward_index").begins_with(begins_with_value),
            "Limit": limit,
        }
        if exclusive_start_key:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key
        response = self.table.query(**query_kwargs)
        raw_items = response.get("Items", [])
        hydrated_items = []
        for item in raw_items:
            if self._needs_reverse_item_hydration(item):
                hydrated_item = self._get_full_edge_item(
                    item.get("graph_index"),
                    item.get("forward_index"),
                )
                hydrated_items.append(hydrated_item or item)
            else:
                hydrated_items.append(item)
        return PageResult(
            items=[self._item_to_edge(item) for item in hydrated_items],
            last_evaluated_key=response.get("LastEvaluatedKey"),
        )

    def _needs_reverse_item_hydration(self, item: Dict[str, Any]) -> bool:
        if not isinstance(item, dict):
            return False
        # If reverse-index projection is KEYS_ONLY or partial, non-key properties
        # such as edge labels/attributes are not present.
        if "created_at" in item or "updated_at" in item:
            return False
        if "graph_index" not in item or "forward_index" not in item:
            return False
        return True

    def _get_full_edge_item(self, graph_index: Any, forward_index: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(graph_index, str) or not isinstance(forward_index, str):
            return None
        response = self.table.get_item(
            Key={
                "graph_index": graph_index,
                "forward_index": forward_index,
            }
        )
        item = response.get("Item")
        return item if isinstance(item, dict) else None

    # -------------------------------------------------------------------------
    # Traversal
    # -------------------------------------------------------------------------

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
        """
        Bounded BFS traversal.

        Features implemented:
        - cycle detection
        - duplicate visit detection
        - depth limit
        - edge filtering via explicit edge_types
        - query timeout/cancellation
        - traversal budgets
        - fan-out pruning
        - optional edge scoring
        - optional partial frontier return
        """
        if direction not in {"forward", "backward"}:
            raise ValueError("direction must be 'forward' or 'backward'")
        if max_depth < 0:
            raise ValueError("max_depth must be >= 0")
        if not edge_types:
            raise ValueError("edge_types cannot be empty")

        started_at = self.clock()
        visited_nodes: Set[str] = {start_node_id}
        visited_edges: Set[str] = set()
        duplicate_visits: Dict[str, int] = {}
        cycles_detected: List[List[str]] = []
        steps: List[TraversalStep] = []
        frontier: List[Tuple[str, int, List[str]]] = [(start_node_id, 0, [start_node_id])]
        stopped_reason: Optional[str] = None

        while frontier:
            self._check_cancel_or_timeout(started_at, timeout_seconds, cancel_check)
            current_node_id, depth, path = frontier.pop(0)

            if depth >= max_depth:
                continue

            neighbors_seen_for_node = 0

            for edge_type in edge_types:
                page_key = None
                while True:
                    self._check_cancel_or_timeout(started_at, timeout_seconds, cancel_check)

                    if direction == "forward":
                        page = self.list_outgoing_edges(
                            portfolio,
                            org,
                            edge_type,
                            current_node_id,
                            limit=per_query_limit,
                            exclusive_start_key=page_key,
                        )
                    else:
                        page = self.list_incoming_edges(
                            portfolio,
                            org,
                            edge_type,
                            current_node_id,
                            limit=per_query_limit,
                            exclusive_start_key=page_key,
                        )

                    for edge in page.items:
                        self._check_cancel_or_timeout(started_at, timeout_seconds, cancel_check)

                        edge_id = edge.sk
                        if edge_id in visited_edges:
                            continue

                        if score_edge is not None and min_score is not None:
                            if score_edge(edge) < min_score:
                                continue

                        visited_edges.add(edge_id)
                        next_node_id = edge.to_node_id if direction == "forward" else edge.from_node_id
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

                        # Do not enqueue cycles or already-visited nodes.
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
            direction=direction,
            visited_nodes=visited_nodes,
            visited_edges=visited_edges,
            steps=steps,
            cycles_detected=cycles_detected,
            duplicate_visits=duplicate_visits,
            stopped_reason=stopped_reason,
            next_frontier=frontier if return_frontier_on_stop else None,
        )

    def _check_cancel_or_timeout(
        self,
        started_at: float,
        timeout_seconds: float,
        cancel_check: Optional[Callable[[], bool]],
    ) -> None:
        if cancel_check and cancel_check():
            raise GraphQueryCancelled("Traversal cancelled")
        if timeout_seconds is not None and self.clock() - started_at > timeout_seconds:
            raise GraphQueryTimeout(f"Traversal exceeded {timeout_seconds} seconds")

    # -------------------------------------------------------------------------
    # Delete node + incident edges
    # -------------------------------------------------------------------------

    def remove_node_edges(
        self,
        portfolio: str,
        org: str,
        node_id: str,
        edge_types: Sequence[str],
        *,
        batch_size: int = 25,
    ) -> Dict[str, int]:
        """
        Remove all edges touching a node for the given edge types.

        Since edges are indexed by edge type, callers must pass the Blueprint-known
        edge types that may touch this node.
        """
        deleted = 0
        inspected = 0

        for edge_type in edge_types:
            outgoing = self._drain_pages(
                lambda key: self.list_outgoing_edges(
                    portfolio,
                    org,
                    edge_type,
                    node_id,
                    limit=100,
                    exclusive_start_key=key,
                )
            )
            incoming = self._drain_pages(
                lambda key: self.list_incoming_edges(
                    portfolio,
                    org,
                    edge_type,
                    node_id,
                    limit=100,
                    exclusive_start_key=key,
                )
            )

            # Deduplicate in case of self-loop.
            edges_by_sk = {edge.sk: edge for edge in [*outgoing, *incoming]}
            inspected += len(edges_by_sk)
            deleted += self._batch_delete_edges(edges_by_sk.values(), batch_size=batch_size)

        verification = self.verify_node_edges_removed(portfolio, org, node_id, edge_types)
        return {
            "inspected": inspected,
            "deleted": deleted,
            "remaining": verification["remaining"],
        }

    def verify_node_edges_removed(
        self,
        portfolio: str,
        org: str,
        node_id: str,
        edge_types: Sequence[str],
    ) -> Dict[str, int]:
        remaining = 0
        for edge_type in edge_types:
            outgoing = self.list_outgoing_edges(portfolio, org, edge_type, node_id, limit=1)
            incoming = self.list_incoming_edges(portfolio, org, edge_type, node_id, limit=1)
            remaining += len(outgoing.items) + len(incoming.items)
        return {"remaining": remaining}

    def _batch_delete_edges(self, edges: Iterable[GraphEdge], *, batch_size: int = 25) -> int:
        count = 0
        with self.table.batch_writer() as batch:
            for edge in edges:
                batch.delete_item(Key={"graph_index": edge.pk, "forward_index": edge.sk})
                count += 1
        return count

    def _drain_pages(self, page_fn: Callable[[Optional[Dict[str, Any]]], PageResult]) -> List[GraphEdge]:
        results: List[GraphEdge] = []
        key = None
        while True:
            page = page_fn(key)
            results.extend(page.items)
            key = page.last_evaluated_key
            if not key:
                return results

    # -------------------------------------------------------------------------
    # Stale edge / orphan detection
    # -------------------------------------------------------------------------

    def find_orphan_edges_for_node(
        self,
        portfolio: str,
        org: str,
        node_id: str,
        edge_types: Sequence[str],
        node_exists: Callable[[str], bool],
    ) -> List[GraphEdge]:
        """
        Check edges touching one node and return edges whose opposite endpoint does
        not exist in the canonical document store.

        node_exists receives normalized node_id, e.g. 'User/u123'.
        """
        orphans: List[GraphEdge] = []
        for edge_type in edge_types:
            outgoing = self._drain_pages(
                lambda key: self.list_outgoing_edges(portfolio, org, edge_type, node_id, exclusive_start_key=key)
            )
            incoming = self._drain_pages(
                lambda key: self.list_incoming_edges(portfolio, org, edge_type, node_id, exclusive_start_key=key)
            )

            for edge in outgoing:
                if not node_exists(edge.to_node_id):
                    orphans.append(edge)
            for edge in incoming:
                if not node_exists(edge.from_node_id):
                    orphans.append(edge)
        return orphans

    def scan_orphan_edges_by_type(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        node_exists: Callable[[str], bool],
        *,
        limit_pages: Optional[int] = None,
    ) -> List[GraphEdge]:
        """
        Async-friendly check for all edges of one type inside an org.

        Use from a background job. For large orgs, call repeatedly with your own
        cursor strategy or limit pages per execution.
        """
        orphans: List[GraphEdge] = []
        pages = 0
        key = None
        while True:
            page = self.list_edges_by_type(
                portfolio,
                org,
                edge_type,
                limit=100,
                exclusive_start_key=key,
            )
            for edge in page.items:
                if not node_exists(edge.from_node_id) or not node_exists(edge.to_node_id):
                    orphans.append(edge)
            key = page.last_evaluated_key
            pages += 1
            if not key or (limit_pages is not None and pages >= limit_pages):
                return orphans

    def remove_edges(self, edges: Iterable[GraphEdge]) -> int:
        """Idempotently remove many edges."""
        return self._batch_delete_edges(edges)

    # -------------------------------------------------------------------------
    # Idempotent sync from desired state
    # -------------------------------------------------------------------------

    def sync_node_edges(
        self,
        portfolio: str,
        org: str,
        from_node_id: str,
        desired_edges: Sequence[Tuple[str, str, Optional[Dict[str, Any]]]],
        *,
        managed_edge_types: Optional[Sequence[str]] = None,
    ) -> Dict[str, int]:
        """
        Idempotently sync all outgoing graph edges for one node.

        desired_edges items:
            (edge_type, to_node_id, properties)

        If managed_edge_types is omitted, it is derived from desired_edges. Pass the
        Blueprint-known edge types when you also need to remove stale edges for edge
        types that are now absent from the document.
        """
        desired_by_key: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {
            (edge_type, to_node_id): properties
            for edge_type, to_node_id, properties in desired_edges
        }

        edge_types = list(managed_edge_types or sorted({e[0] for e in desired_edges}))
        existing_by_key: Dict[Tuple[str, str], GraphEdge] = {}

        for edge_type in edge_types:
            existing = self._drain_pages(
                lambda key, et=edge_type: self.list_outgoing_edges(
                    portfolio,
                    org,
                    et,
                    from_node_id,
                    limit=100,
                    exclusive_start_key=key,
                )
            )
            for edge in existing:
                existing_by_key[(edge.edge_type, edge.to_node_id)] = edge

        added_or_updated = 0
        unchanged = 0
        removed = 0

        for (edge_type, to_node_id), properties in desired_by_key.items():
            existing_edge = existing_by_key.get((edge_type, to_node_id))
            desired_props = dict(properties or {})
            if existing_edge is not None:
                existing_proj = existing_edge.properties.get("projection")
                desired_proj = desired_props.get("projection")
                if isinstance(existing_proj, dict) and isinstance(desired_proj, dict):
                    existing_no_ts = {k: v for k, v in existing_proj.items() if k != "_updated"}
                    desired_no_ts = {k: v for k, v in desired_proj.items() if k != "_updated"}
                    if existing_no_ts == desired_no_ts and "_updated" in existing_proj:
                        desired_props["projection"] = {**desired_proj, "_updated": existing_proj["_updated"]}
            if existing_edge is not None and existing_edge.properties == desired_props:
                unchanged += 1
                continue
            self.put_edge(
                portfolio,
                org,
                edge_type,
                from_node_id,
                to_node_id,
                properties=desired_props,
            )
            added_or_updated += 1

        for edge_type, to_node_id in existing_by_key:
            if (edge_type, to_node_id) not in desired_by_key:
                self.remove_edge(portfolio, org, edge_type, from_node_id, to_node_id)
                removed += 1

        return {
            "added_or_updated": added_or_updated,
            "unchanged": unchanged,
            "removed_stale": removed,
            "desired": len(desired_by_key),
            "existing_before": len(existing_by_key),
        }