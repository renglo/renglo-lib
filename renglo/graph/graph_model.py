"""GraphModel interface.

Callers construct this class. CLOUD in config selects
renglo.graph.submodels.graph_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

import time

from renglo.submodel import load_submodel

from renglo.graph.submodels.graph_model_aws import (
    GraphQueryCancelled,
    GraphQueryTimeout,
    GraphTraversalBudgetExceeded,
    GraphEdge,
    TraversalStep,
    TraversalResult,
    PageResult,
    _dynamodb_resource,
    _describe_or_raise,
    _scan_projected,
)

def _submodel(config):
    return load_submodel("renglo.graph.submodels", "graph_model", "GraphModel", config)


class GraphModel:
    """DynamoDB-backed graph edge model.

    This class intentionally does not own canonical documents/nodes. The document
    table remains the source of truth. This class stores and traverses relationships.
    """
    def __init__(self, config: Optional[Dict[str, Any]]=None, *, region_name: Optional[str]=None, dynamodb_resource: Optional[Any]=None, reverse_index_name: str='backward_index', clock: Callable[[], float]=time.time) -> None:
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, region_name=region_name, dynamodb_resource=dynamodb_resource, reverse_index_name=reverse_index_name, clock=clock))

    @staticmethod
    def make_pk(portfolio: str, org: str) -> str:
        return _submodel(None).make_pk(portfolio, org)

    @staticmethod
    def make_node_id(ring: str, node_id: str) -> str:
        """Recommended normalized node id inside one portfolio#org.

    Example:
        User/u123
        Reservation/resv_123
        Application/booking-api
    """
        return _submodel(None).make_node_id(ring, node_id)

    @staticmethod
    def split_node_id(node_id: str) -> Tuple[str, str]:
        return _submodel(None).split_node_id(node_id)

    @staticmethod
    def make_forward_sk(edge_type: str, from_node_id: str, to_node_id: str) -> str:
        return _submodel(None).make_forward_sk(edge_type, from_node_id, to_node_id)

    @staticmethod
    def make_reverse_edge_type(edge_type: str, to_node_id: Optional[str]=None) -> str:
        return _submodel(None).make_reverse_edge_type(edge_type, to_node_id)

    @staticmethod
    def make_reverse_sk(edge_type: str, to_node_id: str, from_node_id: str) -> str:
        return _submodel(None).make_reverse_sk(edge_type, to_node_id, from_node_id)

    @staticmethod
    def _require_safe_key_part(name: str, value: str) -> None:
        return _submodel(None)._require_safe_key_part(name, value)

    @staticmethod
    def _require_safe_graph_index_part(name: str, value: str) -> None:
        return _submodel(None)._require_safe_graph_index_part(name, value)

    @staticmethod
    def _now_iso() -> str:
        return _submodel(None)._now_iso()

    def _edge_to_item(self, edge: GraphEdge) -> Dict[str, Any]:
        return self._impl._edge_to_item(edge)

    @staticmethod
    def _parse_graph_index(graph_index: str) -> Tuple[str, str]:
        return _submodel(None)._parse_graph_index(graph_index)

    @staticmethod
    def _parse_forward_index(forward_index: str) -> Tuple[str, str, str]:
        return _submodel(None)._parse_forward_index(forward_index)

    def _item_to_edge(self, item: Dict[str, Any]) -> GraphEdge:
        return self._impl._item_to_edge(item)

    def _to_dynamo_value(self, value: Any) -> Any:
        return self._impl._to_dynamo_value(value)

    def put_edge(self, portfolio: str, org: str, edge_type: str, from_node_id: str, to_node_id: str, *, properties: Optional[Dict[str, Any]]=None) -> GraphEdge:
        """Idempotently insert/update an edge.

    Calling this multiple times for the same edge creates one edge only because
    graph_index+forward_index is deterministic.
    """
        return self._impl.put_edge(portfolio, org, edge_type, from_node_id, to_node_id, properties=properties)

    def remove_edge(self, portfolio: str, org: str, edge_type: str, from_node_id: str, to_node_id: str) -> bool:
        """Idempotently remove an edge.

    Returns True if the edge existed before deletion, False otherwise.
    """
        return self._impl.remove_edge(portfolio, org, edge_type, from_node_id, to_node_id)

    def get_edge(self, portfolio: str, org: str, edge_type: str, from_node_id: str, to_node_id: str) -> Optional[GraphEdge]:
        return self._impl.get_edge(portfolio, org, edge_type, from_node_id, to_node_id)

    def list_edges_by_type(self, portfolio: str, org: str, edge_type: str, *, limit: int=100, exclusive_start_key: Optional[Dict[str, Any]]=None) -> PageResult:
        """Discovery: who is using an edge type inside portfolio#org?"""
        return self._impl.list_edges_by_type(portfolio, org, edge_type, limit=limit, exclusive_start_key=exclusive_start_key)

    def list_outgoing_edges(self, portfolio: str, org: str, edge_type: str, from_node_id: str, *, limit: int=100, exclusive_start_key: Optional[Dict[str, Any]]=None) -> PageResult:
        """Find all edges of one type going out from a node."""
        return self._impl.list_outgoing_edges(portfolio, org, edge_type, from_node_id, limit=limit, exclusive_start_key=exclusive_start_key)

    def list_incoming_edges(self, portfolio: str, org: str, edge_type: str, to_node_id: str, *, limit: int=100, exclusive_start_key: Optional[Dict[str, Any]]=None) -> PageResult:
        """Find all edges of one type pointing into a node using the reverse LSI.

    Since backward_index uses wildcard source slots, query by destination node
    prefix and filter by exact edge_type.
    """
        return self._impl.list_incoming_edges(portfolio, org, edge_type, to_node_id, limit=limit, exclusive_start_key=exclusive_start_key)

    def list_incoming_edges_any_type(self, portfolio: str, org: str, to_node_id: str, *, limit: int=100, exclusive_start_key: Optional[Dict[str, Any]]=None) -> PageResult:
        """Discovery fallback: find incoming edges for one node without known edge types.

    Uses wildcard backward_index prefix by exact destination node id.
    """
        return self._impl.list_incoming_edges_any_type(portfolio, org, to_node_id, limit=limit, exclusive_start_key=exclusive_start_key)

    def list_edges_between_nodes(self, portfolio: str, org: str, edge_type: str, from_node_id: str, to_node_id: str) -> List[GraphEdge]:
        """Node-specific search: who is going from A to B?"""
        return self._impl.list_edges_between_nodes(portfolio, org, edge_type, from_node_id, to_node_id)

    def _query_forward(self, portfolio: str, org: str, *, begins_with_value: str, limit: int, exclusive_start_key: Optional[Dict[str, Any]]) -> PageResult:
        return self._impl._query_forward(portfolio, org, begins_with_value=begins_with_value, limit=limit, exclusive_start_key=exclusive_start_key)

    def _query_reverse(self, portfolio: str, org: str, *, begins_with_value: str, limit: int, exclusive_start_key: Optional[Dict[str, Any]]) -> PageResult:
        return self._impl._query_reverse(portfolio, org, begins_with_value=begins_with_value, limit=limit, exclusive_start_key=exclusive_start_key)

    def _needs_reverse_item_hydration(self, item: Dict[str, Any]) -> bool:
        return self._impl._needs_reverse_item_hydration(item)

    def _get_full_edge_item(self, graph_index: Any, forward_index: Any) -> Optional[Dict[str, Any]]:
        return self._impl._get_full_edge_item(graph_index, forward_index)

    def traverse(self, portfolio: str, org: str, start_node_id: str, edge_types: Sequence[str], *, direction: str='forward', max_depth: int=3, per_query_limit: int=100, max_nodes: int=1000, max_edges: int=5000, max_neighbors_per_node: int=100, timeout_seconds: float=10.0, cancel_check: Optional[Callable[[], bool]]=None, score_edge: Optional[Callable[[GraphEdge], float]]=None, min_score: Optional[float]=None, include_duplicate_steps: bool=True, return_frontier_on_stop: bool=False) -> TraversalResult:
        """Bounded BFS traversal.

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
        return self._impl.traverse(portfolio, org, start_node_id, edge_types, direction=direction, max_depth=max_depth, per_query_limit=per_query_limit, max_nodes=max_nodes, max_edges=max_edges, max_neighbors_per_node=max_neighbors_per_node, timeout_seconds=timeout_seconds, cancel_check=cancel_check, score_edge=score_edge, min_score=min_score, include_duplicate_steps=include_duplicate_steps, return_frontier_on_stop=return_frontier_on_stop)

    def _check_cancel_or_timeout(self, started_at: float, timeout_seconds: float, cancel_check: Optional[Callable[[], bool]]) -> None:
        return self._impl._check_cancel_or_timeout(started_at, timeout_seconds, cancel_check)

    def remove_node_edges(self, portfolio: str, org: str, node_id: str, edge_types: Sequence[str], *, batch_size: int=25) -> Dict[str, int]:
        """Remove all edges touching a node for the given edge types.

    Since edges are indexed by edge type, callers must pass the Blueprint-known
    edge types that may touch this node.
    """
        return self._impl.remove_node_edges(portfolio, org, node_id, edge_types, batch_size=batch_size)

    def verify_node_edges_removed(self, portfolio: str, org: str, node_id: str, edge_types: Sequence[str]) -> Dict[str, int]:
        return self._impl.verify_node_edges_removed(portfolio, org, node_id, edge_types)

    def _batch_delete_edges(self, edges: Iterable[GraphEdge], *, batch_size: int=25) -> int:
        return self._impl._batch_delete_edges(edges, batch_size=batch_size)

    def _drain_pages(self, page_fn: Callable[[Optional[Dict[str, Any]]], PageResult]) -> List[GraphEdge]:
        return self._impl._drain_pages(page_fn)

    def find_orphan_edges_for_node(self, portfolio: str, org: str, node_id: str, edge_types: Sequence[str], node_exists: Callable[[str], bool]) -> List[GraphEdge]:
        """Check edges touching one node and return edges whose opposite endpoint does
    not exist in the canonical document store.

    node_exists receives normalized node_id, e.g. 'User/u123'.
    """
        return self._impl.find_orphan_edges_for_node(portfolio, org, node_id, edge_types, node_exists)

    def scan_orphan_edges_by_type(self, portfolio: str, org: str, edge_type: str, node_exists: Callable[[str], bool], *, limit_pages: Optional[int]=None) -> List[GraphEdge]:
        """Async-friendly check for all edges of one type inside an org.

    Use from a background job. For large orgs, call repeatedly with your own
    cursor strategy or limit pages per execution.
    """
        return self._impl.scan_orphan_edges_by_type(portfolio, org, edge_type, node_exists, limit_pages=limit_pages)

    def remove_edges(self, edges: Iterable[GraphEdge]) -> int:
        """Idempotently remove many edges."""
        return self._impl.remove_edges(edges)

    def sync_node_edges(self, portfolio: str, org: str, from_node_id: str, desired_edges: Sequence[Tuple[str, str, Optional[Dict[str, Any]]]], *, managed_edge_types: Optional[Sequence[str]]=None) -> Dict[str, int]:
        """Idempotently sync all outgoing graph edges for one node.

    desired_edges items:
        (edge_type, to_node_id, properties)

    If managed_edge_types is omitted, it is derived from desired_edges. Pass the
    Blueprint-known edge types when you also need to remove stale edges for edge
    types that are now absent from the document.
    """
        return self._impl.sync_node_edges(portfolio, org, from_node_id, desired_edges, managed_edge_types=managed_edge_types)

    def describe_table(self, logical_name='Graph'):
        return self._impl.describe_table(logical_name)

    def scan_projected(self, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
        return self._impl.scan_projected(fields)

    def iter_partition_keys(self, graph_index: str) -> Iterator[Dict[str, Any]]:
        return self._impl.iter_partition_keys(graph_index)

    def clear_edges(self, portfolio: Optional[str]=None, org: Optional[str]=None) -> int:
        return self._impl.clear_edges(portfolio, org)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
