"""
Renglo Graph DB model layer.

This module owns DynamoDB persistence for graph edges. Records, keys, and
traversal live in renglo.graph.records.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, Iterator, Optional

from renglo.config_names import setting, table_name
from renglo.document_stores.select import open_store
from renglo.graph.records import (
    GraphEdge,
    GraphOperations,
    GraphQueryCancelled,
    GraphQueryTimeout,
    GraphTraversalBudgetExceeded,
    PageResult,
    TraversalResult,
    TraversalStep,
    edge_item,
    item_to_edge,
    make_forward_sk,
    make_node_id,
    make_pk,
    make_reverse_edge_type,
    make_reverse_sk,
    now_iso,
    parse_forward_index,
    parse_graph_index,
    require_safe_graph_index_part,
    require_safe_key_part,
    split_node_id,
)


class GraphModel(GraphOperations):
    """
    DynamoDB-backed graph edge model.

    This class intentionally does not own canonical documents/nodes. The document
    table remains the source of truth. This class stores and traverses relationships.
    """

    make_pk = staticmethod(make_pk)
    make_node_id = staticmethod(make_node_id)
    split_node_id = staticmethod(split_node_id)
    make_forward_sk = staticmethod(make_forward_sk)
    make_reverse_edge_type = staticmethod(make_reverse_edge_type)
    make_reverse_sk = staticmethod(make_reverse_sk)
    _require_safe_key_part = staticmethod(require_safe_key_part)
    _require_safe_graph_index_part = staticmethod(require_safe_graph_index_part)
    _now_iso = staticmethod(now_iso)
    _parse_graph_index = staticmethod(parse_graph_index)
    _parse_forward_index = staticmethod(parse_forward_index)

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        region_name: Optional[str] = None,
        dynamodb_resource: Optional[Any] = None,
        reverse_index_name: str = "backward_index",
        clock: Callable[[], float] = time.time,
        store=None,
    ) -> None:
        self.config = config or {}
        resolved_region = region_name or setting(self.config, "REGION", "us-east-1")
        resolved_table_name = table_name(self.config, "GRAPH_TABLE")
        if not resolved_table_name:
            raise ValueError(
                "GRAPH_TABLE is not set and no environment name was found (WL_NAME or renglo.yaml name)"
            )

        self.region = resolved_region
        self.table_name = resolved_table_name
        self.store = store or open_store(
            self.config,
            "graph",
            resolved_table_name,
            "graph_index",
            "forward_index",
            resource=dynamodb_resource,
            region_name=resolved_region,
            use_profile=True,
        )
        self.reverse_index_name = reverse_index_name
        self.clock = clock

    @property
    def dynamodb(self):
        return self.store.ensure_resource()

    @property
    def table(self):
        return self.store._table()

    def _edge_to_item(self, edge: GraphEdge) -> Dict[str, Any]:
        return edge_item(edge, self._now_iso(), self._to_dynamo_value)

    def _item_to_edge(self, item: Dict[str, Any]) -> GraphEdge:
        return item_to_edge(item)

    def _to_dynamo_value(self, value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, dict):
            return {key: self._to_dynamo_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._to_dynamo_value(item) for item in value]
        return value

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
        """Idempotently insert or update an edge."""
        edge = GraphEdge(
            portfolio=portfolio,
            org=org,
            edge_type=edge_type,
            from_node_id=from_node_id,
            to_node_id=to_node_id,
            properties=properties or {},
        )
        item = self._edge_to_item(edge)
        self.store.upsert(item, preserve=("created_at",))
        return edge

    def remove_edge(
        self,
        portfolio: str,
        org: str,
        edge_type: str,
        from_node_id: str,
        to_node_id: str,
    ) -> bool:
        """Remove an edge. Returns True when the edge existed."""
        pk = self.make_pk(portfolio, org)
        sk = self.make_forward_sk(edge_type, from_node_id, to_node_id)
        return self.store.delete(pk, sk, return_old=True)

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
        item = self.store.get(pk, sk)
        return self._item_to_edge(item) if item else None

    def _query_forward(
        self,
        portfolio: str,
        org: str,
        *,
        begins_with_value: str,
        limit: int,
        exclusive_start_key: Optional[Dict[str, Any]],
    ) -> PageResult:
        items, token = self.store.query(
            self.make_pk(portfolio, org),
            prefix=begins_with_value,
            limit=limit,
            continuation=exclusive_start_key,
        )
        return PageResult(
            items=[self._item_to_edge(item) for item in items],
            last_evaluated_key=token,
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
        raw_items, token = self.store.query(
            self.make_pk(portfolio, org),
            path="backward_index",
            prefix=begins_with_value,
            limit=limit,
            continuation=exclusive_start_key,
            index=self.reverse_index_name,
        )
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
            last_evaluated_key=token,
        )

    def _needs_reverse_item_hydration(self, item: Dict[str, Any]) -> bool:
        if not isinstance(item, dict):
            return False
        if "created_at" in item or "updated_at" in item:
            return False
        if "graph_index" not in item or "forward_index" not in item:
            return False
        return True

    def _get_full_edge_item(self, graph_index: Any, forward_index: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(graph_index, str) or not isinstance(forward_index, str):
            return None
        item = self.store.get(graph_index, forward_index)
        return item if isinstance(item, dict) else None

    def _batch_delete_edges(self, edges: Iterable[GraphEdge], *, batch_size: int = 25) -> int:
        keys = [{"graph_index": edge.pk, "forward_index": edge.sk} for edge in edges]
        return self.store.batch_delete(keys)

    def describe_table(self, logical_name="Graph"):
        self.store.describe(
            logical_name,
            hint="Use --region, --env, --ring-table, or --graph-table.",
        )

    def scan_projected(self, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
        yield from self.store.scan(fields)

    def iter_partition_keys(self, graph_index: str) -> Iterator[Dict[str, Any]]:
        last_evaluated_key = None
        while True:
            items, last_evaluated_key = self.store.query(
                graph_index,
                fields=["graph_index", "forward_index"],
                continuation=last_evaluated_key,
            )
            yield from items
            if not last_evaluated_key:
                break

    def clear_edges(self, portfolio: Optional[str] = None, org: Optional[str] = None) -> int:
        if portfolio and org:
            rows = self.iter_partition_keys(f"irn:edge:{portfolio}:{org}")
        else:
            rows = self.scan_projected(["graph_index", "forward_index"])
        keys = [
            {"graph_index": item["graph_index"], "forward_index": item["forward_index"]}
            for item in rows
        ]
        return self.store.batch_delete(keys)
