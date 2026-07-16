#!/usr/bin/env python3
"""
Regenerate graph edges from canonical documents.

Default behavior:
- Clears existing graph edges (for selected scope)
- Rebuilds edges by re-running sync_document_graph_edges for each document

Usage examples:
  python dev/renglo-lib/renglo/graph/regenerate_edges.py
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --profile default --region us-east-1
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --portfolio p1 --org o1
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --portfolio p1 --org o1 --profile default --region us-east-1
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --portfolio p1 --org o1 --ring productora_candidates
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --portfolio p1 --org o1 --ring productora_candidates --profile default --region us-east-1
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --no-clear
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --no-clear --profile default --region us-east-1
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --dry-run
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --dry-run --profile default --region us-east-1
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --ring-table productora_data --graph-table productora_graph --profile default --region us-east-1
  python dev/renglo-lib/renglo/graph/regenerate_edges.py --debug-skips --debug-limit 200 --profile default --region us-east-1
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from renglo.common import load_config
from renglo.graph.graph_controller import GraphController


def _parse_portfolio(portfolio_index: str) -> Optional[str]:
    prefix = "irn:data:"
    if not isinstance(portfolio_index, str) or not portfolio_index.startswith(prefix):
        return None
    return portfolio_index[len(prefix) :]


def _parse_doc_index(doc_index: str) -> Optional[Tuple[str, str, str]]:
    if not isinstance(doc_index, str):
        return None
    parts = doc_index.split(":", 2)
    if len(parts) != 3:
        return None
    return parts[0], parts[1], parts[2]


def _parse_blueprint_handle(blueprint_uri: Any) -> Optional[str]:
    if not isinstance(blueprint_uri, str):
        return None
    marker = "/_blueprint/"
    if marker not in blueprint_uri:
        return None
    tail = blueprint_uri.split(marker, 1)[1]
    parts = [p for p in tail.split("/") if p]
    if len(parts) < 2:
        return None
    return parts[0]


def _scan_all(table, projection_fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
    fields = list(projection_fields)
    expr_attr_names = {f"#f{i}": field for i, field in enumerate(fields)}
    projection_expression = ", ".join(expr_attr_names.keys())
    last_evaluated_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "ProjectionExpression": projection_expression,
            "ExpressionAttributeNames": expr_attr_names,
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        response = table.scan(**kwargs)
        for item in response.get("Items", []):
            yield item
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break


def _clear_graph_edges(graph_table, portfolio: Optional[str], org: Optional[str]) -> int:
    deleted = 0
    with graph_table.batch_writer() as batch:
        if portfolio and org:
            graph_index = f"irn:edge:{portfolio}:{org}"
            last_evaluated_key = None
            while True:
                query_kwargs = {
                    "KeyConditionExpression": Key("graph_index").eq(graph_index),
                    "ProjectionExpression": "graph_index, forward_index",
                }
                if last_evaluated_key:
                    query_kwargs["ExclusiveStartKey"] = last_evaluated_key
                response = graph_table.query(**query_kwargs)
                for item in response.get("Items", []):
                    batch.delete_item(
                        Key={
                            "graph_index": item["graph_index"],
                            "forward_index": item["forward_index"],
                        }
                    )
                    deleted += 1
                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break
            return deleted

        for item in _scan_all(graph_table, ["graph_index", "forward_index"]):
            batch.delete_item(
                Key={
                    "graph_index": item["graph_index"],
                    "forward_index": item["forward_index"],
                }
            )
            deleted += 1
    return deleted


def _iter_documents(
    ring_table,
    portfolio_filter: Optional[str],
    org_filter: Optional[str],
    ring_filter: Optional[str],
) -> Iterable[Tuple[str, str, str, str, Optional[str], Dict[str, Any]]]:
    for item in _scan_all(ring_table, ["portfolio_index", "doc_index", "_id", "blueprint", "attributes"]):
        portfolio = _parse_portfolio(item.get("portfolio_index"))
        doc_bits = _parse_doc_index(item.get("doc_index"))
        if not portfolio or not doc_bits:
            continue
        org, ring, idx = doc_bits
        if portfolio_filter and portfolio != portfolio_filter:
            continue
        if org_filter and org != org_filter:
            continue
        if ring_filter and ring != ring_filter:
            continue
        attributes = item.get("attributes")
        if not isinstance(attributes, dict):
            attributes = {}
        doc_id = str(item.get("_id") or idx)
        blueprint_handle = _parse_blueprint_handle(item.get("blueprint"))
        yield portfolio, org, ring, doc_id, blueprint_handle, attributes


def _estimate_desired_edges_count(
    grc: GraphController,
    ring: str,
    blueprint_handle: Optional[str],
    attributes: Dict[str, Any],
    blueprint_cache: Dict[str, Dict[str, Any]],
) -> int:
    cache_key = f"{blueprint_handle or 'default'}::{ring}"
    if cache_key not in blueprint_cache:
        blueprint_cache[cache_key] = grc._get_blueprint_for_ring(ring, blueprint_handle=blueprint_handle)
    blueprint = blueprint_cache[cache_key]
    edge_specs = grc._get_edge_specs_from_blueprint(blueprint, ring)  # internal helper
    if not edge_specs:
        return 0
    desired = grc._build_desired_edges(edge_specs, attributes)  # internal helper
    return len(desired)


def _diagnose_document_edges(
    grc: GraphController,
    ring: str,
    blueprint_handle: Optional[str],
    attributes: Dict[str, Any],
    blueprint_cache: Dict[str, Dict[str, Any]],
) -> Tuple[int, str]:
    cache_key = f"{blueprint_handle or 'default'}::{ring}"
    if cache_key not in blueprint_cache:
        blueprint_cache[cache_key] = grc._get_blueprint_for_ring(ring, blueprint_handle=blueprint_handle)
    blueprint = blueprint_cache[cache_key]

    if not isinstance(blueprint, dict) or "fields" not in blueprint:
        return 0, "Blueprint not found or invalid for ring"
    if not grc._is_graph_enabled(blueprint):
        return 0, "Blueprint has enable_graph=false"

    fields = blueprint.get("fields")
    if not isinstance(fields, list) or not fields:
        return 0, "Blueprint has no fields"

    source_declared = 0
    source_valid = 0
    source_invalid = 0
    source_missing_attr = 0
    source_empty_value = 0
    source_with_values = 0

    for field in fields:
        if not isinstance(field, dict):
            continue
        source = field.get("source")
        if not source:
            continue
        source_declared += 1
        parsed = grc._parse_edge_source(source)
        if not parsed:
            source_invalid += 1
            continue
        source_valid += 1
        field_name = field.get("name")
        if field_name is None:
            source_missing_attr += 1
            continue
        field_name_str = str(field_name)
        if field_name_str not in attributes:
            source_missing_attr += 1
            continue
        temp_spec = {
            "field_name": field_name_str,
            "edge_type": "diagnostic",
            "to_ring": parsed.get("to_ring"),
            "id_token": parsed.get("id_token"),
            "attribute_keys": parsed.get("attribute_keys", []),
            "allow_extras": parsed.get("allow_extras", True),
        }
        declarations = grc._extract_edge_declarations(attributes.get(field_name_str), temp_spec)
        if not declarations:
            source_empty_value += 1
            continue
        source_with_values += 1

    edge_specs = grc._get_edge_specs_from_blueprint(blueprint, ring)
    if not edge_specs:
        if source_declared == 0:
            return 0, "No source fields declared in blueprint"
        if source_invalid > 0 and source_valid == 0:
            return 0, "All source fields have invalid source format"
        return 0, "No valid graph edge specs inferred from blueprint"

    desired = grc._build_desired_edges(edge_specs, attributes)
    desired_count = len(desired)
    if desired_count > 0:
        return desired_count, ""

    return 0, (
        "No desired edges from attributes "
        f"(source_declared={source_declared}, valid={source_valid}, invalid={source_invalid}, "
        f"missing_attr={source_missing_attr}, empty_value={source_empty_value}, with_values={source_with_values})"
    )


def _resolve_table_name(
    override: Optional[str],
    config: Dict[str, Any],
    config_key: str,
) -> Optional[str]:
    if override:
        return override
    return config.get(config_key)


def _describe_table_or_raise(dynamodb_client, table_name: str, region: str, logical_name: str) -> None:
    try:
        dynamodb_client.describe_table(TableName=table_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        if code == "ResourceNotFoundException":
            raise RuntimeError(
                f"{logical_name} table '{table_name}' was not found in region '{region}'. "
                f"Use --region/--ring-table/--graph-table (and optionally --profile) to target the right environment."
            ) from exc
        raise RuntimeError(
            f"Failed to describe {logical_name} table '{table_name}' in region '{region}': {code} {msg}"
        ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenerate graph edges from canonical documents.")
    parser.add_argument("--portfolio", help="Restrict to one portfolio_id.")
    parser.add_argument("--org", help="Restrict to one org_id.")
    parser.add_argument("--ring", help="Restrict to one ring name.")
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear graph edges before rebuilding.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write/delete edges. Only report what would be processed.",
    )
    parser.add_argument("--region", help="AWS region override (default: config AWS_REGION).")
    parser.add_argument("--profile", help="AWS profile name override.")
    parser.add_argument("--ring-table", help="Ring data table name override.")
    parser.add_argument("--graph-table", help="Graph table name override.")
    parser.add_argument(
        "--debug-skips",
        action="store_true",
        help="Print per-document skip diagnostics (for troubleshooting).",
    )
    parser.add_argument(
        "--debug-limit",
        type=int,
        default=100,
        help="Maximum number of per-document debug lines to print when --debug-skips is enabled.",
    )
    args = parser.parse_args()

    config = load_config()
    region = args.region or config.get("AWS_REGION", "us-east-1")
    ring_table_name = _resolve_table_name(args.ring_table, config, "DYNAMODB_RINGDATA_TABLE")
    graph_table_name = _resolve_table_name(args.graph_table, config, "DYNAMODB_GRAPH_TABLE")
    if not ring_table_name or not graph_table_name:
        raise RuntimeError(
            "Missing DYNAMODB_RINGDATA_TABLE or DYNAMODB_GRAPH_TABLE. "
            "Set them in config or pass --ring-table / --graph-table."
        )

    session_kwargs: Dict[str, Any] = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    session = boto3.session.Session(**session_kwargs)
    dynamodb = session.resource("dynamodb", region_name=region)
    dynamodb_client = session.client("dynamodb", region_name=region)
    _describe_table_or_raise(dynamodb_client, ring_table_name, region, "Ring data")
    _describe_table_or_raise(dynamodb_client, graph_table_name, region, "Graph")

    print(f"Using region={region}, ring_table={ring_table_name}, graph_table={graph_table_name}")
    if args.profile:
        print(f"Using AWS profile={args.profile}")

    ring_table = dynamodb.Table(ring_table_name)
    graph_table = dynamodb.Table(graph_table_name)
    grc = GraphController(
        config=config,
        region_name=region,
        dynamodb_resource=dynamodb,
    )

    if args.dry_run:
        print("Dry run enabled: no graph writes/deletes will be performed.")
    elif not args.no_clear:
        deleted = _clear_graph_edges(graph_table, args.portfolio, args.org)
        print(f"Cleared {deleted} graph edges.")
    else:
        print("Skipping graph table clear (--no-clear).")

    processed = 0
    succeeded = 0
    skipped = 0
    failed = 0
    would_create_edges = 0
    blueprint_cache: Dict[str, Dict[str, Any]] = {}
    skip_reasons: Dict[str, int] = {}
    debug_printed = 0

    for portfolio, org, ring, idx, blueprint_handle, attributes in _iter_documents(
        ring_table,
        args.portfolio,
        args.org,
        args.ring,
    ):
        processed += 1
        if args.dry_run:
            try:
                edge_count, diag_reason = _diagnose_document_edges(
                    grc,
                    ring,
                    blueprint_handle,
                    attributes,
                    blueprint_cache,
                )
                would_create_edges += edge_count
                if edge_count == 0:
                    skipped += 1
                    reason = diag_reason or "No desired edges found (blueprint/source/value mismatch)"
                    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                    if args.debug_skips and debug_printed < max(0, args.debug_limit):
                        print(
                            f"[SKIP][dry-run] {portfolio}/{org}/{ring}/{idx} "
                            f"(blueprint_handle={blueprint_handle or 'unknown'}): {reason}"
                        )
                        debug_printed += 1
                else:
                    succeeded += 1
            except Exception as exc:  # pragma: no cover
                failed += 1
                print(f"[ERROR] dry-run estimate failed for {portfolio}/{org}/{ring}/{idx}: {exc}")
        else:
            try:
                result = grc.sync_document_graph_edges(
                    portfolio,
                    org,
                    ring,
                    idx,
                    attributes,
                    blueprint_handle=blueprint_handle,
                )
                if result.get("skipped"):
                    skipped += 1
                    reason = str(result.get("reason") or "unknown")
                    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                    if args.debug_skips and debug_printed < max(0, args.debug_limit):
                        print(
                            f"[SKIP] {portfolio}/{org}/{ring}/{idx} "
                            f"(blueprint_handle={blueprint_handle or 'unknown'}): {reason}"
                        )
                        debug_printed += 1
                elif result.get("success"):
                    succeeded += 1
                else:
                    failed += 1
                    print(
                        f"[WARN] sync failed for {portfolio}/{org}/{ring}/{idx}: {result}"
                    )
            except Exception as exc:  # pragma: no cover
                failed += 1
                print(f"[ERROR] {portfolio}/{org}/{ring}/{idx}: {exc}")

        if processed % 1000 == 0:
            if args.dry_run:
                print(
                    f"Progress {processed} docs | docs_with_edges={succeeded} skipped={skipped} failed={failed} estimated_edges={would_create_edges}"
                )
            else:
                print(
                    f"Progress {processed} docs | ok={succeeded} skipped={skipped} failed={failed}"
                )

    if args.dry_run:
        print(
            f"Dry run done. processed={processed}, docs_with_edges={succeeded}, skipped={skipped}, failed={failed}, estimated_edges={would_create_edges}"
        )
    else:
        print(
            f"Done. processed={processed}, ok={succeeded}, skipped={skipped}, failed={failed}"
        )
    if skip_reasons:
        print("Skip reasons:")
        for reason, count in sorted(skip_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {reason}: {count}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
