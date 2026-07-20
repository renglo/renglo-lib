#!/usr/bin/env python3
"""
Unified admin tool for ring documents and graph edges.

Subcommands
-----------
purge-ring
    Delete all documents in a portfolio/org/ring (path_index query).
    Optional --with-graph-edges removes managed graph edges per document first.

clear-graph-edges
    Delete all graph edges for a portfolio/org partition (or entire graph table).

sync-graph-edges
    Rebuild graph edges from canonical documents without clearing first.

regenerate-graph-edges
    Clear graph edges for the scope, then sync from documents.

Usage examples
--------------

1. purge-ring (dry run)
python dev/renglo-lib/renglo/graph/data_admin.py purge-ring \
  --env arbitium --portfolio P --org O --ring infrastructure_elements \
  --profile default --dry-run
Purpose: Preview how many documents would be deleted — no changes.

What it touches:

Reads from arbitium_data via the path_index for P / O / infrastructure_elements
Counts matching documents
What it does not touch:

Does not delete documents
Does not delete graph edges
Does not refresh S3 cache
Use this to confirm scope before a real purge.

--------------
2. purge-ring (with graph edges)
python dev/renglo-lib/renglo/graph/data_admin.py purge-ring \
  --env arbitium --portfolio P --org O --ring infrastructure_elements \
  --with-graph-edges --yes
Purpose: Delete all documents in that ring and clean up their graph edges.

Per document:

--with-graph-edges: Calls remove_document_graph_edges on arbitium_graph — removes outgoing/incoming edges for that node for edge types declared on that ring’s blueprint
Deletes the document from arbitium_data
Also:

--yes: Skips the interactive Type DELETE prompt
After purge: refreshes the S3 cache for that ring (unless you pass --skip-cache-refresh)
What it does not touch:

Documents in other rings (e.g. business_elements)
Graph edges for other rings’ nodes
Edges from other rings pointing into deleted nodes if that edge type isn’t in the purged ring’s blueprint (existing platform limitation)

--------------
3. clear-graph-edges
python dev/renglo-lib/renglo/graph/data_admin.py clear-graph-edges \
  --portfolio P --org O --profile default
Purpose: Flush the graph index for that portfolio/org — edges only.

What it touches:

Deletes every row under graph_index = irn:edge:P:O in the graph table (from config or --env / --graph-table)
What it does not touch:

Ring documents in *_data
Edges for other portfolio/org pairs
Use when you want an empty graph for P/O before re-ingesting or rebuilding. Pair with sync-graph-edges or regenerate-graph-edges if you want edges back.

Note: this example omits --env; table names come from config (DYNAMODB_GRAPH_TABLE) unless you pass --env arbitium or --graph-table.

4. sync-graph-edges
python dev/renglo-lib/renglo/graph/data_admin.py sync-graph-edges \
  --portfolio P --org O --ring infrastructure_elements
Purpose: Rebuild graph edges from existing documents without clearing first.

What it does:

Scans documents (scoped to P, O, and optionally infrastructure_elements)
For each doc, runs sync_document_graph_edges — upserts desired edges from links and other blueprint source fields
What it does not do:

Does not delete documents
Does not bulk-clear the graph partition first
Stale edges may remain if documents were removed or links changed without going through normal DELETE. For a clean slate, use clear-graph-edges or regenerate-graph-edges first.

5. regenerate-graph-edges
python dev/renglo-lib/renglo/graph/data_admin.py regenerate-graph-edges \
  --portfolio P --org O --profile default --region us-east-1
Purpose: Full graph rebuild for that portfolio/org.

Steps:

Clear: Deletes all edges in irn:edge:P:O (same as clear-graph-edges for that scope)
Rebuild: Scans all documents for P and O (all rings unless you pass --ring) and syncs edges from each
What it does not touch:

Ring documents themselves
Use after blueprint/link model changes, or when you suspect graph drift across multiple rings. Heavier than sync-graph-edges because it wipes the whole P/O graph partition first.

"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from renglo.common import load_config
from renglo.data.data_controller import DataController
from renglo.graph.graph_controller import GraphController


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------


def get_available_aws_profiles() -> List[str]:
    profiles: List[str] = []
    aws_credentials_path = os.path.expanduser("~/.aws/credentials")
    aws_config_path = os.path.expanduser("~/.aws/config")

    if os.path.exists(aws_credentials_path):
        cfg = configparser.ConfigParser()
        cfg.read(aws_credentials_path)
        profiles.extend(cfg.sections())

    if os.path.exists(aws_config_path):
        cfg = configparser.ConfigParser()
        cfg.read(aws_config_path)
        for section in cfg.sections():
            if section.startswith("profile "):
                name = section.replace("profile ", "")
                if name not in profiles:
                    profiles.append(name)

    return profiles if profiles else ["default"]


def get_profile_region(profile_name: str) -> str:
    config_path = os.path.expanduser("~/.aws/config")
    if os.path.exists(config_path):
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        section = f"profile {profile_name}" if profile_name != "default" else "default"
        if section in cfg and "region" in cfg[section]:
            return cfg[section]["region"]
    return "us-east-1"


@dataclass
class AdminContext:
    config: Dict[str, Any]
    region: str
    profile: Optional[str]
    ring_table_name: str
    graph_table_name: str
    dynamodb: Any
    dynamodb_client: Any
    ring_table: Any
    graph_table: Any
    grc: GraphController


def _resolve_table_name(
    override: Optional[str],
    env_prefix: Optional[str],
    env_suffix: str,
    config: Dict[str, Any],
    config_key: str,
) -> Optional[str]:
    if override:
        return override
    if env_prefix:
        return f"{env_prefix}_{env_suffix}"
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
                f"Use --region, --env, --ring-table, or --graph-table."
            ) from exc
        raise RuntimeError(
            f"Failed to describe {logical_name} table '{table_name}' in region '{region}': {code} {msg}"
        ) from exc


def build_admin_context(
    *,
    profile: Optional[str] = None,
    region: Optional[str] = None,
    env: Optional[str] = None,
    ring_table: Optional[str] = None,
    graph_table: Optional[str] = None,
    verify_tables: bool = True,
) -> AdminContext:
    config = load_config()
    resolved_region = region or config.get("AWS_REGION") or get_profile_region(profile or "default")
    ring_table_name = _resolve_table_name(ring_table, env, "data", config, "DYNAMODB_RINGDATA_TABLE")
    graph_table_name = _resolve_table_name(graph_table, env, "graph", config, "DYNAMODB_GRAPH_TABLE")
    if not ring_table_name or not graph_table_name:
        raise RuntimeError(
            "Missing ring/graph table names. Set DYNAMODB_RINGDATA_TABLE and DYNAMODB_GRAPH_TABLE "
            "in config, or pass --env, --ring-table, and --graph-table."
        )

    session_kwargs: Dict[str, Any] = {}
    if profile:
        session_kwargs["profile_name"] = profile
    session = boto3.session.Session(**session_kwargs)
    dynamodb = session.resource("dynamodb", region_name=resolved_region)
    dynamodb_client = session.client("dynamodb", region_name=resolved_region)

    if verify_tables:
        _describe_table_or_raise(dynamodb_client, ring_table_name, resolved_region, "Ring data")
        _describe_table_or_raise(dynamodb_client, graph_table_name, resolved_region, "Graph")

    grc = GraphController(
        config={**config, "DYNAMODB_GRAPH_TABLE": graph_table_name},
        region_name=resolved_region,
        dynamodb_resource=dynamodb,
    )

    return AdminContext(
        config=config,
        region=resolved_region,
        profile=profile,
        ring_table_name=ring_table_name,
        graph_table_name=graph_table_name,
        dynamodb=dynamodb,
        dynamodb_client=dynamodb_client,
        ring_table=dynamodb.Table(ring_table_name),
        graph_table=dynamodb.Table(graph_table_name),
        grc=grc,
    )


def _print_context(ctx: AdminContext) -> None:
    print(
        f"Using region={ctx.region}, ring_table={ctx.ring_table_name}, "
        f"graph_table={ctx.graph_table_name}"
    )
    if ctx.profile:
        print(f"Using AWS profile={ctx.profile}")


# ---------------------------------------------------------------------------
# Document / graph parsing helpers
# ---------------------------------------------------------------------------


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


def _iter_documents(
    ring_table,
    portfolio_filter: Optional[str],
    org_filter: Optional[str],
    ring_filter: Optional[str],
) -> Iterator[Tuple[str, str, str, str, Optional[str], Dict[str, Any]]]:
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


def _query_ring_items_page(
    table,
    portfolio: str,
    org: str,
    ring: str,
    limit: int,
    last_evaluated_key: Optional[Dict] = None,
) -> Tuple[List[Dict], Optional[Dict]]:
    portfolio_index = f"irn:data:{portfolio}"
    path_index_prefix = f"irn:h_index:{org}:{ring}"
    kwargs = {
        "IndexName": "path_index",
        "KeyConditionExpression": Key("portfolio_index").eq(portfolio_index)
        & Key("path_index").begins_with(path_index_prefix),
        "Limit": limit,
    }
    if last_evaluated_key:
        kwargs["ExclusiveStartKey"] = last_evaluated_key
    response = table.query(**kwargs)
    return response.get("Items", []), response.get("LastEvaluatedKey")


def _iter_ring_items(table, portfolio: str, org: str, ring: str, page_limit: int):
    last_key: Optional[Dict] = None
    while True:
        items, last_key = _query_ring_items_page(
            table=table,
            portfolio=portfolio,
            org=org,
            ring=ring,
            limit=page_limit,
            last_evaluated_key=last_key,
        )
        for item in items:
            yield item
        if not last_key:
            break


# ---------------------------------------------------------------------------
# Graph edge operations
# ---------------------------------------------------------------------------


def clear_graph_edges(
    graph_table,
    portfolio: Optional[str] = None,
    org: Optional[str] = None,
) -> int:
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
    if desired:
        return len(desired), ""

    return 0, (
        "No desired edges from attributes "
        f"(source_declared={source_declared}, valid={source_valid}, invalid={source_invalid}, "
        f"missing_attr={source_missing_attr}, empty_value={source_empty_value}, with_values={source_with_values})"
    )


def sync_graph_edges(
    ctx: AdminContext,
    *,
    portfolio: Optional[str] = None,
    org: Optional[str] = None,
    ring: Optional[str] = None,
    dry_run: bool = False,
    debug_skips: bool = False,
    debug_limit: int = 100,
) -> Dict[str, int]:
    processed = 0
    succeeded = 0
    skipped = 0
    failed = 0
    would_create_edges = 0
    blueprint_cache: Dict[str, Dict[str, Any]] = {}
    skip_reasons: Dict[str, int] = {}
    debug_printed = 0

    for doc_portfolio, doc_org, doc_ring, idx, blueprint_handle, attributes in _iter_documents(
        ctx.ring_table,
        portfolio,
        org,
        ring,
    ):
        processed += 1
        if dry_run:
            try:
                edge_count, diag_reason = _diagnose_document_edges(
                    ctx.grc,
                    doc_ring,
                    blueprint_handle,
                    attributes,
                    blueprint_cache,
                )
                would_create_edges += edge_count
                if edge_count == 0:
                    skipped += 1
                    reason = diag_reason or "No desired edges found"
                    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                    if debug_skips and debug_printed < max(0, debug_limit):
                        print(
                            f"[SKIP][dry-run] {doc_portfolio}/{doc_org}/{doc_ring}/{idx}: {reason}"
                        )
                        debug_printed += 1
                else:
                    succeeded += 1
            except Exception as exc:  # pragma: no cover
                failed += 1
                print(f"[ERROR] dry-run failed for {doc_portfolio}/{doc_org}/{doc_ring}/{idx}: {exc}")
        else:
            try:
                result = ctx.grc.sync_document_graph_edges(
                    doc_portfolio,
                    doc_org,
                    doc_ring,
                    idx,
                    attributes,
                    blueprint_handle=blueprint_handle,
                )
                if result.get("skipped"):
                    skipped += 1
                    reason = str(result.get("reason") or "unknown")
                    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                    if debug_skips and debug_printed < max(0, debug_limit):
                        print(
                            f"[SKIP] {doc_portfolio}/{doc_org}/{doc_ring}/{idx}: {reason}"
                        )
                        debug_printed += 1
                elif result.get("success"):
                    succeeded += 1
                else:
                    failed += 1
                    print(f"[WARN] sync failed for {doc_portfolio}/{doc_org}/{doc_ring}/{idx}: {result}")
            except Exception as exc:  # pragma: no cover
                failed += 1
                print(f"[ERROR] {doc_portfolio}/{doc_org}/{doc_ring}/{idx}: {exc}")

        if processed % 1000 == 0:
            if dry_run:
                print(
                    f"Progress {processed} docs | with_edges={succeeded} skipped={skipped} "
                    f"failed={failed} estimated_edges={would_create_edges}"
                )
            else:
                print(f"Progress {processed} docs | ok={succeeded} skipped={skipped} failed={failed}")

    return {
        "processed": processed,
        "succeeded": succeeded,
        "skipped": skipped,
        "failed": failed,
        "would_create_edges": would_create_edges,
        "skip_reasons": skip_reasons,
    }


def sanitize_ring_links(
    ctx: AdminContext,
    *,
    portfolio: str,
    org: str,
    ring: str,
    dry_run: bool = False,
) -> Dict[str, int]:
    try:
        from arbitiumtriage.handlers.aid_element_helpers import sanitize_persisted_links
    except ImportError as exc:
        raise RuntimeError(
            "sanitize-ring-links requires the arbitiumtriage package to be installed"
        ) from exc

    dac = DataController(config=ctx.config)
    processed = 0
    updated = 0
    skipped = 0
    failed = 0

    for doc_portfolio, doc_org, doc_ring, idx, _blueprint_handle, attributes in _iter_documents(
        ctx.ring_table,
        portfolio,
        org,
        ring,
    ):
        if doc_portfolio != portfolio or doc_org != org or doc_ring != ring:
            continue
        processed += 1
        raw_links = attributes.get("links")
        if not isinstance(raw_links, list) or not raw_links:
            skipped += 1
            continue
        clean_links = sanitize_persisted_links(raw_links)
        if json.dumps(clean_links, sort_keys=True, default=str) == json.dumps(
            raw_links, sort_keys=True, default=str
        ):
            skipped += 1
            continue
        if dry_run:
            updated += 1
            continue
        try:
            resp, _status = dac.put_a_b_c(
                doc_portfolio,
                doc_org,
                doc_ring,
                idx,
                {"links": clean_links},
            )
            if isinstance(resp, dict) and resp.get("success"):
                updated += 1
            else:
                failed += 1
                print(f"[WARN] sanitize failed for {doc_portfolio}/{doc_org}/{doc_ring}/{idx}: {resp}")
        except Exception as exc:  # pragma: no cover
            failed += 1
            print(f"[ERROR] {doc_portfolio}/{doc_org}/{doc_ring}/{idx}: {exc}")

    return {
        "processed": processed,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
    }


def refresh_ring_cache(
    portfolio: str,
    org: str,
    ring: str,
    profile: Optional[str],
    region: str,
    config: Dict[str, Any],
) -> bool:
    try:
        session_kwargs: Dict[str, Any] = {}
        if profile:
            session_kwargs["profile_name"] = profile
        boto3.session.Session(**session_kwargs)
        from renglo.data.data_controller import DataController

        dac = DataController(config=config)
        dac.refresh_s3_cache(portfolio, org, ring, None)
        print(f"Refreshed cache for ring={ring} portfolio={portfolio} org={org}")
        return True
    except Exception as exc:
        print(f"WARNING: Cache refresh failed ({ring}): {exc}")
        return False


# ---------------------------------------------------------------------------
# Ring purge
# ---------------------------------------------------------------------------


def purge_ring(
    ctx: AdminContext,
    *,
    portfolio: str,
    org: str,
    ring: str,
    dry_run: bool = False,
    with_graph_edges: bool = False,
    page_limit: int = 200,
) -> Dict[str, int]:
    scanned = 0
    deleted = 0
    graph_removed = 0
    graph_failed = 0
    failed = 0

    print(f"Purging ring={ring} portfolio={portfolio} org={org} table={ctx.ring_table_name}")
    if dry_run:
        print("Dry run enabled. No deletes will be executed.")
    if with_graph_edges:
        print("Graph edges will be removed per document before delete.")

    for item in _iter_ring_items(ctx.ring_table, portfolio, org, ring, page_limit):
        scanned += 1
        portfolio_index = item.get("portfolio_index")
        doc_index = item.get("doc_index")
        doc_id = item.get("_id")
        if not portfolio_index or not doc_index:
            failed += 1
            print(f"Skip invalid item missing key fields (_id={doc_id})")
            continue

        doc_bits = _parse_doc_index(str(doc_index))
        if not doc_bits:
            failed += 1
            print(f"Skip invalid doc_index (_id={doc_id}, doc_index={doc_index})")
            continue
        item_org, item_ring, idx = doc_bits
        attributes = item.get("attributes")
        if not isinstance(attributes, dict):
            attributes = {}
        blueprint_handle = _parse_blueprint_handle(item.get("blueprint"))

        if dry_run:
            continue

        if with_graph_edges:
            try:
                result = ctx.grc.remove_document_graph_edges(
                    portfolio,
                    item_org,
                    item_ring,
                    idx,
                    attributes,
                    blueprint_handle=blueprint_handle,
                )
                if result.get("success"):
                    graph_removed += 1
                else:
                    graph_failed += 1
                    print(f"[WARN] graph cleanup for _id={doc_id}: {result}")
            except Exception as exc:
                graph_failed += 1
                print(f"[WARN] graph cleanup failed for _id={doc_id}: {exc}")

        try:
            ctx.ring_table.delete_item(
                Key={
                    "portfolio_index": portfolio_index,
                    "doc_index": doc_index,
                }
            )
            deleted += 1
            if deleted % 100 == 0:
                print(f"Deleted {deleted} documents...")
        except Exception as exc:
            failed += 1
            print(f"Failed delete _id={doc_id}: {exc}")

    return {
        "scanned": scanned,
        "deleted": deleted if not dry_run else 0,
        "failed": failed,
        "graph_removed": graph_removed if not dry_run else 0,
        "graph_failed": graph_failed if not dry_run else 0,
    }


def _confirm_destructive(action: str, details: str) -> bool:
    print(f"WARNING: This operation will permanently {action}.")
    print(details)
    token = input("Type DELETE to continue: ").strip()
    return token == "DELETE"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _add_common_aws_args(parser: argparse.ArgumentParser) -> None:
    profiles = get_available_aws_profiles()
    parser.add_argument(
        "--profile",
        "--aws-profile",
        dest="profile",
        choices=profiles,
        default="default",
        help=f"AWS profile (available: {', '.join(profiles)})",
    )
    parser.add_argument("--region", "--aws-region", dest="region", help="AWS region override")
    parser.add_argument(
        "--env",
        "--environment",
        dest="env",
        help="Environment prefix for <env>_data and <env>_graph tables",
    )
    parser.add_argument("--ring-table", help="Ring data table override")
    parser.add_argument("--graph-table", help="Graph table override")


def _add_scope_args(parser: argparse.ArgumentParser, *, require_portfolio_org: bool = False) -> None:
    parser.add_argument(
        "--portfolio",
        required=require_portfolio_org,
        help="Portfolio id",
    )
    parser.add_argument(
        "--org",
        required=require_portfolio_org,
        help="Org id",
    )
    parser.add_argument("--ring", help="Restrict to one ring (blueprint name)")
    parser.add_argument("--blueprint", help="Alias for --ring")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Admin tool for ring documents and graph edges.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    purge = subparsers.add_parser(
        "purge-ring",
        help="Delete all documents in a portfolio/org/ring",
    )
    _add_common_aws_args(purge)
    _add_scope_args(purge, require_portfolio_org=True)
    purge.add_argument("--dry-run", action="store_true", help="Preview without deleting")
    purge.add_argument(
        "--with-graph-edges",
        action="store_true",
        help="Remove managed graph edges for each document before delete",
    )
    purge.add_argument("--yes", action="store_true", help="Skip interactive confirmation")
    purge.add_argument("--page-limit", type=int, default=200, help="path_index page size")
    purge.add_argument(
        "--skip-cache-refresh",
        action="store_true",
        help="Do not refresh S3 cache after purge",
    )

    clear = subparsers.add_parser(
        "clear-graph-edges",
        help="Delete graph edges for a portfolio/org (or entire graph table)",
    )
    _add_common_aws_args(clear)
    _add_scope_args(clear)
    clear.add_argument("--dry-run", action="store_true", help="Report scope without deleting")
    clear.add_argument(
        "--yes",
        action="store_true",
        help="Skip interactive confirmation when clearing without portfolio/org",
    )

    sync = subparsers.add_parser(
        "sync-graph-edges",
        help="Rebuild graph edges from documents without clearing first",
    )
    _add_common_aws_args(sync)
    _add_scope_args(sync)
    sync.add_argument("--dry-run", action="store_true", help="Estimate edges without writing")
    sync.add_argument("--debug-skips", action="store_true", help="Print per-document skip reasons")
    sync.add_argument("--debug-limit", type=int, default=100)

    regen = subparsers.add_parser(
        "regenerate-graph-edges",
        help="Clear graph edges for scope, then rebuild from documents",
    )
    _add_common_aws_args(regen)
    _add_scope_args(regen)
    regen.add_argument(
        "--no-clear",
        action="store_true",
        help="Skip graph clear (equivalent to sync-graph-edges)",
    )
    regen.add_argument("--dry-run", action="store_true", help="Estimate only; no clear or writes")
    regen.add_argument("--debug-skips", action="store_true")
    regen.add_argument("--debug-limit", type=int, default=100)

    sanitize = subparsers.add_parser(
        "sanitize-ring-links",
        help="Strip legacy to.*/from.* projection duplicates from link bags and dedupe targets",
    )
    _add_common_aws_args(sanitize)
    _add_scope_args(sanitize, require_portfolio_org=True)
    sanitize.add_argument("--dry-run", action="store_true", help="Report changes without writing")

    return parser


def _resolved_ring(args: argparse.Namespace) -> str:
    ring = (getattr(args, "ring", None) or getattr(args, "blueprint", None) or "").strip()
    return ring


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    ring = _resolved_ring(args)

    if args.command == "purge-ring" and not ring:
        parser.error("purge-ring requires --ring (or --blueprint)")

    ctx = build_admin_context(
        profile=args.profile,
        region=args.region,
        env=args.env,
        ring_table=args.ring_table,
        graph_table=args.graph_table,
    )
    _print_context(ctx)

    if args.command == "purge-ring":
        if not args.dry_run and not args.yes:
            if not _confirm_destructive(
                "delete ring documents",
                f"portfolio={args.portfolio} org={args.org} ring={ring}",
            ):
                print("Cancelled.")
                return 0

        results = purge_ring(
            ctx,
            portfolio=args.portfolio,
            org=args.org,
            ring=ring,
            dry_run=args.dry_run,
            with_graph_edges=args.with_graph_edges,
            page_limit=args.page_limit,
        )
        print("\nPurge summary")
        print(f"scanned       : {results['scanned']}")
        print(f"deleted       : {results['deleted']}")
        print(f"failed        : {results['failed']}")
        if args.with_graph_edges:
            print(f"graph_removed : {results['graph_removed']}")
            print(f"graph_failed  : {results['graph_failed']}")

        if not args.dry_run and not args.skip_cache_refresh:
            refresh_ring_cache(args.portfolio, args.org, ring, args.profile, ctx.region, ctx.config)
        return 0 if results["failed"] == 0 else 2

    if args.command == "clear-graph-edges":
        if not args.portfolio or not args.org:
            if not args.dry_run and not args.yes:
                if not _confirm_destructive(
                    "delete ALL graph edges in the table",
                    f"graph_table={ctx.graph_table_name}",
                ):
                    print("Cancelled.")
                    return 0
            scope = "entire graph table"
        else:
            scope = f"portfolio={args.portfolio} org={args.org}"

        if args.dry_run:
            print(f"Dry run: would clear graph edges for {scope}.")
            return 0

        deleted = clear_graph_edges(ctx.graph_table, args.portfolio, args.org)
        print(f"Cleared {deleted} graph edges ({scope}).")
        return 0

    if args.command in {"sync-graph-edges", "regenerate-graph-edges"}:
        if args.dry_run:
            print("Dry run enabled: no graph writes/deletes will be performed.")
        elif args.command == "regenerate-graph-edges" and not args.no_clear:
            if not args.portfolio or not args.org:
                parser.error("regenerate-graph-edges requires --portfolio and --org when clearing")
            deleted = clear_graph_edges(ctx.graph_table, args.portfolio, args.org)
            print(f"Cleared {deleted} graph edges.")
        elif args.command == "regenerate-graph-edges":
            print("Skipping graph clear (--no-clear).")

        results = sync_graph_edges(
            ctx,
            portfolio=args.portfolio,
            org=args.org,
            ring=ring or None,
            dry_run=args.dry_run,
            debug_skips=args.debug_skips,
            debug_limit=args.debug_limit,
        )

        if args.dry_run:
            print(
                f"Dry run done. processed={results['processed']}, with_edges={results['succeeded']}, "
                f"skipped={results['skipped']}, failed={results['failed']}, "
                f"estimated_edges={results['would_create_edges']}"
            )
        else:
            print(
                f"Done. processed={results['processed']}, ok={results['succeeded']}, "
                f"skipped={results['skipped']}, failed={results['failed']}"
            )
        skip_reasons = results.get("skip_reasons") or {}
        if skip_reasons:
            print("Skip reasons:")
            for reason, count in sorted(skip_reasons.items(), key=lambda x: x[1], reverse=True):
                print(f"  - {reason}: {count}")
        return 0 if results["failed"] == 0 else 2

    if args.command == "sanitize-ring-links":
        if not ring:
            parser.error("sanitize-ring-links requires --ring (or --blueprint)")
        results = sanitize_ring_links(
            ctx,
            portfolio=args.portfolio,
            org=args.org,
            ring=ring,
            dry_run=args.dry_run,
        )
        print(
            f"Done. processed={results['processed']} updated={results['updated']} "
            f"skipped={results['skipped']} failed={results['failed']}"
        )
        if not args.dry_run and results["updated"] > 0:
            refresh_ring_cache(args.portfolio, args.org, ring, args.profile, ctx.region, ctx.config)
        return 0 if results["failed"] == 0 else 2

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
