#!/usr/bin/env python3
"""
Regenerate Renglo search index from canonical documents.

Modes:
- rebuild: clear selected search index scope, then reindex all matching documents
- sync: reindex only documents with missing, stale, or inconsistent search rows
- clear: clear selected search index scope only (no reindex)

Supports --dry-run for non-destructive planning.

Examples:
  python dev/renglo-lib/renglo/search/regenerate_search_index.py <env> --profile <profile> --mode rebuild
  python dev/renglo-lib/renglo/search/regenerate_search_index.py <env> --profile <profile> --mode sync
  python dev/renglo-lib/renglo/search/regenerate_search_index.py <env> --profile <profile> --mode clear
  python dev/renglo-lib/renglo/search/regenerate_search_index.py <env> --profile <profile> --portfolio <portfolio> --org <org> --ring reservation --mode sync
  python dev/renglo-lib/renglo/search/regenerate_search_index.py <env> --profile <profile> --mode rebuild --dry-run
"""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from renglo.common import load_config
from renglo.search.search_controller import SearchController
from renglo.search.search_model import SearchModel


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


def _extract_blueprint_handle(blueprint_uri: Any) -> Optional[str]:
    if not isinstance(blueprint_uri, str) or "/_blueprint/" not in blueprint_uri:
        return None
    tail = blueprint_uri.split("/_blueprint/", 1)[1]
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
) -> Iterable[Tuple[str, str, str, str, Dict[str, Any]]]:
    projection = ["portfolio_index", "doc_index", "_id", "attributes", "modified", "updated_at", "added", "blueprint"]
    for item in _scan_all(ring_table, projection):
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
        doc_id = str(item.get("_id") or idx)
        attrs = item.get("attributes")
        if not isinstance(attrs, dict):
            attrs = {}
        doc = {
            "_id": doc_id,
            "attributes": attrs,
            "modified": item.get("modified"),
            "updated_at": item.get("updated_at"),
            "added": item.get("added"),
            "blueprint": item.get("blueprint"),
        }
        yield portfolio, org, ring, doc_id, doc


def _parse_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _doc_updated_at(doc: Dict[str, Any]) -> Optional[datetime]:
    return _parse_iso(doc.get("modified")) or _parse_iso(doc.get("updated_at")) or _parse_iso(doc.get("added"))


def _get_doc_index_health(
    search_table,
    portfolio: str,
    org: str,
    ring: str,
    doc_id: str,
) -> Dict[str, Any]:
    doc_ref_pk = SearchModel.make_doc_ref_pk(portfolio, org, ring)
    ref_items = []
    cursor = None
    pk_name = SearchModel.KEY_PK
    sk_name = SearchModel.KEY_SK
    while True:
        query_kwargs: Dict[str, Any] = {
            "KeyConditionExpression": Key(pk_name).eq(doc_ref_pk)
            & Key(sk_name).begins_with(f"{doc_id}#"),
            "ProjectionExpression": "#pk, #sk",
            "ExpressionAttributeNames": {"#pk": pk_name, "#sk": sk_name},
            "Limit": 200,
        }
        if cursor:
            query_kwargs["ExclusiveStartKey"] = cursor
        response = search_table.query(**query_kwargs)
        ref_items.extend(response.get("Items", []))
        cursor = response.get("LastEvaluatedKey")
        if not cursor:
            break

    if not ref_items:
        return {"status": "missing", "row_count": 0, "max_source_updated_at": None}

    missing_token_rows = 0
    max_source_updated_at: Optional[datetime] = None
    valid_token_rows = 0

    for ref in ref_items:
        sk = str(ref.get(sk_name, ""))
        parts = sk.split("#", 2)
        if len(parts) != 3:
            missing_token_rows += 1
            continue
        _, field, token = parts
        token_item = search_table.get_item(
            Key={
                pk_name: SearchModel.make_pk(portfolio, org, ring, token),
                sk_name: SearchModel.make_sk(field, doc_id),
            }
        ).get("Item")
        if not token_item:
            missing_token_rows += 1
            continue
        valid_token_rows += 1
        token_ts = _parse_iso(token_item.get("source_updated_at"))
        if token_ts and (max_source_updated_at is None or token_ts > max_source_updated_at):
            max_source_updated_at = token_ts

    if valid_token_rows == 0:
        return {"status": "missing", "row_count": 0, "max_source_updated_at": None}
    if missing_token_rows > 0:
        return {
            "status": "inconsistent",
            "row_count": valid_token_rows,
            "max_source_updated_at": max_source_updated_at,
            "missing_token_rows": missing_token_rows,
        }
    return {
        "status": "present",
        "row_count": valid_token_rows,
        "max_source_updated_at": max_source_updated_at,
    }


def _should_reindex_sync(doc: Dict[str, Any], health: Dict[str, Any]) -> Tuple[bool, str]:
    status = health.get("status")
    if status == "missing":
        return True, "missing"
    if status == "inconsistent":
        return True, "inconsistent"

    doc_ts = _doc_updated_at(doc)
    idx_ts = health.get("max_source_updated_at")
    if doc_ts and (idx_ts is None or idx_ts < doc_ts):
        return True, "stale"
    return False, "up_to_date"


def _clear_search_rows_for_scope(
    search_table,
    portfolio: Optional[str],
    org: Optional[str],
    ring: Optional[str],
) -> int:
    deleted = 0
    pk_name = SearchModel.KEY_PK
    sk_name = SearchModel.KEY_SK
    with search_table.batch_writer() as batch:
        for item in _scan_all(search_table, [pk_name, sk_name]):
            index_value = str(item.get(pk_name, ""))
            # Full rebuild with no scope filters should clear every row, regardless of key shape.
            if not portfolio and not org and not ring:
                batch.delete_item(Key={pk_name: index_value, sk_name: item[sk_name]})
                deleted += 1
                continue
            parts = index_value.split("#", 3)
            if len(parts) != 4:
                continue
            row_portfolio, row_org, row_ring, _ = parts
            if portfolio and row_portfolio != portfolio:
                continue
            if org and row_org != org:
                continue
            if ring and row_ring != ring:
                continue
            batch.delete_item(Key={pk_name: index_value, sk_name: item[sk_name]})
            deleted += 1
    return deleted


def _count_search_rows_for_scope(
    search_table,
    portfolio: Optional[str],
    org: Optional[str],
    ring: Optional[str],
) -> int:
    count = 0
    pk_name = SearchModel.KEY_PK
    for item in _scan_all(search_table, [pk_name]):
        index_value = str(item.get(pk_name, ""))
        if not portfolio and not org and not ring:
            count += 1
            continue
        parts = index_value.split("#", 3)
        if len(parts) != 4:
            continue
        row_portfolio, row_org, row_ring, _ = parts
        if portfolio and row_portfolio != portfolio:
            continue
        if org and row_org != org:
            continue
        if ring and row_ring != ring:
            continue
        count += 1
    return count


def _describe_table_or_raise(dynamodb_client, table_name: str, region: str, logical_name: str) -> None:
    try:
        dynamodb_client.describe_table(TableName=table_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        if code == "ResourceNotFoundException":
            raise RuntimeError(
                f"{logical_name} table '{table_name}' was not found in region '{region}'. "
                f"Use --region (and optionally --profile) to target the right environment."
            ) from exc
        raise RuntimeError(
            f"Failed to describe {logical_name} table '{table_name}' in region '{region}': {code} {msg}"
        ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenerate Renglo search index from canonical documents.")
    parser.add_argument(
        "environment_name",
        type=str,
        help="Environment name prefix used for DynamoDB tables (e.g. dev, productora, prod).",
    )
    parser.add_argument("--portfolio", help="Restrict to one portfolio_id.")
    parser.add_argument("--org", help="Restrict to one org_id.")
    parser.add_argument("--ring", help="Restrict to one ring name.")
    parser.add_argument(
        "--mode",
        choices=["rebuild", "sync", "clear"],
        default="sync",
        help="rebuild: clear+reindex all in scope. sync: only reindex missing/stale/inconsistent docs. clear: clear only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write/delete search rows. Only report actions.",
    )
    parser.add_argument("--region", help="AWS region override (default: config AWS_REGION).")
    parser.add_argument(
        "--profile",
        required=True,
        help="AWS profile to use (required).",
    )
    parser.add_argument("--max-docs", type=int, default=0, help="Optional cap on documents processed (0 = no cap).")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-document decisions and indexing outcomes.",
    )
    parser.add_argument(
        "--debug-plan",
        action="store_true",
        help="Print resolved search plans (ring -> handle -> fields/modes/weights).",
    )
    args = parser.parse_args()

    config = load_config()
    region = args.region or config.get("AWS_REGION", "us-east-1")
    ring_table_name = f"{args.environment_name}_data"
    search_table_name = f"{args.environment_name}_search"

    session_kwargs: Dict[str, Any] = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    session = boto3.session.Session(**session_kwargs)
    dynamodb = session.resource("dynamodb", region_name=region)
    dynamodb_client = session.client("dynamodb", region_name=region)
    _describe_table_or_raise(dynamodb_client, ring_table_name, region, "Ring data")
    _describe_table_or_raise(dynamodb_client, search_table_name, region, "Search")

    print(
        f"Using region={region}, ring_table={ring_table_name}, search_table={search_table_name}, "
        f"mode={args.mode}, dry_run={args.dry_run}"
    )
    if args.profile:
        print(f"Using AWS profile={args.profile}")
    print(
        f"Scope filters: portfolio={args.portfolio or '*'} org={args.org or '*'} ring={args.ring or '*'} "
        f"max_docs={args.max_docs or 'unlimited'} verbose={args.verbose} debug_plan={args.debug_plan}"
    )

    ring_table = dynamodb.Table(ring_table_name)
    search_table = dynamodb.Table(search_table_name)

    runtime_config = dict(config)
    runtime_config["DYNAMODB_SEARCH_TABLE"] = search_table_name
    shc = SearchController(
        config=runtime_config,
        region_name=region,
        dynamodb_resource=dynamodb,
    )

    if args.mode == "rebuild" and not args.dry_run:
        deleted = _clear_search_rows_for_scope(search_table, args.portfolio, args.org, args.ring)
        print(f"Cleared {deleted} search rows in selected scope.")
    elif args.mode == "rebuild" and args.dry_run:
        print("Dry run rebuild: skipping clear/write.")
    elif args.mode == "clear":
        if args.dry_run:
            to_clear = _count_search_rows_for_scope(search_table, args.portfolio, args.org, args.ring)
            print(f"Dry run clear: would clear {to_clear} search rows in selected scope.")
        else:
            deleted = _clear_search_rows_for_scope(search_table, args.portfolio, args.org, args.ring)
            print(f"Cleared {deleted} search rows in selected scope.")
        return 0

    processed = 0
    reindexed = 0
    skipped = 0
    failed = 0
    reasons: Dict[str, int] = {}
    printed_plan_keys = set()

    for portfolio, org, ring, doc_id, doc in _iter_documents(
        ring_table,
        args.portfolio,
        args.org,
        args.ring,
    ):
        if args.max_docs > 0 and processed >= args.max_docs:
            break
        processed += 1
        doc_path = f"{portfolio}/{org}/{ring}/{doc_id}"
        blueprint_handle = _extract_blueprint_handle(doc.get("blueprint"))

        if args.debug_plan and shc.index_service:
            plan_cache_key = f"{blueprint_handle or 'default'}::{ring}"
            if plan_cache_key not in printed_plan_keys:
                plan = shc.index_service.get_index_plan(ring, blueprint_handle=blueprint_handle)
                print(
                    f"[PLAN] ring={ring} blueprint_handle={blueprint_handle or 'default'} "
                    f"searchable_fields={plan.get('searchable_fields', [])} "
                    f"field_modes={plan.get('field_modes', {})} "
                    f"field_weights={plan.get('field_weights', {})}"
                )
                printed_plan_keys.add(plan_cache_key)

        if args.verbose:
            print(
                f"[DOC] {doc_path} blueprint_handle={blueprint_handle or 'default'} "
                f"doc_updated_at={doc.get('modified') or doc.get('updated_at') or doc.get('added')}"
            )

        if args.mode == "rebuild":
            should_index, reason = True, "rebuild"
        else:
            try:
                health = _get_doc_index_health(search_table, portfolio, org, ring, doc_id)
                should_index, reason = _should_reindex_sync(doc, health)
                if args.verbose:
                    print(
                        f"[SYNC] {doc_path} status={health.get('status')} "
                        f"row_count={health.get('row_count')} "
                        f"missing_token_rows={health.get('missing_token_rows', 0)} "
                        f"max_source_updated_at={health.get('max_source_updated_at')} "
                        f"decision={reason}"
                    )
            except Exception as exc:  # pragma: no cover
                should_index, reason = True, "health_check_error"
                print(f"[WARN] Health check failed for {doc_path}: {exc}")

        reasons[reason] = reasons.get(reason, 0) + 1

        if not should_index:
            skipped += 1
            if args.verbose:
                print(f"[SKIP] {doc_path} reason={reason}")
            continue

        if args.dry_run:
            reindexed += 1
            if args.verbose:
                print(f"[DRY-RUN] {doc_path} action=index reason={reason}")
            continue

        try:
            result = shc.index_document(portfolio, org, ring, doc)
            if result.get("success"):
                reindexed += 1
                if args.verbose:
                    print(
                        f"[INDEXED] {doc_path} reason={reason} "
                        f"deleted_rows={result.get('deleted_rows')} indexed_rows={result.get('indexed_rows')} "
                        f"token_count={result.get('token_count')} searchable_fields={result.get('searchable_fields')}"
                    )
            else:
                failed += 1
                print(f"[WARN] Index failed for {doc_path}: {result}")
        except Exception as exc:  # pragma: no cover
            failed += 1
            print(f"[ERROR] {doc_path}: {exc}")

        if processed % 1000 == 0:
            print(f"Progress {processed} docs | reindexed={reindexed} skipped={skipped} failed={failed}")

    if args.dry_run:
        print(f"Dry run done. processed={processed}, would_reindex={reindexed}, skipped={skipped}, failed={failed}")
    else:
        print(f"Done. processed={processed}, reindexed={reindexed}, skipped={skipped}, failed={failed}")

    if reasons:
        print("Decision reasons:")
        for reason, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {reason}: {count}")

    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

