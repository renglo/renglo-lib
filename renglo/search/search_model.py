"""
Renglo Search DB model layer.

This module owns reverse-index DynamoDB persistence/query behavior.
Controller-level callers compose this model for business logic.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Optional

from renglo.config_names import setting
from renglo.search.keys import (
    DOC_REF_TOKEN,
    make_doc_ref_pk,
    make_doc_ref_sk,
    make_pk,
    make_sk,
    now_iso,
    parse_sk,
    require_safe_part,
    to_dynamo_value,
)
from renglo.document_stores.select import open_store


class SearchModel:
    KEY_PK = "index"
    KEY_SK = "search_index"
    DOC_REF_TOKEN = DOC_REF_TOKEN
    _require_safe_part = staticmethod(require_safe_part)
    _now_iso = staticmethod(now_iso)
    _to_dynamo_value = staticmethod(to_dynamo_value)
    make_pk = staticmethod(make_pk)
    make_sk = staticmethod(make_sk)
    make_doc_ref_pk = staticmethod(make_doc_ref_pk)
    make_doc_ref_sk = staticmethod(make_doc_ref_sk)
    _parse_sk = staticmethod(parse_sk)

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        region_name: Optional[str] = None,
        dynamodb_resource: Optional[Any] = None,
        store=None,
    ) -> None:
        self.config = config or {}
        resolved_region = region_name or setting(self.config, "REGION", "us-east-1")
        resolved_table_name = setting(self.config, "SEARCH_TABLE")
        if not resolved_table_name:
            raise ValueError("SEARCH_TABLE configuration is required but not found")

        self.region = resolved_region
        self.table_name = resolved_table_name
        self.store = store or open_store(
            self.config,
            "search",
            resolved_table_name,
            self.KEY_PK,
            self.KEY_SK,
            resource=dynamodb_resource,
            region_name=resolved_region,
            use_profile=True,
        )

    @property
    def dynamodb(self):
        return self.store.ensure_resource()

    @property
    def table(self):
        return self.store._table()

    def put_index_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        now = self._now_iso()
        documents = []
        for row in rows:
            portfolio = str(row["portfolio_id"])
            org = str(row["org_id"])
            ring = str(row["ring"])
            token = str(row["token"])
            field = str(row["field"])
            doc_id = str(row["doc_id"])
            documents.append(self._to_dynamo_value({
                self.KEY_PK: self.make_pk(portfolio, org, ring, token),
                self.KEY_SK: self.make_sk(field, doc_id),
                "entry_type": "token_match",
                "field_weight": float(row.get("field_weight", 1.0)),
                "positions": list(row.get("positions", [])),
                "source_updated_at": row.get("source_updated_at") or now,
                "updated_at": now,
            }))
            documents.append({
                self.KEY_PK: self.make_doc_ref_pk(portfolio, org, ring),
                self.KEY_SK: self.make_doc_ref_sk(doc_id, field, token),
                "entry_type": "doc_ref",
                "updated_at": now,
            })
        self.store.batch_put(documents)
        return len(rows)

    def delete_index_rows(self, keys: List[Dict[str, str]]) -> int:
        if not keys:
            return 0
        return self.store.batch_delete(
            [{self.KEY_PK: key[self.KEY_PK], self.KEY_SK: key[self.KEY_SK]} for key in keys]
        )

    def delete_document_rows(self, portfolio: str, org: str, ring: str, doc_id: str) -> int:
        doc_ref_pk = self.make_doc_ref_pk(portfolio, org, ring)
        ref_keys_to_delete: List[Dict[str, str]] = []
        token_keys_to_delete: List[Dict[str, str]] = []
        cursor = None

        while True:
            items, cursor = self.store.query(
                doc_ref_pk,
                prefix=f"{doc_id}#",
                limit=200,
                continuation=cursor,
            )
            for item in items:
                ref_keys_to_delete.append({self.KEY_PK: item[self.KEY_PK], self.KEY_SK: item[self.KEY_SK]})
                sk_parts = item[self.KEY_SK].split("#", 2)
                if len(sk_parts) != 3:
                    continue
                _, field, token = sk_parts
                token_keys_to_delete.append(
                    {
                        self.KEY_PK: self.make_pk(portfolio, org, ring, token),
                        self.KEY_SK: self.make_sk(field, doc_id),
                    }
                )

            if not cursor:
                break

        return self.delete_index_rows([*token_keys_to_delete, *ref_keys_to_delete])

    def query_token(
        self,
        portfolio: str,
        org: str,
        ring: str,
        token: str,
        *,
        field_prefix: Optional[str] = None,
        limit: int = 100,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        items, token = self.store.query(
            self.make_pk(portfolio, org, ring, token),
            prefix=f"{field_prefix}#" if field_prefix else None,
            limit=limit,
            continuation=exclusive_start_key,
        )
        rows: List[Dict[str, Any]] = []
        for item in items:
            if item.get("entry_type") != "token_match":
                continue
            parsed = self._parse_sk(item[self.KEY_SK])
            rows.append(
                {
                    "portfolio_id": portfolio,
                    "org_id": org,
                    "ring": ring,
                    "token": token,
                    "field": parsed["field"],
                    "doc_id": parsed["doc_id"],
                    "token_count": 1,
                    "field_weight": float(item.get("field_weight", 1.0)),
                    "positions": item.get("positions", []),
                    "source_updated_at": item.get("source_updated_at"),
                }
            )

        return {
            "items": rows,
            "last_evaluated_key": token,
        }

    def describe_table(self, logical_name="Search"):
        self.store.describe(
            logical_name,
            hint="Use --region (and optionally --profile) to target the right environment.",
        )

    def scan_projected(self, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
        yield from self.store.scan(fields)

    def query_doc_refs(self, portfolio: str, org: str, ring: str, doc_id: str) -> List[Dict[str, Any]]:
        doc_ref_pk = self.make_doc_ref_pk(portfolio, org, ring)
        ref_items: List[Dict[str, Any]] = []
        cursor = None
        while True:
            items, cursor = self.store.query(
                doc_ref_pk,
                prefix=f"{doc_id}#",
                limit=200,
                continuation=cursor,
                fields=[self.KEY_PK, self.KEY_SK],
            )
            ref_items.extend(items)
            if not cursor:
                break
        return ref_items

    def get_item(self, pk: str, sk: str) -> Optional[Dict[str, Any]]:
        return self.store.get(pk, sk)

    def delete_items(self, items: Iterable[Dict[str, Any]]) -> int:
        return self.store.batch_delete(
            [{self.KEY_PK: item[self.KEY_PK], self.KEY_SK: item[self.KEY_SK]} for item in items]
        )

