"""
Renglo Search DB model layer.

This module owns reverse-index DynamoDB persistence/query behavior.
Controller-level callers compose this model for business logic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, Iterator, List, Optional

from renglo.config_names import setting
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


class SearchModel:
    KEY_PK = "index"
    KEY_SK = "search_index"
    DOC_REF_TOKEN = "__docref__"

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        region_name: Optional[str] = None,
        dynamodb_resource: Optional[Any] = None,
    ) -> None:
        self.config = config or {}
        resolved_region = region_name or setting(self.config, "REGION", "us-east-1")
        resolved_table_name = setting(self.config, "SEARCH_TABLE")
        if not resolved_table_name:
            raise ValueError("SEARCH_TABLE configuration is required but not found")

        self.region = resolved_region
        self.dynamodb = dynamodb_resource or _dynamodb_resource(self.config, resolved_region)
        self.table = self.dynamodb.Table(resolved_table_name)
        self.table_name = resolved_table_name

    @staticmethod
    def _require_safe_part(name: str, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string")
        if "#" in value:
            raise ValueError(f"{name} cannot contain '#': {value!r}")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _to_dynamo_value(value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, dict):
            return {k: SearchModel._to_dynamo_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [SearchModel._to_dynamo_value(v) for v in value]
        return value

    @classmethod
    def make_pk(cls, portfolio: str, org: str, ring: str, token: str) -> str:
        cls._require_safe_part("portfolio", portfolio)
        cls._require_safe_part("org", org)
        cls._require_safe_part("ring", ring)
        cls._require_safe_part("token", token)
        return f"{portfolio}#{org}#{ring}#{token}"

    @classmethod
    def make_sk(cls, field: str, doc_id: str) -> str:
        cls._require_safe_part("field", field)
        cls._require_safe_part("doc_id", doc_id)
        return f"{field}#{doc_id}"

    @classmethod
    def make_doc_ref_pk(cls, portfolio: str, org: str, ring: str) -> str:
        cls._require_safe_part("portfolio", portfolio)
        cls._require_safe_part("org", org)
        cls._require_safe_part("ring", ring)
        return f"{portfolio}#{org}#{ring}#{cls.DOC_REF_TOKEN}"

    @classmethod
    def make_doc_ref_sk(cls, doc_id: str, field: str, token: str) -> str:
        cls._require_safe_part("doc_id", doc_id)
        cls._require_safe_part("field", field)
        cls._require_safe_part("token", token)
        return f"{doc_id}#{field}#{token}"

    @classmethod
    def _parse_sk(cls, sk: str) -> Dict[str, str]:
        parts = sk.split("#", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid search sort key format: {sk!r}")
        return {"field": parts[0], "doc_id": parts[1]}

    def put_index_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        now = self._now_iso()
        inserted = 0
        with self.table.batch_writer() as batch:
            for row in rows:
                portfolio = str(row["portfolio_id"])
                org = str(row["org_id"])
                ring = str(row["ring"])
                token = str(row["token"])
                field = str(row["field"])
                doc_id = str(row["doc_id"])

                item = {
                    self.KEY_PK: self.make_pk(portfolio, org, ring, token),
                    self.KEY_SK: self.make_sk(field, doc_id),
                    "entry_type": "token_match",
                    "field_weight": float(row.get("field_weight", 1.0)),
                    "positions": list(row.get("positions", [])),
                    "source_updated_at": row.get("source_updated_at") or now,
                    "updated_at": now,
                }

                batch.put_item(Item=self._to_dynamo_value(item))
                inserted += 1

                ref_item = {
                    self.KEY_PK: self.make_doc_ref_pk(portfolio, org, ring),
                    self.KEY_SK: self.make_doc_ref_sk(doc_id, field, token),
                    "entry_type": "doc_ref",
                    "updated_at": now,
                }
                batch.put_item(Item=ref_item)

        return inserted

    def delete_index_rows(self, keys: List[Dict[str, str]]) -> int:
        if not keys:
            return 0
        deleted = 0
        with self.table.batch_writer() as batch:
            for key in keys:
                batch.delete_item(Key={self.KEY_PK: key[self.KEY_PK], self.KEY_SK: key[self.KEY_SK]})
                deleted += 1
        return deleted

    def delete_document_rows(self, portfolio: str, org: str, ring: str, doc_id: str) -> int:
        doc_ref_pk = self.make_doc_ref_pk(portfolio, org, ring)
        ref_keys_to_delete: List[Dict[str, str]] = []
        token_keys_to_delete: List[Dict[str, str]] = []
        cursor = None

        while True:
            query_kwargs: Dict[str, Any] = {
                "KeyConditionExpression": Key(self.KEY_PK).eq(doc_ref_pk)
                & Key(self.KEY_SK).begins_with(f"{doc_id}#"),
                "Limit": 200,
            }
            if cursor:
                query_kwargs["ExclusiveStartKey"] = cursor

            response = self.table.query(**query_kwargs)
            for item in response.get("Items", []):
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

            cursor = response.get("LastEvaluatedKey")
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
        query_kwargs: Dict[str, Any] = {
            "KeyConditionExpression": Key(self.KEY_PK).eq(self.make_pk(portfolio, org, ring, token)),
            "Limit": limit,
        }
        if field_prefix:
            query_kwargs["KeyConditionExpression"] = (
                Key(self.KEY_PK).eq(self.make_pk(portfolio, org, ring, token))
                & Key(self.KEY_SK).begins_with(f"{field_prefix}#")
            )
        if exclusive_start_key:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key

        response = self.table.query(**query_kwargs)
        rows: List[Dict[str, Any]] = []
        for item in response.get("Items", []):
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
            "last_evaluated_key": response.get("LastEvaluatedKey"),
        }

    def describe_table(self, logical_name="Search"):
        _describe_or_raise(
            self.dynamodb.meta.client,
            self.table_name,
            self.region,
            logical_name,
            hint="Use --region (and optionally --profile) to target the right environment.",
        )

    def scan_projected(self, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
        yield from _scan_projected(self.table, fields)

    def query_doc_refs(self, portfolio: str, org: str, ring: str, doc_id: str) -> List[Dict[str, Any]]:
        doc_ref_pk = self.make_doc_ref_pk(portfolio, org, ring)
        ref_items: List[Dict[str, Any]] = []
        cursor = None
        while True:
            query_kwargs: Dict[str, Any] = {
                "KeyConditionExpression": Key(self.KEY_PK).eq(doc_ref_pk)
                & Key(self.KEY_SK).begins_with(f"{doc_id}#"),
                "ProjectionExpression": "#pk, #sk",
                "ExpressionAttributeNames": {"#pk": self.KEY_PK, "#sk": self.KEY_SK},
                "Limit": 200,
            }
            if cursor:
                query_kwargs["ExclusiveStartKey"] = cursor
            response = self.table.query(**query_kwargs)
            ref_items.extend(response.get("Items", []))
            cursor = response.get("LastEvaluatedKey")
            if not cursor:
                break
        return ref_items

    def get_item(self, pk: str, sk: str) -> Optional[Dict[str, Any]]:
        return self.table.get_item(Key={self.KEY_PK: pk, self.KEY_SK: sk}).get("Item")

    def delete_items(self, items: Iterable[Dict[str, Any]]) -> int:
        deleted = 0
        with self.table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={self.KEY_PK: item[self.KEY_PK], self.KEY_SK: item[self.KEY_SK]})
                deleted += 1
        return deleted


def _dynamodb_resource(config, region):
    profile = str(setting(config, "PROFILE") or "").strip()
    if profile:
        session = boto3.session.Session(profile_name=profile)
        return session.resource("dynamodb", region_name=region)
    return boto3.resource("dynamodb", region_name=region)


def _describe_or_raise(dynamodb_client, table_name, region, logical_name, hint):
    try:
        dynamodb_client.describe_table(TableName=table_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        if code == "ResourceNotFoundException":
            raise RuntimeError(
                f"{logical_name} table '{table_name}' was not found in region '{region}'. {hint}"
            ) from exc
        raise RuntimeError(
            f"Failed to describe {logical_name} table '{table_name}' in region '{region}': {code} {msg}"
        ) from exc


def _scan_projected(table, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
    names = list(fields)
    expr_attr_names = {f"#f{i}": field for i, field in enumerate(names)}
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

