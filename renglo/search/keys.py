"""Search index keys shared by every search store."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict


DOC_REF_TOKEN = "__docref__"


def require_safe_part(name: str, value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    if "#" in value:
        raise ValueError(f"{name} cannot contain '#': {value!r}")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_dynamo_value(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: to_dynamo_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_dynamo_value(item) for item in value]
    return value


def make_pk(portfolio: str, org: str, ring: str, token: str) -> str:
    require_safe_part("portfolio", portfolio)
    require_safe_part("org", org)
    require_safe_part("ring", ring)
    require_safe_part("token", token)
    return f"{portfolio}#{org}#{ring}#{token}"


def make_sk(field: str, doc_id: str) -> str:
    require_safe_part("field", field)
    require_safe_part("doc_id", doc_id)
    return f"{field}#{doc_id}"


def make_doc_ref_pk(portfolio: str, org: str, ring: str) -> str:
    require_safe_part("portfolio", portfolio)
    require_safe_part("org", org)
    require_safe_part("ring", ring)
    return f"{portfolio}#{org}#{ring}#{DOC_REF_TOKEN}"


def make_doc_ref_sk(doc_id: str, field: str, token: str) -> str:
    require_safe_part("doc_id", doc_id)
    require_safe_part("field", field)
    require_safe_part("token", token)
    return f"{doc_id}#{field}#{token}"


def parse_sk(sk: str) -> Dict[str, str]:
    parts = sk.split("#", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid search sort key format: {sk!r}")
    return {"field": parts[0], "doc_id": parts[1]}
