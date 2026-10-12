"""Map a Cognito identities claim onto one application user.

Email is not the key. The provider name and provider subject come from the
token claim and are stored as an opaque pair.
"""

from __future__ import annotations

import json
from typing import Any

USER_IDENTITY_INDEX = "irn:entity:user_identity:*"


def identities_from_claims(claims: dict | None) -> list[dict[str, str]]:
    """Return provider/subject pairs. A token with no identities claim is native."""
    if not claims:
        return []
    raw = claims.get("identities")
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []
    found: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        provider_name = str(item.get("providerName") or "").strip()
        provider_subject = str(item.get("userId") or "").strip()
        if provider_name and provider_subject:
            found.append(
                {
                    "provider_name": provider_name,
                    "provider_subject": provider_subject,
                }
            )
    return found


def identity_key(provider_name: str, provider_subject: str) -> str:
    return f"{provider_name}#{provider_subject}"


def identity_document(
    *,
    provider_name: str,
    provider_subject: str,
    user_id: str,
    cognito_sub: str,
    email: str,
    linked_at: str,
) -> dict[str, Any]:
    key = identity_key(provider_name, provider_subject)
    return {
        "_id": key,
        "index": USER_IDENTITY_INDEX,
        "irn": f"irn:entity:user_identity:{key}",
        "type": "user_identity",
        "provider_name": provider_name,
        "provider_subject": provider_subject,
        "user_id": user_id,
        "cognito_sub": cognito_sub,
        "email": email,
        "status": "active",
        "linked_at": linked_at,
        "last_authenticated_at": linked_at,
    }
