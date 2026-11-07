"""Resolve Entra / External ID OpenID settings from the tenant id alone.

CIAM (External ID) uses a different host for JWKS than the token issuer. Optional
ENTRA_AUTHORITY / ENTRA_JWKS_URI in config still override discovery.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request


def _fetch_json(url: str, timeout: float = 20.0) -> dict | None:
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "Renglo/1.0"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            if not raw:
                return None
            return json.loads(raw.decode())
    except (OSError, ValueError, urllib.error.HTTPError):
        return None


def _authority_base_from_issuer(issuer: str) -> str:
    issuer = str(issuer or "").strip().rstrip("/")
    if issuer.endswith("/v2.0"):
        return issuer[: -len("/v2.0")]
    return issuer


def discover_entra_openid_metadata(tenant_id: str) -> dict[str, str] | None:
    """Return authority_base, issuer, jwks_uri, token_endpoint for this tenant.

    Tries External ID (tenant-id host) first, then workforce login.microsoftonline.com.
    """
    tid = str(tenant_id or "").strip()
    if not tid:
        return None

    candidates = [
        f"https://{tid}.ciamlogin.com/{tid}/v2.0/.well-known/openid-configuration",
        f"https://login.microsoftonline.com/{tid}/v2.0/.well-known/openid-configuration",
    ]
    for url in candidates:
        meta = _fetch_json(url)
        if not meta:
            continue
        issuer = str(meta.get("issuer") or "").strip()
        jwks = str(meta.get("jwks_uri") or "").strip()
        token_endpoint = str(meta.get("token_endpoint") or "").strip()
        authorization_endpoint = str(meta.get("authorization_endpoint") or "").strip()
        if not issuer or not jwks:
            continue
        authority_base = _authority_base_from_issuer(issuer)
        return {
            "authority_base": authority_base,
            "issuer": issuer if issuer.endswith("/v2.0") else f"{authority_base}/v2.0",
            "jwks_uri": jwks,
            "token_endpoint": token_endpoint,
            "authorization_endpoint": authorization_endpoint,
        }
    return None


def sign_in_authority_base(discovered: dict[str, str] | None) -> str | None:
    """Browser sign-in host may differ from issuer on CIAM; prefer authorization_endpoint."""
    if not discovered:
        return None
    auth = str(discovered.get("authorization_endpoint") or "").strip()
    if not auth:
        return str(discovered.get("authority_base") or "").strip() or None
    match = re.match(r"^(.*)/oauth2/v2\.0/authorize", auth)
    if match:
        return match.group(1).rstrip("/")
    return str(discovered.get("authority_base") or "").strip() or None


def merge_with_metadata(
    tenant_id: str,
    authority_override: str,
    jwks_override: str,
) -> dict[str, str] | None:
    """Apply optional overrides; discover when overrides omit jwks or authority."""
    authority = str(authority_override or "").strip().rstrip("/")
    if authority.endswith("/v2.0"):
        authority = authority[: -len("/v2.0")]
    jwks = str(jwks_override or "").strip()
    if authority and jwks:
        issuer = f"{authority}/v2.0"
        return {
            "authority_base": authority,
            "issuer": issuer,
            "jwks_uri": jwks,
            "token_endpoint": f"{authority}/oauth2/v2.0/token",
        }

    meta = discover_entra_openid_metadata(tenant_id)
    if meta:
        if not authority:
            authority = sign_in_authority_base(meta) or meta.get("authority_base") or ""
        if not jwks:
            jwks = meta.get("jwks_uri") or ""
        issuer = meta.get("issuer") or (f"{authority}/v2.0" if authority else "")
        token_endpoint = meta.get("token_endpoint") or (
            f"{authority}/oauth2/v2.0/token" if authority else ""
        )
        return {
            "authority_base": authority,
            "issuer": issuer,
            "jwks_uri": jwks,
            "token_endpoint": token_endpoint,
        }
    if authority:
        issuer = f"{authority}/v2.0"
        return {
            "authority_base": authority,
            "issuer": issuer,
            "jwks_uri": jwks or f"{authority}/discovery/v2.0/keys",
            "token_endpoint": f"{authority}/oauth2/v2.0/token",
        }
    return None
