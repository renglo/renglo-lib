"""Generic config names.

Deployed processes still export the previous names. `setting` reads the
generic name first, then that previous name, so either form works.
"""

from __future__ import annotations

import os
from typing import Any, Mapping, Optional


LEGACY = {
    "FILES_BUCKET": "S3_BUCKET_NAME",
    "DATA_TABLE": "DYNAMODB_RINGDATA_TABLE",
    "ENTITY_TABLE": "DYNAMODB_ENTITY_TABLE",
    "BLUEPRINT_TABLE": "DYNAMODB_BLUEPRINT_TABLE",
    "REL_TABLE": "DYNAMODB_REL_TABLE",
    "CHAT_TABLE": "DYNAMODB_CHAT_TABLE",
    "GRAPH_TABLE": "DYNAMODB_GRAPH_TABLE",
    "SEARCH_TABLE": "DYNAMODB_SEARCH_TABLE",
    "SESSION_TABLE": "DYNAMODB_SESSION_TABLE",
    "TIMESERIES_TABLE": "DYNAMODB_TIMESERIES_TABLE",
    "REGION": "AWS_REGION",
    "PROFILE": "AWS_PROFILE",
}


def _present(value: Any) -> bool:
    return value is not None and value != ""


def setting(
    config: Optional[Mapping[str, Any]],
    name: str,
    default: Any = None,
    *,
    environ: bool = False,
):
    """Return a config value by its generic name."""
    source = config or {}
    if _present(source.get(name)):
        return source.get(name)
    legacy = LEGACY.get(name)
    if legacy and _present(source.get(legacy)):
        return source.get(legacy)
    if environ:
        env_value = os.getenv(name)
        if _present(env_value):
            return env_value
        if legacy:
            env_value = os.getenv(legacy)
            if _present(env_value):
                return env_value
    return default
