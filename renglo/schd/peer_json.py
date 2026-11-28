"""JSON for a peer invoke.

DynamoDB returns numbers as Decimal. A peer payload has to survive json.dumps
on the way to Lambda, Docker, or S3.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def peer_json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def dumps_peer_event(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, default=peer_json_default)
