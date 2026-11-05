"""Reading math shared by every TimeSeries store."""

from __future__ import annotations

import time
from decimal import Decimal, ROUND_HALF_UP

from renglo.timeseries.keys import ITEM_SOFT_LIMIT


def stored(item, appended):
    copied = dict(item or {})
    copied["appended"] = appended
    return copied


def has_key(item, idempotency_key):
    if not item:
        return False
    return str(idempotency_key) in set(item.get("idempotency_keys") or [])


def is_full(item, limit):
    count = item.get("reading_count")
    if count is None:
        count = len(item.get("readings") or [])
    if int(count) >= limit:
        return True
    payload = item.get("payload_bytes")
    if payload is None:
        payload = payload_bytes(item.get("readings") or [])
    return int(payload) >= ITEM_SOFT_LIMIT


def payload_bytes(readings):
    total = 0
    for reading in readings:
        total += len(str(reading)) + 8
    return total


def reading_time(timestamp):
    """Unix seconds with millisecond precision, the resolution of a browser clock."""
    raw = Decimal(str(time.time() if timestamp is None else timestamp))
    return raw.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def apply_reading(statistics, timestamp, value):
    moment = decimal(timestamp)
    number = decimal(value)
    if not statistics:
        return {
            "count": Decimal(1),
            "sum": number,
            "min": number,
            "max": number,
            "first": number,
            "last": number,
            "first_timestamp": Decimal(moment),
            "last_timestamp": Decimal(moment),
        }
    count = decimal(statistics.get("count") or 0) + 1
    total = decimal(statistics.get("sum") or 0) + number
    low = decimal(statistics.get("min"))
    high = decimal(statistics.get("max"))
    first = decimal(statistics.get("first"))
    last = decimal(statistics.get("last"))
    first_timestamp = decimal(statistics.get("first_timestamp") or moment)
    last_timestamp = decimal(statistics.get("last_timestamp") or moment)
    if number < low:
        low = number
    if number > high:
        high = number
    if moment < first_timestamp:
        first = number
        first_timestamp = moment
    if moment >= last_timestamp:
        last = number
        last_timestamp = moment
    return {
        "count": count,
        "sum": total,
        "min": low,
        "max": high,
        "first": first,
        "last": last,
        "first_timestamp": Decimal(first_timestamp),
        "last_timestamp": Decimal(last_timestamp),
    }


def consolidate(stats_list):
    count = Decimal(0)
    total = Decimal(0)
    low = None
    high = None
    first = None
    first_timestamp = None
    last = None
    last_timestamp = None
    for statistics in stats_list:
        if not statistics or statistics.get("count") in (None, 0):
            continue
        count += decimal(statistics.get("count") or 0)
        total += decimal(statistics.get("sum") or 0)
        candidate_min = decimal(statistics.get("min"))
        candidate_max = decimal(statistics.get("max"))
        low = candidate_min if low is None else min(low, candidate_min)
        high = candidate_max if high is None else max(high, candidate_max)
        stamp = statistics.get("first_timestamp")
        if stamp is not None and (first_timestamp is None or decimal(stamp) < first_timestamp):
            first_timestamp = decimal(stamp)
            first = decimal(statistics.get("first"))
        stamp = statistics.get("last_timestamp")
        if stamp is not None and (last_timestamp is None or decimal(stamp) >= last_timestamp):
            last_timestamp = decimal(stamp)
            last = decimal(statistics.get("last"))
    return {
        "count": count,
        "sum": total,
        "min": low if low is not None else Decimal(0),
        "max": high if high is not None else Decimal(0),
        "first": first if first is not None else Decimal(0),
        "last": last if last is not None else Decimal(0),
        "first_timestamp": Decimal(first_timestamp or 0),
        "last_timestamp": Decimal(last_timestamp or 0),
    }


def merge_documents(documents, base):
    documents = sorted(documents, key=lambda item: int(item.get("segment") or 0))
    readings = []
    for item in documents:
        readings.extend(item.get("readings") or [])
    readings.sort(key=lambda pair: pair[0])
    head = documents[0]
    return {
        "index": head.get("index"),
        "metric_index": base,
        "resolution": head.get("resolution"),
        "period": head.get("period"),
        "dimensions": list(head.get("dimensions") or []),
        "readings": readings,
        "statistics": consolidate([item.get("statistics") or {} for item in documents]),
    }


def same_document(existing, item):
    if existing.get("resolution") != item.get("resolution"):
        return False
    if not stats_equal(existing.get("statistics") or {}, item.get("statistics") or {}):
        return False
    left = existing.get("readings") or []
    right = item.get("readings") or []
    if len(left) != len(right):
        return False
    for one, two in zip(left, right):
        if str(one[0]) != str(two[0]) or decimal(one[1]) != decimal(two[1]):
            return False
    return True


def stats_equal(left, right):
    fields = (
        "count",
        "sum",
        "min",
        "max",
        "first",
        "last",
        "first_timestamp",
        "last_timestamp",
    )
    return all(decimal(left.get(name) or 0) == decimal(right.get(name) or 0) for name in fields)


def decimal(value):
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def plain(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [plain(item) for item in value]
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


def public_statistics(statistics):
    shown = plain(statistics or {})
    shown.pop("avg", None)
    count = shown.get("count") or 0
    shown["avg"] = (shown["sum"] / count) if count else None
    return shown
