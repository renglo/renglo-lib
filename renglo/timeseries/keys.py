"""Partition and sort keys for the TimeSeries table."""

from __future__ import annotations

from datetime import datetime, timezone

RESOLUTIONS = ("year", "month", "day", "hour", "minute", "second")
READING_LIMIT = 5000
ITEM_SOFT_LIMIT = 350_000
SORT_KEY_LIMIT = 1024


def make_index(portfolio: str, org: str, key: str) -> str:
    return f"irn:metric:{portfolio}:{org}:{key}"


def encode_dimension(value) -> str:
    text = str(value)
    return text.replace("%", "%25").replace("#", "%23")


def format_period(timestamp, resolution: str) -> str:
    moment = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    year = f"{moment.year:04d}"
    month = f"{year}-{moment.month:02d}"
    day = f"{month}-{moment.day:02d}"
    hour = f"{day}:{moment.hour:02d}"
    minute = f"{hour}:{moment.minute:02d}"
    if resolution == "year":
        return year
    if resolution == "month":
        return month
    if resolution == "day":
        return day
    if resolution == "hour":
        return hour
    if resolution == "minute":
        return minute
    if resolution == "second":
        return f"{minute}:{moment.second:02d}"
    raise ValueError(f"Unknown resolution {resolution}")


def metric_index(period: str, dimensions, segment: int = 0) -> str:
    parts = [period]
    parts.extend(encode_dimension(value) for value in dimensions or [])
    key = "#".join(parts)
    if segment:
        key = f"{key}#{int(segment)}"
    if len(key.encode("utf-8")) > SORT_KEY_LIMIT:
        raise ValueError("metric_index exceeds 1024 bytes")
    return key


def query_prefix(period: str, dimensions_prefix=None) -> str:
    if dimensions_prefix:
        encoded = "#".join(encode_dimension(value) for value in dimensions_prefix)
        return f"{period}#{encoded}#"
    if len(period) == 4 and period.isdigit():
        return period
    return f"{period}#"


def month_of(period: str) -> str:
    if len(period) >= 7 and period[4] == "-":
        return period[:7]
    raise ValueError(f"Cannot derive a month from {period}")


def year_of(period: str) -> str:
    if len(period) >= 4 and period[:4].isdigit():
        return period[:4]
    raise ValueError(f"Cannot derive a year from {period}")
