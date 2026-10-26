"""DynamoDB store for timestamped readings. The caller never supplies the partition key."""

from __future__ import annotations

import time
from decimal import Decimal, ROUND_HALF_UP

import boto3
from boto3.dynamodb.conditions import Key

from renglo.logger import get_logger
from renglo.timeseries.keys import (
    ITEM_SOFT_LIMIT,
    READING_LIMIT,
    format_period,
    make_index,
    metric_index,
    month_of,
    query_prefix,
    year_of,
)


class TimeSeriesModel:
    def __init__(self, config=None, table=None):
        self.config = config or {}
        self.logger = get_logger()
        self.reading_limit = READING_LIMIT
        if table is not None:
            self.table = table
            return
        region = self.config.get("AWS_REGION", "us-east-1")
        table_name = self.config.get("DYNAMODB_TIMESERIES_TABLE")
        if not table_name:
            raise ValueError("DYNAMODB_TIMESERIES_TABLE configuration is required but not found")
        self.table = boto3.resource("dynamodb", region_name=region).Table(table_name)

    def put(
        self,
        portfolio,
        org,
        key,
        value,
        dimensions=None,
        timestamp=None,
        resolution="day",
        idempotency_key=None,
    ):
        dimensions = list(dimensions or [])
        moment = _reading_time(timestamp)
        period = format_period(moment, resolution)
        number = _decimal(value)
        index = make_index(portfolio, org, key)
        base = metric_index(period, dimensions, 0)
        segment = self._open_segment(index, base, period, dimensions)
        for _attempt in range(12):
            sort_key = metric_index(period, dimensions, segment)
            current = self._get(index, sort_key)
            if idempotency_key and _has_key(current, idempotency_key):
                return _stored(current, appended=False)
            if current and _is_full(current, self.reading_limit):
                segment += 1
                continue
            statistics = _apply_reading(
                (current or {}).get("statistics"), moment, number
            )
            version = int((current or {}).get("version") or 0)
            previous_count = int((current or {}).get("reading_count") or len((current or {}).get("readings") or []))
            previous_bytes = (current or {}).get("payload_bytes")
            if previous_bytes is None:
                previous_bytes = _payload_bytes((current or {}).get("readings") or [])
            reading_count = previous_count + 1
            payload_bytes = int(previous_bytes) + len(str([moment, number])) + 8
            keys = set((current or {}).get("idempotency_keys") or [])
            if idempotency_key:
                keys.add(str(idempotency_key))
            try:
                self._update(
                    index=index,
                    sort_key=sort_key,
                    reading=[moment, number],
                    resolution=resolution,
                    period=period,
                    dimensions=dimensions,
                    segment=segment,
                    statistics=statistics,
                    reading_count=reading_count,
                    payload_bytes=payload_bytes,
                    expected_version=version,
                    idempotency_key=str(idempotency_key) if idempotency_key else None,
                    idempotency_keys=keys if idempotency_key else None,
                )
            except Exception as exc:
                if not _is_conditional(exc):
                    raise
                continue
            stored = self._get(index, sort_key)
            return _stored(stored, appended=True)
        raise RuntimeError("TimeSeries put could not commit")

    def get_series(self, portfolio, org, key, date, dimensions=None):
        dimensions = list(dimensions or [])
        index = make_index(portfolio, org, key)
        base = metric_index(date, dimensions, 0)
        found = {}
        for item in self._query_prefix(index, base):
            if list(item.get("dimensions") or []) != dimensions or item.get("period") != date:
                continue
            found[item.get("metric_index")] = item
        exact = self._get(index, base)
        if exact:
            found[exact.get("metric_index")] = exact
        if not found:
            return None
        return _merge_documents(list(found.values()), base)

    def query_documents(self, portfolio, org, key, prefix):
        index = make_index(portfolio, org, key)
        return self._query_prefix(index, prefix)

    def documents_for_period(self, portfolio, org, key, period, dimensions_prefix=None):
        index = make_index(portfolio, org, key)
        prefix = query_prefix(period, dimensions_prefix)
        documents = self._query_prefix(index, prefix)
        if not dimensions_prefix and not (len(period) == 4 and period.isdigit()):
            exact = self._get(index, metric_index(period, [], 0))
            if exact:
                documents.append(exact)
        return documents

    def query_range(self, portfolio, org, key, start, end, dimensions=None):
        dimensions = list(dimensions or [])
        index = make_index(portfolio, org, key)
        low = metric_index(start, dimensions, 0)
        high = metric_index(end, dimensions, 0) + "#\uffff"
        return [
            item
            for item in self._query_between(index, low, high)
            if list(item.get("dimensions") or []) == dimensions
        ]

    def reconcile(self, portfolio, org, key, dimensions=None, period=None):
        dimensions = list(dimensions or [])
        month = month_of(period)
        year = year_of(month)
        monthly = self._write_rollup(
            portfolio,
            org,
            key,
            dimensions,
            source_resolution="day",
            source_prefix=f"{month}-",
            target_period=month,
            target_resolution="month",
        )
        yearly = self._write_rollup(
            portfolio,
            org,
            key,
            dimensions,
            source_resolution="month",
            source_prefix=f"{year}-",
            target_period=year,
            target_resolution="year",
        )
        return {"month": monthly, "year": yearly}

    def _write_rollup(
        self,
        portfolio,
        org,
        key,
        dimensions,
        source_resolution,
        source_prefix,
        target_period,
        target_resolution,
    ):
        index = make_index(portfolio, org, key)
        grouped = {}
        for item in self._query_prefix(index, source_prefix):
            if item.get("resolution") != source_resolution:
                continue
            if list(item.get("dimensions") or []) != dimensions:
                continue
            grouped.setdefault(item.get("period"), []).append(item)
        if not grouped:
            return None
        points = []
        consolidated = []
        for period in sorted(grouped):
            stats = _consolidate([doc.get("statistics") or {} for doc in grouped[period]])
            consolidated.append(stats)
            count = _decimal(stats["count"])
            average = _decimal(stats["sum"]) / count if count else Decimal(0)
            points.append([period, average])
        statistics = _consolidate(consolidated)
        sort_key = metric_index(target_period, dimensions, 0)
        item = {
            "index": index,
            "metric_index": sort_key,
            "resolution": target_resolution,
            "period": target_period,
            "dimensions": dimensions,
            "segment": 0,
            "readings": points,
            "statistics": statistics,
            "version": 1,
        }
        existing = self._get(index, sort_key)
        if existing and _same_document(existing, item):
            return existing
        self.table.put_item(Item=item)
        return self._get(index, sort_key)

    def _open_segment(self, index, base, period, dimensions):
        documents = [
            item
            for item in self._query_prefix(
                index,
                base,
                projection="segment, reading_count, payload_bytes, period, dimensions",
            )
            if item.get("period") == period and list(item.get("dimensions") or []) == list(dimensions)
        ]
        if not documents:
            return 0
        documents.sort(key=lambda item: int(item.get("segment") or 0))
        last = documents[-1]
        if _is_full(last, self.reading_limit):
            return int(last.get("segment") or 0) + 1
        return int(last.get("segment") or 0)

    def _update(
        self,
        index,
        sort_key,
        reading,
        resolution,
        period,
        dimensions,
        segment,
        statistics,
        reading_count,
        payload_bytes,
        expected_version,
        idempotency_key,
        idempotency_keys,
    ):
        names = {"#readings": "readings", "#segment": "segment"}
        values = {
            ":empty": [],
            ":reading": [reading],
            ":resolution": resolution,
            ":period": period,
            ":dimensions": dimensions,
            ":segment": int(segment),
            ":statistics": statistics,
            ":reading_count": int(reading_count),
            ":payload_bytes": int(payload_bytes),
            ":new_version": int(expected_version) + 1,
            ":expected_version": int(expected_version),
            ":cap": self.reading_limit,
        }
        update = (
            "SET #readings = list_append(if_not_exists(#readings, :empty), :reading), "
            "resolution = :resolution, period = :period, dimensions = :dimensions, "
            "#segment = :segment, statistics = :statistics, "
            "reading_count = :reading_count, payload_bytes = :payload_bytes, "
            "version = :new_version"
        )
        condition = (
            "(attribute_not_exists(version) OR version = :expected_version) "
            "AND (attribute_not_exists(#readings) OR size(#readings) < :cap)"
        )
        if idempotency_key:
            values[":idempotency_key"] = idempotency_key
            values[":idempotency_keys"] = set(idempotency_keys)
            update += ", idempotency_keys = :idempotency_keys"
            condition += (
                " AND (attribute_not_exists(idempotency_keys) "
                "OR NOT contains(idempotency_keys, :idempotency_key))"
            )
        self.table.update_item(
            Key={"index": index, "metric_index": sort_key},
            UpdateExpression=update,
            ConditionExpression=condition,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    def _get(self, index, sort_key):
        response = self.table.get_item(Key={"index": index, "metric_index": sort_key})
        return response.get("Item")

    def _query_prefix(self, index, prefix, projection=None):
        return self._collect(
            Key("index").eq(index) & Key("metric_index").begins_with(prefix),
            projection=projection,
        )

    def _query_between(self, index, low, high):
        return self._collect(
            Key("index").eq(index) & Key("metric_index").between(low, high)
        )

    def _collect(self, expression, projection=None):
        items = []
        start = None
        while True:
            kwargs = {"KeyConditionExpression": expression}
            if projection:
                projected, names = _projection_expression(projection)
                kwargs["ProjectionExpression"] = projected
                if names:
                    kwargs["ExpressionAttributeNames"] = names
            if start:
                kwargs["ExclusiveStartKey"] = start
            response = self.table.query(**kwargs)
            items.extend(response.get("Items") or [])
            start = response.get("LastEvaluatedKey")
            if not start:
                return items


def _projection_expression(projection):
    """Alias DynamoDB reserved words. `segment` cannot appear bare in a projection."""
    names = {}
    fields = []
    for field in [part.strip() for part in projection.split(",") if part.strip()]:
        if field.lower() == "segment":
            names["#segment"] = "segment"
            fields.append("#segment")
        else:
            fields.append(field)
    return ", ".join(fields), names


def _stored(item, appended):
    copied = dict(item or {})
    copied["appended"] = appended
    return copied


def _has_key(item, idempotency_key):
    if not item:
        return False
    return str(idempotency_key) in set(item.get("idempotency_keys") or [])


def _is_full(item, limit):
    count = item.get("reading_count")
    if count is None:
        count = len(item.get("readings") or [])
    if int(count) >= limit:
        return True
    payload = item.get("payload_bytes")
    if payload is None:
        payload = _payload_bytes(item.get("readings") or [])
    return int(payload) >= ITEM_SOFT_LIMIT


def _payload_bytes(readings):
    total = 0
    for reading in readings:
        total += len(str(reading)) + 8
    return total


def _reading_time(timestamp):
    """Unix seconds with millisecond precision, the resolution of a browser clock."""
    raw = Decimal(str(time.time() if timestamp is None else timestamp))
    return raw.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def _apply_reading(statistics, timestamp, value):
    moment = _decimal(timestamp)
    number = _decimal(value)
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
    count = _decimal(statistics.get("count") or 0) + 1
    total = _decimal(statistics.get("sum") or 0) + number
    low = _decimal(statistics.get("min"))
    high = _decimal(statistics.get("max"))
    first = _decimal(statistics.get("first"))
    last = _decimal(statistics.get("last"))
    first_timestamp = _decimal(statistics.get("first_timestamp") or moment)
    last_timestamp = _decimal(statistics.get("last_timestamp") or moment)
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


def _consolidate(stats_list):
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
        count += _decimal(statistics.get("count") or 0)
        total += _decimal(statistics.get("sum") or 0)
        candidate_min = _decimal(statistics.get("min"))
        candidate_max = _decimal(statistics.get("max"))
        low = candidate_min if low is None else min(low, candidate_min)
        high = candidate_max if high is None else max(high, candidate_max)
        stamp = statistics.get("first_timestamp")
        if stamp is not None and (first_timestamp is None or _decimal(stamp) < first_timestamp):
            first_timestamp = _decimal(stamp)
            first = _decimal(statistics.get("first"))
        stamp = statistics.get("last_timestamp")
        if stamp is not None and (last_timestamp is None or _decimal(stamp) >= last_timestamp):
            last_timestamp = _decimal(stamp)
            last = _decimal(statistics.get("last"))
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


def _merge_documents(documents, base):
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
        "statistics": _consolidate([item.get("statistics") or {} for item in documents]),
    }


def _same_document(existing, item):
    if existing.get("resolution") != item.get("resolution"):
        return False
    if not _stats_equal(existing.get("statistics") or {}, item.get("statistics") or {}):
        return False
    left = existing.get("readings") or []
    right = item.get("readings") or []
    if len(left) != len(right):
        return False
    for one, two in zip(left, right):
        if str(one[0]) != str(two[0]) or _decimal(one[1]) != _decimal(two[1]):
            return False
    return True


def _stats_equal(left, right):
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
    return all(_decimal(left.get(name) or 0) == _decimal(right.get(name) or 0) for name in fields)


def _decimal(value):
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _is_conditional(exc) -> bool:
    response = getattr(exc, "response", None) or {}
    code = (response.get("Error") or {}).get("Code")
    return code == "ConditionalCheckFailedException"


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
