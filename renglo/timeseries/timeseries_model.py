"""DynamoDB store for timestamped readings. The caller never supplies the partition key."""

from __future__ import annotations

from decimal import Decimal

from renglo.config_names import setting, table_name
from renglo.document_stores.select import PreconditionFailed, open_store
from renglo.logger import get_logger
from renglo.timeseries.rollup import (
    apply_reading as _apply_reading,
    consolidate as _consolidate,
    decimal as _decimal,
    has_key as _has_key,
    is_full as _is_full,
    merge_documents as _merge_documents,
    payload_bytes as _payload_bytes,
    plain,
    public_statistics,
    reading_time as _reading_time,
    same_document as _same_document,
    stored as _stored,
)
from renglo.timeseries.keys import (
    READING_LIMIT,
    format_period,
    make_index,
    metric_index,
    month_of,
    query_prefix,
    year_of,
)


class TimeSeriesModel:
    def __init__(self, config=None, table=None, store=None):
        self.config = config or {}
        self.logger = get_logger()
        self.reading_limit = READING_LIMIT
        region = setting(self.config, "REGION", "us-east-1")
        resolved = None if table is not None else table_name(self.config, "TIMESERIES_TABLE")
        if table is None and not resolved:
            raise ValueError(
                "TIMESERIES_TABLE is not set and no environment name was found (WL_NAME or renglo.yaml name)"
            )
        self.store = store or open_store(
            self.config,
            "timeseries",
            resolved,
            "index",
            "metric_index",
            table=table,
            region_name=region,
            use_profile=False,
        )

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
        self.store.put(item)
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
        self.store.append_checked(
            index,
            sort_key,
            "readings",
            reading,
            sets={
                "resolution": resolution,
                "period": period,
                "dimensions": dimensions,
                "segment": int(segment),
                "statistics": statistics,
                "reading_count": int(reading_count),
                "payload_bytes": int(payload_bytes),
            },
            expected_version=expected_version,
            cap=self.reading_limit,
            idempotency_key=idempotency_key,
            idempotency_keys=idempotency_keys,
        )

    def _get(self, index, sort_key):
        return self.store.get(index, sort_key)

    def _query_prefix(self, index, prefix, projection=None):
        fields = [part.strip() for part in projection.split(",") if part.strip()] if projection else None
        return self._collect(index, prefix=prefix, fields=fields)

    def _query_between(self, index, low, high):
        return self._collect(index, low=low, high=high)

    def _collect(self, index, prefix=None, low=None, high=None, fields=None):
        items = []
        start = None
        while True:
            page, start = self.store.query(
                index,
                prefix=prefix,
                low=low,
                high=high,
                fields=fields,
                continuation=start,
            )
            items.extend(page or [])
            if not start:
                return items


def _is_conditional(exc) -> bool:
    if isinstance(exc, PreconditionFailed):
        return True
    response = getattr(exc, "response", None) or {}
    code = (response.get("Error") or {}).get("Code")
    return code == "ConditionalCheckFailedException"
