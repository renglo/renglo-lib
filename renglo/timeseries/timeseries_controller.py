"""Caller-facing TimeSeries API. Portfolio and org come from the caller."""

from __future__ import annotations

from renglo.auth.authorize import authorize
from renglo.timeseries.keys import RESOLUTIONS, metric_index, month_of, year_of
from renglo.timeseries.timeseries_model import (
    TimeSeriesModel,
    _merge_documents,
    plain,
    public_statistics,
)


class TimeSeriesController:
    def __init__(self, config=None, model=None, auth=None):
        self.config = config or {}
        self._model = model
        self._auth = auth

    @property
    def AUC(self):
        if self._auth is None:
            from renglo.auth.auth_controller import AuthController

            self._auth = AuthController(config=self.config)
        return self._auth

    @property
    def TSM(self):
        if self._model is None:
            self._model = TimeSeriesModel(config=self.config)
        return self._model

    @authorize()
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
        invalid = self._validate_target("put", portfolio, org, key)
        if invalid:
            return invalid
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return _fail("put", "value must be a number", 400)
        if dimensions is None:
            dimensions = []
        if not isinstance(dimensions, (list, tuple)) or any(isinstance(item, (list, dict)) for item in dimensions):
            return _fail("put", "dimensions must be a list", 400)
        if resolution not in RESOLUTIONS:
            return _fail("put", "resolution must be year, month, day, hour, minute, or second", 400)
        if timestamp is not None and isinstance(timestamp, bool):
            return _fail("put", "timestamp must be a number", 400)
        if timestamp is not None and not isinstance(timestamp, (int, float)):
            return _fail("put", "timestamp must be a number", 400)
        try:
            stored = self.TSM.put(
                portfolio,
                org,
                key,
                value,
                dimensions=list(dimensions),
                timestamp=timestamp,
                resolution=resolution,
                idempotency_key=idempotency_key,
            )
        except ValueError as exc:
            return _fail("put", str(exc), 400)
        return _ok("put", _public_document(stored))

    @authorize()
    def get(self, portfolio, org, key, date, dimensions=None):
        invalid = self._validate_target("get", portfolio, org, key)
        if invalid:
            return invalid
        if not date:
            return _fail("get", "date is required", 400)
        dimensions = list(dimensions or [])
        try:
            document = self.TSM.get_series(portfolio, org, key, date, dimensions)
        except ValueError as exc:
            return _fail("get", str(exc), 400)
        if document is None:
            return _fail("get", "series not found", 404)
        return _ok("get", _public_document(document))

    @authorize()
    def query(
        self,
        portfolio,
        org,
        key,
        period=None,
        dimensions_prefix=None,
        start=None,
        end=None,
        resolution=None,
        dimensions=None,
    ):
        invalid = self._validate_target("query", portfolio, org, key)
        if invalid:
            return invalid
        dimensions = list(dimensions or [])
        try:
            if start or end:
                if not start or not end:
                    return _fail("query", "start and end are required together", 400)
                documents = self.TSM.query_range(
                    portfolio, org, key, start, end, dimensions
                )
                if resolution:
                    documents = [
                        item for item in documents if item.get("resolution") == resolution
                    ]
            elif period:
                documents = self.TSM.documents_for_period(
                    portfolio, org, key, period, dimensions_prefix
                )
            else:
                return _fail("query", "period or start and end are required", 400)
        except ValueError as exc:
            return _fail("query", str(exc), 400)
        series = _group_series(documents)
        return _ok("query", {"series": [_public_document(item) for item in series]})

    def reconcile(self, portfolio, org, key, dimensions=None, period=None):
        invalid = self._validate_target("reconcile", portfolio, org, key)
        if invalid:
            return invalid
        if not period:
            return _fail("reconcile", "period is required", 400)
        try:
            month_of(period)
            year_of(period)
            written = self.TSM.reconcile(
                portfolio, org, key, dimensions=list(dimensions or []), period=period
            )
        except ValueError as exc:
            return _fail("reconcile", str(exc), 400)
        return _ok(
            "reconcile",
            {
                "month": _public_document(written.get("month")),
                "year": _public_document(written.get("year")),
            },
        )

    def _validate_target(self, action, portfolio, org, key):
        if not str(portfolio or "").strip() or not str(org or "").strip():
            return _fail(action, "portfolio and org are required", 400)
        if not str(key or "").strip():
            return _fail(action, "key is required", 400)
        if "#" in str(portfolio) or "#" in str(org) or "#" in str(key):
            return _fail(action, "portfolio, org, and key cannot contain #", 400)
        return None


def _group_series(documents):
    grouped = {}
    for item in documents:
        if not isinstance(item, dict) or not item.get("period"):
            continue
        identity = (item.get("period"), tuple(item.get("dimensions") or []))
        grouped.setdefault(identity, []).append(item)
    series = []
    for (period, dimensions), items in grouped.items():
        base = metric_index(period, list(dimensions), 0)
        series.append(_merge_documents(items, base))
    series.sort(key=lambda item: item.get("metric_index") or "")
    return series


def _public_document(document):
    if not document:
        return None
    dimensions = list(document.get("dimensions") or [])
    period = document.get("period")
    statistics = public_statistics(document.get("statistics") or {})
    shown_index = document.get("metric_index")
    if period is not None:
        shown_index = metric_index(period, dimensions, 0)
    return {
        "index": document.get("index"),
        "metric_index": shown_index,
        "resolution": document.get("resolution"),
        "period": document.get("period"),
        "dimensions": plain(document.get("dimensions") or []),
        "readings": plain(document.get("readings") or []),
        "statistics": statistics,
    }


def _ok(action, output):
    return {"success": True, "action": action, "output": output}


def _fail(action, error, status):
    return {"success": False, "action": action, "error": error, "status": status}
