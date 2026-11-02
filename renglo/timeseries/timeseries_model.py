"""TimeSeriesModel interface.

Callers construct this class. CLOUD in config selects
renglo.timeseries.submodels.timeseries_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.timeseries.submodels.timeseries_model_aws import (
    _projection_expression,
    _stored,
    _has_key,
    _is_full,
    _payload_bytes,
    _reading_time,
    _apply_reading,
    _consolidate,
    _merge_documents,
    _same_document,
    _stats_equal,
    _decimal,
    _is_conditional,
    plain,
    public_statistics,
)

def _submodel(config):
    return load_submodel("renglo.timeseries.submodels", "timeseries_model", "TimeSeriesModel", config)


class TimeSeriesModel:
    def __init__(self, config=None, table=None):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, table))

    def put(self, portfolio, org, key, value, dimensions=None, timestamp=None, resolution='day', idempotency_key=None):
        return self._impl.put(portfolio, org, key, value, dimensions, timestamp, resolution, idempotency_key)

    def get_series(self, portfolio, org, key, date, dimensions=None):
        return self._impl.get_series(portfolio, org, key, date, dimensions)

    def query_documents(self, portfolio, org, key, prefix):
        return self._impl.query_documents(portfolio, org, key, prefix)

    def documents_for_period(self, portfolio, org, key, period, dimensions_prefix=None):
        return self._impl.documents_for_period(portfolio, org, key, period, dimensions_prefix)

    def query_range(self, portfolio, org, key, start, end, dimensions=None):
        return self._impl.query_range(portfolio, org, key, start, end, dimensions)

    def reconcile(self, portfolio, org, key, dimensions=None, period=None):
        return self._impl.reconcile(portfolio, org, key, dimensions, period)

    def _write_rollup(self, portfolio, org, key, dimensions, source_resolution, source_prefix, target_period, target_resolution):
        return self._impl._write_rollup(portfolio, org, key, dimensions, source_resolution, source_prefix, target_period, target_resolution)

    def _open_segment(self, index, base, period, dimensions):
        return self._impl._open_segment(index, base, period, dimensions)

    def _update(self, index, sort_key, reading, resolution, period, dimensions, segment, statistics, reading_count, payload_bytes, expected_version, idempotency_key, idempotency_keys):
        return self._impl._update(index, sort_key, reading, resolution, period, dimensions, segment, statistics, reading_count, payload_bytes, expected_version, idempotency_key, idempotency_keys)

    def _get(self, index, sort_key):
        return self._impl._get(index, sort_key)

    def _query_prefix(self, index, prefix, projection=None):
        return self._impl._query_prefix(index, prefix, projection)

    def _query_between(self, index, low, high):
        return self._impl._query_between(index, low, high)

    def _collect(self, expression, projection=None):
        return self._impl._collect(expression, projection)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
