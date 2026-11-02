"""SessionModel interface.

Callers construct this class. CLOUD in config selects
renglo.session.submodels.session_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.session.submodels.session_model_aws import (
    DecimalEncoder,
)

def _submodel(config):
    return load_submodel("renglo.session.submodels", "session_model", "SessionModel", config)


class SessionModel:
    def __init__(self, config=None, tid=False, ip=False):
        """Initialize SessionModel with configuration.

    Args:
        config (dict): Configuration dictionary containing SESSION_TABLE
        tid: Transaction ID (optional)
        ip: IP address (optional)
    """
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, tid, ip))

    def sanitize(self, obj):
        return self._impl.sanitize(obj)

    def query_session(self, index, entity_index_prefix, limit=50, lastkey=None, sort='asc'):
        return self._impl.query_session(index, entity_index_prefix, limit, lastkey, sort)

    def list_session(self, index, entity_index, limit=50, lastkey=None, sort='asc'):
        return self._impl.list_session(index, entity_index, limit, lastkey, sort)

    def get_session(self, index, entity_index, message_id):
        return self._impl.get_session(index, entity_index, message_id)

    def create_session(self, data):
        return self._impl.create_session(data)

    def _for_append(self, obj):
        """Drop None and turn floats into strings. DynamoDB rejects both."""
        return self._impl._for_append(obj)

    def append_events(self, index, entity_index, documents):
        """Append documents onto a turn without rewriting the rest of the item.

    A full put loses events another writer added after this process read
    the turn. list_append keeps both.
    """
        return self._impl.append_events(index, entity_index, documents)

    def update_session(self, data):
        return self._impl.update_session(data)

    def delete_session(self, **data):
        return self._impl.delete_session(**data)

    def put_continuation_lock(self, index, entity_index, holder, now, lease):
        return self._impl.put_continuation_lock(index, entity_index, holder, now, lease)

    def delete_continuation_lock(self, index, entity_index, holder):
        return self._impl.delete_continuation_lock(index, entity_index, holder)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
