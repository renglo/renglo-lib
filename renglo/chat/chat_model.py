"""ChatModel interface.

Callers construct this class. CLOUD in config selects
renglo.chat.submodels.chat_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.chat.submodels.chat_model_aws import (
    DecimalEncoder,
)

def _submodel(config):
    return load_submodel("renglo.chat.submodels", "chat_model", "ChatModel", config)


class ChatModel:
    def __init__(self, config=None, tid=False, ip=False):
        """Initialize ChatModel with configuration.

    Args:
        config (dict): Configuration dictionary containing CHAT_TABLE
        tid: Transaction ID (optional)
        ip: IP address (optional)
    """
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, tid, ip))

    def sanitize(self, obj):
        return self._impl.sanitize(obj)

    def query_chat(self, index, entity_index_prefix, limit=50, lastkey=None, sort='asc'):
        return self._impl.query_chat(index, entity_index_prefix, limit, lastkey, sort)

    def list_chat(self, index, entity_index, limit=50, lastkey=None, sort='asc'):
        return self._impl.list_chat(index, entity_index, limit, lastkey, sort)

    def get_chat(self, index, entity_index, message_id):
        return self._impl.get_chat(index, entity_index, message_id)

    def create_chat(self, data):
        return self._impl.create_chat(data)

    def update_chat(self, data):
        return self._impl.update_chat(data)

    def delete_chat(self, **data):
        return self._impl.delete_chat(**data)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
