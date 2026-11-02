"""ConnectionModel interface.

Callers construct this class. CLOUD in config selects
renglo.chat.submodels.connection_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.chat.submodels.connection_model_aws import (
    DecimalEncoder,
)

def _submodel(config):
    return load_submodel("renglo.chat.submodels", "connection_model", "ConnectionModel", config)


class ConnectionModel:
    """Post a payload to one live socket. The cloud endpoint stays in this class."""
    def __init__(self, endpoint_url=None, config=None):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(endpoint_url, config))

    def client(self):
        return self._impl.client()

    def post(self, connection_id, payload):
        """Send payload. Dicts are JSON-encoded. Strings and bytes are sent as given."""
        return self._impl.post(connection_id, payload)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
