"""PeerModel interface.

Callers construct this class. CLOUD in config selects
renglo.schd.submodels.peer_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

def _submodel(config):
    return load_submodel("renglo.schd.submodels", "peer_model", "PeerModel", config)


class PeerModel:
    """Lambda, ECS, EC2 network lookup, and SSM reads for a peer."""
    def __init__(self, config=None):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls())

    def invoke(self, function_name, payload, invocation_type, region):
        return self._impl.invoke(function_name, payload, invocation_type, region)

    def function_configuration(self, function_name, region):
        return self._impl.function_configuration(function_name, region)

    def update_environment(self, function_name, region, variables):
        return self._impl.update_environment(function_name, region, variables)

    def run_task(self, region, **params):
        return self._impl.run_task(region, **params)

    def default_network(self, region):
        return self._impl.default_network(region)

    def parameter_json(self, path, region):
        return self._impl.parameter_json(path, region)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
