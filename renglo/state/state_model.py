"""StateModel interface.

Callers construct this class. CLOUD in config selects
renglo.state.submodels.state_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

def _submodel(config):
    return load_submodel("renglo.state.submodels", "state_model", "StateModel", config)


class StateModel:
    def __init__(self, config=None, tid=False, ip=False):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, tid, ip))

    def get_state(self, name, v):
        return self._impl.get_state(name, v)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
