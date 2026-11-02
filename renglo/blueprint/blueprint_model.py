"""BlueprintModel interface.

Callers construct this class. CLOUD in config selects
renglo.blueprint.submodels.blueprint_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

def _submodel(config):
    return load_submodel("renglo.blueprint.submodels", "blueprint_model", "BlueprintModel", config)


class BlueprintModel:
    def __init__(self, config=None, tid=False, ip=False, *, dynamodb_resource=None, region_name=None):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, tid, ip, dynamodb_resource=dynamodb_resource, region_name=region_name))

    def put_blueprint(self, data):
        return self._impl.put_blueprint(data)

    def get_blueprint(self, handle, name, v):
        return self._impl.get_blueprint(handle, name, v)

    def update_blueprint(self, data):
        return self._impl.update_blueprint(data)

    def delete_blueprint(self, handle, name, v):
        return self._impl.delete_blueprint(handle, name, v)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
