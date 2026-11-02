"""RagModel interface.

Callers construct this class. CLOUD in config selects
renglo.rag.submodels.rag_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

def _submodel(config):
    return load_submodel("renglo.rag.submodels", "rag_model", "RagModel", config)


class RagModel:
    """Bedrock Agent runtime and control-plane calls."""
    def __init__(self, config: Optional[Dict[str, Any]]=None, *, region_name: Optional[str]=None, bedrock_client: Optional[Any]=None):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, region_name=region_name, bedrock_client=bedrock_client))

    def retrieve(self, **params):
        return self._impl.retrieve(**params)

    def retrieve_and_generate(self, **params):
        return self._impl.retrieve_and_generate(**params)

    def start_ingestion_job(self, **params):
        return self._impl.start_ingestion_job(**params)

    def get_ingestion_job(self, **params):
        return self._impl.get_ingestion_job(**params)

    def _agent_client(self):
        return self._impl._agent_client()

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
