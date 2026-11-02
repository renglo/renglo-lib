"""VectorModel interface.

Callers construct this class. CLOUD in config selects
renglo.vector.submodels.vector_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.vector.submodels.vector_model_aws import (
    log,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_DIM,
    RESERVED_META_KEYS,
    ATTRS_KEY,
    _INDEX_SAFE,
    _INDEX_DASHES,
    _cosine,
    org_index_name,
    vector_key,
    _now_iso,
    _encode_attrs,
    _decode_attrs,
    _stamp_metadata,
    _vector_from_data,
    _hit_from_row,
)

def _submodel(config):
    return load_submodel("renglo.vector.submodels", "vector_model", "VectorModel", config)


class VectorModel:
    """Persistence/backend for Amazon S3 Vectors (+ Bedrock Titan embed)."""
    def __init__(self, config: Optional[Dict[str, Any]]=None) -> None:
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config))

    @property
    def use_local(self) -> bool:
        return self._impl.use_local

    def index_name(self, portfolio: str, org: str) -> str:
        return self._impl.index_name(portfolio, org)

    def status(self, portfolio: str='', org: str='') -> Dict[str, Any]:
        return self._impl.status(portfolio, org)

    def _get_s3vectors(self):
        return self._impl._get_s3vectors()

    def _get_bedrock_runtime(self):
        return self._impl._get_bedrock_runtime()

    def embed_text(self, text: str) -> List[float]:
        return self._impl.embed_text(text)

    def _local_hash_embedding(self, text: str) -> List[float]:
        return self._impl._local_hash_embedding(text)

    def _local_path(self, index_name: str) -> Path:
        return self._impl._local_path(index_name)

    def _local_load(self, index_name: str) -> Dict[str, Any]:
        return self._impl._local_load(index_name)

    def _local_save(self, index_name: str, data: Dict[str, Any]) -> None:
        return self._impl._local_save(index_name, data)

    def ensure_index(self, portfolio: str, org: str, dimension: Optional[int]=None) -> Dict[str, Any]:
        return self._impl.ensure_index(portfolio, org, dimension)

    def put_vector(self, *, portfolio: str, org: str, entity_type: str, entity_id: str, vector: Sequence[float], attrs: Optional[Dict[str, Any]]=None) -> Dict[str, Any]:
        return self._impl.put_vector(portfolio=portfolio, org=org, entity_type=entity_type, entity_id=entity_id, vector=vector, attrs=attrs)

    def get_vector(self, *, portfolio: str, org: str, entity_type: str, entity_id: str) -> Dict[str, Any]:
        return self._impl.get_vector(portfolio=portfolio, org=org, entity_type=entity_type, entity_id=entity_id)

    def delete_vector(self, *, portfolio: str, org: str, entity_type: str, entity_id: str) -> Dict[str, Any]:
        return self._impl.delete_vector(portfolio=portfolio, org=org, entity_type=entity_type, entity_id=entity_id)

    def purge_index(self, *, portfolio: str, org: str) -> Dict[str, Any]:
        return self._impl.purge_index(portfolio=portfolio, org=org)

    def list_vectors(self, *, portfolio: str, org: str, entity_type: Optional[str]=None, max_results: int=100, next_token: Optional[str]=None) -> Dict[str, Any]:
        return self._impl.list_vectors(portfolio=portfolio, org=org, entity_type=entity_type, max_results=max_results, next_token=next_token)

    def query(self, *, portfolio: str, org: str, vector: Sequence[float], entity_type: Optional[str]=None, top_k: int=10, extra_filter: Optional[Dict[str, Any]]=None) -> List[Dict[str, Any]]:
        return self._impl.query(portfolio=portfolio, org=org, vector=vector, entity_type=entity_type, top_k=top_k, extra_filter=extra_filter)

    def _sanitize_extra_filter(self, extra_filter: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        return self._impl._sanitize_extra_filter(extra_filter)

    def _row_matches(self, meta: Dict[str, Any], portfolio: str, org: str, entity_type: str, extra_filter: Dict[str, Any]) -> bool:
        return self._impl._row_matches(meta, portfolio, org, entity_type, extra_filter)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
