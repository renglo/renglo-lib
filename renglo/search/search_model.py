"""SearchModel interface.

Callers construct this class. CLOUD in config selects
renglo.search.submodels.search_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.search.submodels.search_model_aws import (
    _dynamodb_resource,
    _describe_or_raise,
    _scan_projected,
)

def _submodel(config):
    return load_submodel("renglo.search.submodels", "search_model", "SearchModel", config)


class SearchModel:
    KEY_PK = "index"

    KEY_SK = "search_index"

    DOC_REF_TOKEN = "__docref__"

    def __init__(self, config: Optional[Dict[str, Any]]=None, *, region_name: Optional[str]=None, dynamodb_resource: Optional[Any]=None) -> None:
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, region_name=region_name, dynamodb_resource=dynamodb_resource))

    @staticmethod
    def _require_safe_part(name: str, value: str) -> None:
        return _submodel(None)._require_safe_part(name, value)

    @staticmethod
    def _now_iso() -> str:
        return _submodel(None)._now_iso()

    @staticmethod
    def _to_dynamo_value(value: Any) -> Any:
        return _submodel(None)._to_dynamo_value(value)

    @classmethod
    def make_pk(cls, portfolio: str, org: str, ring: str, token: str) -> str:
        return _submodel(None).make_pk(portfolio, org, ring, token)

    @classmethod
    def make_sk(cls, field: str, doc_id: str) -> str:
        return _submodel(None).make_sk(field, doc_id)

    @classmethod
    def make_doc_ref_pk(cls, portfolio: str, org: str, ring: str) -> str:
        return _submodel(None).make_doc_ref_pk(portfolio, org, ring)

    @classmethod
    def make_doc_ref_sk(cls, doc_id: str, field: str, token: str) -> str:
        return _submodel(None).make_doc_ref_sk(doc_id, field, token)

    @classmethod
    def _parse_sk(cls, sk: str) -> Dict[str, str]:
        return _submodel(None)._parse_sk(sk)

    def put_index_rows(self, rows: List[Dict[str, Any]]) -> int:
        return self._impl.put_index_rows(rows)

    def delete_index_rows(self, keys: List[Dict[str, str]]) -> int:
        return self._impl.delete_index_rows(keys)

    def delete_document_rows(self, portfolio: str, org: str, ring: str, doc_id: str) -> int:
        return self._impl.delete_document_rows(portfolio, org, ring, doc_id)

    def query_token(self, portfolio: str, org: str, ring: str, token: str, *, field_prefix: Optional[str]=None, limit: int=100, exclusive_start_key: Optional[Dict[str, Any]]=None) -> Dict[str, Any]:
        return self._impl.query_token(portfolio, org, ring, token, field_prefix=field_prefix, limit=limit, exclusive_start_key=exclusive_start_key)

    def describe_table(self, logical_name='Search'):
        return self._impl.describe_table(logical_name)

    def scan_projected(self, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
        return self._impl.scan_projected(fields)

    def query_doc_refs(self, portfolio: str, org: str, ring: str, doc_id: str) -> List[Dict[str, Any]]:
        return self._impl.query_doc_refs(portfolio, org, ring, doc_id)

    def get_item(self, pk: str, sk: str) -> Optional[Dict[str, Any]]:
        return self._impl.get_item(pk, sk)

    def delete_items(self, items: Iterable[Dict[str, Any]]) -> int:
        return self._impl.delete_items(items)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
