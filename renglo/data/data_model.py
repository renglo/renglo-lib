"""DataModel interface.

Callers construct this class. CLOUD in config selects
renglo.data.submodels.data_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.data.submodels.data_model_aws import (
    _dynamodb_resource,
    _describe_or_raise,
    _scan_projected,
)

def _submodel(config):
    return load_submodel("renglo.data.submodels", "data_model", "DataModel", config)


class DataModel:
    def __init__(self, config=None, tid=False, ip=False, region_name=None, dynamodb_resource=None, bind_auth=True):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, tid, ip, region_name, dynamodb_resource, bind_auth))

    def post_a_b(self, portfolio, org, ring, item):
        return self._impl.post_a_b(portfolio, org, ring, item)

    def get_a_b(self, portfolio, org, ring, limit=10000, lastkey=None):
        return self._impl.get_a_b(portfolio, org, ring, limit, lastkey)

    def get_a_b_batch(self, portfolio, org, ring, limit=10000, lastkey=None):
        return self._impl.get_a_b_batch(portfolio, org, ring, limit, lastkey)

    def get_a_b_c(self, portfolio, org, ring, idx):
        return self._impl.get_a_b_c(portfolio, org, ring, idx)

    def put_a_b_c(self, portfolio, org, ring, idx, item):
        return self._impl.put_a_b_c(portfolio, org, ring, idx, item)

    def delete_a_b_c(self, portfolio, org, ring, idx):
        return self._impl.delete_a_b_c(portfolio, org, ring, idx)

    def get_a_b_beginswith(self, query):
        """Incoming object instance  
        {
        'portfolio':<portfolio_id>,
        'org':<org_id>,
        'ring':<ring_id>,
        'operator':'begins_with',
        'value':<value>,
        'limit':<page_limit>,
        'lastkey':<page_lastkey>,
        'sort': <asc|desc>
        }
    """
        return self._impl.get_a_b_beginswith(query)

    def get_a_b_greaterthan(self, portfolio_index, path_index):
        return self._impl.get_a_b_greaterthan(portfolio_index, path_index)

    def get_a_b_lessthan(self, portfolio_index, path_index):
        return self._impl.get_a_b_lessthan(portfolio_index, path_index)

    def get_a_b_equalto(self, portfolio_index, path_index):
        return self._impl.get_a_b_equalto(portfolio_index, path_index)

    def describe_table(self, logical_name='Ring data'):
        return self._impl.describe_table(logical_name)

    def scan_projected(self, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
        return self._impl.scan_projected(fields)

    def query_path_prefix_page(self, portfolio: str, org: str, ring: str, limit: int, last_evaluated_key: Optional[Dict]=None) -> Tuple[List[Dict], Optional[Dict]]:
        return self._impl.query_path_prefix_page(portfolio, org, ring, limit, last_evaluated_key)

    def delete_document(self, portfolio_index, doc_index):
        return self._impl.delete_document(portfolio_index, doc_index)

    def batch_get(self, keys: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self._impl.batch_get(keys)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
