#data_model.py

from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from renglo.config_names import setting
from renglo.document_stores.select import StoreError, open_store


class DataModel:

    def __init__(self, config=None, tid=False, ip=False, region_name=None, dynamodb_resource=None, bind_auth=True, store=None):
        self.config = config or {}

        if bind_auth:
            from renglo.auth.auth_controller import AuthController
            from renglo.blueprint.blueprint_controller import BlueprintController

            self.AUC = AuthController(config=self.config, tid=tid, ip=ip)
            self.BPC = BlueprintController(config=self.config, tid=tid, ip=ip)
        else:
            self.AUC = None
            self.BPC = None

        # Callers that omit region_name keep the historical us-east-1 client.
        resolved_region = region_name or "us-east-1"
        self.region = resolved_region
        table_name = setting(self.config, "DATA_TABLE")
        if not table_name:
            raise ValueError("DATA_TABLE configuration is required but not found")
        self.table_name = table_name
        self.store = store or open_store(
            self.config,
            "data",
            table_name,
            "portfolio_index",
            "doc_index",
            resource=dynamodb_resource,
            region_name=resolved_region,
            use_profile=True,
        )

    @property
    def dynamodb(self):
        return self.store.ensure_resource()

    def post_a_b(self, portfolio, org, ring, item):
        item['portfolio_index'] = 'irn:data:'+portfolio
        item['doc_index'] = org+':'+ring+':'+item['_id']
        try:
            self.store.put(item)
            return {"message": "Item created", "document": item}
        except StoreError as e:
            return {"error": e.message}

    def _page(self, portfolio, org, ring, prefix, limit, lastkey):
        portfolio_index = f'irn:data:{portfolio}'
        continuation = None
        if lastkey:
            continuation = {
                'doc_index': f'{org}:{ring}:{lastkey}',
                'portfolio_index': portfolio_index,
            }
        items, endkey = self.store.query(
            portfolio_index,
            prefix=prefix,
            limit=limit,
            continuation=continuation,
        )
        result = {'items': items}
        if isinstance(endkey, dict) and endkey.get("doc_index"):
            result["last_id"] = endkey["doc_index"].split(":")[-1]
        elif endkey and items:
            result["last_id"] = items[-1]["doc_index"].split(":")[-1]
        else:
            result["last_id"] = None
        return result

    def get_a_b(self, portfolio, org, ring, limit=10000, lastkey=None):
        prefix_doc_index = f'{org}:{ring}:'
        try:
            return self._page(portfolio, org, ring, prefix_doc_index, limit, lastkey)
        except StoreError as e:
            return {"error": str(e)}

    def get_a_b_batch(self, portfolio, org, ring, limit=10000, lastkey=None):
        prefix_doc_index = f'{org}:{ring}'
        try:
            return self._page(portfolio, org, ring, prefix_doc_index, limit, lastkey)
        except StoreError as e:
            return {"error": str(e)}

    def get_a_b_c(self, portfolio, org, ring, idx):
        portfolio_index = 'irn:data:'+portfolio
        doc_index = org+':'+ring+':'+idx
        try:
            item = self.store.get(portfolio_index, doc_index)
            if item:
                return item
            return {"error": "Document not found"}
        except StoreError as e:
            return {"error": e.message}

    def put_a_b_c(self, portfolio, org, ring, idx, item):
        portfolio_index = 'irn:data:'+portfolio
        doc_index = org+':'+ring+':'+item['_id']
        new_item = item.get('attributes', {})

        if not all([portfolio_index, doc_index, new_item]):
            return {'error': 'Missing portfolio_index, doc_index, or items'}

        try:
            current = self.store.get(portfolio_index, doc_index)
            if not current:
                return {'error': 'Item not found'}

            current_items = current.get('attributes', {})
            changes = {}
            for key, new_value in new_item.items():
                if new_value != current_items.get(key):
                    changes[key] = new_value

            if not changes:
                return {'message': 'No changes detected'}

            timestamp = datetime.now().isoformat()
            attributes = self.store.merge_map(
                portfolio_index,
                doc_index,
                "attributes",
                changes,
                also={"modified": timestamp},
            )
            return {'message': 'Item updated', 'response': str(attributes)}
        except StoreError as e:
            return {'error': str(e)}

    def delete_a_b_c(self, portfolio, org, ring, idx):
        portfolio_index = 'irn:data:'+portfolio
        doc_index = org+':'+ring+':'+idx
        try:
            response = self.store.delete(portfolio_index, doc_index)
            return {'message':'Item deleted', 'response':str(response)}
        except StoreError as e:
            return {'error': str(e)}

    def get_a_b_beginswith(self, query):
        portfolio_index = f'irn:data:{query["portfolio"]}'
        path_index_head = f'irn:h_index:{query["org"]}:{query["ring"]}'
        if 'value' not in query or not query['value']:
            path_index_tail = ''
        else:
            path_index_tail = f':{query["value"]}'
        path_index = path_index_head + path_index_tail

        try:
            print(f'portfolio_index >>> {portfolio_index}')
            print(f'path_index >>> {path_index}')
            items, endkey = self.store.query(
                portfolio_index,
                path="path_index",
                prefix=path_index,
                limit=query['limit'],
                continuation=query['lastkey'] or None,
                descending=query['sort'] == 'asc',
                index="path_index",
            )
            return {
                'items': items,
                'lastkey': endkey,
            }
        except StoreError as e:
            return {"error": str(e)}

    def get_a_b_greaterthan(self, portfolio_index, path_index):
        return {'message': 'This feature is not implemented yet'}

    def get_a_b_lessthan(self, portfolio_index, path_index):
        return {'message': 'This feature is not implemented yet'}

    def get_a_b_equalto(self, portfolio_index, path_index):
        return {'message': 'This feature is not implemented yet'}

    def describe_table(self, logical_name="Ring data"):
        self.store.describe(
            logical_name,
            hint="Use --region, --env, --ring-table, or --graph-table.",
        )

    def scan_projected(self, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
        yield from self.store.scan(fields)

    def query_path_prefix_page(
        self,
        portfolio: str,
        org: str,
        ring: str,
        limit: int,
        last_evaluated_key: Optional[Dict] = None,
    ) -> Tuple[List[Dict], Optional[Dict]]:
        portfolio_index = f"irn:data:{portfolio}"
        path_index_prefix = f"irn:h_index:{org}:{ring}"
        return self.store.query(
            portfolio_index,
            path="path_index",
            prefix=path_index_prefix,
            limit=limit,
            continuation=last_evaluated_key,
            index="path_index",
        )

    def delete_document(self, portfolio_index, doc_index):
        self.store.delete(portfolio_index, doc_index)

    def batch_get(self, keys: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.store.batch_get(keys)
