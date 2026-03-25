# search_index_service.py - Agnostic document indexer driven by blueprint schema

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Set

from renglo.logger import get_logger
from renglo.search.search_client import create_opensearch_client


class SearchIndexService:
    """
    Agnostic indexer: indexes documents based on blueprint schema.
    Only fields with searchable=true in the blueprint are indexed.
    Org isolation is enforced via org field.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, opensearch_client=None):
        self.config = config or {}
        self.logger = get_logger()
        self.endpoint = self.config.get('OPENSEARCH_ENDPOINT')
        self.index_name = self.config.get('OPENSEARCH_INDEX', 'renglo-documents')
        self.region = self.config.get('AWS_REGION', 'us-east-1')
        self.enabled = bool(self.endpoint)

        self.client = opensearch_client
        if self.client is None and self.enabled:
            self.client = create_opensearch_client(
                endpoint=self.endpoint,
                region=self.region,
            )
            if self.client is None:
                self.enabled = False

        self._bpc = None

    def is_enabled(self) -> bool:
        return self.enabled and self.client is not None

    def _get_blueprint_controller(self):
        """Lazy-load BlueprintController to avoid Flask import at module load."""
        if self._bpc is None and self.enabled:
            from renglo.blueprint.blueprint_controller import BlueprintController
            self._bpc = BlueprintController(config=self.config)
        return self._bpc

    def _get_searchable_fields(self, blueprint: dict) -> Set[str]:
        """Return field names with searchable=true in blueprint."""
        if not blueprint or blueprint.get('success') is False:
            return set()
        return {
            f['name'] for f in blueprint.get('fields', [])
            if f.get('searchable', False)
        }

    def _extract_indexable_fields(
        self,
        doc: dict,
        searchable_fields: Set[str],
    ) -> Dict[str, Any]:
        """
        Extract and flatten fields from doc for indexing.
        Handles doc.attributes (DynamoDB format) or flat doc.
        """
        attrs = doc.get('attributes', doc)
        if not isinstance(attrs, dict):
            return {}

        result = {}
        for key, value in attrs.items():
            if key not in searchable_fields:
                continue
            if value is None:
                continue
            if isinstance(value, Decimal):
                result[key] = int(value) if value % 1 == 0 else float(value)
            elif isinstance(value, (dict, list)):
                result[key] = self._stringify_for_search(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result

    def _stringify_for_search(self, value: Any) -> str:
        """Convert nested structures to searchable text."""
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value, default=str)
            except (TypeError, ValueError):
                return str(value)
        return str(value)

    def _build_index_document(
        self,
        doc: dict,
        portfolio: str,
        org: str,
        ring: str,
        searchable_fields: Set[str],
    ) -> Dict[str, Any]:
        """Build the document to index in OpenSearch."""
        indexable_attrs = self._extract_indexable_fields(doc, searchable_fields)

        doc_id = doc.get('_id', '')
        doc_index = f"{org}:{ring}:{doc_id}"

        search_text_parts = []
        for v in indexable_attrs.values():
            if isinstance(v, str):
                search_text_parts.append(v)
            elif v is not None:
                search_text_parts.append(self._stringify_for_search(v))

        index_doc = {
            'org': org,
            'datatype': ring,
            'portfolio': portfolio,
            'doc_id': doc_id,
            'doc_index': doc_index,
            'added': doc.get('added'),
            'modified': doc.get('modified'),
            'attributes': indexable_attrs,
            '_search_text': ' '.join(search_text_parts) if search_text_parts else '',
        }
        return index_doc

    def index_document(
        self,
        portfolio: str,
        org: str,
        ring: str,
        doc: dict,
    ) -> bool:
        """
        Index or update a document in OpenSearch.
        Extracts only searchable fields from blueprint.
        """
        if not self.is_enabled():
            return False

        try:
            bpc = self._get_blueprint_controller()
            blueprint = bpc.get_blueprint('irma', ring, 'last') if bpc else {}
            searchable_fields = self._get_searchable_fields(blueprint)
            if not searchable_fields:
                return True

            index_doc = self._build_index_document(
                doc, portfolio, org, ring, searchable_fields
            )
            doc_id = index_doc['doc_index']

            self.ensure_index_exists()

            self.client.index(
                index=self.index_name,
                id=doc_id,
                body=index_doc,
                refresh=('wait_for' if self.config.get('OPENSEARCH_REFRESH') else False),
            )
            self.logger.debug(f"Indexed document {doc_id}")
            return True
        except Exception as e:
            self.logger.error(f"Search index failed for {org}:{ring}:{doc.get('_id')}: {e}")
            return False

    def delete_document(
        self,
        portfolio: str,
        org: str,
        ring: str,
        doc_id: str,
    ) -> bool:
        """Remove document from OpenSearch index."""
        if not self.is_enabled():
            return False

        try:
            os_doc_id = f"{org}:{ring}:{doc_id}"
            self.client.delete(
                index=self.index_name,
                id=os_doc_id,
                refresh=(self.config.get('OPENSEARCH_REFRESH') or False),
            )
            self.logger.debug(f"Deleted from index: {os_doc_id}")
            return True
        except Exception as e:
            if e.__class__.__name__ in ('NotFoundError', 'NotFound'):
                return True
            self.logger.error(f"Search delete failed for {os_doc_id}: {e}")
            return False

    def ensure_index_exists(self) -> bool:
        """Create index with mapping if it does not exist."""
        if not self.is_enabled():
            return False

        try:
            if self.client.indices.exists(index=self.index_name):
                return True

            mapping = {
                "mappings": {
                    "properties": {
                        "org": {"type": "keyword"},
                        "datatype": {"type": "keyword"},
                        "portfolio": {"type": "keyword"},
                        "doc_id": {"type": "keyword"},
                        "doc_index": {"type": "keyword"},
                        "added": {"type": "date" if self._supports_date() else "keyword"},
                        "modified": {"type": "date" if self._supports_date() else "keyword"},
                        "attributes": {
                            "type": "object",
                            "dynamic": True,
                        },
                        "_search_text": {"type": "text"},
                    }
                }
            }
            self.client.indices.create(index=self.index_name, body=mapping)
            self.logger.info(f"Created OpenSearch index: {self.index_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create index {self.index_name}: {e}")
            return False

    def _supports_date(self) -> bool:
        """Check if index supports date type (OpenSearch vs Serverless)."""
        return True
