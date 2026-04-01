# search_controller.py - Search API with mandatory tenant isolation

from typing import Any, Dict, List, Optional

from renglo.search.search_index_service import SearchIndexService


class SearchController:
    """
    Search API controller. All searches require org - no cross-org results.
    
    
    Usage
        Option A – Restrict search to specific fields

        {
        "query": "Miami Jun 2nd",
        "datatypes": ["travels"],
        "search_fields": ["title"]
        }
        Only attributes.title is searched; _search_text is ignored.

        Option B – Boost fields while still searching all

        {
        "query": "Miami Jun 2nd",
        "datatypes": ["travels"],
        "boost_fields": {"title": 4}
        }
        Searches attributes.title (boosted) and _search_text.

        Option C – Restrict and boost

        {
        "query": "Miami Jun 2nd",
        "datatypes": ["travels"],
        "search_fields": ["title", "flights"],
        "boost_fields": {"title": 4}
        }
        Searches only title and flights, with title boosted.

        For the search_trip tool, add to init:

        "init": "{\"datatypes\":[\"travels\"],\"limit\":20,\"offset\":0,\"boost_fields\":{\"title\":4}}"
        If search_fields is set and a document lacks that attribute (e.g. x_attendants without title), that document will not match, which is expected when restricting by field.


    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.search_index = SearchIndexService(config=self.config)

    def search(
        self,
        org: str,
        query: str,
        datatypes: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        offset: int = 0,
        search_fields: Optional[List[str]] = None,
        boost_fields: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Full-text search with MANDATORY org filter.
        Never returns cross-org results.

        search_fields: If provided, search ONLY on these attributes (attributes.<field>).
            Ignores _search_text. Use when the caller knows which fields to search.
        boost_fields: Dict of field_name -> boost factor. Boosts attributes.<field> in ranking.
            E.g. {"title": 4} boosts title matches. Works with or without search_fields.
        """
        if not self.search_index.is_enabled():
            return {
                'success': False,
                'message': 'Search is not configured',
                'items': [],
                'total': 0,
            }

        if not org:
            return {
                'success': False,
                'message': 'org is required',
                'items': [],
                'total': 0,
            }

        try:
            must = [{'term': {'org': org}}]

            if datatypes:
                must.append({'terms': {'datatype': datatypes}})

            if filters:
                for field, value in filters.items():
                    if isinstance(value, list):
                        must.append({'terms': {f'attributes.{field}': value}})
                    else:
                        must.append({'term': {f'attributes.{field}': value}})

            bool_query = {'must': must}

            if query and query.strip():
                should_clauses = []
                if search_fields:
                    for field in search_fields:
                        if not field or not isinstance(field, str):
                            continue
                        boost = (boost_fields or {}).get(field, 1.0)
                        should_clauses.append({
                            'match': {
                                f'attributes.{field}': {
                                    'query': query,
                                    'operator': 'or',
                                    'boost': boost,
                                },
                            },
                        })
                else:
                    if boost_fields:
                        for field, boost in boost_fields.items():
                            if field and isinstance(field, str) and isinstance(boost, (int, float)):
                                should_clauses.append({
                                    'match': {
                                        f'attributes.{field}': {
                                            'query': query,
                                            'operator': 'or',
                                            'boost': float(boost),
                                        },
                                    },
                                })
                    should_clauses.append({
                        'match': {'_search_text': {'query': query, 'operator': 'or'}},
                    })
                if should_clauses:
                    bool_query['should'] = should_clauses
                    bool_query['minimum_should_match'] = 1

            search_body = {
                'query': {'bool': bool_query},
                'from': offset,
                'size': min(limit, 100),
                '_source': ['org', 'datatype', 'portfolio', 'doc_id', 'doc_index', 'attributes', 'added', 'modified'],
                'sort': [{'_score': 'desc'}] if (query and query.strip()) else [{'modified': 'desc'}],
            }

            response = self.search_index.client.search(
                index=self.search_index.index_name,
                body=search_body,
            )

            hits = response.get('hits', {})
            total = hits.get('total', {})
            if isinstance(total, dict):
                total_count = total.get('value', 0)
            else:
                total_count = total

            items = []
            for hit in hits.get('hits', []):
                doc = hit.get('_source', {})
                doc['_score'] = hit.get('_score')
                items.append(doc)

            return {
                'success': True,
                'items': items,
                'total': total_count,
                'query': query,
            }
        except Exception as e:
            self.search_index.logger.error(f"Search failed: {e}")
            return {
                'success': False,
                'message': str(e),
                'items': [],
                'total': 0,
            }
