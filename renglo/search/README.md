# renglo.search - OpenSearch Integration

Document indexing and full-text search using AWS OpenSearch Service.

## Configuration

Add to `system/env_config.py` or environment variables:

```python
OPENSEARCH_ENDPOINT = 'https://your-domain.us-east-1.es.amazonaws.com'
OPENSEARCH_INDEX = 'renglo-documents'
OPENSEARCH_REFRESH = False  # Set True for immediate visibility (slower writes)
```

If `OPENSEARCH_ENDPOINT` is not set, search indexing is disabled and the app runs normally.

## Blueprint: searchable Fields

Add `"searchable": true` to blueprint fields to index them:

```json
{
  "name": "title",
  "type": "string",
  "searchable": true,
  ...
}
```

Only fields with `searchable: true` are indexed. Default is `false`.

## Indexing

Documents are indexed automatically on:
- **POST** `/_data/<portfolio>/<org>/<ring>` (create)
- **PUT** `/_data/<portfolio>/<org>/<ring>/<id>` (update)
- **DELETE** `/_data/<portfolio>/<org>/<ring>/<id>` (delete)

## Search API

**POST** `/_search/<portfolio>/<org>`

```json
{
  "query": "search terms",
  "datatypes": ["noma_travels", "noma_rel"],
  "filters": {"status": "confirmed"},
  "limit": 20,
  "offset": 0,
  "search_fields": ["title"],
  "boost_fields": {"title": 4}
}
```

- `org` (mandatory, from URL)
- Results are always scoped to the tenant
- `search_fields`: Optional. If provided, search ONLY on these attributes (`attributes.<field>`). Ignores `_search_text`. Use when the caller knows which fields to search.
- `boost_fields`: Optional. Dict of `field_name` -> boost factor. Boosts `attributes.<field>` in ranking. E.g. `{"title": 4}` boosts title matches. Works with or without `search_fields`.

## Tenant Isolation

All documents include `org`. Search queries always filter by `org` - no cross-org results.
