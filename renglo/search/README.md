# Renglo Search (V1)

Renglo Search V1 is a DynamoDB-native reverse index for structured Blueprint documents.

It is designed to:

- index tokens from document fields
- execute org- and portfolio-scoped token search
- support ring-aware and cross-ring retrieval
- return ranked candidate document ids for hydration from canonical storage

It is **not** a canonical data store.

## Module Structure

- `search_controller.py`: business logic (tokenization, indexing orchestration, search planning, ranking)
- `search_index_service.py`: blueprint-driven indexing plan resolver (which fields are searchable and their optional weights)
- `search_model.py`: DynamoDB read/write/query layer
- `search_engine_v_1_technical_spec.md`: architecture-level technical spec
- `search_engine_v_1_implementation_spec.md`: implementation-oriented spec

## Configuration

Required:

- `DYNAMODB_SEARCH_TABLE`: DynamoDB table name for search index

Optional:

- `AWS_REGION`: defaults to `us-east-1`
- `SEARCH_DEFAULT_RINGS`: default rings used by `search()` when request does not provide `rings` or `datatypes`

## DynamoDB Table Contract

Table key schema:

- partition key: `index`
- sort key: `search_index`

Main index rows (`entry_type = token_match`):

- `index = <portfolio>#<org>#<ring>#<token>`
- `search_index = <field>#<doc_id>`

Internal document reference rows (`entry_type = doc_ref`) are also stored in the same table:

- `index = <portfolio>#<org>#<ring>#__docref__`
- `search_index = <doc_id>#<field>#<token>`

These `doc_ref` rows are used to perform deterministic delete/reindex without table scans.

## Public API

`SearchController` is the public integration point used by other Renglo components.
It internally uses `SearchIndexService` to resolve searchable fields from Blueprints,
so callers do not need to provide field lists.

### Blueprint search field rule (canonical)

Use exactly one field property: `search` (integer).

- `search: 0` -> field is not indexed
- `search: N` where `N >= 1` -> field is indexed with weight `N`
- missing `search` -> treated as `0` (not indexed)

Example:

```json
{
  "name": "title",
  "type": "string",
  "search": 4
}
```

Optional field mode:

- `search_mode` defaults to `text`
- set `search_mode: "exact"` to index the field as a single exact token (no text tokenization)

Example:

```json
{
  "name": "talent_id",
  "type": "string",
  "search": 1,
  "search_mode": "exact"
}
```

### Index document

```python
from renglo.search.search_controller import SearchController

config = {
    "DYNAMODB_SEARCH_TABLE": "productora_search",
    "AWS_REGION": "us-east-1",
}

shc = SearchController(config=config)
result = shc.index_document(
    portfolio="p1",
    org="o1",
    ring="reservation",
    doc={
        "_id": "reservation_123",
        "attributes": {
            "title": "Hilton Cancun Reservation",
            "notes": "VIP guest",
        },
    },
)
```

### Search

```python
result = shc.search(
    portfolio="p1",
    org="o1",
    query="hilton cancun",
    datatypes=["reservation"],  # alias for rings
    limit=20,
    offset=0,
)
```

Notes:

- `portfolio` + `org` are required for tenant isolation.
- Use `rings` or `datatypes` to control search scope.
- Optional `search_fields` narrows field matching.

### Delete document index

```python
result = shc.delete_document(
    portfolio="p1",
    org="o1",
    ring="reservation",
    doc_id="reservation_123",
)
```

## Tokenization and Ranking (V1)

Tokenization includes:

- lowercase normalization
- unicode normalization
- punctuation cleanup
- whitespace split
- stopword filtering
- minimum token length policy (with short-token exceptions for field names like `*_code`, `*_id`, `zip`, `sku`)

Ranking is lightweight and based on:

- stored field weight
- token frequency in field (`token_count`)
- exact token bonus
- title-like field bonus (`title`, `name`, `label`)
- multi-token and multi-field match bonuses

Field weight is sourced from Blueprint `search` value.

## Integration Points

- `DataController` calls:
  - `index_document(...)` on create/update
  - `delete_document(...)` on delete
- API route:
  - `/_search/<portfolio>/<org>` calls `SearchController.search(...)`

## Provisioning and Smoke Test

Create required tables (includes search table):

```bash
python3 dev/launcher/scripts/create_dynamodb_tables.py <environment_name> --aws-profile <profile> --region us-east-1
```

Run smoke test:

```bash
python3 dev/launcher/scripts/test_search_engine_v1.py <environment_name> --aws-profile <profile> --region us-east-1
```

The smoke test validates index -> search -> delete -> search lifecycle.

## Non-Goals (V1)

Not included in V1:

- fuzzy matching / typo tolerance
- semantic/vector search
- regex/wildcard search
- ML-driven ranking

