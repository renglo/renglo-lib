# Renglo Search Engine V1 — Implementation Specification

## Purpose

This document translates the generic technical spec into a concrete implementation plan for `renglo-lib`, aligned with Renglo component boundaries and existing coding patterns.

The V1 search engine is a DynamoDB-native reverse index that:

- indexes searchable tokens for Blueprint documents
- supports org-isolated and ring-aware retrieval
- returns ranked candidates for hydration by canonical data services

It is not a source of truth for business documents.

---

## Scope and Boundaries

### In scope (V1)

- deterministic document indexing, reindexing, and deletion
- token-based search (single token and multi-token)
- ring-scoped and cross-ring search composition
- optional field-scoped filtering
- lightweight ranking using token/field metadata

### Out of scope (V1)

- fuzzy search, typo tolerance, semantic/vector search
- snippet generation
- wildcard/regex search
- ML-based ranking

---

## Renglo Component Responsibilities

### `search_controller.py` (business logic)

The controller owns:

- API contracts used by the data controller and search router
- orchestration for tokenization, planning, ranking, and pagination
- validation of input parameters
- composition across rings/fields/tokens
- canonical response DTOs

The controller must **not** contain raw DynamoDB query/write logic beyond calling model methods.

### `search_model.py` (DynamoDB interaction)

Create a new model module responsible for:

- table connection/bootstrap
- PK/SK composition helpers
- low-level writes, deletes, and queries
- batched operations for index lifecycle
- pagination via `LastEvaluatedKey`

The model must **not** implement search planner/ranking business rules.

### `__init__.py`

Keep exporting `SearchController`; export `SearchModel` when model is added.

---

## Configuration and Runtime Wiring

Follow `graph_model.py` conventions.

### Required config keys

- `DYNAMODB_SEARCH_TABLE` (required): DynamoDB table name for reverse index
- `AWS_REGION` (optional): defaults to `us-east-1`

### Constructor pattern (model)

`SearchModel` should follow this pattern:

- accept `config: Optional[Dict[str, Any]] = None`
- resolve region from config
- require `DYNAMODB_SEARCH_TABLE` and fail fast with `ValueError` if absent
- initialize table via `boto3.resource("dynamodb", region_name=...)`
- allow dependency injection of `dynamodb_resource` for tests

Recommended constructor:

```python
def __init__(
    self,
    config: Optional[Dict[str, Any]] = None,
    *,
    region_name: Optional[str] = None,
    dynamodb_resource: Optional[Any] = None,
) -> None:
    ...
```

---

## DynamoDB Data Design

### Table

Use configured table from `DYNAMODB_SEARCH_TABLE`.

### Primary key

- `PK = portfolio#org#ring#token`
- `SK = field#doc_id`

### Attributes per row

Required:

- `portfolio_id`
- `org_id`
- `ring`
- `token`
- `field`
- `doc_id`

Recommended for ranking and maintenance:

- `blueprint_id`
- `token_count`
- `field_weight`
- `positions`
- `source_updated_at`
- `created_at`
- `updated_at`

### Secondary indexes

V1 baseline can run without extra indexes for token retrieval.

If delete-by-document performance becomes critical, add a GSI in a subsequent phase:

- `GSI1PK = portfolio#org#ring#doc_id`
- `GSI1SK = field#token`

This enables efficient full cleanup for update/delete without scanning.

---

## Tokenization and Normalization Pipeline

Implement deterministic V1 pipeline in controller utility layer:

1. read searchable fields from blueprint config (or explicit search fields)
2. extract string content from document field values
3. normalize:
   - lowercase
   - unicode normalization
   - punctuation removal
   - whitespace token split
4. apply token policies:
   - stopword suppression
   - min-length filtering
   - field-specific exceptions for short identifiers
5. emit per-field token metadata:
   - token
   - token_count
   - positions
   - field_weight

The same input must always generate the same token set.

---

## Controller API Contract (Proposed)

Current signatures are placeholders and should be adjusted to enforce `portfolio + org` security scope and richer return contracts.

### `index_document`

```python
def index_document(
    self,
    portfolio: str,
    org: str,
    ring: str,
    doc: Dict[str, Any],
    *,
    blueprint_id: Optional[str] = None,
    searchable_fields: Optional[List[str]] = None,
    field_weights: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
```

Behavior:

- validate identity (`portfolio`, `org`, `ring`, `doc["id"]`)
- compute deterministic token rows
- remove stale rows for document (reindex strategy)
- write new rows in batches
- return summary (`indexed_rows`, `token_count`, `doc_id`, `ring`)

### `delete_document`

```python
def delete_document(
    self,
    portfolio: str,
    org: str,
    ring: str,
    doc_id: str,
) -> Dict[str, Any]:
```

Behavior:

- delete all reverse index rows for doc within ring
- idempotent success for missing docs
- return summary (`deleted_rows`, `doc_id`, `ring`)

### `search`

```python
def search(
    self,
    portfolio: str,
    org: str,
    query: str,
    *,
    rings: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 20,
    offset: int = 0,
    search_fields: Optional[List[str]] = None,
    boost_fields: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
```

Behavior:

- tokenize query
- resolve target rings (explicit list or search profile)
- query model per `(ring, token)` and optional field prefix
- merge hits by `doc_id`
- compute score from ranking inputs
- sort + paginate
- return candidates + ranking metadata + cursor/paging info

Note: `portfolio` must be required to satisfy security model.

---

## `SearchModel` API Contract (Proposed)

Create `dev/renglo-lib/renglo/search/search_model.py` with methods like:

- `put_index_rows(rows: List[Dict[str, Any]]) -> int`
- `delete_index_rows(keys: List[Dict[str, str]]) -> int`
- `delete_document_rows(portfolio: str, org: str, ring: str, doc_id: str) -> int`
- `query_token(portfolio: str, org: str, ring: str, token: str, *, field_prefix: Optional[str] = None, limit: int = 100, exclusive_start_key: Optional[Dict[str, Any]] = None) -> Dict[str, Any]`
- `query_tokens_multi_ring(...)` helper (optional in model; can stay in controller)

Also include static key helpers:

- `make_pk(portfolio, org, ring, token) -> str`
- `make_sk(field, doc_id) -> str`
- parse helpers for PK/SK back into structured metadata

---

## Search Planner (Controller Layer)

Implement planner functions in `SearchController` or helper module:

- `resolve_target_rings(...)`
- `plan_token_queries(...)`
- `merge_hits(...)`
- `rank_results(...)`

Cross-ring search must execute as multiple targeted token queries and then merge.

---

## Ranking V1 (Controller Layer)

Initial score formula should incorporate:

- field weight
- token frequency in field (`token_count`)
- exact token match bonus
- title/primary-field bonus
- multi-field bonus
- multi-token bonus

Store enough metadata in rows to compute ranking without reading canonical documents.

---

## Integration Points

### Data controller integration

`data_controller` relies on:

- `index_document(...)` during create/update flows
- `delete_document(...)` during delete flows

Both operations must be idempotent and safe on retries.

### Search router integration

`search_router` relies on:

- `search(...)` as query entrypoint

Response should include:

- `results`: list of ranked candidates (doc ids + ring + score + match metadata)
- `total` and paging info
- query diagnostics (tokens, rings searched) when requested

---

## Error Handling and Observability

Implement explicit error classes:

- `SearchConfigError`
- `SearchValidationError`
- `SearchStorageError`

Controller should:

- log request context (`portfolio`, `org`, `ring`, `doc_id`)
- avoid logging raw sensitive document payloads
- return deterministic, typed error payloads upstream

---

## Test Plan

### Unit tests

- key composition/parsing helpers
- tokenization deterministic output
- ranking formula behavior
- planner ring selection and merge behavior
- validation and config failures

### Integration tests (DynamoDB-local/mocked)

- index -> search -> delete lifecycle
- update-reindex removes stale tokens
- field-scoped search with `begins_with(SK, "<field>#")`
- cross-ring search merge ordering
- tenant isolation (`portfolio + org`)

### Contract tests

- data controller call compatibility
- search router response compatibility

---

## Delivery Phases

### Phase 1: Model foundation

- add `search_model.py`
- wire config (`DYNAMODB_SEARCH_TABLE`)
- implement key helpers and base query/write methods

### Phase 2: Index lifecycle in controller

- implement `index_document` and `delete_document`
- add deterministic tokenization utilities
- add idempotent reindex path

### Phase 3: Query path in controller

- implement `search`
- add ring planner, merge, and ranking
- add paging and response DTO

### Phase 4: Hardening

- error taxonomy and logging
- test coverage and edge cases
- optional performance improvements (batching, optional doc GSI)

---

## Acceptance Criteria (V1)

- reverse index table is fully driven by `DYNAMODB_SEARCH_TABLE`
- `SearchController` owns business flow; `SearchModel` owns DynamoDB access
- data controller can index/delete documents through stable controller contract
- search router can execute org-scoped (and portfolio-scoped) query flow
- ring-aware and cross-ring token search returns ranked candidate documents
- delete and reindex operations are deterministic and idempotent
- no V1 non-goals are implemented implicitly

