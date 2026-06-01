# Literal Edges V1

Literal edges are derived graph index edges used to accelerate filtering and traversal.
Canonical document attributes remain the source of truth.

## V1 decisions

- One literal edge per declared field (`literal_edge=true`).
- No backward label for literal edges.
- Synchronous updates with document writes (POST/PUT/DELETE sync path).
- No history/versioning in graph index (latest-value only).

## Blueprint contract

Literal edges are opt-in at field level:

```json
{
  "name": "status",
  "type": "string",
  "literal_edge": true
}
```

For field `status` in ring `productora_candidates` with primary id field `talent_id`:

- `edge_label`: `HAS_STATUS`
- `edge_type`: `productora_candidates:talent_id:_literal:status`
- `from_node_id`: `<ring>/<doc_id>`
- `properties`: `{ "value": <canonical_value> }` plus transport metadata (`label_forward`, empty `qualifiers`)

## Canonicalization

- Strings are trimmed.
- Numbers/booleans are preserved as values.
- Maps/lists are normalized recursively.
- Empty values are ignored (`None`, empty string, empty list/map).

For `cardinality=multiple` or list values, one literal edge is generated per normalized value.

## Deterministic uniqueness / idempotency

Literal edges are keyed by:

- `from_node_id`
- `edge_type`
- canonical value token (`sha1(json(canonical_value))`) encoded in synthetic literal `to_node_id`

This guarantees idempotent retries and prevents duplicates for the same node + field + value.

## Synchronous write semantics

- POST: create all desired literal edges from current attributes.
- PUT: recompute desired literal edges from merged current document; stale edges are removed and new edges are upserted.
- DELETE: remove all managed literal edge types for the deleted document.

## Query patterns

Primary query path is by edge type with optional filters:

- `edge_type`
- optional `edge_label` (for example `HAS_STATUS`)
- optional `properties.value` exact match

API route: `POST /_graph/{portfolio}/{org}/edges-by-type`

Payload example:

```json
{
  "edge_type": "productora_candidates:talent_id:_literal:status",
  "edge_label": "HAS_STATUS",
  "property_key": "value",
  "property_value": "approved",
  "limit": 100
}
```

