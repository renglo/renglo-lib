# Renglo Search Engine V1 — Technical Specification

## Overview

The Renglo Search Engine is a DynamoDB-based inverted index system designed specifically for the Renglo Operating System.

The objective of the engine is to provide:

- Fast structured search across Renglo Blueprint objects
- Multi-tenant isolation
- Ring-aware search semantics
- Cross-ring organizational search
- Field-aware indexing and ranking
- A lightweight search engine optimized for Renglo’s architecture

This system is NOT intended to compete with general-purpose internet-scale search engines like Elasticsearch. Instead, it is a focused reverse index engine specialized for:

- Structured Blueprint objects
- Tenant-isolated search
- Known schemas
- Moderate document sizes
- Operational business systems

The search engine is intentionally designed as:

```text
A reverse index and retrieval accelerator
```

NOT:

```text
A second canonical database
```

Original Renglo Blueprint objects remain the source of truth.

---

# Core Concepts

## Portfolio

The highest isolation boundary.

## Organization (Org)

The operational tenant boundary.

All search queries are scoped to an Organization.

## Ring

A semantic grouping of Blueprint objects.

Examples:

- reservation
- employee
- building
- vehicle
- contract
- invoice

Rings are first-class search boundaries.

## Blueprint

Defines the schema and search configuration for an object type.

Blueprints control:

- searchable fields
- field weights
- tokenizer behavior
- stemming behavior
- synonym rules
- search projections

## Document

A single Blueprint object instance.

Example:

```json
{
  "id": "reservation_123",
  "hotel_name": "Hilton Cancun",
  "guest_name": "John Smith"
}
```

## Token

A normalized searchable term extracted from indexed fields.

Examples:

- hilton
- cancun
- john
- smith

## Field

The Blueprint field where a token was discovered.

Examples:

- title
- description
- notes
- city
- hotel_name

---

# Architectural Philosophy

The Renglo Search Engine separates:

```text
Search index structures
```

from:

```text
Canonical business documents
```

The reverse index only stores enough information to:

- locate matching documents
- rank matches
- identify matching fields

The original Blueprint objects remain stored in the primary Renglo data layer.

---

# High-Level Search Flow

## Indexing Flow

```text
Blueprint Object
    ↓
Tokenizer
    ↓
Normalizer
    ↓
Token Extraction
    ↓
Reverse Index Write
```

## Query Flow

```text
Search Query
    ↓
Search Planner
    ↓
Ring Selection
    ↓
Token Queries
    ↓
Result Merge
    ↓
Ranking
    ↓
Document Hydration
    ↓
Final Results
```

---

# DynamoDB Table Design

## Table: renglo_search_index

### Primary Key

```text
PK = portfolio#org#ring#token
SK = field#doc_id
```

### Example

```text
PK = p1#o1#reservation#salami
SK = title#reservation_123
```

Meaning:

```text
The token “salami” appears in the title field of reservation_123.
```

---

# Why Ring Is Included In The Partition Key

Ring is included in the PK because:

- Rings are semantic search boundaries
- Rings reduce hot partition concentration
- Rings improve query selectivity
- Rings allow targeted search plans
- Rings reduce noisy global result sets

Cross-ring search is implemented by the Search Planner through multiple targeted queries.

---

# Why Field Is Included In The Sort Key

Field is included in the SK because:

- Field-aware ranking is important
- Title matches are stronger than notes matches
- Field-specific queries become efficient
- Search-only-title queries become possible
- DynamoDB can efficiently prefix-scan fields

Example:

```text
begins_with(SK, "title#")
```

---

# Reverse Index Structure

The system stores:

```text
One item per token + document + field relationship
```

NOT:

```text
One item per token containing a giant document list
```

This design avoids:

- giant mutable posting lists
- hot write amplification
- large record rewrites
- expensive delete operations

---

# Example Index Entries

Document:

```json
{
  "id": "recipe_123",
  "title": "Salami Pizza",
  "notes": "Customer likes spicy salami"
}
```

Generated index items:

```text
PK = p1#o1#recipe#salami
SK = title#recipe_123

PK = p1#o1#recipe#salami
SK = notes#recipe_123

PK = p1#o1#recipe#pizza
SK = title#recipe_123

PK = p1#o1#recipe#spicy
SK = notes#recipe_123
```

---

# Item Attributes

## Recommended Attributes

```json
{
  "portfolio_id": "p1",
  "org_id": "o1",
  "ring": "recipe",
  "field": "title",
  "doc_id": "recipe_123",
  "blueprint_id": "renglo.recipe.v1",
  "token": "salami",
  "token_count": 2,
  "field_weight": 5,
  "positions": [1, 8],
  "source_updated_at": "2026-05-19T10:00:00Z"
}
```

---

# Search Planner

The Search Planner determines:

- which rings to search
- which fields to prioritize
- which tokenization rules apply
- how results are merged
- how ranking is applied

The planner is responsible for:

```text
Cross-ring search composition
```

NOT the reverse index itself.

---

# Cross-Ring Search

Cross-ring search is implemented as:

```text
Multiple targeted reverse-index queries
```

Example:

Search:

```text
Lex
```

Planner:

```text
Rings:
- building
- employee
- car
```

Generated queries:

```text
p1#o1#building#lex
p1#o1#employee#lex
p1#o1#car#lex
```

Results are merged and ranked.

---

# Search Profiles

Different global searches may target different rings.

Examples:

## People Search

```text
employee
user
vendor_contact
agency_contact
```

## Asset Search

```text
building
device
vehicle
room
```

## Financial Search

```text
invoice
payment
contract
vendor
```

---

# Ranking Model

V1 ranking should remain lightweight.

## Initial Ranking Inputs

- field weight
- token count
- exact match bonus
- title match bonus
- multiple field match bonus
- multiple token match bonus

## Example

```text
title > subtitle > description > notes
```

---

# Query Types

## Exact Token Search

```text
salami
```

## Multi-Token Search

```text
salami cancun
```

## Ring-Scoped Search

```text
Search reservations for “salami”
```

## Field-Scoped Search

```text
Search title for “salami”
```

Implemented through:

```text
begins_with(SK, "title#")
```

---

# Document Hydration

The search engine returns:

- doc_id
- ring
- matching field
- ranking metadata

The original Blueprint object is then loaded from the canonical Renglo data layer.

---

# Search Projections (Optional Future Layer)

Future versions may include:

```text
Search Projection Tables
```

to accelerate search result rendering.

Projection tables may contain:

- title
- subtitle
- preview/snippet
- status
- icon
- display metadata

Projection tables are:

```text
Optional optimization layers
```

NOT canonical storage.

---

# Tokenization Pipeline

V1 tokenization should support:

- lowercase normalization
- punctuation removal
- unicode normalization
- whitespace splitting

Future versions may support:

- stemming
- lemmatization
- multilingual tokenization
- synonym expansion
- phonetic matching
- typo tolerance
- n-grams
- autocomplete indexing

---

# Token Policies

To reduce hot partitions and noisy results:

## Stopwords

Ignore extremely common terms.

Examples:

```text
and
the
of
for
inc
llc
```

## Minimum Token Length

Avoid indexing meaningless short tokens.

## Field-Specific Exceptions

Allow short tokens for:

- SKUs
- airport codes
- zip codes
- identifiers

## Frequency Tracking

Track approximate token document frequency.

Future versions may suppress:

```text
extremely common low-value tokens
```

---

# Hot Partition Mitigation

The chosen PK structure intentionally distributes tokens by ring.

This reduces concentration from very common terms.

Future mitigation strategies may include:

- token sharding
- frequency-aware suppression
- ring-specific stopwords
- partial indexing
- adaptive ranking suppression

---

# Update Strategy

## Insert

- tokenize searchable fields
- create index rows

## Update

- remove old token rows
- regenerate token rows

## Delete

- remove all token rows for document

The engine should maintain:

```text
deterministic index regeneration
```

instead of incremental mutation complexity.

---

# Security Model

All queries are scoped to:

```text
portfolio + org
```

No cross-organization queries are permitted.

Search planners may additionally:

- filter by team access
- filter by ring permissions
- filter by Blueprint visibility

---

# V1 Non-Goals

The following are intentionally excluded from V1:

- semantic vector search
- fuzzy matching
- typo correction
- BM25 compatibility
- distributed query coordination
- internet-scale indexing
- full-text snippet generation
- wildcard scanning
- regex search
- search clustering
- AI ranking

---

# Future Extensions

Potential future subsystems:

- synonym engine
- multilingual analyzer
- stemming engine
- phonetic search
- autocomplete engine
- vector search
- hybrid lexical/vector search
- ranking learning
- search analytics
- popularity boosting
- recency boosting
- semantic query expansion

---

# Final Architectural Principle

The Renglo Search Engine should remain:

```text
A lightweight DynamoDB-native reverse index optimized for structured operational systems.
```

Its role is:

```text
Find candidate documents quickly.
```

NOT:

```text
Replace the canonical Renglo data layer.
```

