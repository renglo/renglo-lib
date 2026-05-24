## Renglo Query Planner & Optimizer (RQPO) — V1 Technical Specification

### Overview

The Renglo Query Planner & Optimizer (RQPO) is the subsystem responsible for transforming high-level graph queries into efficient executable traversal plans over the Renglo Graph Engine.

The Query Planner is NOT responsible for:

- executing graph traversals
- storing graph edges
- natural language understanding
- graph mutation

Instead, its responsibility is to:

1. Analyze graph query intent
2. Generate possible traversal plans
3. Estimate execution cost
4. Select the optimal plan
5. Produce an executable traversal strategy

The optimizer operates on top of existing Renglo graph primitives:

- forward traversal
- backward traversal
- node retrieval
- property filtering
- edge expansion

⸻

### Goals

The V1 optimizer should:

- Avoid expensive graph traversals
- Reduce intermediate node explosion
- Select efficient traversal anchors
- Support multi-hop heterogeneous queries
- Support hybrid traversal + property filtering
- Produce deterministic execution plans
- Operate without requiring a full graph database

⸻

### Non Goals (V1)

The V1 optimizer will NOT support:

- dynamic runtime re-planning
- distributed execution
- shortest path algorithms
- probabilistic planning
- machine learning-based cost estimation
- graph mutation optimization
- semantic/NL query parsing
- recursive cycle-cost optimization
- adaptive caching
- parallel traversal scheduling

⸻

### Core Concepts

Query Pattern

A Query Pattern represents the logical graph relationship requested by the user/system.

Example:

Find hotels in Rio with 5-star reviews

Logical Pattern:

```
(Hotel)-[:LOCATED_IN]->(City)

(Review)-[:REVIEWS]->(Hotel)

WHERE:

City.name = "Rio"

Review.stars = 5
```

The Query Pattern is NOT executable yet.

⸻

### Traversal Plan

A Traversal Plan is the ordered sequence of operations used to execute a Query Pattern.

Example:

```
1. Resolve City(name=Rio)
2. Traverse reverse LOCATED_IN edges
3. Retrieve Hotels
4. Traverse reverse REVIEWS edges
5. Filter Review.stars = 5
6. Aggregate by Hotel
```

Different plans may produce identical results with vastly different costs.

⸻

### Anchor Set

An Anchor Set is the initial candidate node set selected as the traversal starting point.

Examples:

```
City(name=Rio)
Review(stars=5)
Hotel(type=Luxury)
```

The optimizer attempts to select the most selective anchor set.

⸻

### Selectivity

Selectivity estimates how restrictive a constraint is.

Examples:

```
Constraint                  Estimated Matches

City.name = Rio             1

Review.stars = 5            30,000,000

Hotel.category = Luxury     12,000
```

Higher selectivity generally produces better anchors.

⸻

### Intermediate Explosion

Intermediate Explosion occurs when traversal produces large temporary result sets.

Example:

```
Review(stars=5)
→ 30M reviews
→ millions of hotels
→ filter Rio
```

The optimizer seeks to minimize intermediate explosion.

### Architecture

```
Graph Query
    ↓
Query Parser
    ↓
Logical Query Pattern
    ↓
Constraint Extractor
    ↓
Candidate Plan Generator
    ↓
Cost Estimator
    ↓
Plan Ranker
    ↓
Traversal Plan
    ↓
Graph Execution Engine
```

### Components

1. Query Parser

Responsibility

Convert incoming graph query definitions into normalized internal structures.

Input

```
{
  "target": "Hotel",
  "constraints": [
    {
      "node": "City",
      "property": "name",
      "operator": "=",
      "value": "Rio"
    },
    {
      "node": "Review",
      "property": "stars",
      "operator": "=",
      "value": 5
    }
  ],
  "relationships": [
    {
      "from": "Hotel",
      "edge": "LOCATED_IN",
      "to": "City"
    },
    {
      "from": "Review",
      "edge": "REVIEWS",
      "to": "Hotel"
    }
  ]
}
```

Output

Normalized QueryPattern object.

⸻

### 2. Constraint Extractor

Responsibility

Identify:

* candidate anchor constraints
* property filters
* traversal edges
* aggregation requirements

Output

```
{
  "anchors": [
    "City.name=Rio",
    "Review.stars=5"
  ]
}
```

### 3. Graph Statistics Registry

Responsibility

Maintain approximate graph metadata required for cost estimation.

Stored Metrics

Node Counts
```
{
  "Hotel": 1000000,
  "Review": 100000000,
  "City": 50000
}
```

Property Cardinality
```
{
  "City.name=Rio": 1,
  "Review.stars=5": 30000000
}
```

Edge Fanout
```
{
  "City<-LOCATED_IN-Hotel": 1200,
  "Hotel<-REVIEWS-Review": 250
}
```

Edge Distribution

Optional V2 enhancement.



### 4. Candidate Plan Generator

Responsibility

Generate alternative traversal sequences.

Example

Plan A

```
City
→ Hotels
→ Reviews
```

Plan B

```
Reviews
→ Hotels
→ City
```

Plan C

```
Hotels
→ City + Reviews
```

⸻

### 5. Cost Estimator

Responsibility

Estimate computational cost of candidate plans.

⸻

V1 Cost Factors

```
Factor                              Description

Candidate Set Size                  Estimated starting node count
Traversal Fanout                    Average edge expansion
Property Filter Cost                Cost of post-traversal filtering
Intermediate Result Size            Temporary node count during execution
Traversal Depth                     Number of hops
```

Simplified Cost Formula
```
estimated_cost =
    candidate_count
    * cumulative_fanout
    * traversal_depth
```

Example
Plan A
```
City(name=Rio) = 1
Hotels in Rio = 1200
Reviews per Hotel = 250
Cost:
1 × 1200 × 250
= 300,000
```


Plan B
```
Review(stars=5) = 30,000,000
```
Cost immediately exceeds Plan A.


⸻

### 6. Plan Ranker

Responsibility

Sort candidate plans by estimated cost.

Selection Criteria

Primary:

* lowest estimated cost

Secondary:

* lowest traversal depth
* smallest intermediate explosion


### 7. Execution Plan Builder

Responsibility
Convert logical plan into executable traversal operations.
⸻
Output Example

```
[
  {
    "op": "find_nodes",
    "type": "City",
    "filter": {
      "name": "Rio"
    }
  },
  {
    "op": "traverse_reverse",
    "edge": "LOCATED_IN",
    "target_type": "Hotel"
  },
  {
    "op": "traverse_reverse",
    "edge": "REVIEWS",
    "target_type": "Review"
  },
  {
    "op": "filter",
    "property": "stars",
    "operator": "=",
    "value": 5
  }
]
```

Query Execution Principles

Principle 1 — Prefer Highly Selective Anchors

Preferred:
```City.name = Rio```

Avoid:
```Review.stars = 5```

Principle 2 — Minimize Intermediate Explosion

Avoid plans producing massive temporary node sets.

Principle 3 — Prefer Indexed Constraints

Use indexed properties whenever possible.

Principle 4 — Delay Expensive Expansions

Expand high-fanout relationships late in the plan.

⸻

Primitive Operations Assumed Existing

The planner assumes the existence of:

```
get_node(node_id)
find_nodes(type, filters)
get_forward_edges(node_id, edge_type)
get_reverse_edges(node_id, edge_type)
filter_nodes(nodes, filters)
```

Supported Query Types (V1)

Query Type                              Supported

Multi-hop traversal                     Yes

Hybrid property filtering               Yes

Reverse traversal                       Yes

Heterogeneous node types                Yes

Aggregation hints                       Limited

Ranking                                 Limited

Recursive traversal                     No

Pathfinding                             No

Graph analytics                         No



### Failure Modes

Graph Explosion

Mitigation:

* traversal depth limits
* candidate set limits
* max intermediate thresholds

⸻

Cycles

Mitigation:

* visited node tracking

⸻

Broad Property Filters

Mitigation:

* fallback heuristics
* warning scoring

⸻

Future V2 Enhancements

Adaptive Runtime Replanning

Switch plans dynamically during execution.

⸻

Cost Learning

Learn real traversal costs from execution history.

⸻

Parallel Traversal

Execute independent branches concurrently.

⸻

Semantic Query Translation

LLM converts natural language into Query Patterns.

⸻

Graph Cache Layer

Cache common anchor resolutions.

⸻

#### Example End-to-End

Input
```
What hotels in Rio usually receive 5-star reviews?
```

Logical Pattern
````
Hotel
← REVIEWS ← Review(stars=5)
Hotel
→ LOCATED_IN → City(name=Rio)
````

Candidate Plans
A
```
City → Hotels → Reviews
```

B
```
Review → Hotels → City
```

Optimizer Decision

Select Plan A due to:

* lower candidate set
* lower fanout
* lower intermediate expansion

⸻

Final Execution Plan

```
1. Resolve Rio City node
2. Traverse reverse LOCATED_IN
3. Retrieve Hotels
4. Traverse reverse REVIEWS
5. Filter Review.stars=5
6. Aggregate ratings
7. Rank Hotels
```