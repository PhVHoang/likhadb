# RFC: DataFusion as Post-ANN Execution Layer for Native Lakehouse Vector Search

| Field | Value |
|---|---|
| **RFC ID** | TBD |
| **Status** | Draft |
| **Author(s)** | TBD |
| **Created** | 2026-05-19 |
| **Last Updated** | 2026-05-19 |
| **Target Milestone** | TBD |

---

## Table of Contents

1. [Summary](#1-summary)
2. [Motivation](#2-motivation)
3. [Background and Prior Art](#3-background-and-prior-art)
4. [Design Goals](#4-design-goals)
5. [Non-Goals](#5-non-goals)
6. [Proposed Design](#6-proposed-design)
7. [Component Specifications](#7-component-specifications)
8. [Data Flow](#8-data-flow)
9. [Failure Modes and Mitigations](#9-failure-modes-and-mitigations)
10. [Operational Concerns](#10-operational-concerns)
11. [Alternatives Considered](#11-alternatives-considered)
12. [Open Questions](#12-open-questions)
13. [Appendix](#13-appendix)

---

## 1. Summary

This RFC proposes integrating **Apache DataFusion** as the post-ANN execution layer in
the native Lakehouse vector search pipeline. DataFusion does not replace the ANN index.
It owns the execution layer that begins where the ANN store's responsibility ends:
metadata enrichment from Iceberg, access control enforcement, multi-signal score fusion,
and tiered model-based reranking.

The result is a unified, SQL-expressible, auditable query layer that sits between the ANN
store and the final result — with no new storage systems introduced.

---

## 2. Motivation

### 2.1 The Problem

The current vector search pipeline returns ANN candidates (ID, distance, rank) and passes
them directly to application code for any downstream processing. This creates several
compounding problems:

**Business logic is scattered.** ACL enforcement, sensitivity filtering, author reputation
boosts, and recency decay are implemented inconsistently across query paths. Changes
require coordinated updates across multiple services.

**Reranking is row-by-row.** Model-based reranking (bi-encoder, cross-encoder) is invoked
per candidate in application code, not in a batched columnar execution. At 500 candidates
this serializes up to 500 model calls per query.

**Metadata joins are expensive and ad hoc.** Fetching document metadata, author
attributes, and classification labels for 500 candidates via point lookups (one request
per candidate) is the dominant latency contributor in the current query path.

**Scoring logic is opaque.** There is no auditable, inspectable representation of how
a final score was produced for a given candidate. Debugging recall regressions requires
tracing through application code rather than reading a query plan.

### 2.2 Why This Matters

These problems manifest as:

- Inconsistent ACL enforcement across query surfaces — a correctness and compliance risk.
- P95 query latency dominated by sequential metadata fetches and per-row model calls,
  rather than the ANN search itself.
- Inability to A/B test scoring weight changes without deploying new application code.
- No mechanism to explain why a result ranked where it did.

### 2.3 Why DataFusion Specifically

DataFusion is an embeddable query engine built on Apache Arrow. Its execution model —
columnar batches, vectorized operators, a cost-based optimizer — is structurally well
matched to the post-ANN workload:

- The candidate set is small (hundreds of rows) and fits in L2 cache as an Arrow
  `RecordBatch`.
- Joins against large Iceberg tables are hash joins with the candidate set as the build
  side — exactly the pattern DataFusion's optimizer selects automatically.
- UDFs receive full Arrow arrays (not scalar rows), enabling SIMD distance computation
  and batched model calls within the execution plan.
- Iceberg integration is first-class via `datafusion-iceberg`, with Parquet pushdown and
  partition pruning handled by the query engine rather than application code.

---

## 3. Background and Prior Art

### 3.1 Two-Phase Retrieval (Retrieve-then-Rerank)

The retrieve-then-rerank pattern is well established in information retrieval. Phase 1
(ANN retrieval) optimises for recall over a large corpus at low latency. Phase 2
(reranking) applies more expensive, higher-quality scoring to a small candidate set.
DataFusion is the execution engine for phase 2.

### 3.2 DataFusion in Production Lakehouse Systems

DataFusion is used as an embedded query engine in Delta Lake (delta-rs), LanceDB, and
InfluxDB IOx. In each case it is embedded within a larger system rather than deployed as
a standalone query service. This RFC follows the same pattern — DataFusion is a library
dependency of the vector search service, not a separately deployed component.

### 3.3 Columnar Execution for Vector Workloads

Arrow's `FixedSizeListArray` is the natural representation for fixed-dimension embeddings.
SIMD distance kernels operating over the flat primitive buffer of a `FixedSizeListArray`
can process hundreds of embeddings in a single vectorized loop. This is only possible when
the execution model passes full arrays to UDFs rather than invoking them row-by-row.

### 3.4 Relationship to the Real-Time Insert RFC

This RFC describes the query path only. The staging tier flat scan described in the
Real-Time Insert RFC (rfc_realtime_insert_vectordb.md) is a direct extension of the
enrichment stage specified here — the staging Iceberg table is simply an additional join
source in Stage 3, searched via the same distance UDFs defined in Section 7.3 of this RFC.

---

## 4. Design Goals

| ID | Goal |
|---|---|
| G1 | All business logic (ACL, scoring, filtering) is expressed as SQL or registered UDFs — no business logic in application code downstream of DataFusion |
| G2 | Model-based reranking (bi-encoder, cross-encoder) is batched over the full candidate set — no per-row model invocation |
| G3 | Metadata enrichment for 500 candidates completes via a single multi-join DataFusion query — no per-candidate point lookups |
| G4 | Scoring weights are configuration-driven and changeable without code deployment |
| G5 | The physical query plan is inspectable via `EXPLAIN ANALYZE` — recall regressions are debuggable without application code changes |
| G6 | No new storage systems are introduced — all data sources are Iceberg tables on GCS |
| G7 | The DataFusion layer is ANN-store-agnostic — its only dependency on the ANN store is the `(id, distance, rank)` output contract |

---

## 5. Non-Goals

| ID | Non-Goal | Rationale |
|---|---|---|
| NG1 | ANN index construction or management | Owned by the ANN store; DataFusion has no role in index build or recall tuning |
| NG2 | Embedding generation | Owned by the embedding service upstream of the ANN store |
| NG3 | Sub-millisecond reranking latency | Cross-encoder calls have irreducible model inference cost; DataFusion minimises overhead but cannot eliminate it |
| NG4 | Replacing the ANN store with DataFusion full-scan | Full-scan over a large Iceberg corpus is not competitive with ANN recall/latency at scale; addressed by the Real-Time Insert RFC for the staging tier only |
| NG5 | Distributed DataFusion deployment (Ballista) | The candidate set is small enough for single-node execution; distributed execution adds operational complexity with no benefit at this cardinality |

---

## 6. Proposed Design

### 6.1 Overview

DataFusion is embedded in the vector search service as a library. It operates on the
output of the ANN store (a small ranked candidate list) and produces the final scored,
enriched, reranked result set returned to the caller.

The pipeline consists of four sequential stages, all executing within the DataFusion
`SessionContext`:

```
ANN Store output
(id, distance, rank) × N
        │
        ▼
┌───────────────────────────────────────────────────────────────┐
│                    DataFusion SessionContext                  │
│                                                               │
│  Stage 2: Candidate Registration  →  MemTable                 │
│                │                                              │
│  Stage 3: Enrichment              →  Iceberg joins + ACL      │
│                │                                              │
│  Stage 4a: Score Fusion           →  SQL window functions     │
│                │                                              │
│  Stage 4b: Bi-encoder Reranking   →  AsyncUDF, top-M → top-P  │
│                │                                              │
│  Stage 4c: Cross-encoder Reranking → materialize-then-call    │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
Final top-K results
```

Stage 1 (ANN retrieval) is outside DataFusion and is the ANN store's responsibility.
Stage 4c (cross-encoder) materializes out of DataFusion at top-P rows before the external
model call.

### 6.2 Guiding Constraint

The ANN store's output is the only interface between the ANN store and DataFusion. The
contract is:

```
output: List<{ id: String, distance: Float32, rank: UInt64 }>
```

How the ANN store produces this output is not specified by this RFC.

---

## 7. Component Specifications

### 7.1 SessionContext Lifecycle

The `SessionContext` is initialized once at service startup and shared across requests.
It holds the Iceberg catalog registration and all UDF registrations. These are expensive
to construct and must not be recreated per request.

The `candidates` MemTable (Stage 2) is request-scoped and must be isolated between
concurrent requests. Three strategies are available:

| Strategy | Mechanism | Trade-off |
|---|---|---|
| A — per-request table name | Register as `candidates_{request_id}`, interpolate name into SQL | Simple; introduces string interpolation in SQL |
| B — child context per request | Clone shared context; isolated table registry; catalog and UDFs inherited | Clean isolation; profile clone cost under load |
| C — session pool | Pre-allocate pool of `SessionContext` instances | Eliminates clone cost; higher idle memory footprint |

**Recommended starting point:** Strategy B. The clone is shallow for catalog and UDF
registrations. Profile under realistic concurrency before committing to Strategy C.

**Startup sequence:**

```
1. Build SessionContext with runtime config (batch_size, target_partitions)
2. Register Iceberg catalog (REST or Hive — implementation-specific)
3. Register all UDFs (distance kernels, bi-encoder async UDF)
4. Service ready
```

**Per-request sequence:**

```
1. Derive child context (or acquire from pool)
2. Register candidates MemTable
3. Execute Stages 3 → 4b as a chained DataFusion plan
4. Collect top-P RecordBatch
5. Cross-encoder call (Stage 4c, outside DataFusion)
6. Return top-K
```

---

### 7.2 Stage 2 — Candidate Registration

| Field | Value |
|---|---|
| **Input** | `List<{ id, distance, rank }>` from ANN store |
| **Output** | In-memory `MemTable` registered as `candidates` in the child context |
| **Schema** | `id: Utf8, ann_distance: Float32, ann_rank: UInt64` |

The candidate list is materialized as an Arrow `RecordBatch` and wrapped in a `MemTable`.
At 500 rows × 3 columns this is on the order of 10 KB — it fits in L1 cache and acts as
the build side of all downstream hash joins with zero I/O cost.

**Critical constraint:** The `MemTable` must not be registered on the shared parent
context. It must be registered on the per-request child context or pool-acquired context.
Registration on the shared context under concurrent load will cause query interference.

---

### 7.3 Stage 3 — Enrichment

| Field | Value |
|---|---|
| **Input** | `candidates` MemTable |
| **Output** | Enriched DataFrame: metadata, ACL attributes, and business signals for each candidate |
| **Iceberg tables joined** | `embeddings`, `documents`, `authors`, `classifications`, `access_control` |

#### 7.3.1 Join Strategy

DataFusion's cost-based optimizer selects **hash join with broadcast** automatically when
one join input is significantly smaller than the other. With `candidates` at 500 rows and
Iceberg tables at millions of rows, the optimizer will always choose `candidates` as the
build side. No manual join hints are required.

Build side behaviour: `candidates` is loaded into a hash map keyed on `id`. Each Iceberg
table is scanned once as the probe side, with join resolution via hash lookup.

#### 7.3.2 Required Pushdowns

The following pushdowns must be verified via `EXPLAIN ANALYZE` before the enrichment
query is considered production-ready. If any are absent, the partition design or table
statistics must be corrected before proceeding.

| Pushdown | Mechanism | Verification check |
|---|---|---|
| Sensitivity filter | Parquet row group min/max statistics on `sensitivity_label` | `FilterExec` appears before `HashJoinExec` in physical plan |
| Partition pruning | Iceberg partition spec on `embeddings` | File count in scan is proportional to partition selectivity |
| Embedding column projection | Parquet column pruning | `embedding` column absent from physical plan when not selected in output |

#### 7.3.3 Access Control Enforcement

ACL enforcement is expressed as a `WHERE` clause predicate in the enrichment SQL, not in
application code. This is a correctness requirement, not a performance optimisation. ACL
logic expressed in application code is not guaranteed to run before scoring or model
inference; ACL logic in the DataFusion `WHERE` clause is guaranteed to eliminate rows
before any downstream operator executes.

The ACL predicate must appear in the enrichment query and must not be optional. Queries
that do not specify an ACL context must be rejected at the API boundary before reaching
the DataFusion layer.

#### 7.3.4 Embedding Column Selection

The `embedding` column (a `FixedSizeList<f32>` of dimension D) is large — at D=1536 each
row is 6 KB. It must only be included in the enrichment SELECT if it is consumed by a
downstream UDF in Stage 4b (dot product reranking). If Stage 4b uses a bi-encoder
(text-in, score-out), the `embedding` column must be excluded from the enrichment query.
This is enforced at configuration time, not at runtime.

---

### 7.4 Stage 4a — Score Fusion

| Field | Value |
|---|---|
| **Input** | Enriched DataFrame from Stage 3 |
| **Output** | DataFrame with `fusion_score` column, ordered descending, limited to top-M |
| **M** | Configurable; controls the input cardinality to Stage 4b |

#### 7.4.1 Signal Taxonomy

Score fusion combines signals from multiple sources. The taxonomy below defines the
categories independently of the specific signals present in any given deployment:

| Category | Examples |
|---|---|
| Retrieval signals | ANN distance (inverted), reciprocal rank, hybrid dense/sparse scores |
| Temporal signals | Recency decay over `created_at`, time-since-last-modified |
| Authority signals | Author reputation score, verification status, domain expertise match |
| Content signals | Document completeness, word count normalized to corpus distribution |
| Policy signals | Sensitivity label — used for **filtering only**, never as a scoring signal |

#### 7.4.2 Normalization Contract

All signals must be normalized to `[0, 1]` before linear combination. The normalization
is computed within the candidate set using SQL window functions:

```
normalized_signal = signal / MAX(signal) OVER ()
```

For signals that are inherently bounded (boolean flags, probability outputs), normalization
is identity or a fixed mapping. Normalization must not depend on precomputed global
statistics — only on the values present in the current candidate set.

#### 7.4.3 Fusion Formula

```
fusion_score = Σ (weight_i × normalized_signal_i)
```

Weights are loaded from configuration at startup. Weight sum must equal 1.0; this is
validated at startup and the service must refuse to start if validation fails. Weights
must not be hardcoded in SQL — they must be injected as query parameters or computed
from the config struct before SQL execution.

#### 7.4.4 Recency Decay

For temporal signals, exponential decay is preferred over linear to avoid a hard cliff
at the recency boundary:

```
recency_score = exp(-λ × max(0, age_days - grace_period))
```

Both `λ` (decay rate) and `grace_period` (flat scoring window in days) are configuration
parameters. This function is expressible as a SQL scalar expression and requires no UDF.

---

### 7.5 Stage 4b — Bi-encoder Reranking

| Field | Value |
|---|---|
| **Input** | Top-M candidates from Stage 4a |
| **Output** | DataFrame with `bi_score` and `combined_score` columns, ordered descending, limited to top-P |
| **P** | Configurable; controls the input cardinality to Stage 4c |

#### 7.5.1 Execution Model

The bi-encoder UDF must operate **batch-over-column**. The UDF receives the full
`chunk_text` column as an Arrow `StringArray` covering all M candidates and issues a
**single batched request** to the model service. Per-row invocation is explicitly
prohibited — it serializes M model calls and negates the benefit of the DataFusion
execution layer.

UDF signature:

```
biencoder_similarity(query_text: Utf8, chunk_text: Utf8) → Float32
```

The query is passed as a scalar literal (broadcast to all rows). The passage column is
the varying input. The UDF returns a `Float32Array` of length M.

The UDF must be implemented as an `AsyncScalarUDF` (DataFusion 37+) to allow the model
HTTP call to be `await`ed without blocking the executor thread pool.

#### 7.5.2 Score Combination

```
combined_score = α × bi_score + (1 - α) × fusion_score
```

`α` is a configuration parameter. It controls how much weight the bi-encoder's
text-relevance judgment has relative to the multi-signal fusion score from Stage 4a.

#### 7.5.3 Error Handling

The bi-encoder UDF must propagate model service errors as DataFusion `DataFusionError`
values. Retry logic must not be implemented inside the UDF — it belongs at the pipeline
orchestration level. A failed bi-encoder call must fail the query, not silently return
zero scores.

#### 7.5.4 Dot Product Alternative

If embeddings are stored in the `embeddings` Iceberg table and are already retrieved in
Stage 3, the bi-encoder UDF may be replaced by a sync `dot_product` UDF operating over
the `embedding` column. This eliminates the external HTTP call entirely. The trade-off
is recall quality: the dot product score is identical to the ANN distance (modulo
quantization) and may not add information beyond Stage 4a. This must be evaluated
empirically against the bi-encoder alternative before a choice is made.

---

### 7.6 Stage 4c — Cross-encoder Reranking

| Field | Value |
|---|---|
| **Input** | Top-P candidates from Stage 4b (materialized RecordBatch) |
| **Output** | Final top-K results, ordered by cross-encoder score |
| **K** | Request-scoped parameter |

#### 7.6.1 Why Not an AsyncUDF

Cross-encoders are significantly more expensive per call than bi-encoders. At P ≤ 20,
the overhead of the DataFusion execution plan (operator scheduling, RecordBatch routing)
is not justified. The materialize-then-call pattern — collect Stage 4b output into memory,
call the model service once with all P pairs, zip scores back to IDs, sort — is simpler
and has equivalent throughput at this cardinality.

#### 7.6.2 Pattern

```
1. Collect Stage 4b DataFrame → RecordBatch (top-P rows)
2. Extract (id, chunk_text) as plain vectors
3. Single batched cross-encoder call: List<(query, passage)> → List<Float32>
4. Zip scores to IDs
5. Sort descending by cross-encoder score
6. Truncate to top-K
```

Steps 4–6 do not require DataFusion. They are in-memory operations on P ≤ 20 rows.

---

### 7.7 UDF Contracts

#### 7.7.1 Sync Distance UDFs

All distance UDFs share the following contract:

| Property | Requirement |
|---|---|
| Input arg 0 | Query embedding — scalar `FixedSizeList<f32>[D]`, broadcast to all rows |
| Input arg 1 | Candidate embedding column — `FixedSizeListArray` of length N |
| Output | `Float32Array` of length N |
| Allocation | No per-row allocation; operate over the flat primitive buffer of arg 1 |
| Vectorization | Must use SIMD or auto-vectorization over the flat buffer; scalar loop is not acceptable |
| Volatility | `Immutable` — same inputs always produce the same output |

Required UDFs:

| Name | Formula |
|---|---|
| `dot_product(a, b)` | `Σ aᵢ × bᵢ` |
| `cosine_similarity(a, b)` | `dot_product(a, b) / (‖a‖ × ‖b‖)` |
| `l2_distance(a, b)` | `√(Σ (aᵢ - bᵢ)²)` |

The implementer registers only the UDFs consumed by the configured scoring pipeline.
All three need not be registered if the pipeline uses only one distance metric.

#### 7.7.2 Async Model UDFs

| Property | Requirement |
|---|---|
| Batching | One HTTP request per `RecordBatch` invocation — not one per row |
| Error propagation | Surface as `DataFusionError`; no internal retry |
| Volatility | `Volatile` — model outputs may change across service versions |
| Timeout | Configurable; must be shorter than the overall query timeout |

---

## 8. Data Flow

### 8.1 Query Path (steady state)

```
Caller
  │
  │ search(query_text, allowed_teams, top_k)
  ▼
Vector Search Service
  │
  ├─ embed(query_text) → query_vector           [Embedding Service]
  │
  ├─ ann_search(query_vector, N) →              [ANN Store]
  │    List<{ id, distance, rank }>
  │
  ├─ register candidates MemTable              [DataFusion]
  │
  ├─ enrichment join (Stage 3)                 [DataFusion + Iceberg]
  │    WHERE sensitivity_label != 'confidential'
  │    AND allowed_teams ∩ acl.allowed_teams ≠ ∅
  │
  ├─ score fusion (Stage 4a) → top-M           [DataFusion SQL]
  │
  ├─ bi-encoder reranking (Stage 4b) → top-P   [DataFusion AsyncUDF]
  │
  ├─ collect RecordBatch (top-P rows)          [materialize]
  │
  ├─ cross-encoder call → scores               [Cross-encoder Service]
  │
  └─ sort + truncate → top-K                  [in-memory]
  │
  ▼
Caller ← List<RankedResult>
```

### 8.2 Candidate Cardinality Through the Pipeline

| Stage | Output cardinality | Controlled by |
|---|---|---|
| ANN retrieval | N (e.g. 500) | `ann.top_n` config |
| Enrichment | ≤ N (ACL filtering reduces) | ACL predicate |
| Score fusion | M ≤ enriched count | `scoring.fusion.top_m` config |
| Bi-encoder reranking | P ≤ M | `scoring.biencoder.top_p` config |
| Cross-encoder reranking | K ≤ P | Request parameter |

The over-retrieval ratio at each stage (N/M, M/P, P/K) must be set such that the true
top-K is very unlikely to be outside the candidate set at any stage. The recommended
starting ratios are N/M = 5, M/P = 5, P/K = 2–4, to be tuned against a recall
evaluation set.

---

## 9. Failure Modes and Mitigations

| Failure | Impact | Mitigation |
|---|---|---|
| **Iceberg catalog unreachable at startup** | Service fails to start | Catalog registration must be retried with exponential backoff; startup probe must not pass until catalog is reachable |
| **Iceberg file read error during enrichment** | Query fails | Surface as query error; do not return partial results silently |
| **ACL predicate missing from enrichment query** | Unrestricted data returned | API layer validates ACL context before dispatching to DataFusion; requests without ACL context are rejected with 400 |
| **Bi-encoder service timeout** | Stage 4b fails; query fails | Configurable timeout shorter than overall query timeout; alert on timeout rate; consider degraded mode (skip Stage 4b, serve Stage 4a output) as a configurable fallback |
| **Cross-encoder service timeout** | Stage 4c fails; query fails | Same pattern as bi-encoder; Stage 4b output is a valid degraded fallback |
| **SessionContext child clone is slow under concurrency** | Query latency increase | Profile; switch to pool strategy (Strategy C) if clone cost is measurable |
| **Weight config sums to ≠ 1.0** | Scores not in [0, 1] | Validated at startup; service refuses to start if validation fails |
| **Embedding column included when not needed** | Stage 3 reads 6 KB/row unnecessarily | Controlled by configuration flag; verified in integration tests via `EXPLAIN` plan inspection |

---

## 10. Operational Concerns

### 10.1 Query Plan Observability

The physical query plan for Stages 3 and 4a must be logged at `DEBUG` level on every
request. The logical plan must be logged at `TRACE` level. During development and after
any schema or partition change, `EXPLAIN ANALYZE` must be run manually and the following
verified:

1. `candidates` is the build side of all `HashJoinExec` operators
2. `FilterExec` for `sensitivity_label` appears before the first `HashJoinExec`
3. Iceberg file scan count reflects partition pruning (not a full table scan)
4. `embedding` column is absent from the physical plan output when Stage 4b uses a
   bi-encoder (text-in) rather than dot product (embedding-in)

### 10.2 Scoring Weight Changes

Scoring weights are loaded from configuration at startup. A weight change requires a
service restart (rolling restart in GKE). No data migration is required. The new weights
take effect immediately for all queries after the restart.

Weight changes must be accompanied by a recall evaluation run against the held-out
evaluation set before deployment to production.

### 10.3 Adding a New Scoring Signal

Adding a signal to Stage 4a requires:

1. Confirming the signal column exists in an Iceberg table already joined in Stage 3 (or
   adding a new join source)
2. Adding the normalization expression and weight to the score fusion SQL
3. Adding the weight to the `ScoringWeights` config struct and updating the sum-to-1
   validation
4. Running the recall evaluation set to verify no regression

No change to UDF registration or the DataFusion session setup is required for a pure
SQL signal addition.

### 10.4 Embedding Model Version Changes

When the embedding model version changes, the `embedding` column dimension D may change.
The distance UDFs are registered with a fixed D at startup. A model version change
requires:

1. Updating the UDF registration dimension in configuration
2. Restarting the service
3. Ensuring the `embeddings` Iceberg table has been re-partitioned for the new model
   version (per the Iceberg partition design in Section 13.2)

Serving queries with mixed model versions (query embedded with model A, candidates
indexed with model B) is undefined behaviour and must be prevented at the API boundary.

---

## 11. Alternatives Considered

### 11.1 Application-Code Enrichment and Scoring

Perform metadata joins and scoring in application code using a database client and
per-candidate point lookups.

**Why rejected:** Per-candidate point lookups serialize N database round trips for the
enrichment join. ACL logic scattered across application code is not auditable as a unit.
Scoring weights require code changes to update. Query plans are not inspectable.
DataFusion's columnar execution and optimizer address all of these directly.

### 11.2 Dedicated Reranking Microservice

Introduce a separate microservice that accepts candidate lists, fetches metadata, and
applies scoring. DataFusion is deployed inside this service.

**Why rejected:** Structurally equivalent to embedding DataFusion in the vector search
service, but adds a network hop and an independently deployable component with its own
lifecycle. The candidate list is small enough that in-process execution is appropriate.
A separate service is only justified if the reranking logic must be shared across multiple
query services — which is not the case here.

### 11.3 Spark or Trino for Post-ANN Processing

Use an existing distributed query engine for enrichment and scoring.

**Why rejected:** Spark and Trino are optimised for large-scale batch and interactive
analytics, not for sub-second query path execution on 500-row candidate sets. Session
startup latency (Spark driver, Trino coordinator) is orders of magnitude higher than
DataFusion's embedded execution. Neither engine supports the AsyncUDF pattern needed for
bi-encoder integration.

### 11.4 Vector Database with Native Metadata Filtering

Use an ANN store that natively supports metadata filtering (pre-filter or filtered HNSW)
to eliminate the DataFusion enrichment layer.

**Why rejected:** Native metadata filtering in ANN stores handles simple scalar predicates
well but does not support multi-table joins, complex ACL predicates over array columns,
or multi-signal score fusion. The Lakehouse remains the authoritative source for document
metadata, author attributes, and classification data — these cannot be duplicated into the
ANN store without introducing a synchronization problem. DataFusion over Iceberg is the
correct home for this logic.

---

## 12. Open Questions

| ID | Question | Owner | Resolution Deadline |
|---|---|---|---|
| OQ1 | ANN store result format — does the ANN store expose results as an Arrow-compatible structure, or does a serialization step occur at the Stage 2 boundary? The answer affects Stage 2 implementation cost. | TBD | Before Phase 1 |
| OQ2 | Iceberg catalog type — REST catalog or Hive Metastore? Affects the catalog registration in the `SessionContext` startup sequence. | TBD | Before Phase 1 |
| OQ3 | Session strategy — Strategy B (child context) vs. Strategy C (pool)? Requires a load test to determine whether child context clone cost is measurable at production concurrency. | TBD | Before Phase 1 |
| OQ4 | Stage 4b implementation — bi-encoder (external HTTP, text-in) vs. dot product UDF (SIMD, embedding-in)? The dot product is faster but may not improve recall over Stage 4a. Requires empirical evaluation against a recall dataset. | TBD | Before Phase 3 |
| OQ5 | Over-retrieval ratios — the recommended ratios (N/M=5, M/P=5, P/K=2–4) have not been validated against the production corpus and query distribution. Who owns the recall evaluation set and on what cadence is it refreshed? | TBD | Before Phase 4 |
| OQ6 | Degraded mode for Stage 4b/4c failures — is it acceptable to serve Stage 4a output when the bi-encoder or cross-encoder service is unavailable? This must be a deliberate product decision, not a silent fallback. | TBD | Before Phase 3 |

---

## 13. Appendix

### 13.1 Glossary

| Term | Definition |
|---|---|
| **ANN** | Approximate Nearest Neighbour. A class of algorithms that retrieve vectors similar to a query vector with high recall but without exhaustive search. |
| **MemTable** | A DataFusion in-memory table backed by one or more Arrow `RecordBatch` instances. Used here to hold the small candidate set as the build side of hash joins. |
| **SessionContext** | The DataFusion entry point that holds the catalog, UDF registry, and runtime configuration. Shared across requests; candidates are registered on a per-request child context. |
| **AsyncScalarUDF** | A DataFusion UDF that returns a `Future`. Allows the UDF to await an external call (HTTP, gRPC) without blocking the executor thread pool. Available from DataFusion 37. |
| **FixedSizeListArray** | An Arrow array type where each row is a fixed-length list of a primitive type. Used to represent embedding vectors of a fixed dimension. |
| **Pushdown** | A query optimization where a filter or projection is moved earlier in the execution plan — into the storage layer (Parquet reader or Iceberg file scanner) — to reduce the volume of data read. |
| **Build side** | In a hash join, the smaller input that is loaded into a hash map. The larger input (probe side) is scanned and matched against the hash map. DataFusion always selects the `candidates` MemTable as the build side automatically. |
| **Over-retrieval** | Retrieving more than K candidates at each pipeline stage to ensure the true top-K is not lost due to imperfect score ordering at earlier stages. |
| **RRF** | Reciprocal Rank Fusion. A rank aggregation method that combines ranked lists without requiring score normalization. Used in the Real-Time Insert RFC for staging tier + main index merge; referenced here as a candidate for the staging tier query path extension. |

### 13.2 Iceberg Table Schema Requirements

The following columns are required by the enrichment and scoring stages. Additional
columns are domain-specific and left to the implementer. Columns marked † are required
for specific stages only — see the stage specification for conditionality.

#### `embeddings`

| Column | Type | Required by | Notes |
|---|---|---|---|
| `chunk_id` | `STRING` | Stage 2 join | Primary join key to `candidates.id` |
| `doc_id` | `STRING` | Stage 3 join | Foreign key to `documents` |
| `chunk_text` | `STRING` | Stage 4b (bi-encoder) | Input to bi-encoder UDF |
| `embedding` | `ARRAY<FLOAT>` | Stage 4b (dot product) † | `FixedSizeList<f32>[D]`; D is model-dependent |
| `embedding_model_version` | `STRING` | Partition key | Enables model version isolation |
| `created_at` | `TIMESTAMP` | Stage 4a recency | Partition key; enables time-based pruning |

Partition by `(embedding_model_version, months(created_at))`.

#### `documents`

| Column | Type | Required by | Notes |
|---|---|---|---|
| `id` | `STRING` | Stage 3 join | |
| `author_id` | `STRING` | Stage 3 join | Foreign key to `authors` |
| `created_at` | `TIMESTAMP` | Stage 4a | Recency decay input |

#### `authors`

| Column | Type | Required by | Notes |
|---|---|---|---|
| `id` | `STRING` | Stage 3 join | |
| `reputation_score` | `FLOAT` | Stage 4a | Must be documented with scale (e.g. 0–100) |
| `is_verified` | `BOOLEAN` | Stage 4a | |

#### `classifications`

| Column | Type | Required by | Notes |
|---|---|---|---|
| `doc_id` | `STRING` | Stage 3 join | |
| `sensitivity_label` | `STRING` | Stage 3 ACL | Must have Parquet row group statistics for pushdown |

#### `access_control`

| Column | Type | Required by | Notes |
|---|---|---|---|
| `doc_id` | `STRING` | Stage 3 join | |
| `allowed_teams` | `ARRAY<STRING>` | Stage 3 ACL | Unnested in `WHERE` predicate |

### 13.3 Configuration Schema

The following groups define what must be externalized. Format (TOML, YAML, env vars) is
implementation-specific.

```
[ann]
top_n                       # N: candidate count from ANN store

[datafusion]
batch_size                  # RecordBatch row count; tune for candidate set cardinality
target_partitions           # executor thread count; tune for available cores
session_strategy            # "child" | "pool"
pool_size                   # only when session_strategy = "pool"

[stage4b]
implementation              # "biencoder" | "dot_product"
alpha                       # bi_score weight in combined_score; (1-alpha) = fusion_score weight
top_m                       # M: output cardinality of Stage 4a / input to Stage 4b

[stage4c]
top_p                       # P: output cardinality of Stage 4b / input to Stage 4c
default_top_k               # K: default result count if not specified per request

[scoring.weights]
# one float per signal; must sum to 1.0; validated at startup
vector_score                = float
rrf_score                   = float
recency_score               = float
author_score                = float
# additional domain-specific signals added by implementer

[scoring.recency]
grace_period_days           = int     # flat window before exponential decay begins
decay_lambda                = float   # exponential decay rate λ

[model.biencoder]
endpoint                    = string
timeout_ms                  = int
degraded_mode_fallback      = bool    # serve Stage 4a output on failure if true

[model.cross_encoder]
endpoint                    = string
timeout_ms                  = int
degraded_mode_fallback      = bool
```

### 13.4 Implementation Phases

| Phase | Deliverable | Acceptance criteria |
|---|---|---|
| 1 | `SessionContext` + Iceberg catalog registration | All required Iceberg tables queryable; auth via Workload Identity Federation |
| 2 | Candidate `MemTable` registration | 500-row MemTable registers in < 1 ms; concurrent registration does not interfere |
| 3 | Enrichment SQL (Stage 3) | ACL filter verified; all three pushdowns confirmed via `EXPLAIN ANALYZE` |
| 4 | Score fusion SQL (Stage 4a) | All signal scores in [0, 1]; weight sum validation fires on misconfiguration; output ordered correctly |
| 5 | Sync distance UDFs | Correctness verified against non-SIMD reference; throughput measured over 500-row batch |
| 6 | Async bi-encoder UDF (Stage 4b) | Single HTTP call per RecordBatch confirmed via request log; error propagation tested |
| 7 | Cross-encoder materialize-then-call (Stage 4c) | Correct final ordering; latency within query budget |
| 8 | Full pipeline orchestration | End-to-end query executes; all stage metrics emitted; query plan logged |
| 9 | GKE deployment + KEDA autoscaling | Service autoscales under synthetic load; rolling restart does not drop in-flight queries |
