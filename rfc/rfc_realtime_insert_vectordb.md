# RFC: Real-Time Insert Semantics for Native Lakehouse Vector Database

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

This RFC proposes a **tiered index architecture** that brings real-time insert semantics
to a native Lakehouse vector database built on Apache Iceberg without abandoning the
IVF-PQ index as the primary search structure.

The core mechanism is an **LSM-inspired two-tier design**: new vectors land in a mutable
staging tier (exact flat search, bounded size), while the main IVF-PQ index serves the
bulk of the corpus. Queries merge results from both tiers. An asynchronous merge job,
triggered by a continuous drift monitor, promotes vectors from the staging tier into the
main index on a data-driven rather than time-based cadence.

---

## 2. Motivation

### 2.1 The Problem

The current IVF-PQ index is batch-oriented by construction. Its lifecycle is:

```
Train k-means centroids on corpus snapshot
    → assign all vectors to nearest centroid
    → build inverted lists per centroid
    → serve read-only
    → rebuild from scratch when drift accumulates
```

This creates a hard gap between insert time and search visibility. A vector inserted after
the last index build is invisible to ANN search until the next full rebuild. Rebuild
frequency is bounded by cost — a full corpus rebuild is expensive, and running it
continuously is not feasible.

### 2.2 Why This Matters

Real-time insert visibility is required for the following use cases that are currently
blocked or degraded:

- **Live document ingestion pipelines** where newly ingested content must be searchable
  within seconds to minutes, not hours.
- **Feedback loop architectures** where embedding quality signals are written back as new
  vectors and must immediately influence retrieval.
- **Multi-tenant isolation** where a tenant's newly uploaded corpus must be searchable
  before the next global rebuild window.
- **Correctness of time-sensitive queries** where a query explicitly filtered to recent
  content (`created_at > now() - interval '1 hour'`) returns an empty or incomplete
  result set because the index lags reality.

### 2.3 Why the Existing Architecture Cannot Be Patched

The IVF index cannot be incrementally updated without structural compromise:

- Appending a new vector to an existing centroid's inverted list is safe for recall only
  if the centroid is still the geometrically nearest centroid for that vector. As the
  corpus distribution drifts, this assumption breaks silently.
- There is no mechanism within IVF to detect or correct centroid drift without retraining.
- Increasing rebuild frequency reduces the latency gap but does not eliminate it, and
  increases infrastructure cost linearly.

A structural solution is required.

---

## 3. Background and Prior Art

### 3.1 LSM-Trees (LevelDB, RocksDB, Cassandra)

Log-Structured Merge-Trees solve the same tension for key-value stores: writes must be
fast and immediately visible; the underlying sorted structure (SSTable) is immutable. The
solution is a mutable in-memory buffer (memtable) that absorbs writes, with periodic
compaction into immutable levels. Reads merge results from all levels.

This RFC applies the same principle to vector indexes: a mutable flat-search staging tier
absorbs inserts; periodic merge (compaction) promotes vectors into the immutable IVF index.

### 3.2 Lance / LanceDB

LanceDB's open-source `lance` columnar format implements a staging buffer pattern. New
vectors are written to an unindexed delta segment. Queries search the delta segment via
brute force and merge with the IVF-PQ result. An async compaction job periodically merges
delta segments into the indexed main segment. This is the closest prior art to the design
proposed here.

Key difference from this RFC: Lance uses a custom columnar format. This RFC is designed
for an existing Iceberg-native Lakehouse and must work within Iceberg's snapshot and
catalog model.

### 3.3 Milvus Growing / Sealed Segments

Milvus distinguishes between **growing segments** (mutable, held in memory, brute-force
searched) and **sealed segments** (immutable, indexed, disk-resident). New inserts land
in growing segments; background jobs seal and index them. This is architecturally
equivalent to the tiered design proposed here, adapted for an external vector store rather
than a Lakehouse.

### 3.4 Streaming Database Principles

Streaming databases (Flink, RisingWave, Materialize) maintain incrementally updated
materialized views over append-only event streams. This RFC borrows two principles:

- **Incremental state maintenance** — centroid assignment statistics are maintained as
  a running aggregate over the insert stream, not recomputed from scratch on each rebuild.
- **Data-driven triggers** — rebuild is triggered by a measured drift metric crossing a
  threshold, not by a fixed schedule.

Streaming databases do not directly solve the IVF retraining problem because centroid
drift is a geometric distribution problem, not a data freshness problem. But their
principles inform the drift monitor design in Section 7.3.

---

## 4. Design Goals

| ID | Goal |
|---|---|
| G1 | New vectors are searchable within a configurable SLO (target: ≤ 60 seconds) after insert acknowledgement |
| G2 | Search recall over the full corpus (staged + indexed) does not degrade relative to the current IVF-only baseline |
| G3 | The main IVF index is never partially rebuilt — readers always see a consistent, complete index snapshot |
| G4 | Rebuild is triggered by measured index drift, not a fixed schedule |
| G5 | The design is native to Iceberg — no new storage systems are introduced |
| G6 | Insert throughput is not the bottleneck — the staging tier must absorb burst inserts without backpressure to the producer |
| G7 | The architecture supports embedding model version changes without data loss |

---

## 5. Non-Goals

| ID | Non-Goal | Rationale |
|---|---|---|
| NG1 | In-place mutation of existing IVF centroids or inverted lists | Violates Iceberg's immutability model; addressed by full rebuild with atomic swap |
| NG2 | Real-time centroid retraining on every insert | Statistically unsound for small insert batches; addressed by drift-triggered rebuild |
| NG3 | Sub-second insert-to-searchable latency | Requires HNSW or in-memory indexes; incompatible with Lakehouse-native storage |
| NG4 | Replacing the IVF-PQ index with HNSW | HNSW's pointer-graph structure is incompatible with Iceberg's columnar immutable model (see Section 11.1) |
| NG5 | Cross-embedding-model query federation | A model version change requires a full corpus re-embedding; out of scope for this RFC |

---

## 6. Proposed Design

### 6.1 Overview

The design introduces three new components alongside the existing IVF-PQ index:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Insert Path                                 │
│                                                                     │
│   Producer → [Insert API] → [Staging Tier (Iceberg)]               │
│                                   │                                 │
│                                   │ async, data-driven              │
│                                   ▼                                 │
│                          [Merge Job] → [Main IVF Index (Iceberg)]   │
│                               ↑                                     │
│                     [Drift Monitor]                                 │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                         Query Path                                  │
│                                                                     │
│   Query → [ANN Store: IVF search] ──────────────────────┐          │
│         → [DataFusion: staging flat scan] ───────────────┤          │
│                                                          ▼          │
│                                              [Merge + RRF fusion]   │
│                                              [DataFusion post-ANN]  │
└─────────────────────────────────────────────────────────────────────┘
```

**Component summary:**

| Component | Responsibility |
|---|---|
| **Staging Tier** | Iceberg table receiving all new inserts; searched via DataFusion brute-force flat scan |
| **Merge Job** | Async job that promotes staging vectors into the main IVF index via full rebuild with atomic snapshot swap |
| **Drift Monitor** | Streaming job that measures centroid assignment skew and triggers merge when drift threshold is exceeded |
| **Query Merger** | DataFusion layer that searches both tiers, merges results via RRF, and returns a unified ranked list |

### 6.2 Iceberg Snapshot Model

The design leverages Iceberg's snapshot isolation guarantees:

- The staging tier and the main index are **separate Iceberg tables** within the same
  namespace. This allows independent write lifecycles.
- The main index table is **never mutated in place**. A rebuild writes a new Iceberg
  snapshot (new data files, new manifest). The catalog pointer is atomically updated only
  after the new snapshot is complete and validated.
- Readers pinned to the old snapshot continue serving queries without interruption during
  rebuild. The old snapshot is eligible for expiry only after all active readers have
  advanced past it.

This provides the consistency guarantee stated in G3 with no reader-side coordination.

---

## 7. Component Specifications

### 7.1 Staging Tier

#### 7.1.1 Storage

The staging tier is an Iceberg table with the same schema as the main `embeddings` table,
with two additional columns:

| Column | Type | Purpose |
|---|---|---|
| `staged_at` | `TIMESTAMP WITH TIME ZONE` | Insert timestamp; used for staging tier TTL and recency queries |
| `merge_status` | `STRING` | `pending` \| `merging` \| `merged`; used by merge job for idempotent processing |

Partition by `hours(staged_at)` to enable efficient time-range scans and TTL expiry.

#### 7.1.2 Size Bound

The staging tier must be bounded to keep flat-scan latency within the query SLO. The
bound is expressed as a **maximum age** (e.g., vectors older than 24 hours must have been
merged) and a **maximum row count** (e.g., 500,000 vectors). Whichever bound is hit first
triggers an immediate merge regardless of drift monitor state.

The flat-scan latency model for the staging tier:

```
latency_ms ≈ (staging_row_count × embedding_dim × 4 bytes) / memory_bandwidth_bytes_per_ms
```

At 100,000 vectors × 1536 dim × 4 bytes = ~600 MB. Acceptable for in-memory DataFusion
flat scan on a 4-core node with ~40 GB/s memory bandwidth: ~15 ms. At 500,000 vectors
this reaches ~75 ms — the upper bound before query SLO is at risk.

The row count bound must be set conservatively below the latency cliff.

#### 7.1.3 Write Path

Insert API writes to the staging tier only. The write is acknowledged after the Iceberg
append commits. No synchronous IVF index update occurs on insert.

Insert idempotency: vectors carry a producer-assigned `chunk_id`. The staging table
enforces uniqueness via an Iceberg equality delete on `chunk_id` before append, or via
deduplication in the merge job — implementer choice based on write throughput requirements.

---

### 7.2 Merge Job

#### 7.2.1 Trigger Conditions

The merge job is triggered by any of the following, evaluated in priority order:

| Priority | Condition | Source |
|---|---|---|
| 1 (immediate) | Staging tier row count exceeds hard limit | Staging tier row count metric |
| 2 (immediate) | Staging tier maximum age exceeded | Staged_at watermark |
| 3 (data-driven) | Drift monitor emits trigger event | Drift monitor (Section 7.3) |
| 4 (safety net) | Time-based fallback interval elapsed | Cron / Dagster schedule |

The time-based fallback (priority 4) exists only as a safety net. In normal operation,
priorities 1–3 should trigger before it fires. The fallback interval should be set long
enough (e.g., 6 hours) that it never fires under normal load.

#### 7.2.2 Merge Algorithm

```
1. SNAPSHOT: Record the current staging tier Iceberg snapshot ID.
   All vectors with staged_at ≤ snapshot watermark are in scope.
   Vectors inserted after this point continue accumulating in staging.

2. READ: Scan the full corpus:
   - All vectors in the current main IVF index
   - All vectors in staging with merge_status = 'pending'
     and staged_at ≤ snapshot watermark

3. TRAIN: Run k-means on a sample of the combined corpus to produce
   new centroids. Sample size: configurable (default: min(1M, corpus_size)).
   k: configurable (default: sqrt(corpus_size)).

4. ASSIGN: Assign all corpus vectors to nearest new centroid.
   Build new inverted lists.

5. QUANTIZE: Apply Product Quantization to compress embeddings.
   PQ codebook is retrained from the same sample used in step 3.

6. WRITE: Write the new IVF-PQ index as a new Iceberg snapshot on the
   main index table. Do not update the catalog pointer yet.

7. VALIDATE: Run a recall validation query set against the new snapshot.
   Compare recall@10 against the baseline threshold (configurable, default: 0.90).
   If recall falls below threshold, abort and alert. Do not swap.

8. SWAP: Atomically update the Iceberg catalog pointer from the old
   main index snapshot to the new one.

9. MARK: Update merge_status = 'merged' for all processed staging vectors.
   These are now eligible for TTL expiry from the staging table.

10. EXPIRE: After a configurable grace period (default: 2 × query timeout),
    delete merged vectors from the staging tier via Iceberg position deletes.
```

#### 7.2.3 Idempotency

Steps 1–6 are idempotent — if the job crashes and restarts, it re-reads from the same
snapshot watermark and produces an equivalent new index. Step 8 (swap) is atomic. Step 9
is a best-effort mark; if it fails, the merge job will re-process those vectors on the
next run (they will be deduplicated in step 4 by `chunk_id`).

#### 7.2.4 Concurrency

Only one merge job instance may run at a time. A distributed lock (e.g., GCS object lock
or a Kubernetes lease) must be acquired before step 1 and released after step 9. A
concurrent trigger while a merge is in progress is queued, not dropped — it will fire
immediately after the current merge completes.

---

### 7.3 Drift Monitor

#### 7.3.1 Purpose

The drift monitor measures how well the current IVF centroids still partition the vector
space given the incoming insert stream. It emits a trigger event when centroid assignment
skew exceeds a threshold, indicating that recall is at risk if the index is not rebuilt.

#### 7.3.2 Drift Metric: Assignment Entropy

For each new vector insert, compute its nearest centroid assignment using the current
centroid set. Maintain a running histogram of assignments across all `k` centroids:

```
H(t) = { c_i: count(vectors assigned to c_i in the last window W) }
```

The drift metric is the **Jensen-Shannon divergence** between the training-time assignment
distribution and the current windowed distribution:

```
drift_score = JSD(H_train, H_current)
```

Where `H_train` is the centroid assignment distribution recorded at the last merge. JSD
is bounded in [0, 1]. A score of 0 means no drift; a score approaching 1 means the
current insert stream is landing in a completely different part of vector space than the
training distribution expected.

#### 7.3.3 Trigger Threshold

Emit a merge trigger event when `drift_score > threshold` for two consecutive measurement
windows (to avoid triggering on transient spikes). The threshold is configurable
(default: 0.15, which corresponds to a moderate distribution shift detectable before
recall degrades significantly).

#### 7.3.4 Implementation

The drift monitor is a stateful streaming job (Flink or Dataflow) consuming the insert
event stream (Pub/Sub topic). It maintains the running histogram in operator state and
emits trigger events to a separate Pub/Sub topic consumed by the merge job orchestrator.

State schema:

```
{
  centroid_assignment_counts: Map<centroid_id, int>,   // current window
  training_assignment_distribution: Map<centroid_id, float>,  // from last merge
  window_start: Timestamp,
  window_size: Duration,  // configurable, default: 15 minutes
}
```

The centroid set used for assignment must be the same set as in the current main IVF
index. When the main index is swapped (step 8 of merge), the drift monitor must reload
the new centroid set and reset `training_assignment_distribution` to the uniform baseline.

---

### 7.4 Query Merger

#### 7.4.1 Dual-Tier Search

At query time, both tiers are searched concurrently:

```
parallel {
    ivf_results   = ann_store.search(query_vector, top_k = N)     // IVF main index
    flat_results  = datafusion.flat_scan(staging_tier, query_vector, top_k = N)
}

merged = reciprocal_rank_fusion(ivf_results, flat_results)
final  = merged[:top_k]
```

The flat scan of the staging tier is a DataFusion query using the `dot_product` or
`cosine_similarity` UDF over the staging Iceberg table, as described in the DataFusion
integration design.

#### 7.4.2 Over-Retrieval Requirement

To ensure that the merged top-K is correct, each tier must retrieve more than K candidates
before merging. If the final desired result is top-K, each tier should retrieve top-N
where N ≥ K × over_retrieval_factor (default: 5). This accounts for the possibility that
the true top-K is distributed across both tiers in an unknown ratio.

The over-retrieval factor should be tuned empirically against a recall evaluation set.

#### 7.4.3 Staging Tier Scan Optimisation

To minimise flat scan latency on the staging tier:

- **Time filter first**: if the query carries a recency filter (e.g., `created_at > t`),
  push it into the staging scan before the distance UDF is evaluated. The staging tier
  is partitioned by `hours(staged_at)`, so this translates to Iceberg partition pruning.
- **Projection**: only select `chunk_id`, `embedding`, and `staged_at` for the flat scan.
  All other enrichment columns are fetched from the main Iceberg tables in the downstream
  DataFusion enrichment join (as described in the DataFusion integration design).
- **Parallel execution**: the IVF search and staging flat scan are issued concurrently.
  The merger awaits both before producing output.

---

## 8. Data Flow

### 8.1 Insert Path (steady state)

```
Producer
  │
  │ insert(chunk_id, embedding, metadata)
  ▼
Insert API
  │
  │ Iceberg append (staging tier)
  │ staged_at = now(), merge_status = 'pending'
  ▼
Staging Tier (Iceberg)
  │
  │ Pub/Sub event: { chunk_id, centroid_assignment }
  ▼
Drift Monitor (Flink)
  │
  │ [if drift_score > threshold for 2 windows]
  │ emit trigger event
  ▼
Merge Job Orchestrator (Dagster)
  │
  │ acquire distributed lock
  │ execute merge algorithm (Section 7.2.2)
  │ atomic snapshot swap
  │ release lock
  ▼
Main IVF Index (Iceberg) — new snapshot live
```

### 8.2 Query Path (steady state)

```
Query
  │
  ├──────────────────────────────────────┐
  │ IVF search (ANN store)               │ flat scan (DataFusion + staging Iceberg)
  │ top-N candidates                     │ top-N candidates
  ▼                                      ▼
  └──────────────────────┬───────────────┘
                         │ RRF merge
                         ▼
                  Merged top-N candidates
                         │
                         ▼
             DataFusion enrichment + score fusion
             (as per DataFusion integration design)
                         │
                         ▼
                    Final top-K results
```

### 8.3 Merge Job Execution (during rebuild)

```
During rebuild, the staging tier continues to accept inserts.
Queries continue to search:
  - Old main IVF snapshot (still pinned by readers via Iceberg snapshot isolation)
  - Current staging tier (including vectors inserted after merge job started)

After atomic swap:
  - New main IVF snapshot becomes the query target
  - Staging tier contains only vectors inserted after the merge snapshot watermark
  - No query gap: vectors in [watermark, now] are in staging tier, still searchable
```

---

## 9. Failure Modes and Mitigations

| Failure | Impact | Mitigation |
|---|---|---|
| **Merge job crash mid-rebuild** | Partial new index written but not swapped | Job restarts from step 1 using same snapshot watermark; idempotent. Partial write is an uncommitted Iceberg snapshot — not visible to readers. |
| **Recall validation fails (step 7)** | Bad index not promoted | Alert fired; old index remains in service; on-call investigates training sample quality or k value. |
| **Staging tier grows beyond hard limit** | Flat scan latency degrades | Priority-1 trigger fires immediate merge. If merge is already running, the concurrent trigger is queued. Producers are not backpressured. |
| **Drift monitor lag or outage** | Drift-triggered merges stop; fallback cron fires instead | Priority-4 safety net (Section 7.2.1). Drift monitor outage is non-critical. |
| **ANN store unavailable** | IVF search half of dual-tier query fails | Query falls back to staging-only flat scan with degraded recall. This is acceptable if staging tier is bounded and the fallback is explicitly logged as a degraded-mode event. |
| **Distributed lock not released (merge job crash after lock acquisition)** | Next merge job blocked | Lock TTL must be set to max(expected_merge_duration × 2). Lock holder must heartbeat; lock is released on heartbeat timeout. |
| **Centroid set version mismatch** | Drift monitor assigns vectors using stale centroids | Drift monitor subscribes to a `index_swapped` event emitted at step 8. On receipt, it reloads centroids and resets histogram. The window during reload is skipped (not counted toward drift threshold). |

---

## 10. Operational Concerns

### 10.1 Merge Job Compute Sizing

Full corpus k-means training is the dominant cost. Approximate compute model:

```
training_cost ≈ corpus_size × k × embedding_dim × n_iterations × bytes_per_flop
```

For 10M vectors, k=3162 (sqrt(10M)), dim=1536, 20 iterations: ~4 × 10¹² FLOPs.
On a node with 10 TFLOPS (e.g., an A100 at reduced precision): ~400 seconds.

The merge job should run on a GPU-accelerated Cloud Run Job or GKE batch node. It must
not run on the same node pool as query-serving workloads.

### 10.2 Recall Validation Dataset

Step 7 of the merge algorithm requires a held-out recall evaluation set. This set must:

- Contain at least 1,000 (query, ground_truth_ids) pairs
- Be representative of the production query distribution
- Be stored in Iceberg and versioned alongside the index
- Not be drawn from the staging tier (to avoid contamination)

The recall@10 threshold (default: 0.90) must be agreed upon and documented before the
first production merge.

### 10.3 Embedding Model Version Handling

When the embedding model is upgraded (e.g., from version A to version B):

1. All new inserts use model B and land in the staging tier tagged with
   `embedding_model_version = 'B'`.
2. The staging tier is queryable immediately via flat scan, but only for model-B queries.
3. Model-A IVF index serves model-A queries unchanged.
4. A full re-embedding job must re-encode all model-A vectors using model B, writing
   them to a new staging batch.
5. Once re-embedding is complete, a merge job produces a model-B IVF index from the
   combined corpus.
6. Model-A index and model-A vectors are deprecated after a migration window.

This is explicitly out of scope for this RFC as a real-time concern (see NG5). It is
included here to confirm the tiered design does not block this migration path.

### 10.4 Monitoring and Alerting

The following alerts must be configured:

| Alert | Condition | Severity |
|---|---|---|
| Staging tier size warning | row_count > 0.7 × hard_limit | Warning |
| Staging tier size critical | row_count > 0.9 × hard_limit | Critical |
| Merge job duration | duration > 2 × p95_historical | Warning |
| Recall validation failure | recall@10 < threshold | Critical (page on-call) |
| Drift monitor lag | consumer_lag > 2 × window_size | Warning |
| Distributed lock TTL approach | lock_age > 0.8 × TTL | Warning |
| Staging scan latency | p95 > 100ms | Warning |

---

## 11. Alternatives Considered

### 11.1 Replace IVF with HNSW

HNSW supports real-time inserts natively — new nodes are wired into the proximity graph
without retraining. This would eliminate the staging tier and drift monitor entirely.

**Why rejected:** HNSW's graph structure (pointer-linked nodes with mutable neighbor
lists) is incompatible with Iceberg's immutable columnar model. HNSW must live in a
sidecar service (e.g., an external vector store), which abandons the native Lakehouse
integration goal (G5). The tiered IVF design preserves Iceberg-native storage for the
full corpus while accepting bounded insert latency.

### 11.2 Increase IVF Rebuild Frequency

Run the full rebuild on a fixed short interval (e.g., every 15 minutes) without a staging
tier or drift monitor.

**Why rejected:** Does not meet G1 (inserts may be invisible for up to the rebuild
interval). Does not meet G4 (schedule-driven, not data-driven). Linear infrastructure
cost scaling with rebuild frequency. Does not handle burst inserts gracefully.

### 11.3 Online k-means Centroid Update (No Full Rebuild)

Update centroids incrementally using a streaming mean update formula. Avoids full rebuilds.

**Why rejected:** Incremental centroid updates are statistically sound only for smooth,
gradual distribution shifts. They fail silently for sudden domain shifts (new corpus
domain, new embedding model). The resulting centroid positions are not reproducible from
the data alone (they depend on insertion order), making recall validation and auditing
difficult. Full rebuild from a corpus snapshot is reproducible, auditable, and testable
via the recall validation step.

### 11.4 Dual-Index Without Merge (Permanent Two-Tier)

Keep a permanent small IVF index for recent vectors and a large IVF index for the bulk
corpus. Never merge — just let recent vectors accumulate in the small index and rebuild
it independently.

**Why rejected:** The small recent index must itself be rebuilt when it grows, reintroducing
the same problem at smaller scale. Two IVF indexes must be kept in sync on model version
changes. Query complexity doubles permanently. The merge-based design reduces to a
single-index steady state after each merge, which is simpler to operate and reason about.

---

## 12. Open Questions

| ID | Question | Owner | Resolution Deadline |
|---|---|---|---|
| OQ1 | What is the acceptable insert-to-searchable SLO? The design targets 60 seconds but this depends on staging tier flush frequency, which has not been load-tested. | TBD | Before Phase 2 |
| OQ2 | Should the staging tier flat scan be served by DataFusion in the query path, or by a separate in-memory cache for the most recent N vectors? A cache would reduce flat scan latency but introduces a new stateful component. | TBD | Before Phase 1 |
| OQ3 | What is the correct drift threshold (default: 0.15 JSD)? This requires calibration against a corpus sample and recall regression data that does not yet exist. | TBD | Before Phase 3 |
| OQ4 | Should the merge job use GPU-accelerated k-means (cuML, FAISS GPU) or CPU-only (FAISS CPU)? The answer depends on available node types in the GKE batch pool and acceptable merge duration. | TBD | Before Phase 2 |
| OQ5 | How is the recall validation dataset maintained as the corpus evolves? Ground truth IDs for evaluation queries may become stale if documents are deleted or re-embedded. | TBD | Before Phase 2 |
| OQ6 | The distributed lock for merge job concurrency control — GCS object lock or Kubernetes Lease? GCS is simpler but has higher latency; K8s Lease integrates with existing Kopf/operator patterns. | TBD | Before Phase 1 |

---

## 13. Appendix

### 13.1 Glossary

| Term | Definition |
|---|---|
| **IVF-PQ** | Inverted File Index with Product Quantization. A two-stage ANN index: coarse quantization via k-means centroids, fine quantization via product quantization codebooks. |
| **Staging Tier** | The mutable Iceberg table receiving all new vector inserts prior to merge into the main IVF index. |
| **Merge Job** | The async background job that rebuilds the IVF index from the combined corpus (main index + staging tier) and atomically promotes the new index via Iceberg snapshot swap. |
| **Drift Monitor** | The stateful streaming job that measures centroid assignment entropy over the insert stream and emits merge triggers when drift exceeds a threshold. |
| **JSD** | Jensen-Shannon Divergence. A symmetric, bounded (0–1) measure of divergence between two probability distributions. Used here to measure centroid assignment drift. |
| **RRF** | Reciprocal Rank Fusion. A rank aggregation method that combines ranked lists from multiple sources without requiring score normalization. |
| **Over-retrieval** | Retrieving more than K candidates from each tier before merging, to ensure the true top-K is not lost due to imperfect distribution across tiers. |
| **Snapshot isolation** | Iceberg's guarantee that a reader pinned to a snapshot sees a consistent, complete view of the data as of that snapshot, regardless of concurrent writes. |

### 13.2 Recall Impact of Staging Tier Size

The following table shows the expected recall@10 degradation as a function of the fraction
of the corpus in the staging tier (un-indexed), assuming the rest is in IVF-PQ with
recall@10 = 0.95 and over-retrieval factor = 5. Values are theoretical estimates pending
empirical validation.

| Staging fraction | Expected recall@10 (merged) |
|---|---|
| 1% | ~0.95 (negligible degradation) |
| 5% | ~0.94 |
| 10% | ~0.93 |
| 20% | ~0.91 |
| 30% | ~0.89 (approaching threshold) |

The hard row count limit on the staging tier should be set to keep staging fraction below
10% of the total corpus under normal load.

### 13.3 Implementation Phases

| Phase | Deliverable | Dependencies |
|---|---|---|
| 1 | Staging tier Iceberg table + Insert API writing to staging | Iceberg catalog, Insert API |
| 2 | DataFusion flat scan of staging tier integrated into query path | DataFusion integration (existing RFC) |
| 3 | Merge job: steps 1–8, manual trigger only, no drift monitor | Staging tier, FAISS or equivalent k-means |
| 4 | Recall validation framework: evaluation dataset + step 7 integration | Merge job |
| 5 | Drift monitor: Flink/Dataflow job, centroid assignment histogram, JSD metric | Staging tier Pub/Sub topic |
| 6 | Automated merge triggers: drift monitor → orchestrator → merge job | Drift monitor, Merge job |
| 7 | Operational hardening: distributed lock, alerting, runbook | All prior phases |
