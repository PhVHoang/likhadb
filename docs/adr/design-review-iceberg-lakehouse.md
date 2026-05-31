# Design Review: Iceberg-Native Architecture and the Log-as-Truth Model

## Overview

This document captures an architectural review of LikhaDB's storage design — specifically
how it maps to the mental model **"the log is the single source of truth; everything else
is a materialized view derived from it"** — and an honest assessment of the strengths,
tensions, and a proposed resolution for the WAL-Iceberg coexistence problem.

---

## Does LikhaDB Follow the Log-as-Truth Model?

Yes, at two nested layers.

**Layer 1 — Local (current state).** The WAL in `crates/likhadb-persist/src/wal/` is the
source of truth. Every mutation (`Insert`, `Delete`, `CreateCollection`) is appended to
`wal.log` before anything touches memory. The in-memory HNSW/IVF/FlatIndex, the FTS index
(Tantivy), and the MetaStore are all derived views. On recovery, the last snapshot is
loaded and WAL entries with LSN > snapshot LSN are replayed in order.

**Layer 2 — Lakehouse (target state).** The Iceberg catalog on object storage is the
ultimate source of truth. ARCHITECTURE.md states this explicitly:

> *"The source of truth for embeddings, metadata, and business data is the Iceberg catalog
> on cloud object storage. LikhaDB's in-memory index is a query accelerator over that data,
> not an independent store."*

The `feat/iceberg-integration-minio` branch is the transition point — wiring up the second
layer so the Iceberg table, not the local WAL, becomes the durability anchor, and
everything local becomes a query accelerator (materialized view) over it.

---

## What "Iceberg Is the Source of Truth" Means

In the target state, embeddings don't primarily live in LikhaDB's local storage. They live
in Iceberg tables on object storage (S3/GCS/ADLS), partitioned by embedding model version
and ingestion time. The ANN index is a derived acceleration structure built from those
tables and rebuildable from them on a cold start.

The design principle is stated in ARCHITECTURE.md as: **"LikhaDB should not own the data."**

---

## Will Iceberg Replace the WAL?

Not wholesale, but Iceberg takes over the WAL's durability job, leaving the WAL as a
short-lived buffer.

From ARCHITECTURE.md:

> *"The WAL's role shrinks to covering the gap between Iceberg ingestion events and the
> live index — a much shorter window than today."*

The RFC (`rfc/rfc_realtime_insert_vectordb.md`) makes this concrete. In the future insert
path, writes go directly to the Iceberg staging tier. The Iceberg append commit *is* the
durability guarantee — Iceberg uses append-only Parquet files plus atomic snapshot commits,
giving the same log-as-truth property, but owned by the lakehouse rather than LikhaDB.

| | Current | Target |
|---|---|---|
| Durable record | WAL (`wal.log`) | Iceberg staging tier |
| WAL role | Primary source of truth | Gap filler (short window only) |
| Cold start | Replay WAL from last snapshot | Rebuild index from Iceberg catalog |
| ANN index | Derived from WAL | Derived from Iceberg tables |

In the fully realised design, the WAL disappears as a **correctness mechanism**. Iceberg
provides atomicity, durability, sequencing, and snapshot isolation natively — the exact
properties the WAL was supplying locally. It could survive as a write-latency optimisation
(batching fast local writes before flushing to Iceberg), but that is a different role with
a different contract, not the current source-of-truth role.

---

## Recovery in the Iceberg Model

### How recovery should work

In the target state, two Iceberg tables together contain everything needed to reconstruct
in-memory state after a crash:

1. **Main IVF index table** — the pre-built index structure (centroids, inverted lists, PQ
   codebooks) stored as an Iceberg snapshot. Loading it directly reconstructs the bulk
   index without retraining.
2. **Staging tier** — an Iceberg table with a `merge_status` column
   (`pending | merging | merged`). Scanning `WHERE merge_status = 'pending'` yields every
   vector acknowledged to clients but not yet promoted to the main index.

Recovery sequence:

```
1. Connect to Iceberg catalog
2. Load latest committed IVF index snapshot    ← pre-built structure, not raw vectors
3. Scan staging tier WHERE merge_status = 'pending'
4. Load pending vectors into in-memory staging flat buffer
5. Ready to serve — no WAL replay required
```

Iceberg's own commit atomicity ensures both tables are always internally consistent: a
mid-write crash leaves an uncommitted snapshot that is never visible to readers.

This is faster than WAL replay for large datasets. WAL replay is sequential and proportional
to write history; Iceberg recovery is parallel (Parquet column projection, partition
pruning) and proportional only to the size of the pending staging tier, which is bounded.

### What the current code actually does

The current Iceberg integration (`crates/likhadb-lakehouse/src/iceberg_io.rs`) implements
`import_iceberg` as a **bulk import** path — it scans raw vectors from an Iceberg table
and inserts them row by row into the in-memory index. Recovery is still entirely
WAL-based: `snapshot.bin` (bincode-serialized `ManagerSnapshot`) plus WAL replay via
`apply_op` in `crates/likhadb-persist/src/wal/recovery.rs`. Iceberg plays no role in
recovery today.

### The gap

For Iceberg-based recovery to work, the codebase still needs:

1. **Index serialization to Iceberg.** The IVF structure must be serializable to Parquet
   columns and written as an Iceberg snapshot by the merge job. The existing `IndexSnapshot`
   in `crates/likhadb-index/src/snapshot.rs` serializes to bincode for local disk only.
2. **Index deserialization from Iceberg.** A startup path that loads a pre-built IVF
   snapshot from Iceberg rather than from `snapshot.bin`.
3. **Staging tier with `merge_status` tracking.** The RFC specifies this table; it has not
   been implemented.
4. **HNSW remains problematic.** HNSW's pointer-graph structure does not serialize
   naturally to columnar Parquet. It works today because the graph lives entirely in memory
   and is checkpointed to a local binary file — a mechanism that does not translate to the
   Iceberg model.

---

## Design Assessment

### What Is Genuinely Good

**The "don't own the data" principle solves a real problem.** Traditional vector databases
create a synchronization tax: ETL pipelines, eventual consistency, duplicate storage.
Treating the ANN index as a rebuildable cache over Iceberg eliminates that class of
operational problems.

**Iceberg snapshot isolation as the consistency primitive is elegant.** The merge job
writes a new index snapshot, validates it, then atomically swaps the catalog pointer.
Readers pinned to the old snapshot are never interrupted. This gives MVCC-like reader
isolation without implementing it from scratch.

**The LSM-inspired tiered design is well-understood.** Staging tier (memtable) + immutable
main IVF index (SSTable) + data-driven compaction (merge job) is a proven pattern. LanceDB
and Milvus use variants of exactly this.

**Separating recall from relevance is architecturally clean.** The ANN index does one
thing: return top-N candidates fast. All business logic — enrichment, ACL enforcement,
multi-signal scoring, reranking — runs downstream in DataFusion. Each layer is
independently tunable without coupling to the other.

---

### Where It Gets Shaky

**The flat scan latency model is optimistic.** Section 7.1.2 of the RFC models staging
scan latency as a memory-bandwidth problem (~15ms at 100k vectors). But the staging tier
is an Iceberg table on object storage. S3/GCS reads add 10–100ms per Parquet file before
distance computation begins. The 60-second insert-to-searchable SLO likely holds; the
per-query latency SLO is the risk.

**Full corpus rebuild does not scale gracefully.** The RFC computes ~400 seconds on an A100
for 10M vectors. At 100M vectors that is hours. During a rebuild, the staging tier keeps
accumulating. If the rebuild takes 30 minutes and the staging hard limit triggers at 500k
vectors, there is a queue problem the design does not fully resolve.

**The drift monitor adds complexity with uncertain payoff.** Operating a stateful
Flink/Dataflow job, a Pub/Sub topic, and a distributed lock just to decide *when* to
rebuild is significant surface area. The RFC acknowledges the fallback cron fires when the
drift monitor is down — which means the cron is doing the real work under failure. Whether
JSD-triggered rebuilds actually outperform a well-tuned schedule enough to justify that
complexity is not empirically demonstrated.

**HNSW was rejected too quickly.** The stated reason is that the pointer-graph structure
is incompatible with Iceberg's columnar model. But LikhaDB already runs HNSW in-memory.
The real issue is serialization — and Weaviate, Qdrant, and Milvus all solve this by
serializing the graph to object storage in a custom format and loading it on demand. The
RFC does not engage with this option.

**The WAL retirement path is undefined.** The design states the WAL "shrinks to a gap
filler" but does not describe the transition: when does the WAL stop being the source of
truth, what happens to existing WAL-backed data, how is migration performed without data
loss, and what new invariant takes its place. See the coexistence design below.

---

## WAL and Iceberg Coexistence — Proposed Design

The current design and the RFC both leave the WAL-Iceberg boundary implicit. This section
makes it explicit.

### The core tension

| | WAL | Iceberg |
|---|---|---|
| Write latency | ~1ms (local disk) | 10–100ms (S3 round trip) |
| Durability scope | Process-local | Distributed, survives node loss |
| Queryability | None | Full SQL, partition pruning |
| Recovery | Sequential replay | Parallel structured scan |

They are not competing — they are good at different things across a timeline. The design
question is where one hands off to the other.

### The invariant: LSN watermark as the boundary

Give each layer a distinct, non-overlapping time window separated by a single value: the
**Iceberg commit watermark** — the highest LSN confirmed durably committed to the Iceberg
staging tier.

```
LSN <= watermark  →  Iceberg's territory  →  WAL entries truncated
LSN >  watermark  →  WAL's territory      →  not yet in Iceberg
```

They never overlap. Below the watermark, Iceberg is authoritative and the WAL has already
discarded those entries. Above the watermark, the WAL holds entries that Iceberg has not
yet seen.

### Write path

```
Client write
  → WAL append (synchronous, <1ms, local disk)        ← ACK to client here
  → apply to in-memory staging buffer immediately      ← visible to queries immediately
  ↓ async (batch every ~100ms or N entries)
  Background flusher
  → batch WAL entries into one Iceberg staging append
  → Iceberg commits (atomic snapshot)
  → advance watermark to flushed LSN
  → truncate WAL up to watermark
```

ACK happens at WAL speed, not Iceberg speed. New vectors are searchable immediately via
the in-memory staging buffer. Iceberg durability follows asynchronously. The WAL stays
small — it holds only seconds of data, not unbounded write history.

This is a direct improvement on the RFC's current proposal (ACK after Iceberg commit),
which accepts 10–100ms write latency per insert. Here write latency is sub-millisecond;
distributed durability follows within the flush interval.

### Recovery path

On crash before Iceberg flush (WAL has entries above the watermark):

```
1. Load IVF index snapshot from Iceberg catalog       ← bulk corpus, pre-built
2. Scan staging tier WHERE merge_status = 'pending'   ← Iceberg-durable pending rows
3. Replay WAL for LSN > iceberg_watermark             ← narrow in-flight gap only
   → re-submit those entries to Iceberg staging
   → once committed, advance watermark, truncate WAL
4. Ready to serve
```

On crash after Iceberg flush (watermark = last WAL LSN): step 3 replays nothing. The WAL
is already empty above the watermark.

The WAL replay window is bounded by the flush interval — seconds of data — regardless of
total write history.

### Responsibility table

| Concern | Owner | Reasoning |
|---|---|---|
| Write ACK latency | WAL | Local disk is orders of magnitude faster than S3 |
| In-flight durability (pre-flush) | WAL | Covers the async flush window |
| Persistent durability (post-flush) | Iceberg | Distributed, survives node loss |
| Query freshness | In-memory buffer (updated on WAL write) | Vectors visible before Iceberg flush |
| Cold-start recovery (bulk) | Iceberg | Parallel scan beats sequential replay |
| Cold-start recovery (gap) | WAL replay for LSN > watermark | Narrow window only |
| Index structure (IVF centroids, inverted lists) | Iceberg | Atomic snapshot swap in merge job |
| Collection schema | Iceberg catalog | Table create/drop is catalog-level |

### Why they do not step on each other

The watermark is an exact cut, not an approximation. Anything Iceberg has committed, the
WAL has already discarded. Anything the WAL still holds, Iceberg has not yet seen. There is
no state that both claim ownership of simultaneously.

This is the same pattern used across the industry:

- **Postgres**: WAL buffers writes; heap pages are authoritative once the WAL is flushed.
  Replica lag is WAL entries the replica has not applied.
- **RocksDB**: WAL covers the memtable; SSTables are authoritative after compaction. WAL is
  truncated on flush.
- **Kafka + consumer offset**: The log is truth below the committed offset; the log holds
  unprocessed entries above it.

LikhaDB's case is the same pattern with Iceberg in the role of the heap, SSTable, or
committed-offset store.

### Watermark persistence

The watermark must itself survive crashes. The simplest placement is a metadata property
on the Iceberg staging table (`last_wal_lsn`), updated atomically as part of each staging
append commit. On recovery, step 3 reads this property to know exactly where WAL replay
should begin. No separate coordination store is required.

---

## Verdict

The design is well-suited for validating the lakehouse-native vector database concept at
moderate scale (sub-10M vectors, queries with lenient latency SLOs). The core ideas are
sound and the tradeoffs are mostly explicit.

It becomes questionable at larger scale because full corpus rebuilds get expensive, S3
latency starts dominating flat scans, and the drift monitor complexity has not proven its
value against a simpler scheduled approach. The design would benefit from:

1. **Adopt the LSN watermark coexistence model** (described above) to give WAL and Iceberg
   non-overlapping, well-defined responsibilities and resolve the undefined retirement path.
2. **A concrete end-to-end query latency budget** accounting for S3 I/O in the staging
   scan path, not just memory-bandwidth throughput.
3. **An empirical benchmark** comparing drift-triggered vs. scheduled rebuilds before
   committing to the Flink/Dataflow dependency.
4. **A re-evaluation of HNSW serialization** to object storage as an alternative to IVF-PQ
   full corpus rebuilds at scale.
5. **Implement the Iceberg recovery path** (index serialization, staging tier
   `merge_status`, startup sequence) before retiring the WAL — recovery must be validated
   end-to-end before the WAL is removed.
