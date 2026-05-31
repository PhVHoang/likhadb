# Design Review: Iceberg-Native Architecture and the Log-as-Truth Model

## Overview

This document captures an architectural review of LikhaDB's storage design — specifically
how it maps to the mental model **"the log is the single source of truth; everything else
is a materialized view derived from it"** — and an honest assessment of the strengths and
tensions in the current design.

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
truth, what happens to existing WAL-backed data, and how is migration performed without
data loss.

---

## Verdict

The design is well-suited for validating the lakehouse-native vector database concept at
moderate scale (sub-10M vectors, queries with lenient latency SLOs). The core ideas are
sound and the tradeoffs are mostly explicit.

It becomes questionable at larger scale because full corpus rebuilds get expensive, S3
latency starts dominating flat scans, and the drift monitor complexity has not proven its
value against a simpler scheduled approach. The design would benefit from:

1. A concrete end-to-end query latency budget (not just throughput) that accounts for
   S3 I/O in the staging scan path.
2. An empirical benchmark comparing drift-triggered vs. scheduled rebuilds before
   committing to the Flink/Dataflow dependency.
3. A defined migration plan for retiring the WAL as Iceberg takes over the durability role.
4. A re-evaluation of HNSW serialization to object storage as an alternative to IVF-PQ
   full corpus rebuilds.
