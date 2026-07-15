# RFC: Incremental Index Maintenance from Iceberg Snapshot Deltas

| Field | Value |
|---|---|
| **RFC ID** | TBD |
| **Status** | Draft (v1) |
| **Author(s)** | TBD |
| **Created** | 2026-06-25 |
| **Last Updated** | 2026-07-15 (`iceberg-rs` maturity reassessed — §3, §12.1) |
| **Target Milestone** | TBD |

---

## Table of Contents

1. [Summary](#1-summary)
2. [Current State and Motivation](#2-current-state-and-motivation)
3. [Background: Iceberg Incremental Scans](#3-background-iceberg-incremental-scans)
4. [Design Goals and Non-Goals](#4-design-goals-and-non-goals)
5. [Proposed Design](#5-proposed-design)
6. [Component Specifications](#6-component-specifications)
7. [Data Flow](#7-data-flow)
8. [The Tombstone / Drift Problem and Compaction](#8-the-tombstone--drift-problem-and-compaction)
9. [Failure Modes and Watermark Interlock](#9-failure-modes-and-watermark-interlock)
10. [Operational Concerns](#10-operational-concerns)
11. [Alternatives Considered](#11-alternatives-considered)
12. [Open Questions](#12-open-questions)
13. [Appendix](#13-appendix)

---

## 1. Summary

This RFC closes the gap between LikhaDB's headline promise — *"reads and writes directly
from Parquet, S3/GCS, and Iceberg — no ETL pipeline required"* — and what the code actually
does today. Right now the in-memory index reflects **only** writes that arrived through
LikhaDB's own REST/gRPC API and went through the WAL → staging table. Rows that another
lakehouse engine (Spark, Trino, dbt) writes to or deletes from a **source embedding table**
are invisible to ANN/FTS search until a full process restart and rebuild — and even then,
only if recovery is pointed at that table.

We propose **incremental index maintenance**: a per-collection background task that diffs a
registered source Iceberg table's snapshots (`last_applied_snapshot_id → current_snapshot`),
materialises the added and deleted rows via an Iceberg incremental scan, and applies them to
the live `VectorIndex` / `FtsIndex` through the existing `insert` / `delete` contract — with
no full rebuild in the steady state.

The design composes with, and does not replace, two existing pieces:

- **The WAL → staging path** (`rfc_realtime_insert_vectordb.md`,
  `docs/adr/design-review-iceberg-lakehouse.md`) stays the low-latency path for writes that
  originate *inside* LikhaDB.
- **Puffin index checkpoints** (`rfc_puffin_backed_index_snapshots.md`) become the durable
  record of *which source snapshot the index reflects*, via one new blob property
  (`likhadb.source_snapshot_id`). That RFC names "incremental index updates
  (manifest-diff-driven merging)" as an explicit non-goal / future RFC. **This is that
  RFC.**

The central hard problem this RFC must own is not ingestion — it is **delete handling**.
`HnswIndex::delete` is tombstone-only (`crates/likhadb-index/src/hnsw.rs:361`): deleted nodes
stay in the graph forever and the `deleted: HashSet` grows without bound. A delete-heavy
source feed degrades recall and leaks memory unless we add a compaction trigger. §8 owns
this.

---

## 2. Current State and Motivation

### 2.1 What the code actually does today

Verified against the current branch:

- **Index population is WAL-sourced only.** `open_with_iceberg`
  (`crates/likhadb-lakehouse/src/iceberg_recovery.rs:34`) loads from
  `likhadb_index_snapshots` (empty — see the puffin RFC), then `scan_pending` over each
  collection's **staging** table, then full WAL replay from LSN 0. Every row it applies
  originated from a LikhaDB `Insert`/`Delete` op. There is no path that reads a
  *non-staging* Iceberg table into an index.
- **Deletes are tombstones in the index.** `HnswIndex::delete`
  (`crates/likhadb-index/src/hnsw.rs:361`) inserts the id into a `deleted: HashSet`, keeps
  the node and its edges as traversal waypoints, and patches the entry point if it was
  tombstoned. There is no compaction; `len()` is "live count" but the graph never shrinks.
  `HnswIndex::insert` overwrites by tombstoning the old node and appending a new one
  (`hnsw.rs:282`), so churn also grows the graph monotonically.
- **IVF inserts drift.** `IvfIndex::insert` (`crates/likhadb-index/src/ivf.rs:565`) assigns
  to the nearest *existing* centroid; `delete` (`ivf.rs:607`) swap-removes from the inverted
  list. Centroids are trained once at build and never re-trained, so a long incremental feed
  silently degrades cluster quality.
- **No incremental Iceberg scan exists.** Recovery does a full table scan
  (`scan_pending`). There is no snapshot-to-snapshot diff anywhere in `likhadb-lakehouse`.

So today a vector that Spark appends to the embeddings table is never searchable through
LikhaDB. The "no ETL" claim holds for the *storage format* (we read/write Parquet/Iceberg)
but not for the *freshness contract* (we don't see other engines' writes).

### 2.2 Why this is the right next investment

1. **It makes the differentiator real.** Every standalone vector DB can ingest via its own
   API. The thing only a lakehouse-native engine can do is reflect writes authored by the
   rest of the lakehouse without a sync pipeline. That is the moat. Today it is aspirational.
2. **It is the missing half of recovery, generalised.** The puffin RFC already needs a
   "scan staging rows with `lsn > checkpoint_lsn`" delta path. Generalising "apply a delta
   between two known points" from *staging LSN ranges* to *arbitrary source-table snapshot
   diffs* is a small conceptual step on top of machinery we are already building.
3. **It forces us to confront tombstone GC**, which is a latent correctness/memory bug today
   regardless of this feature (any long-lived collection with churn degrades). Owning it here
   pays down existing debt.

### 2.3 What this RFC is *not* trying to be

- Not sub-second insert visibility. That is the two-tier staging design in
  `rfc_realtime_insert_vectordb.md`. This task operates on a coarse, snapshot-driven cadence
  (seconds-to-minutes), matched to how often other engines commit Iceberg snapshots.
- Not bidirectional. We **read** source-table deltas into the index. We do not write the
  index's view back into the source table. The source table is owned by whoever writes it.
- Not a new query path. Once a delta is applied, queries are unchanged.

---

## 3. Background: Iceberg Incremental Scans

Iceberg snapshots form a linear (per-branch) log. Each snapshot references manifests that
list data files added or removed relative to its parent. An **incremental append scan**
between `from_snapshot_id` (exclusive) and `to_snapshot_id` (inclusive) yields exactly the
data files whose `ManifestEntry` status is `ADDED` in that range — i.e. the new rows — without
re-reading the unchanged bulk of the table.

Two row-removal mechanisms exist in Iceberg v2 and both must be handled:

| Mechanism | What it means | How LikhaDB maps it |
|---|---|---|
| **Data-file delete** (`DELETED` manifest entry) | A whole data file dropped between snapshots (e.g. partition overwrite, compaction) | Every live row id that was in that file is a candidate delete; reconcile against what is currently in the index |
| **Row-level deletes** (position / equality delete files) | v2 merge-on-read deletes specific rows | Resolve to the affected row ids and issue `index.delete(id)` |

**`iceberg-rs` maturity — reassessed 2026-07-15 (Open Question 12.1):** LikhaDB currently
depends on `iceberg-rs` **0.4**, whose scan layer has **no** incremental (snapshot-range) scan
and **no** delete-file resolution (its `TableScan` errors on any delete file). That is why v1
hand-rolls the snapshot diff (§6.1) and ships append + data-file-drop deletes only. Two facts
have since changed upstream and should drive an explicit upgrade decision:

- **Delete-file resolution now exists upstream.** `iceberg-rs` added merge-on-read delete
  support across **0.8.0** (delete-file loading; position + equality deletes on the same
  `FileScanTask`) and **0.9.0** (positional + equality delete parsing). Row-level delete
  resolution — deferred in v1 as a library limitation — is therefore *available by upgrading to
  ≥ 0.9*, not fundamentally blocked.
- **Incremental scan still does not exist upstream.** The snapshot-range scan (issue #1469 /
  PR #1470) was closed **unmerged** and folded into a broader CDC changelog-scan effort. The
  hand-rolled manifest diff in §6.1 remains necessary regardless of crate version.

Latest published crate is **0.9.1** (2026-05-06); 0.10.0 is tagged upstream but not yet on
crates.io. We design the apply layer to be delete-source-agnostic so the scan layer can adopt
the upstream delete readers independently (§4 Non-Goals, §12.1).

A source table is bound to a collection by **snapshot id**, not timestamp. The
`source_snapshot_id` we persist is the exact point the index reflects, so a diff is always a
precise, replayable range — never a lossy "everything since time T" heuristic.

---

## 4. Design Goals and Non-Goals

### Goals

1. **Reflect external source-table writes in the live index** on a snapshot-driven cadence,
   without a full rebuild in steady state.
2. **Reuse the existing index contract.** Apply deltas through `VectorIndex::insert` /
   `delete` and the FTS equivalents. No new index trait methods for the happy path.
3. **Persist the applied position** as `source_snapshot_id`, bound to the same Puffin
   checkpoint that persists the index, so recovery resumes the diff instead of restarting it.
4. **Bound tombstone/drift degradation** with an explicit, observable compaction trigger
   (§8) rather than letting recall silently rot.
5. **Be safe to pause or disable.** If the maintenance task is off, the index is simply as
   fresh as its last applied snapshot. No corruption, no blocked writes.

### Non-Goals

- Sub-second freshness (→ `rfc_realtime_insert_vectordb.md`).
- Writing the index's state back to the source table.
- **Schema evolution** of the source embedding column mid-stream (dim change, metric change).
  A schema change on the source aborts maintenance for that collection with an operator-
  visible error; re-binding is a manual, full-rebuild operation.
- **Embedding-model-version cutover** semantics (new model = new partition/table). Mentioned
  as future work in §10; the snapshot-binding model makes a clean cutover possible later.
- Row-level (equality/position) delete resolution in **v1**, which ships against the current
  `iceberg-rs` **0.4** dependency (append ingestion + data-file-drop deletes only). This is a
  dependency-version limitation, **not** a design one: `iceberg-rs` ≥ 0.9 provides the delete
  readers (§3), so a follow-up that upgrades the crate can resolve row-level deletes through the
  same delete-source-agnostic apply layer. Called out, not hidden.
- Distributed / multi-writer. Single-node, single owner per collection (consistent with the
  puffin RFC §11.5).

---

## 5. Proposed Design

### 5.1 The two delta sources, unified

LikhaDB now has two streams of change feeding one in-memory index:

```
   (internal)  REST/gRPC ─► WAL ─► staging table ─┐
                                                   ├─► CollectionManager (live index)
   (external)  Spark/Trino ─► source Iceberg table ┘
                              └── this RFC: snapshot-diff ──► apply insert/delete
```

Both are "apply an ordered delta between a known point and a newer point." The internal path
keys off **WAL LSN**; the external path keys off **source snapshot id**. We unify the *apply*
side (one function turns a batch of `(id, vector, payload, is_delete)` rows into index
mutations) and keep the *source* side separate (one reads the WAL/staging, one reads an
incremental Iceberg scan).

### 5.2 Source binding

A collection opts into external maintenance by registering a **source binding**:

```rust
pub struct SourceBinding {
    pub collection: String,
    pub source_table: TableIdent,   // the externally-written Iceberg table
    pub id_column: String,          // maps to VecId
    pub vector_column: String,      // FixedSizeList<Float32, dim> (or vector_json during transition)
    pub payload_columns: Vec<String>,
}
```

Bindings are configured at collection-create time (new optional field on the create-collection
request) and persisted alongside collection metadata. A collection with no binding behaves
exactly as today (internal writes only). This keeps the feature **opt-in and additive** — no
existing deployment changes behaviour until it registers a binding.

### 5.3 The maintenance watermark

One new per-collection watermark, mirroring the puffin RFC's `index_checkpoint_lsn`:

| Watermark | Stored where | Meaning |
|---|---|---|
| `source_snapshot_id` | Puffin checkpoint blob property `likhadb.source_snapshot_id` | The source-table snapshot id whose rows are fully reflected in the persisted index. |

Binding the applied position to the **same Puffin checkpoint** that persists the index is the
key composition with the puffin RFC: a recovered checkpoint tells us both "here is the index"
and "it reflects source snapshot `X`", so maintenance resumes with an incremental scan
`X → current` instead of re-reading the whole source table.

Until a checkpoint exists, the binding's applied position lives only in memory and is
re-derived on restart by a one-time full scan of the source table at the current snapshot
(bounded, logged, and the reason checkpoints matter at scale — same argument as the puffin
RFC §2.2).

### 5.4 The maintenance loop

A background `IndexMaintenanceTask` (mirrors `IcebergFlusher` / `IndexCheckpointTask`
spawn/run patterns) runs on its own tokio interval (default 60 s, configurable). Per tick,
per bound collection:

1. Load the source table, read its current snapshot id `S_now`.
2. If `S_now == source_snapshot_id` already applied → skip (nothing committed upstream).
3. Open an **incremental scan** `from = source_snapshot_id` (exclusive), `to = S_now`.
4. Stream batches; for each row resolve `(id, vector, payload)` and whether it is an add or a
   delete; apply via the unified apply function (§6.2) under the collection write lock.
5. After the full range applies cleanly, advance the in-memory `source_snapshot_id` to
   `S_now`. The next Puffin checkpoint will persist it.
6. Evaluate the compaction trigger (§8); if tripped, enqueue a rebuild.

Apply is **monotonic and snapshot-atomic**: we either advance the watermark to `S_now` after
the whole range applied, or we leave it where it was and retry the same range next tick.
Re-applying a partially-applied range is safe because `insert` is an upsert and `delete` is
idempotent (`hnsw.rs:362` early-returns if already tombstoned).

### 5.5 Ordering vs. the internal path

A row id could be written both internally (via LikhaDB API) and externally (via Spark). We do
**not** attempt cross-source causal ordering — that would require a shared clock LikhaDB does
not own. The rule is **last-writer-wins by application order**, and we make the contract
explicit: *a collection with a source binding should be written predominantly through that
source.* Mixed-authority collections are supported but the resolution is "whichever delta
applied most recently wins," documented as such. This is a deliberate simplicity choice over
a conflict-resolution subsystem nobody asked for.

---

## 6. Component Specifications

### 6.1 New module: `likhadb-lakehouse/src/incremental_scan.rs`

Wraps `iceberg-rs` incremental scanning behind a LikhaDB-shaped iterator:

```rust
pub struct SnapshotDelta {
    pub from_snapshot_id: Option<i64>, // None = full scan at `to`
    pub to_snapshot_id: i64,
}

/// Streams added rows and resolved deletes between two snapshots of `table`.
pub async fn scan_delta(
    table: &Table,
    delta: SnapshotDelta,
    binding: &SourceBinding,
) -> Result<DeltaStream, LakehouseError>;

pub enum DeltaRow {
    Upsert { id: VecId, vector: Vector, payload: serde_json::Value },
    Delete { id: VecId },
}
```

`DeltaStream` yields `DeltaRow`s. Internally it composes the Iceberg added-files scan with
delete-file resolution (or, per §12.1, data-file-drop reconciliation only, for v1). The apply
layer above it never learns which delete mechanism produced a `Delete`.

### 6.2 New: unified apply in `likhadb-store`

```rust
impl Collection {
    /// Apply one delta row. `insert` upserts; `delete` is idempotent.
    pub fn apply_delta_row(&mut self, row: DeltaRow) -> Result<()>;
}
```

This is the single choke point both the maintenance task and (optionally, later) recovery use.
It already exists in spirit — recovery's loop at `iceberg_recovery.rs:91-96` does exactly this
inline for staging rows. We extract it so both callers share one code path.

### 6.3 New module: `likhadb-lakehouse/src/index_maintenance_task.rs`

```rust
pub struct IndexMaintenanceTask {
    manager: Arc<RwLock<CollectionManager>>,
    catalog: Arc<dyn Catalog>,
    bindings: Arc<RwLock<HashMap<String, SourceBinding>>>,
    checkpoint_tracker: Arc<IndexCheckpointTracker>, // shared w/ puffin RFC
    interval: Duration,
}
```

Per-tick logic is §5.4. On graceful shutdown it finishes the in-flight range, then lets the
checkpoint task persist the advanced watermark.

### 6.4 New: compaction in `likhadb-index`

A new trait method, defaulted, so only graph indexes implement it:

```rust
pub trait VectorIndex: Send + Sync {
    // ... existing ...

    /// Fraction of physical nodes that are tombstoned. 0.0 for indexes
    /// that delete in place (IVF/Flat). Default 0.0.
    fn tombstone_ratio(&self) -> f32 { 0.0 }

    /// Rebuild compactly from live entries, dropping tombstones. Returns a
    /// fresh index; default is identity (no-op) for in-place indexes.
    fn compact(&self) -> Box<dyn VectorIndex> { /* default: clone-live-rebuild or self */ }
}
```

`HnswIndex::compact` rebuilds a new graph from live (`!deleted`) nodes only. `IvfIndex`
implements `compact` as **re-train + reassign** to address centroid drift (§8.2). `FlatIndex`
is a no-op. The maintenance task calls `tombstone_ratio()` after each applied range and, above
a threshold, swaps in `compact()`'s result under the write lock (or off-thread with a final
short lock — see §8.3).

### 6.5 Touch points

| File | Change |
|---|---|
| `crates/likhadb-lakehouse/src/incremental_scan.rs` | New |
| `crates/likhadb-lakehouse/src/index_maintenance_task.rs` | New |
| `crates/likhadb-store/src/collection.rs` | Extract `apply_delta_row`; carry optional `SourceBinding` |
| `crates/likhadb-store/src/manager.rs` | Store/lookup bindings per collection |
| `crates/likhadb-index/src/traits.rs` | Add defaulted `tombstone_ratio` / `compact` |
| `crates/likhadb-index/src/hnsw.rs` | Real `compact` (rebuild from live nodes); expose tombstone ratio |
| `crates/likhadb-index/src/ivf.rs` | `compact` = retrain + reassign |
| `crates/likhadb-lakehouse/src/index_checkpoint.rs` (puffin RFC) | Add `likhadb.source_snapshot_id` blob property |
| `crates/likhadb-server/src/routes.rs` + `types.rs` | Optional `source_binding` on create-collection |
| `crates/likhadb-server/src/state.rs` | Spawn `IndexMaintenanceTask` |

---

## 7. Data Flow

### 7.1 Steady state (per bound collection)

```
maintenance tick (60 s; independent of flush + checkpoint)
  S_now ← source_table.current_snapshot_id()
  if S_now == applied_source_snapshot_id: continue
  stream ← scan_delta(source_table, {from: applied, to: S_now}, binding)
  for row in stream:                       # ordered, batched
      collection.apply_delta_row(row)      # insert(upsert) | delete(idempotent)
  applied_source_snapshot_id ← S_now       # in-memory; persisted by next checkpoint
  if collection.index.tombstone_ratio() > threshold:
      enqueue_compaction(collection)
```

### 7.2 Recovery (composed with puffin RFC §7.2)

```
open_with_iceberg
  for each collection:
      (snap, checkpoint_lsn, source_snapshot_id) ← load_index_checkpoint(...)  # puffin
      manager.insert(snap)
      apply staging rows with lsn > checkpoint_lsn          # internal path (puffin RFC)
      if binding present:
          if source_snapshot_id is Some:
              scan_delta(source, {from: source_snapshot_id, to: current}) ► apply
          else:
              full scan of source @ current ► apply         # first-ever bind, bounded+logged
  WAL replay (internal path, unchanged)
```

The two delta sources reconcile cleanly because they touch the same `CollectionManager`
through the same `apply_delta_row`, in a deterministic order (staging first, then source),
with idempotent operations.

---

## 8. The Tombstone / Drift Problem and Compaction

This is the section that justifies the RFC's complexity; the rest is plumbing.

### 8.1 HNSW: tombstone accumulation

`HnswIndex::delete` never removes nodes (`hnsw.rs:361-365`). Under a delete-heavy or churny
source feed:

- **Recall degrades.** Search traverses tombstoned waypoints (`hnsw.rs:421-426` skips them
  only at result-collection time), so beam budget is spent on dead nodes; effective `ef`
  shrinks.
- **Memory grows unbounded.** Vectors and edges for deleted ids stay resident.
- **Entry-point churn.** Each entry-point tombstone triggers a replacement search
  (`hnsw.rs:367-376`).

Today this is masked because deletes only come from LikhaDB's own API at human cadence. A
source feed mirroring an upstream table with continuous deletes/updates makes it acute.

**Mitigation:** `HnswIndex::compact` rebuilds a fresh graph from the live nodes only. Trigger
when `tombstone_ratio = deleted.len() / id_to_node.len()` exceeds a configurable threshold
(default `0.2`). This is the standard HNSW operational answer (FAISS/hnswlib expose the same
"rebuild when X% deleted" knob).

### 8.2 IVF: centroid drift

`IvfIndex::insert` assigns to the nearest *existing* centroid (`ivf.rs:565`). A long
incremental feed whose distribution differs from the training snapshot produces lopsided
inverted lists and poor recall. `IvfIndex::compact` re-runs k-means on the current live set
and reassigns. Trigger on a drift proxy: max/mean inverted-list-length ratio above a
threshold, or simply "every N applied rows," whichever the §12.4 spike shows is cheaper to
compute.

### 8.3 Doing compaction without a long stall

A full rebuild of a 1M-vector HNSW graph is seconds-to-minutes — too long to hold the
collection write lock. Plan:

1. Snapshot live entries under a short read lock.
2. Build the replacement index off-thread (rayon, like the initial build).
3. Re-apply any deltas that landed during the rebuild (tracked by source snapshot id +
   staging LSN at snapshot time).
4. Swap the index pointer under a short write lock.

This is the same "build new, short-swap" pattern the IVF rebuild path already uses; we reuse
it. Compaction emits a `likhadb_index_compactions_total` metric and a structured log line.

### 8.4 Why not delete-in-place for HNSW?

True node removal from an HNSW graph requires repairing every in-bound edge to the removed
node — expensive and complex, and still leaves the graph quality questionable after many
removals. Periodic full compaction is simpler, is what the ecosystem does, and composes
naturally with the checkpoint machinery (a compaction *is* a fresh checkpoint). Rejected as
over-engineering for single-use code (CLAUDE.md §2).

---

## 9. Failure Modes and Watermark Interlock

### 9.1 Failure matrix

| Failure | Effect | Mitigation |
|---|---|---|
| Crash mid-range (some delta rows applied) | In-memory index partially advanced; `applied_source_snapshot_id` **not** advanced | On restart, recovery starts from the last *persisted* `source_snapshot_id`; the range re-applies. `insert` upsert + idempotent `delete` make replay safe. |
| Source snapshot expired before we scanned from it (Iceberg snapshot expiry) | `scan_delta(from: expired)` fails — the incremental range is no longer reconstructable | Fall back to a **full scan at current snapshot** for that collection, log loudly, emit `likhadb_source_full_rescan_total`. Operators size source-table snapshot retention ≥ max maintenance downtime. |
| Source schema changed (dim/metric/column rename) | Vector decode or column lookup fails | Abort maintenance for that collection, surface error, require manual re-bind. Other collections unaffected. |
| `iceberg-rs` can't resolve row-level deletes (§12.1) | Deletes via equality-delete files are missed | v1 scope: data-file-drop deletes only; documented limitation + metric `likhadb_unresolved_delete_files_total > 0` as an alert. |
| Compaction fails (OOM on rebuild) | Index keeps serving with tombstones; no data loss | Trigger backs off; alert on `tombstone_ratio` SLO. Serving is degraded, not broken. |
| Maintenance task disabled/paused | Index freshness frozen at last applied snapshot | Fully safe; `likhadb_source_snapshot_lag` metric goes stale and alerts. |

### 9.2 The watermark interlock

> **Invariant:** `applied_source_snapshot_id` advances to `S_now` **only after** every
> `DeltaRow` in `(previous, S_now]` has been applied to the in-memory index. It is persisted
> **only** by a Puffin checkpoint that also persists that same in-memory index. The two are
> never written independently.

This guarantees recovery never believes the index reflects a source snapshot it does not. It
is the source-table analogue of the puffin RFC's `index_checkpoint_lsn` interlock, and it
relies on that RFC's atomic index-checkpoint write.

### 9.3 Interaction with internal writes

Internal-path WAL truncation (puffin RFC §8.2) is **not** gated on `source_snapshot_id` —
the source table is independently durable in its own catalog, owned by another engine, so
losing the in-memory source position only costs a re-scan, never data. Keeping the two
watermarks independent avoids coupling LikhaDB's WAL lifecycle to an external table's commit
cadence.

---

## 10. Operational Concerns

### 10.1 Configuration

```toml
[maintenance]
enabled = true
interval_s = 60                    # source-snapshot poll cadence
hnsw_compaction_tombstone_ratio = 0.2
ivf_compaction_every_n_rows = 1_000_000   # or drift-ratio trigger; see §12.4
max_rows_per_tick = 0              # 0 = unbounded; >0 caps per-tick apply for backpressure
```

### 10.2 Cadence sizing

The natural cadence is "how often does the source table commit a snapshot." Spark/dbt batch
jobs commit minutes-to-hourly; streaming writers commit seconds-to-minutes. A 60 s default
poll matches batch; streaming sources should lower it. Polling is cheap (read one snapshot
id) when nothing changed.

### 10.3 Observability (new metrics)

- `likhadb_source_snapshot_lag{collection}` — applied vs. current source snapshot age.
- `likhadb_delta_rows_applied_total{collection,op}`.
- `likhadb_index_tombstone_ratio{collection}`.
- `likhadb_index_compactions_total{collection,result}`.
- `likhadb_source_full_rescan_total{collection}` — should stay flat; spikes mean retention
  too short or downtime too long.

### 10.4 Cost

Incremental scans read only added/changed files — cost is proportional to upstream churn, not
table size. Compaction is the expensive event; its frequency is bounded by the tombstone/drift
threshold, giving operators a direct cost/recall dial.

---

## 11. Alternatives Considered

### 11.1 Full periodic rebuild from the source table

Re-scan the whole source table and rebuild the index on a timer. Simple, no incremental-scan
dependency, no tombstone problem (every rebuild is clean). Rejected as the *steady-state*
mechanism: cost scales with table size, not churn, so it does not scale to large tables with
small deltas. **However** it is exactly the fallback path for expired snapshots (§9.1) and the
first-bind path (§7.2), so we implement it anyway and incremental scanning sits on top.

### 11.2 Push-based: have writers notify LikhaDB

A webhook / queue where Spark jobs tell LikhaDB "I committed snapshot S." Lower latency, but
requires changing every upstream writer and a delivery-guarantee layer — exactly the ETL/sync
pipeline the project exists to eliminate. The snapshot id *is* the durable notification;
polling it needs no upstream cooperation. Rejected.

### 11.3 Treat the source table as the staging table

Point the existing recovery `scan_pending` machinery at the source table directly. Rejected:
the staging schema is LikhaDB-private (`is_tombstone`, `merge_status`, `lsn` columns,
`staging_io.rs:46`); source tables have arbitrary user schemas and use Iceberg-native deletes,
not a `lsn`/`is_tombstone` convention. Forcing source tables into the staging contract breaks
"the lakehouse owns the data."

### 11.4 In-place HNSW deletion instead of compaction

Discussed and rejected in §8.4.

### 11.5 Per-row CDC stream instead of snapshot diff

Consume an external CDC feed (Debezium-style) per row. Finer-grained, but reintroduces an
external streaming dependency and ordering/exactly-once concerns. Iceberg snapshot diffs give
us batched, exactly-once-by-construction (idempotent apply) deltas with no extra
infrastructure. Rejected.

---

## 12. Open Questions

### 12.1 `iceberg-rs` incremental scan + v2 delete maturity

**Resolved (2026-07-15).** Findings against the current dependency (0.4) and the latest
published crate (0.9.1):

- **(a) Incremental append scan:** does **not** exist upstream at any released version. The
  snapshot-range scan (issue #1469 / PR #1470) was closed **unmerged** and folded into a wider
  CDC changelog-scan effort. v1 hand-rolls the diff via low-level manifest APIs (§6.1); an
  upgrade does not change this.
- **(b) Delete-file resolution:** does **not** exist in 0.4 (`TableScan` errors on any delete
  file) but **does** exist in ≥ 0.9 (position + equality delete readers landed 0.8.0–0.9.0). v1
  on 0.4 restricts to append + data-file-drop deletes; a follow-up crate upgrade unlocks
  row-level deletes (tracked as its own issue).

The apply layer (§6.2) stays insulated so the scan layer can adopt the upstream delete readers
later without touching it.

### 12.2 Where do `SourceBinding`s live durably?

Options: a LikhaDB metadata table in the catalog, table properties on the source table, or
local config. Leaning toward catalog-side metadata so a recovered process rediscovers its
bindings. Needs to align with however collection metadata is already persisted.

### 12.3 ID column → `VecId` mapping

`VecId` is LikhaDB's internal id type. Source tables key on arbitrary columns (string uuids,
composite keys). v1 assumes an integer id column mappable to `VecId`; non-integer keys need a
stable hash or an id-mapping table. Scope decision for v1: integer ids only, documented.

### 12.4 IVF drift trigger

Is "compact every N rows" sufficient, or do we need a real distribution-drift metric
(inverted-list length variance, or sampled recall against a holdout)? Cheap heuristic first;
measure before adding a drift estimator.

### 12.5 Composition ordering with the realtime-insert two-tier RFC

If `rfc_realtime_insert_vectordb.md`'s two-tier (mutable flat staging + main IVF) lands, the
source delta should feed the **main tier**, not the hot staging tier. Confirm the layering
when both designs are concurrent.

### 12.6 FTS deltas

The same `DeltaRow` stream should update the Tantivy FTS index for bound collections with FTS
enabled. Tantivy supports `delete_term` + re-add, so apply maps cleanly — but confirm the FTS
write path is reachable from `apply_delta_row` without a second source scan.

---

## 13. Appendix

### 13.1 Relation to other RFCs and ADRs

- **`rfc_puffin_backed_index_snapshots.md`** — this RFC is the "incremental index updates
  (manifest-diff-driven merging)" that the puffin RFC lists as an explicit non-goal/future
  RFC (its §4 Non-Goals). It adds exactly one field to the checkpoint blob
  (`likhadb.source_snapshot_id`) and otherwise consumes the puffin machinery unchanged.
- **`rfc_realtime_insert_vectordb.md`** — orthogonal axis. That RFC reduces *internal* insert
  latency via a two-tier index; this RFC adds *external* source-table ingestion. They meet
  only at §12.5.
- **`docs/adr/design-review-iceberg-lakehouse.md`** — extends the "log is the source of
  truth" model with a second, externally-owned log (the source table). The ADR's WAL/Iceberg
  watermark coexistence is untouched; we add a parallel, independent watermark that is
  explicitly *not* allowed to gate WAL truncation (§9.3).
- **`rfc_index_checkpoint_property_pointer.md`** — shares the "bind a derived in-memory
  structure to an Iceberg snapshot id via a property" idea; we reuse the pattern for
  `source_snapshot_id`.

### 13.2 Files changed (summary)

See the table in §6.5. New crates: none. New external deps: none beyond the existing
`iceberg-rs` 0.4 dependency for v1 (§12.1 resolved). Resolving row-level deletes later requires
bumping `iceberg-rs` to ≥ 0.9 — a version upgrade, not a new dependency.

### 13.3 Minimal v1 cut

v1 ships against `iceberg-rs` **0.4**, which has no row-level delete resolution (§12.1), so:

- **Ship:** append-only source ingestion + data-file-drop deletes + HNSW/IVF compaction +
  `source_snapshot_id` checkpoint binding + recovery composition.
- **Defer:** equality/position delete resolution, behind `likhadb_unresolved_delete_files_total`
  as a guardrail metric. **Unblocked by upgrading to `iceberg-rs` ≥ 0.9** (delete readers landed
  0.8.0–0.9.0) — a tracked follow-up, not a permanent gap.

This still delivers the differentiator (external appends become searchable with no ETL) and is
honest about the delete gap rather than silently dropping deletes.
