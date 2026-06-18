# RFC: Puffin-Backed Index Checkpoints for LikhaDB

| Field | Value |
|---|---|
| **RFC ID** | TBD |
| **Status** | Draft (v2 — rewritten against actual code) |
| **Author(s)** | TBD |
| **Created** | 2026-06-16 |
| **Last Updated** | 2026-06-18 |
| **Target Milestone** | TBD |

---

## Table of Contents

1. [Summary](#1-summary)
2. [Current State and Motivation](#2-current-state-and-motivation)
3. [Background: Puffin and Iceberg Statistics Files](#3-background-puffin-and-iceberg-statistics-files)
4. [Design Goals and Non-Goals](#4-design-goals-and-non-goals)
5. [Proposed Design](#5-proposed-design)
6. [Component Specifications](#6-component-specifications)
7. [Data Flow](#7-data-flow)
8. [Failure Modes and the WAL Interlock](#8-failure-modes-and-the-wal-interlock)
9. [Operational Concerns](#9-operational-concerns)
10. [Alternatives Considered](#10-alternatives-considered)
11. [Open Questions](#11-open-questions)
12. [Appendix](#12-appendix)

---

## 1. Summary

This RFC fills a gap: today **no live code path persists `CollectionSnapshot` to Iceberg**.
The flusher (`iceberg_flusher.rs`) writes vectors to a per-collection staging table; the
index serialization code in `index_snapshot_io.rs` exists but is never called from
production. Recovery in `iceberg_recovery.rs` loads from the empty
`likhadb_index_snapshots` table, finds nothing, and falls back to WAL replay. This works
but is wasteful at scale: every cold start replays the entire WAL into a fresh in-memory
graph.

We propose a **Puffin-backed index checkpoint** that snapshots the in-memory `IndexSnapshot`
for each collection and binds it, via Iceberg's `StatisticsFile` mechanism, to the same
snapshot of that collection's staging table. Checkpoints are produced on a separate,
coarse cadence (not every WAL flush), and the in-memory `iceberg_watermark` is gated on the
checkpoint covering all entries up to that LSN.

The change touches `likhadb-lakehouse` only. The `likhadb_index_snapshots` side-table and
its read/write helpers are deleted in the same change; since no production code calls them,
there is no migration window.

---

## 2. Current State and Motivation

### 2.1 What the code actually does today

The honest picture, verified against the current branch:

- **Write path (`IcebergFlusher::flush_once`)**: drains WAL entries, groups by
  collection, calls `append_to_staging` for each, advances `iceberg_watermark`, and
  truncates the WAL up to that watermark when (a) no DDL ops were in the batch and (b) all
  collections flushed cleanly. No index serialization happens.
- **Recovery path (`open_with_iceberg`)**: calls `load_collection_snapshots` against the
  `likhadb_index_snapshots` table. That table is never written, so the call always returns
  an empty `Vec`, and recovery falls through to a "no Iceberg index snapshots found" log
  line and a WAL-only rebuild.
- **`index_snapshot_io.rs`**: writes a bincode `CollectionSnapshot` as a single-row
  Parquet file into the `likhadb_index_snapshots` side-table. Only exercised by unit tests
  that round-trip the bincode payload directly. Not on any live code path.

So the RFC's premise — "replace the side-table" — is misleading. There is nothing to
replace. The real question is: how should LikhaDB persist index state to Iceberg, given
that the placeholder side-table was never wired up?

### 2.2 Why we want any Iceberg-side index persistence at all

Two concrete benefits, neither of which requires multi-engine semantics:

1. **Cold-start cost.** Today the WAL is the only thing that lets a process rebuild its
   in-memory graphs without re-reading every staging row. The WAL is truncated past
   `iceberg_watermark`, so the live window is small — but the gap between "last
   checkpoint" and the WAL head still requires sequential `insert()` calls into HNSW/IVF.
   For a 1M-vector HNSW collection, a fresh `HnswIndex::insert` per vector is hundreds of
   seconds. A persisted index snapshot collapses that to a single bincode load.
2. **Atomic data/index binding.** The checkpoint is tied to a specific snapshot of the
   staging table. On recovery, we know that everything ≤ checkpoint LSN is already
   reflected in the in-memory index, and we only have to scan staging rows above that LSN.
   Without an attached checkpoint, we would have to scan the whole staging table or trust a
   loose "written_at_ms" comparison.

What we are **not** trying to get out of this design:

- **Cross-engine readability.** Spark and Trino will never know the `likhadb-index-v1` blob
  type. The Puffin container is convenient because Iceberg already has lifecycle hooks for
  it, not because anyone else is going to consume the bytes.
- **Time travel into the index.** `AS OF snapshot_id` against the index is interesting, but
  out of scope for this RFC. The binding makes it possible later.
- **Replacing the WAL.** The WAL stays as the local low-latency durability layer below the
  Iceberg flush. See `docs/adr/design-review-iceberg-lakehouse.md`.

### 2.3 What was wrong with the v1 design

The v1 of this RFC made several claims that don't match Iceberg or the code:

- It described `statistics-file` as a **snapshot summary property** set via
  `set-properties`. In Iceberg it is a **table-level** array of `StatisticsFile` entries
  attached to a snapshot via `TableUpdate::SetStatistics` — no new snapshot is needed.
- It required two snapshots per flush (`S` for data, `S+1` for the sidecar). With
  `SetStatistics`, the file pins to the existing snapshot `S`. Snapshot history does not
  double.
- It referenced `assert-current-snapshot-id` as the conflict-detection requirement; the
  actual variant in `iceberg-rs` is `RefSnapshotIdMatch` (`assert-ref-snapshot-id`).
- It claimed a `PuffinReader` exists in `iceberg-rs`. The `puffin` module in
  `iceberg-rs` 0.4 is a stub containing only a compression submodule; both reader and
  writer must be implemented here.
- It modelled a single Puffin file containing both HNSW and IVF blobs for the same
  collection. `CollectionSnapshot` carries exactly one `IndexSnapshot` (a tagged enum), and
  staging tables are per-collection. The multi-blob layout has no use.
- It tied checkpoints to the 100 ms flush cadence, then admitted in §12.2 this was
  untenable.

This rewrite addresses each of those points.

---

## 3. Background: Puffin and Iceberg Statistics Files

### 3.1 The Puffin container

A Puffin file is a flat binary container. Its layout, abbreviated:

```
PFA1 (4B magic)
[blob 0 bytes]
[blob 1 bytes]
...
UTF-8 JSON footer: list of { type, fields, offset, length, compression-codec, properties }
footer_length (4B LE)
flags (4B; bit 0 = footer payload compressed)
PFA1 (4B trailing magic)
```

The footer is small and can be fetched with a single tail range request; individual
blobs are addressable via their `(offset, length)` pair. For a file with one large blob
this addressing buys nothing — the recovery path will fetch the whole blob anyway. For a
file with several smaller blobs (e.g., separate auxiliary blobs alongside the main index)
it lets a reader skip blobs whose types it doesn't understand.

### 3.2 `StatisticsFile` in Iceberg

The Iceberg table metadata carries an array `statistics: Vec<StatisticsFile>`. Each entry
binds a Puffin file to a specific snapshot:

```rust
// iceberg-rs 0.4: src/spec/statistic_file.rs
pub struct StatisticsFile {
    pub snapshot_id: i64,
    pub statistics_path: String,
    pub file_size_in_bytes: i64,
    pub file_footer_size_in_bytes: i64,
    pub key_metadata: Option<String>,
    pub blob_metadata: Vec<BlobMetadata>,
}
pub struct BlobMetadata {
    pub r#type: String,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub fields: Vec<i32>,
    pub properties: HashMap<String, String>,
}
```

`TableUpdate::SetStatistics { statistics: StatisticsFile }` registers a new entry;
`TableUpdate::RemoveStatistics { snapshot_id }` removes one. Both are standard REST
catalog updates. Crucially, **`SetStatistics` does not produce a new snapshot** — it
mutates the table metadata to point an existing snapshot at a new statistics file.

This is the right primitive for our use case. We commit the staging append as we do
today, then issue a `SetStatistics` referencing the snapshot we just produced.

### 3.3 What the iceberg-rs Puffin module gives us

In `iceberg-rs` 0.4, `src/puffin/` contains only a `compression` submodule. There is no
`PuffinReader`, no `PuffinWriter`. We will implement both in `likhadb-lakehouse` as a
small focused module — roughly 300 LOC including tests — kept structurally aligned with
the upstream layout so that when iceberg-rs ships a real implementation we can switch to
it without churn in callers.

---

## 4. Design Goals and Non-Goals

### Goals

1. **Bind index checkpoints to staging snapshots** via `SetStatistics`, so a recovering
   process can answer "which staging rows are already in this index?" with a single LSN
   comparison.
2. **Avoid doubling snapshot churn.** Checkpoints attach to the existing staging snapshot;
   no shadow `S+1` commit.
3. **Decouple checkpoint cadence from flush cadence.** Flush stays at ~100 ms; checkpoints
   run on a coarser timer (default 30 s, configurable) and additionally on shutdown.
4. **Make WAL truncation safe under checkpoint failures.** `iceberg_watermark` may not
   advance past the highest LSN covered by a durable checkpoint, so a crash never strands
   us with truncated WAL and a stale index.
5. **Delete the dead `likhadb_index_snapshots` table code.** No production caller exists.
6. **Stream large blobs.** The writer must be able to upload a multi-GB blob without
   buffering the whole Puffin file in RAM.

### Non-Goals

- Multi-engine readability of the blob payload.
- FTS persistence. Tantivy state is now mmap-backed under the collection's `fts` dir per
  commit `a01b0dd`; that mechanism is unaffected and orthogonal. See §11.3.
- `AS OF snapshot_id` query semantics.
- Incremental index updates (manifest-diff-driven merging). Each checkpoint is a full
  reserialize of the current in-memory `IndexSnapshot`. Future RFC.
- Distributed sharding of index blobs. LikhaDB is single-node.
- Cross-collection bundling. One Puffin file per collection per checkpoint.

---

## 5. Proposed Design

### 5.1 The boundary, restated

```
WAL  ──flush──►  staging table   (snapshot S, per-collection, every ~100 ms)
                       │
                       │   coarser checkpoint cadence (~30 s)
                       ▼
                 PuffinWriter ──upload──►  object store
                       │
                       ▼
                 SetStatistics(snapshot_id = S, statistics_path = ...)
                       │
                       ▼
                 advance index_checkpoint_lsn for this collection
                       │
                       ▼
                 allow WAL truncation past index_checkpoint_lsn
```

Two independent watermarks per collection:

| Watermark | Owner | Meaning |
|---|---|---|
| `staging_watermark` (`last_wal_lsn` table property) | staging table | All WAL LSNs ≤ this are durable in staging Parquet. Owned by `append_to_staging`, already implemented. |
| `index_checkpoint_lsn` (Puffin blob property `likhadb.checkpoint_lsn`) | Puffin checkpoint | All WAL LSNs ≤ this are reflected in the persisted index. Owned by the new checkpoint task. |

WAL truncation, today gated only on `staging_watermark`, becomes gated on
`min(staging_watermark, index_checkpoint_lsn_across_all_collections)`. This is the part of
the design that requires the most care; see §8.

### 5.2 File layout

One Puffin file per collection per checkpoint, written at:

```
<staging_table_location>/metadata/likhadb-idx-<staging_snapshot_id>-<lsn>.puffin
```

Content: **one** blob of type `likhadb-index-v1`, payload = zstd-compressed bincode of
the collection's `CollectionSnapshot`. Single blob because `CollectionSnapshot` already
covers all index variants via the `IndexSnapshot` enum; multi-blob layout has no use case
here.

The `fields` array in the blob footer is set to `[]`. We intentionally do not link it to
the staging schema's `vector_json` column — that column is a JSON-encoded string, not a
typed vector field, so the link would be misleading. If/when staging migrates to a real
`FixedSizeList<Float32, dim>` column, the field ID can be added.

Blob properties (free-form per Puffin spec):

```
"likhadb.collection":          "<name>"
"likhadb.dimension":           "<d>"
"likhadb.metric":              "l2 | cosine | dot"
"likhadb.index_kind":          "flat | ivf | hnsw"
"likhadb.vector_count":        "<n>"
"likhadb.fts_enabled":         "true | false"
"likhadb.checkpoint_lsn":      "<wal LSN through which the index reflects writes>"
"likhadb.staging_snapshot_id": "<i64>"
"likhadb.payload_codec":       "bincode-v1"
```

`likhadb.checkpoint_lsn` is the load-bearing property: it is the value that the recovery
path uses to skip already-applied staging rows, and it is the value that gates WAL
truncation.

### 5.3 Payload format

```
[bincode-encoded CollectionSnapshot bytes]
```

No magic header. The blob type string (`likhadb-index-v1`) is the version discriminator;
re-versioning the bincode format means bumping to `likhadb-index-v2` in the Puffin footer.
This is the convention every other Puffin user follows (`apache-datasketches-theta-v1`,
`deletion-vector-v1`). The v1 RFC's `LFLT`/`LIVF`/`LHNS` magic added a duplicate
discriminator with no extra check value.

Compression: `zstd` (Puffin codec field `"zstd"`). The bincode payload for HNSW graphs is
mostly raw f32 vector bytes, which compress poorly (zstd typical ratio for IEEE-754 floats
is 1.0–1.1×). Compression here is more about graph structure (offsets, neighbour lists)
than vectors. We enable it because the cost is small and the size estimates in §9.2 assume
it; benchmarks may justify turning it off for the largest deployments.

### 5.4 Registration with `SetStatistics`

After the Puffin upload completes, the checkpoint task issues a single REST
`UpdateTable` containing one `TableUpdate::SetStatistics` and one
`TableRequirement::RefSnapshotIdMatch { ref: "main", snapshot_id: S }`:

```rust
let stat = StatisticsFile {
    snapshot_id: staging_snapshot_id,
    statistics_path: puffin_path,
    file_size_in_bytes,
    file_footer_size_in_bytes,
    key_metadata: None,
    blob_metadata: vec![BlobMetadata {
        r#type: "likhadb-index-v1".to_string(),
        snapshot_id: staging_snapshot_id,
        sequence_number: 1,
        fields: vec![],
        properties: blob_properties,
    }],
};
// then construct UpdateTable with TableUpdate::SetStatistics { statistics: stat }
// and TableRequirement::RefSnapshotIdMatch.
```

Conflict handling: if the staging table moved on (another flush slipped in between the
checkpoint task sampling `S` and issuing the update), `RefSnapshotIdMatch` fails, the
uploaded Puffin file becomes an orphan, and the checkpoint task retries from scratch at
the new snapshot. Orphans are reaped by §9.4.

`iceberg-rs` 0.4 exposes `TableUpdate::SetStatistics` in `src/catalog/mod.rs` but
`Transaction` does not yet have a high-level `set_statistics` method. The implementation
will assemble the `UpdateTableRequest` and POST it via the REST client — the same pattern
used by other features that pre-date a Transaction API.

### 5.5 Coarse checkpoint cadence

The checkpoint task runs on its own tokio interval (default 30 s, configurable via
`IcebergConfig::index_checkpoint_interval`). It also fires on graceful shutdown.

On each tick, for each collection:

1. Read the staging table's current snapshot id `S` and `staging_watermark`.
2. If `staging_watermark <= index_checkpoint_lsn` already recorded for this collection,
   skip — nothing has been flushed since the last checkpoint.
3. Otherwise build a `CollectionSnapshot` from `CollectionManager`, serialize, upload,
   register with `SetStatistics`, update `index_checkpoint_lsn`.

This decouples cold-start cost from the 100 ms flush hot path. The flusher is unchanged
in steady state. The checkpoint task can be paused, slowed, or skipped without affecting
write availability.

### 5.6 Recovery

`open_with_iceberg` becomes:

1. List staging tables in the namespace (already done indirectly via per-collection
   `get_or_create_staging_table`; this RFC adds a list step).
2. For each collection:
   a. Load the staging table, inspect the latest snapshot's attached
      `StatisticsFile` entries (table metadata `statistics` array, filtered to that
      snapshot id).
   b. Pick the entry whose blob list contains a `likhadb-index-v1` blob; if multiple,
      pick the one with the highest `likhadb.checkpoint_lsn`.
   c. Fetch the Puffin footer (tail range request), find the blob offset and length,
      fetch the blob, zstd-decompress, bincode-deserialize into `CollectionSnapshot`.
   d. Insert into `CollectionManager`.
3. For each collection, scan staging rows with `lsn > checkpoint_lsn` (extending
   `scan_pending` to take a lower bound — or, more cheaply, push the predicate as a scan
   filter) and apply them.
4. Replay WAL entries for `lsn > min(checkpoint_lsn, staging_watermark)` for each
   collection.

Step 2 produces a useful log line we can monitor migration with:
`"loaded N collections from Puffin checkpoints (LSN window …)"`.

The `likhadb_index_snapshots` table read path is removed in the same change.

---

## 6. Component Specifications

### 6.1 New module: `likhadb-lakehouse/src/puffin.rs`

Single module owning both reader and writer for the local Puffin format. The writer is
streaming: callers feed blobs incrementally, and the module writes each blob to an
`AsyncWrite` (typically the `iceberg::io::FileIO::new_output` write handle wrapped in a
buffered writer) as soon as it has been consumed, then finishes by emitting the JSON
footer and trailer. This avoids holding multi-GB Puffin files in RAM.

```rust
pub struct PuffinWriter<W: AsyncWrite + Unpin> {
    out: W,
    blobs: Vec<BlobFooterEntry>,
    cursor: u64,
}

pub struct BlobFooterEntry {
    pub blob_type: String,
    pub offset: u64,
    pub length: u64,                // post-compression size
    pub compression_codec: &'static str,
    pub fields: Vec<i32>,
    pub properties: HashMap<String, String>,
}

impl<W: AsyncWrite + Unpin> PuffinWriter<W> {
    pub async fn new(mut out: W) -> Result<Self, LakehouseError>; // writes leading PFA1
    pub async fn add_blob_zstd(
        &mut self,
        blob_type: &str,
        fields: Vec<i32>,
        properties: HashMap<String, String>,
        payload: &[u8],            // pre-compression
    ) -> Result<(), LakehouseError>;
    pub async fn finish(self) -> Result<PuffinFinishInfo, LakehouseError>; // footer + trailer
}

pub struct PuffinFinishInfo {
    pub file_size_in_bytes: u64,
    pub file_footer_size_in_bytes: u64,
    pub blob_entries: Vec<BlobFooterEntry>,
}

pub struct PuffinReader { /* takes a FileIO + path; lazy footer + lazy blob fetch */ }
```

Tests: round-trip with one and three blobs; corrupted-footer error; footer length
mismatch; trailing-magic mismatch.

### 6.2 New module: `likhadb-lakehouse/src/index_checkpoint.rs`

Replaces `index_snapshot_io.rs`. Top-level entry points:

```rust
pub async fn write_index_checkpoint<C: Catalog>(
    catalog: &C,
    staging_table: &Table,
    collection: &CollectionSnapshot,
    staging_snapshot_id: i64,
    checkpoint_lsn: u64,
) -> Result<(), LakehouseError>;

pub async fn load_index_checkpoint(
    staging_table: &Table,
) -> Result<Option<LoadedCheckpoint>, LakehouseError>;

pub struct LoadedCheckpoint {
    pub snapshot: CollectionSnapshot,
    pub checkpoint_lsn: u64,
    pub staging_snapshot_id: i64,
}
```

`write_index_checkpoint`:
1. bincode-encodes `collection` into a `Vec<u8>` (single allocation per checkpoint; this
   is acceptable for the bincode payload itself — what we avoid in §6.1 is buffering the
   *entire Puffin file*).
2. Opens an `AsyncWrite` against the chosen path via `staging_table.file_io()`.
3. Drives `PuffinWriter::new` → `add_blob_zstd("likhadb-index-v1", ...)` → `finish()`.
4. Builds a `StatisticsFile` from `PuffinFinishInfo` and posts a `SetStatistics` update
   with `RefSnapshotIdMatch { ref: "main", snapshot_id: staging_snapshot_id }`.

`load_index_checkpoint`:
1. Reads `staging_table.metadata().statistics()`, filters to the latest snapshot, picks
   the entry whose blob list includes `likhadb-index-v1` with the highest
   `likhadb.checkpoint_lsn` property.
2. Uses `PuffinReader` to fetch the single blob, decompress, deserialize.

If no entry exists (fresh deployment), returns `Ok(None)` and the caller falls through to
WAL-only recovery.

### 6.3 Modified: `likhadb-lakehouse/src/iceberg_flusher.rs`

`IcebergFlusher::flush_once` is unchanged in logic. The only change is the WAL truncation
condition. Today truncation requires `flush_errors == 0 && !has_ddl`. It additionally
requires that `max_lsn <= min_index_checkpoint_lsn` across all collections that were
touched in this batch.

The flusher consults a new shared structure:

```rust
struct IndexCheckpointTracker {
    per_collection: RwLock<HashMap<String, u64>>, // collection name → checkpoint_lsn
}
```

This tracker is shared with the new checkpoint task (§6.4). The flusher reads it; the
checkpoint task writes it.

If `max_lsn` exceeds the lowest covered checkpoint, the flusher advances
`iceberg_watermark` (still safe — staging is durable) but **does not** truncate the WAL.
Truncation happens on the next pass once the checkpoint task has caught up.

### 6.4 New module: `likhadb-lakehouse/src/index_checkpoint_task.rs`

Mirror of `IcebergFlusher`'s spawn/run pattern. Independent tokio interval, default 30 s.

```rust
pub struct IndexCheckpointTask {
    manager: Arc<RwLock<CollectionManager>>,
    config: IcebergConfig,
    namespace: NamespaceIdent,
    tracker: Arc<IndexCheckpointTracker>,
    interval: Duration,
}
```

Each tick: for each collection in `manager`, sample `(staging_table, S, watermark)`, skip
if not advanced, else build snapshot, call `write_index_checkpoint`, update the tracker.

On shutdown (a `tokio::sync::Notify` passed from the server) the task runs one final pass
synchronously so that no in-flight LSN range is left without a checkpoint.

### 6.5 Modified: `likhadb-lakehouse/src/iceberg_recovery.rs`

`open_with_iceberg` is restructured around per-collection scanning. The two changes from
the current code:

- Replace `load_collection_snapshots` against `likhadb_index_snapshots` with a per-collection
  `load_index_checkpoint` call.
- When applying staging rows, push down `lsn > checkpoint_lsn` as a scan filter rather than
  scanning the whole table and discarding rows above the watermark.

`iceberg_recovery-migration` feature flag and "fallback to old side-table" path: not
needed. The side-table has no rows in any deployed environment, so a delete + fresh
deploy is a clean cutover.

### 6.6 Deleted

- `crates/likhadb-lakehouse/src/index_snapshot_io.rs` (entire file).
- The `likhadb_index_snapshots` table identifier and `index_snapshot_table_ident` helper.
- `pub use index_snapshot_io::{load_collection_snapshots, write_collection_snapshot}` from
  `lib.rs`.

The bincode round-trip tests in `index_snapshot_io.rs` move into the new `puffin.rs` or
`index_checkpoint.rs` as end-to-end Puffin round-trip tests.

### 6.7 Public constants: `likhadb-lakehouse/src/puffin.rs`

```rust
pub const BLOB_TYPE_INDEX_V1: &str = "likhadb-index-v1";

pub const PROP_COLLECTION:           &str = "likhadb.collection";
pub const PROP_DIMENSION:            &str = "likhadb.dimension";
pub const PROP_METRIC:               &str = "likhadb.metric";
pub const PROP_INDEX_KIND:           &str = "likhadb.index_kind";
pub const PROP_VECTOR_COUNT:         &str = "likhadb.vector_count";
pub const PROP_FTS_ENABLED:          &str = "likhadb.fts_enabled";
pub const PROP_CHECKPOINT_LSN:       &str = "likhadb.checkpoint_lsn";
pub const PROP_STAGING_SNAPSHOT_ID:  &str = "likhadb.staging_snapshot_id";
pub const PROP_PAYLOAD_CODEC:        &str = "likhadb.payload_codec";
```

`likhadb-persist` is not touched. Persistence-format strings live next to the code that
writes them.

---

## 7. Data Flow

### 7.1 Steady state

```
flusher tick (100 ms)
  drain WAL → group by collection → append_to_staging → advance staging_watermark
  if all collections OK AND no DDL AND max_lsn <= min_checkpoint_lsn:
      truncate WAL up to max_lsn

checkpoint tick (30 s; independent)
  for each collection in manager:
      (S, watermark) ← read staging table
      if watermark <= checkpoint_lsn[collection]: continue
      snap ← CollectionManager::to_snapshot_with_lsn(watermark) for this collection
      buf ← bincode(snap)
      open AsyncWrite on table.file_io() at metadata/likhadb-idx-{S}-{watermark}.puffin
      PuffinWriter::new → add_blob_zstd(BLOB_TYPE_INDEX_V1, [], props, buf) → finish
      StatisticsFile { snapshot_id = S, ..., blob_metadata = [...] }
      POST UpdateTable { updates: [SetStatistics(stat)],
                         requirements: [RefSnapshotIdMatch { ref: "main", snapshot_id: S }] }
      checkpoint_lsn[collection] ← watermark
```

### 7.2 Recovery

```
open_with_iceberg
  for each known collection (discovered via catalog.list_tables(namespace) filtered to
                              "likhadb_staging_*"):
      table ← catalog.load_table
      stat  ← table.metadata().statistics().find_for_latest_snapshot()
      if stat is Some:
          (snap, ckpt_lsn) ← load_index_checkpoint(stat)
          manager.insert(snap)
      else:
          // No checkpoint yet — start from empty for this collection;
          // staging scan + WAL replay will populate it.
          manager.insert_empty(collection_name, schema_from_staging_table)
          ckpt_lsn ← 0
      scan staging where lsn > ckpt_lsn → apply to manager
  WAL replay for entries with lsn > min(ckpt_lsn, staging_watermark) per collection
```

The recovery path is **per-collection** rather than a global side-table read. This is
strictly cleaner and matches the per-collection staging tables we already have.

---

## 8. Failure Modes and the WAL Interlock

### 8.1 Failure matrix

| Failure | Effect | Mitigation |
|---|---|---|
| Crash after staging commit, before Puffin upload | Staging snapshot has no statistics file; recovery loads previous Puffin (or none) and re-scans staging from the older `checkpoint_lsn` | WAL was not truncated past the previous `checkpoint_lsn`, so no data is lost. The window between the previous checkpoint and the crash is replayed from staging + WAL. |
| Crash after Puffin upload, before `SetStatistics` returns | Orphan Puffin file in object store | Reaped by §9.4 GC job. `checkpoint_lsn` is not advanced; flusher will not truncate WAL past the still-current checkpoint. Next checkpoint tick produces a fresh file. |
| `SetStatistics` rejected (concurrent staging commit changed `S`) | `RefSnapshotIdMatch` fails | Retry the checkpoint task from scratch at the new `S`; previous Puffin becomes an orphan. |
| Puffin footer corrupt or trailing magic wrong | `load_index_checkpoint` returns `Err` | `open_with_iceberg` surfaces the error; operator either deletes the `StatisticsFile` entry (`RemoveStatistics`) and re-runs recovery from staging + WAL, or rolls back the staging snapshot. |
| Blob payload corrupt (bincode failure) | Same as above | Same handling. Note: WAL has been truncated only up to the *previous* good checkpoint, so the previous checkpoint can be recovered by selecting a stale `StatisticsFile` entry (table metadata may retain multiple). |
| Object store unavailable during recovery | `load_index_checkpoint` fails | `open_with_iceberg` propagates; server does not start. No silent staleness. |
| Checkpoint task chronically falls behind (e.g. very large index, 30s tick is insufficient) | `checkpoint_lsn` lags `staging_watermark`; WAL grows | Operator-visible: `likhadb_wal_log_bytes` and `likhadb_checkpoint_lag_lsn` metrics. Operator increases checkpoint interval slack or scales index hardware. |

### 8.2 The WAL interlock, restated

This is the most important correctness rule introduced by this RFC:

> **Invariant:** `wal.truncate_up_to(L)` is called only if
> `L <= min(staging_watermark, index_checkpoint_lsn)` across every collection observed in
> the batch.

If the checkpoint task is stopped or broken, WAL truncation stops. The WAL grows, but
data is safe. Recovery rebuilds from {previous checkpoint, staging, WAL} without loss.

This is the property the original RFC's failure-modes table implicitly assumed but never
made explicit — and never enforced in code.

### 8.3 Why we keep older `StatisticsFile` entries around for a while

`TableUpdate::SetStatistics` for an existing `snapshot_id` overwrites the existing entry
for that snapshot. But staging produces a new snapshot every ~100 ms, so each new
checkpoint writes against a new snapshot id and the previous entry remains attached to its
old snapshot. We do not call `RemoveStatistics` aggressively — we let Iceberg's standard
snapshot expiry remove old snapshots and their associated entries together. This gives us
fallback options during corruption recovery, at the cost of a small number of orphaned
Puffin files retained for the snapshot retention window.

---

## 9. Operational Concerns

### 9.1 Cadence and configuration

```toml
[iceberg]
flush_interval_ms = 100              # unchanged
index_checkpoint_interval_s = 30     # new; default
index_checkpoint_min_advance_lsns = 1024  # don't checkpoint if fewer LSNs since last
index_checkpoint_zstd_level = 3      # 0 = disable compression
```

The minimum-advance guard prevents pathological cases where a near-idle collection
checkpoints every 30 s just to advance the LSN by 1.

### 9.2 Honest size estimates

For a 1M-vector, 384-dimension collection:

| Index kind | Bytes (raw f32 vectors + structure) | After zstd-3 (realistic) |
|---|---|---|
| `FlatIndex` | ~1.5 GB (vectors only) | **~1.5 GB** (f32s don't compress) |
| `IvfIndex` (256 centroids, no quantization) | ~1.5 GB + ~0.4 MB centroids + inverted-list integers | **~1.45 GB** |
| `IvfIndex` (SQ8) | ~400 MB | ~380 MB |
| `HnswIndex` (M=16) | ~1.5 GB vectors + ~128 MB graph (16 neighbors × 8 B × 1M) | **~1.55 GB** (graph compresses a bit; vectors don't) |

The v1 RFC's "FlatIndex 1.5 GB → 1.2 GB" zstd ratio implied 20% compression on random
f32 data, which is not realistic. We should plan for "compressed size ≈ raw size + a few
percent" for vector-dominated indexes, and reach for SQ8/PQ if size matters.

Implication: for a 1.5 GB Puffin upload at typical S3 throughput (~50–100 MB/s
single-stream), a checkpoint takes 15–30 seconds. The 30 s default interval is matched to
that envelope; deployments with multiple large collections must increase the interval, or
they will checkpoint continuously.

### 9.3 Memory pressure during checkpoint

The bincode buffer for the snapshot is held in memory while the Puffin writer streams it
out (after a zstd pass). For the 1.5 GB sizes above, that is 1.5 GB resident plus the
in-memory `CollectionManager` (also referencing the same vectors). The two share Arc-ed
storage where possible but the bincode serializer produces a fresh buffer.

Mitigation paths (future work, not part of this RFC):
- Per-blob-type streaming bincode (write graph and vectors as separate Puffin blobs and
  build the in-memory image at load time).
- Memory-mappable index payloads (zero-copy `IndexSnapshot` decoding). Requires a new
  serialization format and is the subject of a future RFC.

### 9.4 Orphan-file GC

`SetStatistics` failures and crashes between upload and registration leave Puffin files in
the staging table's `metadata/` prefix with no Iceberg reference. Iceberg does not GC these
automatically — `remove_orphan_files` is a Spark procedure that LikhaDB does not bundle.

For this RFC we accept the orphan accumulation as long as crashes and conflicts are rare
(target: a handful of orphans per week). The follow-up: a simple LikhaDB-internal GC that
lists `metadata/likhadb-idx-*.puffin`, cross-references against `statistics_path` values in
table metadata, and deletes the rest. This is small (~100 LOC) and we should implement it
in the same milestone, but it does not block this design.

### 9.5 Multi-collection deployments

The flusher already iterates collections sequentially per tick. The checkpoint task does
the same. With N collections, a single tick takes O(N × checkpoint duration); operators
with many collections should either tune the interval down per-collection (not modeled in
this RFC; assume uniform interval for v1) or accept proportional lag.

### 9.6 Object store permissions

`SetStatistics` is a catalog update; `PuffinWriter` writes to the staging table's
`metadata/` prefix via the same `FileIO` the flusher already uses. No new credentials.

---

## 10. Alternatives Considered

### 10.1 Wire up the existing side-table

Keep `likhadb_index_snapshots` and start calling it from the flusher. Adds nothing the
Puffin path lacks; loses snapshot-binding semantics (recovery would still need the
"latest by timestamp" heuristic). Rejected on the same grounds as v1: no binding, no
free GC.

### 10.2 Store the index in a fixed-path object outside Iceberg

`<table_location>/likhadb-index.bin`. One file overwritten in place per checkpoint. No
catalog involvement, simple to implement. Loses crash safety (an in-progress overwrite is
visible to a concurrent reader) and loses the LSN→snapshot binding. Could be salvaged
with a temp + rename, but rename is not atomic on S3. Rejected.

### 10.3 Embed the index as a Parquet table

Write `CollectionSnapshot` into an Iceberg table with one row per checkpoint. This is
roughly what the v1 RFC's side-table did. Rejected: Parquet's columnar layout is wrong for
a serialized graph blob, the row count is always 1 so there is no compression or pruning
benefit, and the lifecycle binding to the staging snapshot still requires extra metadata.
Puffin is the right primitive for derived binary state.

### 10.4 Tie checkpoints to every flush

The v1 RFC's approach. Rejected because flushes happen at 100 ms cadence and checkpoints
take seconds for realistic index sizes. The checkpoint would never keep up.

### 10.5 Bundle all collections into one Puffin file

A single file at the namespace level with one blob per collection. Tempting because Puffin
supports many blobs per file. Rejected because LikhaDB's staging tables are per-collection
and `StatisticsFile` entries attach to a single (table, snapshot_id) pair. There is no
shared snapshot id across staging tables to attach a namespace-wide statistics file to.

### 10.6 Use `Transaction::set_properties` to put the path in table properties

Already used in `staging_io.rs` for `last_wal_lsn`. We could put the Puffin path in a
table property too. Loses the snapshot binding (table properties are global to the table,
not per-snapshot) and pollutes the property namespace. `SetStatistics` is the dedicated
mechanism. Rejected.

---

## 11. Open Questions

### 11.1 `set_statistics` in `iceberg-rs`

Transaction does not expose a high-level `set_statistics` helper. The implementation will
POST the `UpdateTableRequest` manually. If iceberg-rs adds an idiomatic API before this
ships, use it. **Decision needed before implementation starts.**

### 11.2 Reading the `statistics` array from a loaded `Table`

Confirm that `iceberg::table::Table::metadata().statistics()` returns the live array of
`StatisticsFile` entries (per the table metadata builder we already saw populating it).
If not surfaced, we have to refresh table metadata via the REST client manually after
each checkpoint to pick up our own writes. **Needs a 30-minute spike against iceberg-rs.**

### 11.3 FTS state and Puffin

The recent `feat/fts` work persists Tantivy state via MmapDirectory at
`<data_dir>/fts/<collection>/`. This RFC explicitly leaves FTS state local. Two follow-up
questions, not blocking:
- Does FTS-local-only state break the "rebuildable from Iceberg" promise? Strictly yes;
  losing local disk loses FTS. But Tantivy's directory can be re-built from staging by
  re-indexing payload text, so cold-start cost is bounded.
- If a future RFC moves FTS to object storage, it would naturally also be a
  `likhadb-fts-v1` blob in the same Puffin file. The single-blob layout in this RFC does
  not preclude that — `add_blob_zstd` can be called twice.

### 11.4 Multiple snapshots ahead of latest checkpoint at recovery time

If staging has advanced through several snapshots since the last `SetStatistics`,
recovery has to scan staging rows for LSNs in `(checkpoint_lsn, staging_watermark]`. That
scan is a normal table scan; with a partition filter on `lsn` it should be cheap. Verify
empirically before assuming.

### 11.5 Concurrent multi-writer

Two LikhaDB processes writing to the same staging table concurrently is unsupported
today (the flusher assumes exclusive ownership). This RFC does not change that. If
multi-writer is required, both the staging append and the checkpoint task need
`RefSnapshotIdMatch` retry loops that are aware of each other — out of scope here.

### 11.6 Blob-type registration with the upstream community

`likhadb-index-v1` is private. If Iceberg's community adopts a "vector index" blob type
later, we may rename. No action required for this RFC.

---

## 12. Appendix

### 12.1 `StatisticsFile` JSON example

```json
{
  "snapshot-id": 8273649201837465,
  "statistics-path": "s3://bucket/warehouse/likhadb_staging_docs/metadata/likhadb-idx-8273649201837465-91423.puffin",
  "file-size-in-bytes": 1614807552,
  "file-footer-size-in-bytes": 412,
  "blob-metadata": [
    {
      "type": "likhadb-index-v1",
      "snapshot-id": 8273649201837465,
      "sequence-number": 1,
      "fields": [],
      "properties": {
        "likhadb.collection":          "docs",
        "likhadb.dimension":           "384",
        "likhadb.metric":              "cosine",
        "likhadb.index_kind":          "hnsw",
        "likhadb.vector_count":        "1000000",
        "likhadb.fts_enabled":         "true",
        "likhadb.checkpoint_lsn":      "91423",
        "likhadb.staging_snapshot_id": "8273649201837465",
        "likhadb.payload_codec":       "bincode-v1"
      }
    }
  ]
}
```

### 12.2 Puffin binary layout (reference)

```
Offset       Content
0            Magic: 0x50 0x46 0x41 0x31  ("PFA1")
4 .. N       Blob payloads, concatenated
N .. N+F     UTF-8 JSON footer (length F)
-12 ..  -8   footer_length (LE u32 = F)
 -8 ..  -4   flags (LE u32; bit 0 = footer compressed; we set 0)
 -4 ..   0   Trailing magic: 0x50 0x46 0x41 0x31
```

Footer JSON shape:
```json
{
  "blobs": [
    { "type": "likhadb-index-v1",
      "fields": [],
      "snapshot-id": 8273649201837465,
      "sequence-number": 1,
      "offset": 4,
      "length": 1614806528,
      "compression-codec": "zstd",
      "properties": { ... } }
  ],
  "properties": {}
}
```

### 12.3 Files changed

| File | Change |
|---|---|
| `crates/likhadb-lakehouse/src/puffin.rs` | New (writer + reader) |
| `crates/likhadb-lakehouse/src/index_checkpoint.rs` | New (replaces `index_snapshot_io.rs`) |
| `crates/likhadb-lakehouse/src/index_checkpoint_task.rs` | New (background task) |
| `crates/likhadb-lakehouse/src/iceberg_flusher.rs` | WAL-truncation gate now consults `IndexCheckpointTracker` |
| `crates/likhadb-lakehouse/src/iceberg_recovery.rs` | Per-collection checkpoint load; drop side-table read |
| `crates/likhadb-lakehouse/src/iceberg_io.rs` | Add checkpoint config fields to `IcebergConfig` |
| `crates/likhadb-lakehouse/src/lib.rs` | Remove `index_snapshot_io` re-exports; add new ones |
| `crates/likhadb-lakehouse/src/index_snapshot_io.rs` | **Deleted** |

### 12.4 Relation to other RFCs

- **`rfc/rfc_realtime_insert_vectordb.md`**: this RFC adds the missing index half of the
  recovery story sketched there. The two-tier staging architecture is unaffected.
- **`rfc/rfc_datafusion_integration.md`**: DataFusion reads staging snapshots; this RFC
  does not change what DataFusion sees. Statistics files registered via `SetStatistics`
  are ignored by DataFusion when the blob type is unknown.
- **`docs/adr/design-review-iceberg-lakehouse.md`**: the LSN-watermark coexistence model
  proposed there is the foundation this RFC builds on. The new `index_checkpoint_lsn`
  is the second watermark that the ADR's "WAL truncation gated on Iceberg progress"
  invariant implicitly required.
