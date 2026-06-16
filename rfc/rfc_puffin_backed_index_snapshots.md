# RFC: Puffin-Backed Index Snapshots for HNSW and IVF

| Field | Value |
|---|---|
| **RFC ID** | TBD |
| **Status** | Draft |
| **Author(s)** | TBD |
| **Created** | 2026-06-16 |
| **Last Updated** | 2026-06-16 |
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

This RFC proposes replacing the current `likhadb_index_snapshots` side-table pattern with
**Puffin-backed index snapshots**: HNSW and IVF index state is serialized as Puffin blobs
and attached directly to the Iceberg data table's snapshot via the `statistics-file`
summary property, eliminating the parallel artifact store.

The change touches two crates: `likhadb-lakehouse` (new `PuffinWriter` module and updated
`index_snapshot_io`) and `likhadb-persist` (new blob-type constants and serialization
helpers). No changes are needed in `likhadb-store`, `likhadb-index`, or the server layer.

---

## 2. Motivation

### 2.1 The Problem with the Current Design

`index_snapshot_io.rs` writes a bincode-serialized `CollectionSnapshot` as a Parquet row
into a separate Iceberg table (`likhadb_index_snapshots`). This creates three structural
problems.

**No snapshot binding.** The index snapshot and the data snapshot are committed
independently, to different tables, at different times. There is no atomic guarantee that
the index in `likhadb_index_snapshots` corresponds to the vector data in the staging table
at snapshot `S`. After a crash between the two commits, recovery may load an index that is
one or more data commits behind — silently, with no error.

**Parallel lifecycle to manage.** `likhadb_index_snapshots` is a second Iceberg table that
must be created, monitored, and cleaned up separately. Old snapshot rows accumulate in it
indefinitely; there is no GC hook that removes them when the corresponding data snapshot
expires.

**No time-travel semantics.** Because the index is stored in a separate table keyed only
by `written_at_ms`, there is no way to ask "give me the index state that was current when
the data table was at snapshot `S`." Any future AS-OF query feature would have to paper
over this gap with approximate timestamp matching.

### 2.2 What Puffin Gives Us

The Apache Iceberg Puffin format is a flat binary container for derived data attached to
a snapshot. A snapshot's `summary["statistics-file"]` property points to a Puffin file in
object storage. Every engine reading that snapshot can locate the file, inspect its footer,
and read blobs whose types it understands — ignoring unknown types without error.

Binding index state to the same snapshot commit as the data write gives us:

- **Atomicity**: data and index are one commit; partial-write cannot produce an incoherent
  pair.
- **Time travel**: a future `AS OF snapshot_id` query reads the index from the Puffin file
  attached to that exact snapshot.
- **GC for free**: Iceberg's orphan-file cleanup reaps Puffin files when their snapshots
  expire, with no additional bookkeeping in LikhaDB.
- **Multi-engine readability**: any engine that learns the `likhadb-hnsw-v1` or
  `likhadb-ivf-v1` blob type can read the same index artifacts.

---

## 3. Background and Prior Art

### 3.1 Apache Iceberg Puffin

The Puffin specification defines a binary container with the structure:

```
PFA1 (4B magic)
[blob payload 0] [blob payload 1] ... [blob payload N]
UTF-8 JSON footer: [{type, fields, offset, length, codec, properties}, ...]
footer_length (4B LE)
flags (4B)
PFA1 (4B trailing magic)
```

Each blob is independently addressable by byte-range request (the footer carries offsets
and lengths). This makes it practical to store multiple large blobs in a single file and
read only the relevant one — important for a collection with both an HNSW and an IVF
index.

The two standardized blob types today are `apache-datasketches-theta-v1` (distinct-value
sketches) and `deletion-vector-v1` (row-level deletion bitmaps). The spec explicitly
permits additional opaque types.

### 3.2 Inspiration

Borycki (2026), "Puffin-Backed Vector Indexes", describes placing Vamana/DiskANN graph
indexes in Puffin blobs for a distributed MPP engine (FlockDB). The lifecycle integration
pattern — one Puffin file per snapshot, committed atomically, GC'd by the table format —
is directly applicable to LikhaDB even though LikhaDB is single-node and uses different
index algorithms. The distributed shard protocol from that paper does not apply here.

### 3.3 Current Index Persistence Path

On WAL flush, `IcebergFlusher` calls `write_collection_snapshot` which:

1. Bincode-serializes `CollectionSnapshot` into a binary blob.
2. Wraps it in a single-row Parquet file.
3. Appends that file to the `likhadb_index_snapshots` Iceberg table via `fast_append`.

Recovery (`open_with_iceberg`) scans the `likhadb_index_snapshots` table, picks the
highest `written_at_ms` row per collection, and deserializes it.

This is the code we are replacing.

---

## 4. Design Goals

1. **Atomic snapshot binding**: index state and data state share one Iceberg snapshot commit.
2. **Byte-range accessible blobs**: a reader can load only the blob it needs without
   downloading the full Puffin file.
3. **No new lifecycle**: Puffin GC is inherited from the table format's snapshot expiry.
4. **Drop-in recovery**: `open_with_iceberg` reads the same `CollectionSnapshot` type; the
   deserialization format is unchanged.
5. **One Puffin file per data table per flush**: no proliferation of sidecar files.

---

## 5. Non-Goals

- Distributed sharding of index blobs across executor nodes — LikhaDB is single-node.
- Cross-engine standardization of blob types — the `likhadb-*-v1` types are internal until
  there is community interest.
- Replacing the WAL or the staging Parquet table — those remain unchanged.
- Supporting AS-OF queries in this RFC — the snapshot binding makes it possible; the query
  layer is out of scope here.
- Streaming/incremental index refresh using manifest diffs — that is a follow-on RFC.

---

## 6. Proposed Design

### 6.1 Overview

After `IcebergFlusher` flushes WAL inserts to the staging Parquet table and commits data
snapshot `S`, it serializes the current index state as a Puffin file and commits a second
**metadata-only snapshot** `S+1` that carries `summary["statistics-file"] = puffin_path`.
The data manifest of `S+1` is identical to `S`; the only change is the sidecar pointer.

```
WAL flush → staging Parquet commit (snapshot S)
          → serialize index → write Puffin to object store
          → metadata-only commit (snapshot S+1, statistics-file = puffin_path)
```

Recovery reads snapshot `S+1` (the latest), follows `statistics-file`, deserializes the
Puffin footer, reads each `likhadb-*-v1` blob, and reconstructs `CollectionSnapshot`.

### 6.2 Blob Types

Three new Puffin blob types are defined:

| Type string | Contents | When present |
|---|---|---|
| `likhadb-flat-v1` | Flat brute-force index | Collection uses `FlatIndex` |
| `likhadb-ivf-v1` | IVF centroid table + inverted lists | Collection uses `IvfIndex` |
| `likhadb-hnsw-v1` | HNSW layer graph + entry point | Collection uses `HnswIndex` |

All three carry identical Puffin blob properties:

```json
{
  "likhadb.collection":   "<collection_name>",
  "likhadb.dimension":    "<d>",
  "likhadb.metric":       "l2 | cosine | dot",
  "likhadb.vector_count": "<n>",
  "likhadb.base_snapshot": "<snapshot_id_of_data_commit>"
}
```

The `fields` array in the blob footer entry contains the Iceberg field ID of the indexed
vector column. Where the field ID is not yet known (e.g., during initial index build before
any Iceberg schema is established), it is set to `[-1]` as a sentinel.

### 6.3 Puffin File Layout

One Puffin file per flush cycle, stored at:

```
<table_location>/metadata/likhadb-idx-snap-<S+1>.puffin
```

For a collection with both an IVF and an HNSW index (unlikely in practice, but supported),
both blobs are packed into the same file. A collection with a single HNSW index produces
a single-blob Puffin file.

```
PFA1
[blob 0: likhadb-hnsw-v1 for collection "docs"]
[blob 1: likhadb-ivf-v1  for collection "docs-ivf"]
Footer JSON (offsets, lengths, types, properties per blob)
footer_length (4B LE) · flags (4B) · PFA1
```

The footer is small (a few kilobytes) and is fetched via HTTP range request from the end
of the file. Individual blobs are then fetched by their `(offset, length)` pair, also via
range request. No full-file download is required during recovery.

### 6.4 Blob Payload Format

Each blob payload is a bincode-encoded `CollectionSnapshot` with a 4-byte magic header:

```
[magic: 4B]  [bincode payload: variable]
```

Magic values:
- `LFLT` (0x4C 0x46 0x4C 0x54): `likhadb-flat-v1`
- `LIVF` (0x4C 0x49 0x56 0x46): `likhadb-ivf-v1`
- `LHNS` (0x4C 0x48 0x4E 0x53): `likhadb-hnsw-v1`

Compression: blobs are `zstd`-compressed (Puffin codec field: `"zstd"`). The magic is part
of the pre-compression payload, not a framing header after decompression.

Rationale for keeping bincode: the `CollectionSnapshot` serialization format is already
exercised by the existing round-trip tests in `index_snapshot_io.rs`. Changing it is out
of scope. The Puffin wrapper adds versioning (type string) and lifecycle management without
touching the inner format.

### 6.5 Metadata-Only Commit

After the Puffin file is written to object storage, a second commit is issued to the REST
catalog:

```
PATCH /v1/{prefix}/namespaces/{ns}/tables/{table}
{
  "updates": [
    { "action": "set-snapshot-ref", ... },
    { "action": "set-properties",
      "updates": { "statistics-file": "<puffin_path>" }
    }
  ],
  "requirements": [{ "type": "assert-current-snapshot-id", "snapshot-id": S }]
}
```

This is a standard `UpdateTableRequirementsAction` via the REST catalog client already used
in `iceberg_flusher.rs`. If the requirement fails (concurrent writer), the flush retries
from step 1 using the new current snapshot as the base.

The `iceberg-rs` `Transaction` API does not yet expose `set-properties` on an existing
snapshot directly. The implementation will use the lower-level REST client to post the
`UpdateTableRequest` manually, mirroring how `index_snapshot_io.rs` already calls
`fast_append` directly on `Transaction`.

---

## 7. Component Specifications

### 7.1 New: `likhadb-lakehouse/src/puffin_writer.rs`

```rust
pub struct PuffinWriter {
    blobs: Vec<PuffinBlobEntry>,
    payload: Vec<u8>,
}

pub struct PuffinBlobEntry {
    pub blob_type: &'static str,
    pub fields: Vec<i32>,
    pub properties: HashMap<String, String>,
    pub offset: u64,
    pub length: u64,
    pub compression_codec: &'static str,
}

impl PuffinWriter {
    pub fn new() -> Self;
    pub fn add_blob(
        &mut self,
        blob_type: &'static str,
        fields: Vec<i32>,
        properties: HashMap<String, String>,
        payload: &[u8],       // pre-compression
    ) -> Result<(), LakehouseError>;
    pub fn finish(self) -> Result<Vec<u8>, LakehouseError>; // complete Puffin bytes
}
```

`add_blob` zstd-compresses the payload, records the offset, and appends to the internal
buffer. `finish` serializes the JSON footer, appends the footer length, flags, and trailing
magic.

Mirrors `PuffinReader` (which already exists in `iceberg-rs`) in structure so the two stay
in sync as the iceberg-rs version evolves.

### 7.2 Modified: `likhadb-lakehouse/src/index_snapshot_io.rs`

`write_collection_snapshot` and `load_collection_snapshots` are replaced with:

```rust
/// Serialize `snapshots` as a Puffin file, write it to object storage,
/// and commit a metadata-only snapshot on `table` pointing to it.
pub async fn write_puffin_snapshot<C: Catalog>(
    catalog: &C,
    table: &Table,
    snapshots: &[CollectionSnapshot],
    base_snapshot_id: i64,
) -> Result<(), LakehouseError>;

/// Read the Puffin file referenced by `snapshot`'s statistics-file property
/// and deserialize all `likhadb-*-v1` blobs into `CollectionSnapshot`s.
pub async fn load_puffin_snapshots(
    table: &Table,
    snapshot_id: Option<i64>,  // None = latest
) -> Result<Vec<CollectionSnapshot>, LakehouseError>;
```

The old `index_snapshot_table_ident` function and the `likhadb_index_snapshots` table are
removed. Migration is handled by a one-time read: if `load_puffin_snapshots` returns empty
(no `statistics-file` on the latest snapshot), the recovery path falls back to
`load_collection_snapshots` from the old side-table, then immediately re-persists via the
new Puffin path.

### 7.3 Modified: `likhadb-lakehouse/src/iceberg_flusher.rs`

`flush_once` gains a third step after the data commit:

```rust
// existing steps 1 & 2 unchanged
// new step 3:
write_puffin_snapshot(&catalog, &table, &snapshots, data_snapshot_id).await?;
```

`snapshots` is obtained by calling `store.to_snapshot_with_lsn(max_lsn)` — the same
value previously passed to `write_collection_snapshot`.

### 7.4 Modified: `likhadb-lakehouse/src/iceberg_recovery.rs`

`open_with_iceberg` calls `load_puffin_snapshots` instead of `load_collection_snapshots`.
The fallback to the old side-table is conditional on getting an empty result and is guarded
by a `#[cfg(feature = "iceberg-recovery-migration")]` flag to keep it out of production
builds after the migration window.

### 7.5 New constants: `likhadb-persist/src/puffin_types.rs`

```rust
pub const BLOB_TYPE_FLAT: &str  = "likhadb-flat-v1";
pub const BLOB_TYPE_IVF:  &str  = "likhadb-ivf-v1";
pub const BLOB_TYPE_HNSW: &str  = "likhadb-hnsw-v1";

pub const MAGIC_FLAT: [u8; 4] = *b"LFLT";
pub const MAGIC_IVF:  [u8; 4] = *b"LIVF";
pub const MAGIC_HNSW: [u8; 4] = *b"LHNS";

pub const PROP_COLLECTION:    &str = "likhadb.collection";
pub const PROP_DIMENSION:     &str = "likhadb.dimension";
pub const PROP_METRIC:        &str = "likhadb.metric";
pub const PROP_VECTOR_COUNT:  &str = "likhadb.vector_count";
pub const PROP_BASE_SNAPSHOT: &str = "likhadb.base_snapshot";
```

---

## 8. Data Flow

### 8.1 Write Path (Flush)

```
IcebergFlusher::flush_once
  │
  ├─ 1. drain WAL entries (read lock, no I/O)
  │
  ├─ 2. append vectors to staging Parquet table
  │       └─ commit → data snapshot S
  │
  ├─ 3. serialize index state
  │       ├─ CollectionManager::to_snapshot_with_lsn(max_lsn)
  │       └─ for each CollectionSnapshot:
  │             bincode::serialize → prepend magic → zstd compress
  │
  ├─ 4. PuffinWriter::add_blob (one per collection × index type)
  │       └─ PuffinWriter::finish → Vec<u8>
  │
  ├─ 5. table.file_io().new_output(puffin_path).write(puffin_bytes)
  │
  ├─ 6. metadata-only REST commit
  │       └─ statistics-file = puffin_path, requirement = snapshot S
  │           → snapshot S+1
  │
  └─ 7. advance WAL iceberg_watermark to max_lsn
```

### 8.2 Read Path (Recovery)

```
open_with_iceberg
  │
  ├─ 1. load_table from REST catalog
  │
  ├─ 2. read latest snapshot → check summary["statistics-file"]
  │       └─ if absent → fallback to old side-table (migration path)
  │
  ├─ 3. HTTP GET puffin_path (footer range: last 12 + footer_length bytes)
  │       └─ parse JSON footer → list of (type, offset, length, properties)
  │
  ├─ 4. for each blob with type in {likhadb-flat-v1, likhadb-ivf-v1, likhadb-hnsw-v1}:
  │       ├─ HTTP GET puffin_path (range: offset..offset+length)
  │       ├─ zstd decompress
  │       ├─ verify magic header
  │       └─ bincode::deserialize → CollectionSnapshot
  │
  └─ 5. CollectionManager::from_snapshot(ManagerSnapshot { collections, last_lsn })
```

---

## 9. Failure Modes and Mitigations

| Failure | Effect | Mitigation |
|---|---|---|
| Crash after data commit (step 2), before Puffin write (step 5) | Snapshot S exists with no `statistics-file`; recovery falls back to previous Puffin snapshot or old side-table | WAL replay re-applies all entries since last `iceberg_watermark`; no data loss |
| Crash after Puffin write (step 5), before metadata commit (step 6) | Orphaned Puffin file in object storage; not referenced by any snapshot | Iceberg orphan-file GC reaps it on next cleanup run |
| Metadata-only commit conflict (concurrent writer) | `assert-current-snapshot-id` requirement fails | `flush_once` retries; the Puffin file from the failed attempt is orphaned and GC'd |
| Puffin footer truncated / corrupt | `load_puffin_snapshots` returns `Err` | `open_with_iceberg` surfaces the error; operator uses `RECOVER FROM SNAPSHOT <S-1>` to roll back one snapshot |
| Blob payload corrupt (bad bincode or magic mismatch) | Deserialization error for that collection | Other collections in the same Puffin file still load; the affected collection is rebuilt from WAL replay |
| Object store unavailable during recovery | `load_puffin_snapshots` fails | `open_with_iceberg` propagates the error; server does not start; no silent data loss |

---

## 10. Operational Concerns

### 10.1 Migration from Side-Table

The old `likhadb_index_snapshots` table is not dropped automatically. The migration
procedure is:

1. Deploy the new binary. On first flush, the new Puffin path is used.
2. After verifying that recovery uses the Puffin path successfully (check logs for
   `"loaded N collections from Puffin blob"`), the old table can be dropped manually via
   the Iceberg catalog CLI.
3. Remove the `iceberg-recovery-migration` feature flag from the Cargo features.

### 10.2 Puffin File Size

For a typical LikhaDB deployment with a single collection of 1M vectors at 384 dimensions:

| Index type | Uncompressed | Estimated zstd |
|---|---|---|
| `FlatIndex` | ~1.5 GB | ~1.2 GB |
| `IvfIndex` (256 centroids) | ~400 MB | ~300 MB |
| `HnswIndex` (M=16, ef=200) | ~600 MB | ~450 MB |

These are large blobs. The HTTP range-request pattern means recovery only downloads what
it needs (per blob), not the full file. For very large indexes (> 2 GB per blob), consider
the incremental refresh RFC (see Open Questions §12.2) before deploying Puffin-backed
snapshots.

### 10.3 Snapshot Cadence

The flush interval (`IcebergFlusher::with_interval`, default 100 ms) now produces two
Iceberg snapshots per cycle instead of one. Iceberg snapshot history grows twice as fast.
Operators should ensure `history.expire.max-snapshot-age-ms` is set appropriately in the
REST catalog to prevent unbounded metadata growth.

### 10.4 Object Store Permissions

Writing Puffin files requires the same object store credentials already used for Parquet
data files (`s3.access-key-id`, `s3.secret-access-key`). No new permissions are needed.

---

## 11. Alternatives Considered

### 11.1 Keep the Side-Table, Fix the Binding

Add a `data_snapshot_id` column to `likhadb_index_snapshots` so the index row is
explicitly linked to the data snapshot. Rejected because: GC is still manual, time-travel
still requires two reads from two tables, and the "parallel lifecycle" problem remains.
The Puffin approach solves all three problems with less code.

### 11.2 Store Index in a Separate Object at a Deterministic Path

Write the bincode blob to `<table_location>/likhadb/<collection>.bin` with no Iceberg
involvement. Simple to implement but loses all lifecycle guarantees — no versioning, no
GC, no time travel, and a crash during write produces a corrupt file with no rollback path.

### 11.3 Use an Iceberg Delete File Instead of a Puffin Blob

Iceberg delete files are an extension point but are semantically tied to row deletions.
Repurposing them for index state would be a misuse of the spec and would confuse
other engines reading the table. Puffin is the correct extension point.

### 11.4 Embed Index State in Snapshot Properties Directly

Snapshot `summary` is a string-to-string map; large binary payloads cannot be stored
there. Puffin exists precisely because this property map is not suitable for arbitrary
derived data.

---

## 12. Open Questions

### 12.1 Puffin footer commit via `iceberg-rs`

The `iceberg-rs` 0.4 `Transaction` API does not expose `set-properties` on an existing
snapshot. The implementation will use a raw REST POST. If `iceberg-rs` exposes this before
implementation, use the idiomatic API. **Decision needed before implementation starts.**

### 12.2 Incremental Puffin Refresh

For large indexes (> 1 GB), writing a full Puffin file every flush is expensive. A future
RFC should describe applying Iceberg manifest diffs to identify changed Parquet files and
performing a partial HNSW/IVF update rather than a full reserialize. This RFC does not
address that and the flush interval should be tuned conservatively (seconds, not
milliseconds) until incremental refresh is available.

### 12.3 `statistics-file` Collision with Other Engines

If the same Iceberg table is also written by Spark or Trino, their flush commits may
overwrite or ignore the `statistics-file` property. The current spec says the property
is advisory and unknown blob types are ignored, but a write from another engine that
clears `summary` properties would drop our sidecar reference. **Assess risk before
enabling in multi-writer environments.**

### 12.4 Blob type registration

Should `likhadb-hnsw-v1`, `likhadb-ivf-v1`, and `likhadb-flat-v1` be proposed to the
Iceberg community as standard types? Borycki (2026) proposes analogous types for DiskANN.
No action required for this RFC, but worth tracking on the `iceberg-dev@` list.

---

## 13. Appendix

### 13.1 Puffin Binary Layout Reference

```
Offset  Size    Content
0       4       Magic: 0x50 0x46 0x41 0x31 ("PFA1")
4       var     Blob payload 0
4+|B0|  var     Blob payload 1
...
-N-12   N       UTF-8 JSON footer
-12     4       footer_length (little-endian uint32)
-8      4       flags (reserved, set to 0)
-4      4       Trailing magic: 0x50 0x46 0x41 0x31 ("PFA1")
```

Footer JSON (one object per blob):
```json
[
  {
    "type":             "likhadb-hnsw-v1",
    "fields":           [7],
    "offset":           4,
    "length":           123456,
    "compression-codec": "zstd",
    "properties": {
      "likhadb.collection":    "docs",
      "likhadb.dimension":     "384",
      "likhadb.metric":        "cosine",
      "likhadb.vector_count":  "1000000",
      "likhadb.base_snapshot": "8273649201837465"
    }
  }
]
```

### 13.2 Files Changed

| File | Change |
|---|---|
| `crates/likhadb-lakehouse/src/puffin_writer.rs` | New |
| `crates/likhadb-lakehouse/src/index_snapshot_io.rs` | Replace write/load functions |
| `crates/likhadb-lakehouse/src/iceberg_flusher.rs` | Add step 3 (Puffin commit) |
| `crates/likhadb-lakehouse/src/iceberg_recovery.rs` | Switch to `load_puffin_snapshots` |
| `crates/likhadb-lakehouse/src/lib.rs` | Update public re-exports |
| `crates/likhadb-persist/src/puffin_types.rs` | New (blob type constants) |
| `crates/likhadb-persist/src/lib.rs` | Re-export `puffin_types` |

### 13.3 Relation to Other RFCs

- **RFC: DataFusion as Post-ANN Execution Layer** — DataFusion reads Iceberg snapshots;
  Puffin-backed index snapshots make the correct index version available at read time
  automatically. No direct dependency.
- **RFC: Real-Time Insert Semantics** — The two-tier staging architecture is orthogonal.
  The Puffin flush happens at the same cadence as the existing WAL flush, regardless of
  whether the staging tier is present.
