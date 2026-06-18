# RFC: Index Checkpoints via Property Pointer (No-Puffin Variant)

| Field | Value |
|---|---|
| **RFC ID** | TBD |
| **Status** | Draft — counter-proposal to `rfc_puffin_backed_index_snapshots.md` |
| **Author(s)** | TBD |
| **Created** | 2026-06-18 |
| **Target Milestone** | TBD |

---

## 1. Position

This RFC is the **deliberately simpler alternative** to the Puffin-backed design. It
covers the same problem — persisting `CollectionSnapshot` to Iceberg so cold-start does
not require WAL-replaying every vector — without introducing Puffin, `StatisticsFile`,
or a `SetStatistics` REST round-trip.

The Puffin design buys two things that LikhaDB does not yet need:

| Puffin feature | Used by Puffin RFC v2? |
|---|---|
| Per-snapshot binding for index time-travel (`AS OF S` reads the matching index) | **Non-goal** (Puffin RFC §4) |
| Multi-engine readability of the derived blob | **Non-goal** (Puffin RFC §2.2, §4) |
| Multiple blobs in one container (e.g., index + FTS in same file) | Not used (single `likhadb-index-v1` blob; FTS stays local) |
| Standard Iceberg lifecycle hook (`remove_orphan_files`) | Acknowledged as needing a custom GC anyway (Puffin RFC §9.4) |

Strip those out and what remains is "blob in object storage + atomic pointer in the
catalog". The pointer mechanism already exists in the codebase: `staging_io.rs:159` uses
`Transaction::set_properties` to atomically commit a data file together with a
`last_wal_lsn` table property. We extend the same pattern with two more properties and
a separate blob object.

---

## 2. Design

### 2.1 The pointer

Three new table properties on each `likhadb_staging_<collection>` table:

| Property | Type | Meaning |
|---|---|---|
| `likhadb.index_checkpoint_path` | string | Absolute path to the latest index blob in object storage |
| `likhadb.index_checkpoint_lsn` | u64 (as string) | All WAL LSNs ≤ this are reflected in the blob |
| `likhadb.index_checkpoint_size_bytes` | u64 (as string) | For sanity-check on load |

`last_wal_lsn` (`STAGING_WATERMARK_PROP`, already defined) is untouched. The two
watermarks coexist exactly like in the Puffin design.

### 2.2 The blob

One file per checkpoint, written at:

```
<staging_table_location>/index/likhadb-index-<lsn>.bin.zst
```

Layout: zstd-compressed bincode of `CollectionSnapshot`. No container. The blob type
discriminator is encoded by the filename prefix; format version is embedded in the
bincode payload (extend `CollectionSnapshot` with a `#[serde(default)] schema_version:
u32` field, defaulting to 1).

That is the entire format. No footer, no magic, no framing.

### 2.3 The commit

After uploading the blob:

```rust
Transaction::new(&staging_table)
    .set_properties(HashMap::from([
        ("likhadb.index_checkpoint_path".to_string(), new_path),
        ("likhadb.index_checkpoint_lsn".to_string(),  new_lsn.to_string()),
        ("likhadb.index_checkpoint_size_bytes".to_string(), size.to_string()),
    ]))?
    .commit(catalog)
    .await?;
```

This is one REST call. iceberg-rs supports it today; the staging append flow already uses
the exact same call. There is no API gap, no spike, no manually-crafted `UpdateTable`
JSON.

The atomicity story: the property update is an Iceberg metadata commit. If it succeeds,
the new path is the canonical one. If it fails (catalog conflict), the uploaded blob is
an orphan and the checkpoint task retries. Conflict detection comes "for free" through
Iceberg's metadata-version optimistic concurrency — no explicit
`RefSnapshotIdMatch` requirement is needed because property-only commits don't conflict
with concurrent staging appends in a way that breaks safety (the new properties simply
land on top of the latest metadata).

### 2.4 Deletion of the previous blob

After a successful property update, the previous `likhadb.index_checkpoint_path` is read
from the pre-update metadata and the old file is deleted. If the delete fails (transient
S3 error), it is logged and the file is treated as an orphan; §3.3 cleans it up.

We do not keep historical checkpoints. The cost is that corruption recovery has to fall
back to "scan staging + replay WAL from `previous_iceberg_watermark`" rather than
"load the previous good checkpoint". For LikhaDB's scale this is acceptable — the WAL is
small and staging is queryable. The Puffin design's multi-checkpoint retention is a
function of Iceberg's snapshot-expiry policy, not anything Puffin-specific; we could
retain blobs the same way here if desired.

### 2.5 Cadence and tasks

Identical to the Puffin design (§5.5, §6.4 of v2):

- Independent tokio interval, default 30 s.
- Skipped if `staging_watermark <= index_checkpoint_lsn`.
- Runs synchronously on graceful shutdown.

### 2.6 WAL interlock

Identical to Puffin design §8.2:

> `wal.truncate_up_to(L)` is called only if
> `L <= min(staging_watermark, index_checkpoint_lsn)` across every collection observed
> in the batch.

This rule is the load-bearing safety property of either design. It is independent of how
the index blob is stored.

### 2.7 Recovery

```
open_with_iceberg
  for each "likhadb_staging_*" table in namespace:
      props ← table.metadata().properties()
      if "likhadb.index_checkpoint_path" in props:
          path ← props["likhadb.index_checkpoint_path"]
          lsn  ← props["likhadb.index_checkpoint_lsn"].parse()
          bytes ← FileIO::new_input(path).read()
          assert bytes.len() == props["likhadb.index_checkpoint_size_bytes"]
          snap  ← bincode::deserialize(zstd::decode(bytes))
          manager.insert(snap)
      else:
          manager.insert_empty(collection_name)
          lsn ← 0
      scan staging where lsn_col > lsn → apply to manager
  WAL replay for entries with lsn > min(checkpoint_lsn, staging_watermark) per collection
```

No statistics-file lookup, no Puffin footer parse, no blob enumeration.

---

## 3. What this design gives up

### 3.1 No per-snapshot index history

The Puffin design retains older `StatisticsFile` entries for the duration of Iceberg's
snapshot retention window, so a corrupted latest checkpoint can be recovered by walking
back to a previous statistics entry. Here, the pointer is overwritten on each
checkpoint. Recovery from a corrupted checkpoint requires the staging-scan + WAL-replay
fallback path.

In practice the WAL holds whatever has not yet been checkpointed plus a small grace
window (we can keep WAL truncation conservative — e.g., truncate to `checkpoint_lsn -
N` rather than exactly `checkpoint_lsn`, retaining N LSNs as a safety buffer). This is a
~10-line change to `iceberg_flusher.rs`.

### 3.2 No future time-travel hook

If LikhaDB later wants `SELECT ... AS OF SNAPSHOT_ID S`, the index for that historic
snapshot is gone — the property pointer only ever names the latest. Migrating then would
mean rewriting the persistence path. This is the main strategic cost of this design.

Mitigation if the team wants to keep the option open: encode the staging snapshot id in
the blob filename (`likhadb-index-<snapshot_id>-<lsn>.bin.zst`) and keep the last K
blobs rather than deleting on every checkpoint. The property pointer still names the
latest; an archive of recent blobs survives, indexed by filename. This gets us 80% of
Puffin's per-snapshot retention with 20% of the machinery, at the cost of an O(K)
listing during corruption recovery.

### 3.3 No standard GC hook

Same problem as the Puffin design (§9.4) — Iceberg's orphan-file procedures are not
bundled. We need a small internal GC anyway. The good news here: the property pointer
gives us an exact "this is the live blob" reference, so the GC is trivial — list
`<staging_table>/index/`, exclude `index_checkpoint_path`, delete the rest. No
`StatisticsFile` cross-reference walk.

### 3.4 No multi-blob layout

If a future RFC moves FTS state to object storage, it gets its own blob and its own
property triple. This is fine as long as the count stays small; if we end up with five or
six independent blobs per collection, the Puffin container's "list of blobs with one
footer" starts to look attractive. Until then it's premature aggregation.

---

## 4. Code delta vs. the Puffin design

| Concern | Puffin RFC v2 | This RFC |
|---|---|---|
| New modules | `puffin.rs`, `index_checkpoint.rs`, `index_checkpoint_task.rs` | `index_checkpoint.rs`, `index_checkpoint_task.rs` |
| Lines of new code (estimate) | ~700 (300 Puffin + 250 checkpoint + 150 task + tests) | ~250 (100 checkpoint + 150 task + tests) |
| iceberg-rs API gaps | `set_statistics` not exposed on `Transaction`; manual REST POST | None — `Transaction::set_properties` is on the happy path |
| New REST request types to construct | `UpdateTableRequest` with `SetStatistics` + `RefSnapshotIdMatch` | Already supported |
| Persistence-format surface area | Puffin container + bincode payload + 9 property keys + `BlobMetadata.fields` semantics + `StatisticsFile` registration semantics | bincode payload + 3 property keys |
| Reader complexity | Tail range request → JSON footer parse → per-blob range request → zstd → bincode | Whole-file read → zstd → bincode |
| Failure modes table rows | 7 | 4 |

The Puffin path is not unreasonable. It is just larger than the problem.

---

## 5. When this design is wrong

Switch to the Puffin design (or a future hybrid) if any of these become true:

1. A concrete need for **index time-travel** appears (e.g., reproducible benchmark
   harnesses comparing a query against a specific historical snapshot, or
   `AS OF SNAPSHOT_ID` semantics surfacing in the SQL layer).
2. A second derived artifact appears that needs to attach to the same staging snapshot
   atomically with the index (e.g., FTS state migrated off local disk, or auxiliary
   re-ranking model weights tied to a specific data version).
3. An external engine wants to consume the index blob, requiring a standard container
   format so the engine can inspect blob types without LikhaDB-specific knowledge.

None of these are on any roadmap visible from current code or docs.

---

## 6. Recommendation

Ship this design first. It solves today's problem (slow cold start, undefined index
persistence) with materially less code and zero `iceberg-rs` API gaps to work around. If
and when one of the §5 triggers fires, migrate to Puffin — the on-disk format change is
self-contained (the in-memory `CollectionSnapshot` is unchanged) and the recovery code
already isolates the persistence concern behind one function call.

The Puffin RFC is not wrong; it is overshooting. Build for the workloads you have, not
the workloads you might have.

---

## 7. Files changed

| File | Change |
|---|---|
| `crates/likhadb-lakehouse/src/index_checkpoint.rs` | New — write/read blob + property update |
| `crates/likhadb-lakehouse/src/index_checkpoint_task.rs` | New — background tokio task |
| `crates/likhadb-lakehouse/src/iceberg_flusher.rs` | Add `IndexCheckpointTracker` consult to WAL-truncation gate |
| `crates/likhadb-lakehouse/src/iceberg_recovery.rs` | Per-collection property read; drop side-table read |
| `crates/likhadb-lakehouse/src/iceberg_io.rs` | Add checkpoint config fields to `IcebergConfig` |
| `crates/likhadb-lakehouse/src/staging_io.rs` | Re-export the three new property-name constants alongside `STAGING_WATERMARK_PROP` |
| `crates/likhadb-lakehouse/src/lib.rs` | Drop `index_snapshot_io` re-exports; add new ones |
| `crates/likhadb-lakehouse/src/index_snapshot_io.rs` | **Deleted** |
| `crates/likhadb-store/src/snapshot.rs` | Add `#[serde(default)] schema_version: u32` to `CollectionSnapshot` for forward-compat |
