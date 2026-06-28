# Incremental Index Maintenance — Implementation Phases

Tracks the staged delivery of `rfc_incremental_index_maintenance.md`. The RFC
describes the full feature; this file records what is built versus deferred and
why, so each phase stays small and independently verifiable.

## Scoping decisions

- **Watermark persistence:** in-memory only for now; on restart the applied
  source position is re-derived by a bounded full source scan (RFC §5.3). Durable
  persistence of `source_snapshot_id` waits on the Puffin / index-checkpoint work
  (neither `rfc_puffin_backed_index_snapshots.md` nor
  `rfc_index_checkpoint_property_pointer.md` is implemented yet).
- **Delete scope:** the minimal v1 cut (RFC §13.3) — append + data-file-drop
  deletes. Row-level (position/equality) delete resolution is deferred behind a
  guardrail metric, pending the `iceberg-rs` 0.4 maturity spike (RFC §12.1).

---

## Phase 1 — Foundations (DONE)

Dependency-free pieces with no Iceberg/checkpoint coupling. Each is unit-tested in
isolation. Compaction also pays down the pre-existing HNSW tombstone/memory-leak
debt regardless of the rest of the feature.

### Index compaction primitives — `crates/likhadb-index`
- `VectorIndex` trait (`src/traits.rs`): added defaulted `tombstone_ratio() -> f32`
  (default `0.0`) and `compact() -> Option<Box<dyn VectorIndex>>` (default `None`).
  - Note: `compact` returns `Option` rather than the RFC's literal
    `Box<dyn VectorIndex>` — a defaulted no-op cannot clone a `dyn`, and `None`
    cleanly means "in-place index, nothing to compact."
- `HnswIndex` (`src/hnsw.rs`): `tombstone_ratio` = dead physical nodes (deletes +
  overwrite ghosts) / `nodes.len()`; `compact` rebuilds a fresh graph from live
  nodes only, in insertion order, via the existing `insert` path.
- `IvfIndex` (`src/ivf.rs`): `compact` rebuilds + re-runs k-means from the live set
  (addresses centroid drift, RFC §8.2); `None` while pre-training. `tombstone_ratio`
  stays `0.0` (in-place deletes).
- `FlatIndex`: defaults (in-place deletes, nothing to compact).

### Unified apply — `crates/likhadb-store`
- New `DeltaRow` enum (`src/delta.rs`): `Upsert { id, vector, payload }` / `Delete { id }`.
- `Collection::apply_delta_row(row, lsn)` (`src/collection.rs`): single choke point
  routing to existing `insert` (upsert) / `delete` (idempotent), incl. FTS.
- `iceberg_recovery.rs` staging loop refactored onto `apply_delta_row` so recovery
  and (future) maintenance share one path.

### Source-binding plumbing (opt-in, dormant)
- `SourceBinding` (`crates/likhadb-core/src/binding.rs`): serde type identifying the
  source table by namespace/name strings (no `iceberg` dep) + id/vector/payload columns.
- `Collection` carries `source_binding: Option<SourceBinding>` and
  `source_snapshot_id: Option<i64>`; both round-trip through `CollectionSnapshot`.
- `CollectionManager::set_source_binding`; `WalManager::set_source_binding` logs a new
  `WalOp::SetSourceBinding` so bindings survive WAL replay (not just snapshots).
- Create-collection request gains optional `source_binding` (`server/src/types.rs`,
  wired in `routes.rs`). No binding ⇒ behaviour identical to before.

### Tests added
- `likhadb-index`: tombstone ratio tracking; HNSW compact (drops tombstones, keeps
  nearest neighbour + live set); IVF compact (retrain, live set preserved, `None`
  pre-training).
- `likhadb-store`: `apply_delta_row` upsert-overwrite + idempotent delete; binding +
  `source_snapshot_id` snapshot round-trip; `set_source_binding`.
- `likhadb-persist`: `source_binding_survives_restart`.

### Not done in Phase 1 (nothing consumes the binding yet)
No background task reads source deltas, no compaction is triggered, no metrics, no
durable watermark. See below.

---

## Phase 2 — Scan layer (READY; spike done)

**Spike (RFC §12.1) resolved** — see `spike_iceberg_incremental_scan.md`. Findings:
`iceberg-rs` 0.4 has **no** incremental scan and **no** delete-file resolution (its
`TableScan` even errors on any delete file: `scan.rs:448`). But the low-level `spec`
manifest APIs are sufficient to hand-roll a snapshot diff. So:

- New `crates/likhadb-lakehouse/src/incremental_scan.rs`: `scan_delta(table, {from, to}, binding)`
  yielding `DeltaRow`s, implemented as a **manifest diff** (walk `parent_snapshot_id`;
  select `ManifestFile`s by `added_snapshot_id ∈ (from, to]`; `Added` data files →
  `Upsert`, `Deleted` data files → `Delete`). Read added data files **directly via
  parquet/arrow**, not `table.scan()`. Resolve `SourceBinding` namespace/name → `TableIdent`.
- v1 = full rescan (RFC §11.1 — also the expired-snapshot / first-bind fallback, the
  one path that may reuse `import_iceberg` for append-only tables) + manifest-diff
  appends + data-file-drop deletes.
- Row-level (position/equality) deletes deferred behind
  `likhadb_unresolved_delete_files_total` — not viable on 0.4 (would require a custom
  delete-file parser).
- Recommended before merge: a runtime validation against a seeded REST-catalog + MinIO
  table (the spike was source-level).

## Phase 3 — Maintenance task (DEFERRED)

- New `index_maintenance_task.rs`, mirroring `IcebergFlusher` spawn/run
  (`iceberg_flusher.rs`), spawned in `server/src/main.rs`. Per-tick loop per RFC §5.4:
  read `S_now`, skip if unchanged, `scan_delta`, `apply_delta_row` under the write lock,
  advance the in-memory `source_snapshot_id`.
- On first bind with no persisted position: bounded full source scan (RFC §5.3/§7.2).

## Phase 4 — Compaction triggering + observability (DEFERRED)

- After each applied range, check `tombstone_ratio()` and swap in `compact()`'s result
  off-thread with a short write-lock swap (RFC §8.3). IVF drift trigger per §12.4.
- Metrics (RFC §10.3): `likhadb_source_snapshot_lag`, `likhadb_delta_rows_applied_total`,
  `likhadb_index_tombstone_ratio`, `likhadb_index_compactions_total`,
  `likhadb_source_full_rescan_total`.
- Maintenance config block (RFC §10.1).

## Phase 5 — Durable watermark + recovery composition (BLOCKED)

**Blocker:** Puffin / index-checkpoint RFC must land first.

- Persist `source_snapshot_id` in the index checkpoint blob property
  (`likhadb.source_snapshot_id`) atomically with the index it reflects (RFC §9.2).
- Recovery resumes the diff from the persisted snapshot instead of a full rescan
  (RFC §7.2).
