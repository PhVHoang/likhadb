# Spike: `iceberg-rs` 0.4 incremental scan + v2 delete maturity

Resolves Open Question §12.1 of `rfc_incremental_index_maintenance.md`. Decides
what the Phase 2 scan layer can rely on before implementation starts.

**Method:** source inspection of `iceberg 0.4.0`
(`~/.cargo/registry/.../iceberg-0.4.0/src`) — the exact version pinned in
`crates/likhadb-lakehouse/Cargo.toml`. References below are `file:line` in that
crate.

## Verdict

| Capability | 0.4 status | Evidence |
|---|---|---|
| Incremental scan (snapshot range `from → to`) | **Absent** | No `incremental` symbol anywhere in `src`. `TableScanBuilder` exposes only `snapshot_id(i64)` — selects *one* snapshot's **full** state, not a delta (`scan.rs:130`). |
| Row-level delete resolution (position/equality) | **Absent — actively errors** | The high-level scan **aborts** on any non-data manifest entry: `content_type() != DataContentType::Data → Err(FeatureUnsupported, "Only Data files currently supported")` (`scan.rs:448`). A v2 merge-on-read table with delete files cannot be scanned at all. |
| Data-file-drop visibility via scan | Hidden by scan | The scan skips non-alive entries (`!is_alive() → return`, `scan.rs:442`), so a `TableScan` only ever yields live data files; it cannot tell you what was *removed* between two snapshots. |
| Low-level manifest primitives for a manual diff | **Present & sufficient** | See below. |

**Bottom line:** the RFC's pessimistic branch (§12.1) and minimal v1 cut (§13.3)
are confirmed. There is **no** usable incremental scan and **no** delete-file
resolution in 0.4. We must hand-roll a manifest-level snapshot diff, and v1 ships
**append + data-file-drop deletes only**; row-level deletes are deferred.

## What 0.4 *does* give us (enough for the minimal cut)

A manual diff is buildable entirely from public `spec` APIs:

- `Snapshot`: `snapshot_id()`, `parent_snapshot_id()`, `sequence_number()`,
  `manifest_list()` (path) — `spec/snapshot.rs:103-120`. Walk `to → from` via
  `parent_snapshot_id`.
- `ManifestList::parse_with_version(bytes, format_version, partition_spec_lookup)`
  (`spec/manifest_list.rs:58`); read the bytes with
  `table.file_io().new_input(path)?.read().await` (`io/file_io.rs:118,270`).
- `ManifestFile` public fields (`spec/manifest_list.rs`): `added_snapshot_id`,
  `content` (Data vs Deletes), `sequence_number`, `manifest_path` — lets us scope
  manifests to the snapshot range and split data vs delete manifests.
- `ManifestFile::load_manifest(&file_io)` → `Manifest::entries()` →
  `[ManifestEntryRef]` (`spec/manifest_list.rs:654`, `spec/manifest.rs:97`).
- `ManifestEntry`: `status()` (`Added`/`Existing`/`Deleted`), `content_type()`,
  `data_file()` (`spec/manifest.rs:877-954`); `ManifestStatus` enum at
  `spec/manifest.rs:961`.

### Resulting Phase 2 approach (revised from the RFC sketch)

The RFC §6.1 assumed we could compose an "Iceberg added-files scan." We can't —
so `scan_delta` is implemented as a **hand-rolled manifest diff**, not on top of
`TableScan`:

1. Resolve `SourceBinding` namespace/name → `TableIdent`; `catalog.load_table`.
2. Collect snapshots in `(from, to]` by walking `parent_snapshot_id` from `to`.
3. Read `to`'s manifest list; select `ManifestFile`s with
   `added_snapshot_id ∈ (from, to]`.
4. **Data manifests** → entries with `status == Added` are new data files
   (→ read rows → `DeltaRow::Upsert`); `status == Deleted` are dropped data files
   (→ every row id in that file is a `DeltaRow::Delete` candidate).
5. **Delete manifests** (`content == Deletes`, i.e. position/equality delete
   files) → **not resolved**. Count them in
   `likhadb_unresolved_delete_files_total` and log; do not silently drop.
6. Read added data files **directly via the parquet/arrow reader on the file
   path** — *not* via `table.scan()`, because the scan errors globally if the
   table contains any delete manifest (`scan.rs:448`). The existing
   `import_iceberg` full-scan path (`iceberg_io.rs`) remains usable only for the
   first-bind / full-rescan fallback on append-only tables.

### Why row-level deletes are out for v1

Resolving position/equality deletes means: read each delete file's parquet
ourselves, materialise `(file_path, pos)` / equality predicates, and apply them
against the data files in scope. 0.4 offers zero help (its reader errors on these
files) — this is a self-contained sub-project, exactly the scope the RFC defers.

## Residual risk / recommended follow-up

- Source inspection is definitive on the **API surface** (no incremental API; the
  `FeatureUnsupported` error is unambiguous). Before Phase 2 lands, validate the
  manual-diff path at **runtime** against a seeded REST-catalog + MinIO table
  (append a snapshot from another writer, diff it) to confirm `added_snapshot_id`
  attribution and manifest parsing behave as read.
- `parse_with_version` needs the table's `format_version` and a partition-spec
  lookup from `table.metadata()`; confirm both are reachable on the loaded `Table`
  (they are used internally by the scan planner, `scan.rs:365`).

## Decision

Proceed with the **minimal v1 cut** (RFC §13.3): hand-rolled manifest diff →
append ingestion + data-file-drop deletes + the unresolved-delete guardrail
metric. Do **not** attempt row-level delete resolution on 0.4. Revisit if/when the
dependency is upgraded to a version with mature incremental scan + MOR delete
support.
