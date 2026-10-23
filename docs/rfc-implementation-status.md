# RFC implementation status

Last audited: 2026-10-23  
Repository revision: `b1a6846` (`2026-10-09`)  
Scope: every Markdown document in `rfc/`, compared with source, feature wiring,
and tests in the current repository.

This document describes the implementation, not the approval status of an RFC.
The RFC headers still mark the main proposals as drafts. Status labels below mean:

- **Implemented** — present in production code and covered by relevant tests.
- **Partial** — a useful subset exists, but one or more RFC requirements are absent
  or implemented with a documented design change.
- **Not implemented** — proposal code and runtime wiring are absent.
- **Superseded / tracking only** — not an independent feature to implement.

## Executive summary

| RFC or spike | Current implementation status | Main conclusion |
|---|---|---|
| `rfc_datafusion_integration.md` | **Partial** | Candidate registration, enrichment, ACL SQL, score fusion, batched reranking primitives, orchestration, and opt-in REST integration exist. Lazy Iceberg scans/pushdowns, distance UDFs, async bi-encoder UDF integration, plan observability, production reranker wiring, and deployment work do not. |
| `rfc_incremental_index_maintenance.md` | **Partial; minimal runtime path is largely implemented** | External snapshot polling, delta application, watermarks, compaction, metrics, and live REST-catalog tests exist. Durable Iceberg-bound checkpointing and graceful task shutdown do not; configuration/backpressure types are not wired into server startup. |
| `rfc_incremental_index_maintenance_phases.md` | **Tracking document; stale** | Phase 1 is done. Phase 2 runtime validation is now done. Phases 3 and 4 are substantially done despite being marked deferred. Phase 5 remains blocked on a checkpoint design. |
| `spike_iceberg_incremental_scan.md` | **Superseded by implementation and dependency upgrade** | Its 0.4 findings were acted on, but LikhaDB now uses `iceberg-rs 0.9.1`; row-level delete effects are handled by the current scanner. |
| `rfc_puffin_backed_index_snapshots.md` | **Not implemented** | There is no Puffin reader/writer, statistics-file registration, checkpoint task, tracker, or WAL interlock. |
| `rfc_index_checkpoint_property_pointer.md` | **Not implemented** | There is no checkpoint blob/property pointer, checkpoint task, GC, recovery path, or WAL interlock. The current side-table implementation is older than both checkpoint RFCs. |

The most important outstanding architectural gap is index checkpoint durability in
Iceberg. It blocks the incremental-maintenance RFC's durable source watermark and leaves
neither competing checkpoint RFC selected or implemented.

## 1. DataFusion post-ANN execution

Source RFC: `rfc/rfc_datafusion_integration.md`.

### Implemented

- `likhadb-query` exists as a workspace crate and uses DataFusion 51 and
  `iceberg-rs 0.9.1` behind feature flags.
- A startup-owned `DataFusionSession` configures batch size and target partitions.
- Per-request candidate batches use the RFC schema (`id`, `ann_distance`, `ann_rank`)
  and are registered as a `MemTable` in a cloned request context.
- Parquet enrichment tables can be registered for library use. Server integration can
  load an opted-in Iceberg namespace.
- Stage 3 joins candidates with `embeddings`, `documents`, `authors`,
  `classifications`, and `access_control`.
- Sensitivity and team ACL predicates are applied in SQL before score fusion. Team names
  are validated before interpolation. Embeddings are projected only when requested.
- Stage 4a implements candidate-set normalization, exponential recency decay, weighted
  fusion, ordering, and top-M limiting. Weight-sum and recency configuration validation
  are tested.
- Bi-encoder and cross-encoder client traits, optional HTTP clients, one-call-per-batch
  behavior, score combination, sorting, and truncation are implemented and unit-tested.
- `Pipeline` composes candidate registration, enrichment, fusion, and optional reranking.
- With server feature `enriched-search`, REST vector and hybrid query routes invoke the
  pipeline when `LIKHADB_ENRICH_NAMESPACE` enables it.

### Differences and incomplete requirements

| RFC requirement | Current state |
|---|---|
| Shared catalog with request-isolated child context | **Partial.** `SessionContext::clone()` is used. The configured `SessionStrategy::Pool` is descriptive only and is not executed. |
| Lazy Iceberg-backed enrichment and optimizer pushdowns | **Not implemented.** Startup scans every table in the namespace and materializes it as a `MemTable`. Consequently, the RFC's Iceberg partition pruning, Parquet projection/filter pushdown, and file-count acceptance criteria are not met by the server path. |
| ACL context is mandatory | **Not implemented as specified.** An empty `allowed_teams` list means open access rather than API rejection. Sensitivity filtering still applies. |
| Sync SIMD distance UDFs | **Not implemented.** No `dot_product`, `cosine_similarity`, or `l2_distance` UDF registration exists. |
| Bi-encoder as `AsyncScalarUDF` | **Implemented with a design change.** The candidate DataFrame is materialized and one batched model call is made, matching the batching/error intent but not the RFC execution mechanism. |
| Cross-encoder materialize-then-call | **Implemented in the library.** |
| Production model clients wired into server startup | **Not implemented.** Server setup passes `None` for both rerankers, so deployed enriched search stops at Stage 4a. The server does not enable the query crate's `rerank` feature. |
| Query-plan logging and `EXPLAIN ANALYZE` verification | **Not implemented.** No per-request logical/physical plan logging or pushdown verification tests were found. |
| Failure behavior at catalog startup | **Different from RFC.** Enrichment logs and disables itself; the service continues, instead of retrying and holding readiness false. |
| Full pipeline metrics | **Not implemented.** General request metrics exist, but RFC stage/model/plan metrics do not. |
| GKE/KEDA phase | **Not implemented in this repository.** |

The status table in `crates/likhadb-query/src/lib.rs` is stale: it still labels enrichment,
fusion, orchestration, and server integration as planned even though those components exist.

## 2. Incremental index maintenance

Sources: `rfc/rfc_incremental_index_maintenance.md` and its phase tracker.

### Implemented

- `SourceBinding` is accepted at collection creation, stored on the collection, serialized
  in snapshots, and persisted/replayed through `WalOp::SetSourceBinding`.
- `DeltaRow` and `Collection::apply_delta_row` provide the shared upsert/delete path used
  by recovery and maintenance.
- `scan_delta` walks snapshot ancestry and manifests, performs full scans for initial
  binding, and emits ordered upserts/deletes.
- `load_source_table` resolves binding namespace/name through the catalog.
- `IndexMaintenanceTask` polls every bound collection, isolates failures per collection,
  avoids holding the store lock during catalog/file I/O, rejects stale scans after a
  concurrent rebind/tick, and advances `source_snapshot_id` only after all rows apply.
- A non-ancestor watermark falls back to a full scan.
- The task is spawned by the server whenever Iceberg recovery/catalog configuration is
  active.
- HNSW tombstone accounting and live-node rebuild exist. Maintenance builds off-thread,
  journals intervening mutations, replays them, and performs a short locked swap.
- IVF compaction retrains centroids and is triggered by successfully applied row counts.
- The RFC metrics are implemented, including source lag, delta rows, tombstone ratio,
  compactions, full rescans, and unresolved delete files.
- Tests cover first-bind scanning, non-ancestor fallback, external append ingestion,
  HNSW/IVF compaction, REST-catalog append and data-file-drop behavior, concurrency/stale
  scans, WAL persistence, and metrics exposure.

### Evolution beyond the original minimal cut

The RFC and phase tracker target `iceberg-rs 0.4` and defer position/equality deletes.
The current lakehouse crate uses `iceberg-rs 0.9.1`. `scan_delta` uses Iceberg's current
reader for delete-aware live rows and reconciles parent/child live ID sets when row-level
delete files are added. This covers positional deletes and equality deletes on arbitrary
columns. Unsupported or unattributable delete situations still increment
`unresolved_delete_files`, preserving the guardrail.

### Remaining gaps and deviations

| RFC requirement | Current state |
|---|---|
| Durable source watermark atomically bound to an Iceberg index checkpoint | **Not implemented.** The watermark survives local bincode checkpoints, but neither proposed Iceberg checkpoint format exists. |
| Recovery from a durable Iceberg checkpoint followed by delta scan | **Not implemented.** Recovery still uses the legacy shared snapshot table plus staging/WAL recovery. |
| `[maintenance]` runtime configuration | **Partial/unwired.** `MaintenanceConfig` and row-budget tests exist, but server startup constructs `IndexMaintenanceTask` with defaults, ignores `enabled` and `max_rows_per_tick`, and starts it whenever an Iceberg catalog is configured. |
| Max rows per tick/backpressure | **Not wired.** `PendingMaintenanceRows` implements the behavior in isolation, but `IndexMaintenanceTask` applies a complete scanned range through `WalManager::apply_source_delta`. |
| Graceful shutdown finishing an in-flight maintenance range | **Not implemented.** The spawned maintenance handle is dropped and has no shutdown signal/final pass. |
| Full-rescan semantics after file removals | **Needs caution.** The scanner supplies the delta rows it observes, but a full scan is not an explicit replace-all operation; IDs absent from the full-scan result are not automatically removed unless represented by delete reconciliation. |
| FTS delta behavior | **Implemented indirectly for payload-bearing upserts/deletes** through `apply_delta_row`; there is no separate source-schema/FTS contract described in runtime configuration. |

### Corrected phase status

| Phase tracker entry | Actual state |
|---|---|
| Phase 1 — foundations | **Done.** |
| Phase 2 — scan layer; runtime validation pending | **Done, including live REST-catalog validation.** The dependency and delete strategy have advanced beyond the phase text. |
| Phase 3 — maintenance task deferred | **Substantially done.** Polling, table loading, first-bind scan, application, watermark advancement, server spawn, and integration tests exist. Backpressure and graceful shutdown remain. |
| Phase 4 — compaction and observability deferred | **Substantially done.** HNSW/IVF triggers and all listed metrics exist. Runtime configuration wiring remains. |
| Phase 5 — durable watermark and recovery blocked | **Still blocked/not implemented.** |

## 3. Iceberg incremental-scan spike

Source: `rfc/spike_iceberg_incremental_scan.md`.

The spike is historical evidence rather than an implementation RFC. Its main 0.4-era
decision—hand-roll manifest attribution and initially limit delete handling—explains the
scanner's structure. The current state has moved on:

- the Iceberg dependency is `0.9.1`, not `0.4`;
- the live REST-catalog/MinIO scan test requested as follow-up exists in
  `crates/likhadb-lakehouse/tests/incremental_scan_rest.rs`;
- row-level delete effects are no longer categorically out of scope;
- manifest history is still used to attribute changes and preserve snapshot ordering.

The spike should therefore be read as resolved historical research, not as the current
capability statement.

## 4. Puffin-backed index checkpoints

Source RFC: `rfc/rfc_puffin_backed_index_snapshots.md`.

Status: **not implemented**.

None of the proposal's defining components are present:

- no `puffin.rs`, `PuffinWriter`, or `PuffinReader`;
- no `index_checkpoint.rs` using `StatisticsFile`/`SetStatistics`;
- no `IndexCheckpointTask` or `IndexCheckpointTracker`;
- no checkpoint LSN/staging snapshot properties in a Puffin blob;
- no flusher rule that gates WAL truncation on per-collection index checkpoint coverage;
- no recovery from a latest valid Puffin checkpoint;
- no Puffin orphan-file GC or checkpoint metrics.

The file the RFC says should be deleted, `index_snapshot_io.rs`, still exists. It writes
bincode `CollectionSnapshot` blobs into rows of a shared
`likhadb_index_snapshots` Iceberg table and recovery loads the latest timestamped row per
collection. This is a legacy side-table design, not a partial Puffin implementation.

## 5. Property-pointer index checkpoints

Source RFC: `rfc/rfc_index_checkpoint_property_pointer.md`.

Status: **not implemented**.

There is no zstd checkpoint blob under a staging table, no
`likhadb.index_checkpoint_{path,lsn,size_bytes}` property set, no background checkpoint
task, no previous-blob deletion/GC, no property-based recovery, and no checkpoint-aware
WAL truncation gate. `CollectionSnapshot` also does not implement the RFC's proposed
checkpoint format/version migration in this path.

This RFC and the Puffin RFC are alternatives. They should not both be counted as missing
cumulative work; the project needs to select and implement one design (or formally choose
a third design). Until then, incremental maintenance cannot persist an Iceberg-bound
source watermark with the index state it describes.

## 6. Current persistence behavior outside both checkpoint RFCs

To avoid conflating existing persistence with either proposal:

- Local durability uses `WalManager`, `snapshot.bin`, and `wal.log`, with periodic and
  final graceful-shutdown local checkpoints.
- Iceberg staging persists WAL-derived rows and a staging watermark.
- Iceberg recovery can load legacy collection snapshots from the shared snapshot table,
  then replay staging/WAL state.
- `source_snapshot_id` is serialized inside `CollectionSnapshot`, so a local checkpoint
  can retain it. That does not provide the RFC invariant of atomically binding it to the
  corresponding index checkpoint in Iceberg.

## 7. Recommended next work

1. Decide between the Puffin and property-pointer checkpoint RFCs, or replace both with a
   newly accepted design. Implement its checkpoint/WAL interlock before claiming durable
   incremental-maintenance completion.
2. Wire `MaintenanceConfig` into server configuration and task construction, including
   the enabled flag, interval, compaction thresholds, row budget, and shutdown behavior.
3. Update `rfc_incremental_index_maintenance_phases.md` and the query crate's inline
   status table so they no longer contradict the source.
4. For DataFusion, replace eager Iceberg-to-`MemTable` loading with a lazy table provider
   before treating pushdown or large-table acceptance criteria as complete.
5. Decide whether Stage 4b's materialize-and-batch implementation formally supersedes the
   AsyncUDF requirement. Then wire production clients, timeouts, error/degraded-mode
   policy, and stage metrics accordingly.
6. Add end-to-end enriched-search tests through the REST routes and automated physical
   plan assertions for ACL/filter/projection behavior.

## 8. Verification performed for this audit

- Read all six files under `rfc/`.
- Traced the relevant implementations across `likhadb-core`, `likhadb-index`,
  `likhadb-store`, `likhadb-persist`, `likhadb-lakehouse`, `likhadb-query`, and
  `likhadb-server`.
- Reviewed unit and integration test coverage, including live-test gates.
- Ran `cargo test --workspace --all-features` from the repository root: all executed
  tests passed. Four external-service lakehouse tests and one documentation test were
  ignored by their existing test attributes/configuration.
