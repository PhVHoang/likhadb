# Codebase Map

The reusable 20% for navigating likhadb: the crate skeleton, the four end-to-end
flows, and the invariants that aren't obvious from any single file. For the
conceptual architecture (tiers, query design, Iceberg rationale) see
`docs/ARCHITECTURE.md`, `rfc/`, and `docs/adr/` — don't duplicate those here.

## Crate skeleton

Cargo workspace, 10 crates. Dependencies flow strictly downward — lower crates
never depend on higher ones.

```
likhadb-core        shared vocabulary (VecId, Vector, Metric, ScoredResult,
                    FilterFn, LikhaDbError, SourceBinding); no internal deps
   ↑
likhadb-index       the VectorIndex trait + 3 ANN algorithms (Flat/IVF/HNSW)
likhadb-fts         Tantivy BM25 full-text (sits beside index)
   ↑
likhadb-store       Collection = index + MetaStore + optional FTS; DeltaRow;
                    CollectionManager; the JSON filter DSL
   ↑
likhadb-persist     WAL + snapshot durability wrapping the store
   ↑
likhadb-lakehouse   Tier L: Parquet / MinIO / Iceberg I/O; flusher;
                    incremental scan
likhadb-query       Tier Q: DataFusion post-retrieval pipeline
   ↑
likhadb-server      axum REST (8080) + tonic gRPC (50051); composition root
```

Non-library crates: `likhadb-bench` (criterion), `likhadb-stress` (load tester),
`sdk/python/` (typed client).

Tier map: **Tier R** (recall) = index + fts, **Tier L** = lakehouse,
**Tier Q** (relevance) = query.

Everything optional is feature-gated to keep the default build lean:
`store: persist, fts` · `persist: fts, iceberg-recovery` ·
`server: enriched-search, iceberg-recovery` · `lakehouse: minio, iceberg,
iceberg-recovery`. Some features intentionally depend on others so invalid
configs fail at compile time rather than at runtime (e.g. server
`enriched-search` pulls in `iceberg-recovery`).

## The four flows

Trace these end-to-end and the codebase stops being mysterious.

**Insert.**
`POST /collections/:name/vectors` → `routes::insert_vector` (write lock) →
`WalManager::insert` (write frame → `sync_data` → `next_lsn++`) →
`CollectionManager::get_mut` → `Collection::insert` → `VectorIndex::insert` +
`MetaStore::set` + optional FTS index. Later `IcebergFlusher` drains it to
Iceberg staging and, when safe, truncates the WAL.

**Query.**
`POST /collections/:name/query` (read lock) → `Collection::search` builds a
`FilterFn` from the JSON predicate → `VectorIndex::search` (SIMD + rayon top-k,
filter applied inline) → optional Tier Q pipeline → JSON. The lock guard is
dropped before any `.await` (including the pipeline).

**Hybrid query.**
Same as Query, but `Collection::hybrid_search` runs vector + BM25 for `2k`
candidates each and fuses with RRF
(`1/(rrf_k+rank_vec) + 1/(rrf_k+rank_fts)`), then optionally Tier Q.

**Crash & recover.**
On restart `WalManager::open` loads `snapshot.bin`, replays WAL frames with
`lsn > last_lsn` through the idempotent `apply_op`, and discards any torn/CRC-bad
tail frame. With Iceberg on, `open_with_iceberg` rebuilds from index snapshots +
staging rows + the WAL gap above the watermark. Either way: no committed op is
lost, and re-applying anything is safe.

## Invariants & recurring idioms

- **`lower = better` distance convention.** L2/cosine are natural; Dot is negated
  (`-dot_product`) so one min-heap comparison works for every metric. Applies to
  `ScoredResult.score` too.
- **Flat-slab vector storage.** All vectors live in one `Vec<f32>`;
  slot `i` = `data[i*dim..(i+1)*dim]`. Deletes use **swap-remove** (move last into
  the gap, truncate). Used by Flat, IVF posting lists, and HNSW.
- **Top-k = `OrderedFloat` + size-k `BinaryHeap` max-heap.** rayon search does a
  per-thread heap `fold` then a `reduce` merge — O(threads·k) allocation, no
  shared state.
- **Durable writes = tmp file + fsync + atomic rename.** Snapshot checkpoint and
  WAL truncation both use it. The WAL append path fences with `sync_data` before
  returning.
- **WAL framing:** `[len u32][crc32 u32][payload]`. A torn/CRC-bad **tail** frame
  = uncommitted crash write → discard and stop. A CRC error with bytes **after**
  it = mid-log corruption → hard error (`has_remaining_bytes` distinguishes them).
- **Idempotent apply everywhere.** `apply_op` swallows `CollectionAlreadyExists` /
  `CollectionNotFound`; `DeltaRow::Upsert` overwrites and `Delete` no-ops on a
  missing id. Re-applying a partially-applied range is always safe — this is what
  makes recovery and incremental scans robust.
- **`DeltaRow` is the single apply choke point.** WAL→staging recovery and
  source-table incremental scan both funnel through `Collection::apply_delta_row`.
- **`lsn` gates re-application.** `Collection::insert/delete` take an `lsn` used to
  skip already-applied FTS/staging writes (`if lsn > last_lsn`). Non-persistent
  callers pass `u64::MAX` (= always apply).
- **JSON-Value-as-String serde shim.** bincode can't `deserialize_any`, so
  `serde_json::Value` payloads are serialized as strings (in both `MetaStore` and
  the WAL entry).
- **Never hold a lock across `.await`.** Tokio's `RwLock` isn't await-safe; every
  async handler scopes its guard and drops it before the next yield point.
- **HNSW deletes are tombstones,** not removals — dead nodes stay as traversal
  stepping-stones and are filtered from results. HNSW is the only index that
  meaningfully implements `tombstone_ratio()` / `compact()`.
- **IVF is always queryable.** Before `nlist` vectors arrive it brute-forces a
  staging buffer; training (k-means) fires automatically at the threshold.
  `nprobe == nlist` gives exact recall.
- **Flusher won't truncate the WAL on DDL.** `CreateCollection`/`DropCollection`/
  `EnableFts` aren't mirrored to staging, so a batch containing DDL advances the
  watermark but keeps the WAL — it's the only durable record of that DDL.
