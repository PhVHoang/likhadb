# likhadb — Development Roadmap

This document describes what needs to be built to turn likhadb from an in-memory index library into a
production vector database. Items are grouped by dependency tier; each tier should be complete before
the next begins.

---

## Current state

| Component | Status | Location |
|---|---|---|
| Exact k-NN (`FlatIndex`) | Done | `crates/likhadb-index/src/flat.rs` |
| Approx k-NN (`IvfIndex`, IVF+SQ8) | Done | `crates/likhadb-index/src/ivf.rs` |
| Approx k-NN (`HnswIndex`) | Done | `crates/likhadb-index/src/hnsw.rs` |
| JSON payload storage | Done | `crates/likhadb-store/src/meta.rs` |
| Metadata filtering (`eq`, `ne`, `exists`, `gt`, `lt`, `in`, `and`, `or`) | Done | `crates/likhadb-store/src/meta.rs` |
| In-process Rust API | Done | `crates/likhadb-store/src/manager.rs` |
| Serde-ready types | Done | `crates/likhadb-core/src/types.rs` |
| Payload in search results | Done | `crates/likhadb-core/src/types.rs` |
| Vector retrieval by ID | Done | `crates/likhadb-store/src/collection.rs` |
| Snapshot persistence | Done | `crates/likhadb-persist/src/snapshot.rs` |
| Write-Ahead Log (WAL) | Done | `crates/likhadb-persist/src/wal.rs` |
| Concurrent access (RwLock) | Done | `crates/likhadb-server/src/state.rs` |
| REST API (axum) | Done | `crates/likhadb-server/` |
| gRPC API (tonic) | Done | `crates/likhadb-server/` |
| Prometheus metrics | Done | `crates/likhadb-server/` |
| Structured tracing | Done | `crates/likhadb-server/` |
| Full-text search (`FtsIndex`, `TantivyFtsIndex`, `enable_fts`, `fts_search`) | Done (F1) | `crates/likhadb-fts/` |
| Lakehouse I/O (Parquet) | Done (L1) | `crates/likhadb-lakehouse/` |
| Vector transforms | **None** | — |
| Hybrid search (vec + FTS) | Done (F2) | `crates/likhadb-store/src/collection.rs`, `crates/likhadb-server/` |

---

## Tier A — Foundation ✅ Complete

### A1 — Payload in search results ✅
`payload: Option<serde_json::Value>` added to `ScoredResult`. `Collection::search` populates it from `MetaStore` behind the `include_payload` flag.

### A2 — Vector retrieval by ID ✅
`get()` / `get_batch()` added to `VectorIndex` trait and all three index types. Exposed on `Collection`.

### A3 — Richer metadata predicates ✅
`make_filter` extended to support `gt`, `lt`, `gte`, `lte`, `in`, `and`, `or` in addition to `eq`, `ne`, `exists`.

---

## Tier B — Persistence ✅ Complete

### B1 — Snapshot serialization ✅
`CollectionManager::save` / `load` implemented in `crates/likhadb-persist/src/snapshot.rs` using `bincode`.

### B2 — Write-Ahead Log (WAL) ✅
Append-only WAL with fsync in `crates/likhadb-persist/src/wal.rs`. Startup replays entries newer than the last snapshot.

---

## Tier C — Concurrency ✅ Complete

### C1 — RwLock-wrapped state ✅
`SharedState` wraps `CollectionManager` in `Arc<tokio::sync::RwLock<...>>` in `crates/likhadb-server/src/state.rs`.

---

## Tier D — API ✅ Complete

### D1 — REST API with axum + tokio ✅
All collection and vector CRUD endpoints plus `POST /collections/:name/query` implemented in `crates/likhadb-server/`.

### D2 — gRPC API ✅
`.proto` schema + `tonic` / `prost` implementation in `crates/likhadb-server/`.

---

## Tier E — Observability ✅ Complete

### E1 — Prometheus metrics ✅
`GET /metrics` exposes `likhadb_collection_vectors_total`, `likhadb_search_duration_seconds`, `likhadb_insert_duration_seconds`, `likhadb_wal_bytes_written_total` via `metrics` + `metrics-exporter-prometheus`.

### E2 — Structured tracing ✅
`tracing` spans on hot paths; JSON formatting via `tracing-subscriber` for log aggregation.

---

## Tier F — Full-Text Search

New crate: `crates/likhadb-fts/` (depends on `tantivy`)

### F1 — Tantivy-backed FTS index ✅

**Goal:** Give each collection an optional full-text index alongside the vector index.

**Approach:**
- Wrap `tantivy::Index` behind a new `FtsIndex` trait (analogous to `VectorIndex`)
- `Collection` gains `fts_index: Option<Box<dyn FtsIndex>>`
- Insert path: if FTS enabled, index the `payload` string fields via Tantivy
- FTS query: `collection.fts_search(query_str, k) -> Vec<FtsResult>`

**New files:**
- `crates/likhadb-fts/src/lib.rs` — `FtsIndex` trait + `FtsResult` type
- `crates/likhadb-fts/src/tantivy_index.rs` — `TantivyFtsIndex` (in-RAM BM25 via `RAMDirectory`)
- `crates/likhadb-store/src/collection.rs` — `fts_index` field, `enable_fts()`, `fts_search()`

**New dependency:** `tantivy = "0.22"` (workspace), gated behind `likhadb-store/fts` feature flag.

**Implementation notes:**
- FTS is opt-in per collection: `collection.enable_fts()` activates it; no tantivy overhead otherwise.
- All string values are recursively extracted from the JSON payload (nested objects and arrays included).
- Thread safety: `IndexWriter` is wrapped in `Mutex`; reader is reloaded after each commit.
- Delete calls `writer.delete_term(Term::from_field_u64(id_field, id))` and commits immediately.

---

### F2 — Hybrid search (vector + FTS) ✅

**Goal:** Single query that fuses vector similarity scores with BM25 text scores using Reciprocal Rank Fusion (RRF):

```
rrf_score(id) = 1/(rrf_k + rank_vector(id)) + 1/(rrf_k + rank_fts(id))
```

**New types in `crates/likhadb-core/src/types.rs`:**
```rust
pub struct HybridQuery {
    pub vector: Vec<f32>,
    pub text: String,
    pub k: usize,
    pub rrf_k: u32,           // default 60
    pub filter: Option<serde_json::Value>,
    pub include_payload: bool,
}
```

**Files changed:**
- `crates/likhadb-core/src/types.rs` — `HybridQuery` type
- `crates/likhadb-store/src/collection.rs` — `Collection::hybrid_search()`
- `crates/likhadb-store/src/manager.rs` — `CollectionManager::enable_fts()`
- `crates/likhadb-store/src/snapshot.rs` — `CollectionSnapshot.fts_enabled` (persists FTS-enabled state across checkpoints)
- `crates/likhadb-persist/src/wal/entry.rs` — `WalOp::EnableFts` for WAL durability
- `crates/likhadb-persist/src/wal/mod.rs` — `WalManager::enable_fts()`
- `crates/likhadb-persist/src/wal/recovery.rs` — replay `EnableFts` ops
- `crates/likhadb-server/proto/likhadb.proto` — `HybridQuery` RPC, `enable_fts` on `CreateCollectionRequest`
- `crates/likhadb-server/src/routes.rs` — `POST /collections/:name/hybrid-query`
- `crates/likhadb-server/src/grpc/service.rs` — `HybridQuery` RPC implementation

**Implementation notes:**
- `enable_fts: bool` on collection creation (REST + gRPC) activates the Tantivy index from the start; WAL logs this as `EnableFts` so it survives restarts, and the snapshot's `fts_enabled` field (with `#[serde(default)]`) handles post-checkpoint recovery.
- Hybrid search retrieves `2k` candidates from each modality, fuses ranks via RRF, truncates to top `k`.
- When FTS is not enabled on a collection, hybrid search falls back gracefully to vector-only results (FTS contributes no rank terms).

**Build order summary:**
```
    F2                  ✅ done (hybrid vector + FTS search, RRF)
         ↓
    L1 → L2 → L3        ← next (parquet → object store → delta lake)
```

---

## Tier L — Lakehouse I/O

New crate: `crates/likhadb-lakehouse/` (depends on `arrow-rs`, `parquet`, `delta-rs`, `object_store`)

### L1 — Parquet import / export ✅ Done

**Goal:** Load vectors + metadata from Parquet files; export collections back to Parquet.

**API:**
```rust
// Import: read a Parquet file into a collection
manager.import_parquet(
    collection_name: &str,
    path: &Path,
    id_col: &str,
    vector_col: &str,        // Arrow FixedSizeList<f32>
    payload_cols: &[&str],
) -> Result<usize>           // returns number of vectors imported

// Export: write a collection's vectors + metadata to Parquet
manager.export_parquet(collection_name: &str, path: &Path) -> Result<()>
```

**New files:**
- `crates/likhadb-lakehouse/src/parquet_io.rs`

**New dependencies:** `arrow = "53"`, `parquet = "53"`

---

### L2 — Object storage (S3 / GCS / ADLS)

**Goal:** Read/write Parquet directly from cloud object storage without local download.

**API extension:**
```rust
manager.import_parquet_url("s3://bucket/path/vectors.parquet", ...) -> Result<usize>
manager.export_parquet_url("gs://bucket/out/", ...) -> Result<()>
```

**New dependency:** `object_store = "0.10"` (AWS, GCS, Azure features)

---

### L3 — Delta Lake integration

**Goal:** Scan a Delta table as the source for vectors + metadata; support incremental sync.

**API:**
```rust
// Full load from a Delta table
manager.import_delta(collection_name: &str, table_uri: &str, ...) -> Result<usize>

// Incremental sync: load only rows added since last_version
manager.sync_delta(collection_name: &str, table_uri: &str, since_version: u64) -> Result<usize>
```

**New dependency:** `deltalake = "0.18"` (with `datafusion` feature for predicate pushdown)

---

## Tier T — Vector Transforms

New crate: `crates/likhadb-transform/`

### T1 — Pluggable insert-time transforms

**Goal:** Apply a transformation to vectors at insert time (normalize, scale, project).

**Trait:**
```rust
pub trait VectorTransform: Send + Sync {
    fn transform(&self, vec: &[f32]) -> Vec<f32>;
}
```

**Built-in transforms:**
- `L2Normalizer` — normalize to unit length before storage (common for cosine search)
- `ScalarScaler { scale: f32 }` — multiply all dimensions by a constant

**Files to change:**
- `crates/likhadb-transform/src/lib.rs` — trait + built-ins
- `crates/likhadb-store/src/collection.rs` — `transform: Option<Box<dyn VectorTransform>>`

---

### T2 — Derived metadata fields

**Goal:** Compute metadata fields from the raw vector at insert time (e.g., L2 norm, max component).

**Example:** Auto-store `{"norm": 1.0, "max_dim": 3}` alongside each vector for filter-friendly access.

---

## Workspace layout

```
likhadb/
├── crates/
│   ├── likhadb-core/      # Primitives, error types, distance kernels       ✅
│   ├── likhadb-index/     # VectorIndex trait + FlatIndex + IvfIndex + HnswIndex  ✅
│   ├── likhadb-store/     # Collection, CollectionManager, MetaStore         ✅
│   ├── likhadb-persist/   # Snapshot + WAL                                   ✅
│   ├── likhadb-server/    # axum REST + tonic gRPC + Prometheus + tracing    ✅
│   ├── likhadb-fts/       # Tantivy-backed FTS + hybrid query                ✅ (F1 done)
│   ├── likhadb-lakehouse/ # Parquet / Delta Lake import-export               [ Tier L ]
│   ├── likhadb-transform/ # Insert-time vector transforms                    [ Tier T ]
│   └── likhadb-bench/     # Criterion benchmarks                             ✅
```

---

## Build order summary

```
A1 → A2 → A3           ✅ done
         ↓
    B1 → B2             ✅ done (likhadb-persist)
         ↓
         C1             ✅ done (shared state in likhadb-server)
         ↓
    D1 → D2 → E1 → E2  ✅ done (likhadb-server complete)
         ↓
    F1                  ✅ done (Tantivy FTS index, likhadb-fts)
         ↓
    F2                  ✅ done (hybrid vector + FTS search, RRF)
         ↓
    L1 → L2 → L3        ← next (parquet → object store → delta lake)
         ↓
    T1 → T2             (vector transforms)
```

---

## Verification checkpoints

```sh
# After each A-tier item
cargo test --workspace && cargo clippy --workspace -- -D warnings

# After B1 (snapshot)
# Write a round-trip test: insert 1000 vectors, save, reload, assert search results identical.

# After B2 (WAL)
# Write a crash-recovery test: insert, kill process mid-write, restart, verify consistency.

# After D1 (HTTP API)
cargo run -p likhadb-server &
curl -s -X POST localhost:8080/collections \
     -H 'Content-Type: application/json' \
     -d '{"name":"smoke","dim":4,"metric":"l2"}' | jq .
curl -s -X POST localhost:8080/collections/smoke/vectors \
     -H 'Content-Type: application/json' \
     -d '{"id":1,"vector":[1,2,3,4],"payload":{"label":"test"}}' | jq .
curl -s -X POST localhost:8080/collections/smoke/query \
     -H 'Content-Type: application/json' \
     -d '{"vector":[1,2,3,4],"k":1,"include_payload":true}' | jq .

# After F1 (Tantivy FTS)
# Insert 1k docs with text payloads, run FTS query, assert top result is the best text match.

# After F2 (Hybrid search)
# Run hybrid query; verify merged results contain top vector AND top FTS hits.

# After L1 (Parquet)
# Round-trip: insert 10k vectors → export to Parquet → import into new collection → assert identical search results.

# After L3 (Delta Lake)
# Incremental sync: import Delta table v0, add rows, sync with since_version=1, verify new vectors searchable.

# After T1 (transforms)
# Insert un-normalized vectors with L2Normalizer; assert stored vectors have unit length (norm ≈ 1.0).
```
