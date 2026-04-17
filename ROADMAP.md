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
| Metadata filtering (`eq`, `ne`, `exists`) | Partial | `meta.rs:57-68` |
| In-process Rust API | Done | `crates/likhadb-store/src/manager.rs` |
| Serde-ready types | Done | `crates/likhadb-core/src/types.rs` |
| Persistence | **None** | — |
| HTTP / gRPC API | **None** | — |
| Concurrent access | **None** | — |
| Payload in search results | **Missing** | `ScoredResult` lacks `payload` field |
| Vector retrieval by ID | **Missing** | No `get(id)` on `Collection` |
| Rich filtering | **Partial** | No `gt`/`lt`/`and`/`or` |

---

## Tier A — Foundation (library improvements, no new crates)

These are backward-compatible additions to the existing store crate. They must be done first because
the HTTP API and persistence layer depend on them.

### A1 — Payload in search results

**Gap:** `ScoredResult` (`types.rs:7-11`) only returns `id` and `score`. Users cannot retrieve the
associated JSON payload from a search result without a second lookup.

**Fix:** Add `payload: Option<serde_json::Value>` to `ScoredResult`. `Collection::search` populates
it from `MetaStore`. Gate behind a query flag (`include_payload: bool`) so callers that don't need
it pay zero allocation cost.

**Files to change:**
- `crates/likhadb-core/src/types.rs` — add `payload` field
- `crates/likhadb-store/src/collection.rs` — populate payload in `search()`

---

### A2 — Vector retrieval by ID

**Gap:** There is no way to fetch a stored vector or its payload by ID. Production apps need this for
debugging, deduplication, and re-ranking pipelines.

**Fix:**
```rust
// On Collection
pub fn get(&self, id: VecId) -> Result<Option<(Vector, Option<Value>)>>
pub fn get_batch(&self, ids: &[VecId]) -> Result<Vec<Option<(Vector, Option<Value>)>>>
```

Requires adding `get(id)` to the `VectorIndex` trait and implementing it for all three index types.
FlatIndex and IvfIndex already have flat `Vec<f32>` slabs with `id_to_node` maps. HnswIndex uses
`id_to_node: HashMap<VecId, usize>` + `data: Vec<f32>`.

**Files to change:**
- `crates/likhadb-index/src/traits.rs` — add `fn get(&self, id: VecId) -> Option<Vector>`
- `crates/likhadb-index/src/flat.rs`, `ivf.rs`, `hnsw.rs` — implement `get()`
- `crates/likhadb-store/src/collection.rs` — expose `get()` / `get_batch()`

---

### A3 — Richer metadata predicates

**Gap:** `meta.rs` only supports `eq`, `ne`, `exists`. A `TODO` comment at `meta.rs:38` already marks
this gap. Real workloads need range queries and compound logic.

**Fix:** Extend `make_filter` to handle:
```json
{ "op": "gt",  "field": "price", "value": 10.0 }
{ "op": "lt",  "field": "year",  "value": 2024 }
{ "op": "gte", "field": "score", "value": 0.5 }
{ "op": "lte", "field": "rank",  "value": 100 }
{ "op": "in",  "field": "tag",   "value": ["sports", "news"] }
{ "op": "and", "filters": [ { "op": "eq", ... }, { "op": "gt", ... } ] }
{ "op": "or",  "filters": [ { "op": "eq", ... }, { "op": "eq", ... } ] }
```

**Files to change:**
- `crates/likhadb-store/src/meta.rs`

---

## Tier B — Persistence (makes it a database, not a library)

New crate: `crates/likhadb-persist/`

### B1 — Snapshot serialization

**Goal:** Serialize the full `CollectionManager` state to disk and reload it on startup. No live
mutation safety required — this is point-in-time (offline) snapshots.

**Approach:**
- Derive or implement `serde::Serialize / Deserialize` for `Collection`, `MetaStore`, and all three
  index types (flat `Vec<f32>` slabs are trivially serializable; HnswIndex needs `nodes`, `data`,
  `id_to_node`, `deleted`).
- Persist to a single binary file using `bincode` or `rmp-serde` (compact, fast, no schema).
- Expose `CollectionManager::save(path: &Path)` and `CollectionManager::load(path: &Path)`.

**New files:**
- `crates/likhadb-persist/src/snapshot.rs`

---

### B2 — Write-Ahead Log (WAL)

**Goal:** Crash durability. Every mutation is appended to an append-only WAL before being applied to
the in-memory index. On startup, load the last snapshot and replay WAL entries newer than it.

**WAL entry schema:**
```rust
enum WalOp { Insert, Delete }
struct WalEntry {
    lsn:        u64,
    collection: String,
    op:         WalOp,
    id:         VecId,
    vec:        Option<Vec<f32>>,   // present for Insert
    payload:    Option<Value>,      // present for Insert
}
```

**Lifecycle:**
1. Append entry → fsync (configurable: per-write or per-batch).
2. Apply to in-memory index.
3. Periodic checkpoint: flush snapshot, truncate WAL to entries with LSN > snapshot LSN.

**New files:**
- `crates/likhadb-persist/src/wal.rs`

---

## Tier C — Concurrency

### C1 — RwLock-wrapped state

**Goal:** Allow concurrent reads (`search`, `get`) from multiple threads/tasks while serializing
writes (`insert`, `delete`, `create_collection`).

```rust
pub struct SharedState {
    inner: Arc<tokio::sync::RwLock<CollectionManager>>,
}
```

`VectorIndex: Send + Sync` is already satisfied — no changes to index crates needed.

**New files:**
- `crates/likhadb-server/src/state.rs`

---

## Tier D — HTTP API

New crate: `crates/likhadb-server/` (depends on `likhadb-persist` + `likhadb-store`)

### D1 — REST API with axum + tokio

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/collections` | List all collections |
| `POST` | `/collections` | Create a collection |
| `GET` | `/collections/:name` | Get collection info (dim, metric, count, index type) |
| `DELETE` | `/collections/:name` | Drop a collection |
| `POST` | `/collections/:name/vectors` | Insert or upsert a vector |
| `GET` | `/collections/:name/vectors/:id` | Get a vector by ID |
| `DELETE` | `/collections/:name/vectors/:id` | Delete a vector |
| `POST` | `/collections/:name/query` | k-NN search with optional filter |

**Request / response (query):**
```json
// POST /collections/docs/query
{
  "vector": [0.1, 0.2, 0.3],
  "k": 10,
  "filter": { "op": "eq", "field": "tag", "value": "news" },
  "include_payload": true
}

// 200 OK
{
  "results": [
    { "id": 42, "score": 0.12, "payload": { "tag": "news", "title": "..." } }
  ]
}
```

**Stack:** `axum` + `tokio` + `tower` + `serde_json`.

---

### D2 — gRPC API (follow-on)

Define a `.proto` schema mirroring the REST API. Use `tonic` + `prost`. Lower priority than REST;
higher value for ML inference pipelines that prefer streaming.

---

## Tier E — Observability

### E1 — Prometheus metrics

Expose via `GET /metrics`:

| Metric | Type | Labels |
|---|---|---|
| `likhadb_collection_vectors_total` | Gauge | `collection`, `index_type` |
| `likhadb_search_duration_seconds` | Histogram | `collection`, `index_type` |
| `likhadb_insert_duration_seconds` | Histogram | `collection` |
| `likhadb_wal_bytes_written_total` | Counter | — |

**Dependency:** `metrics` + `metrics-exporter-prometheus`

---

### E2 — Structured tracing

Add `tracing` spans to hot paths (`insert`, `search`, WAL append). Use `tracing-subscriber` with
JSON formatting for production log aggregation (Datadog, Loki, etc.).

---

## New workspace layout (after all tiers)

```
likhadb/
├── crates/
│   ├── likhadb-core/      # Primitives, error types, distance kernels
│   ├── likhadb-index/     # VectorIndex trait + FlatIndex + IvfIndex + HnswIndex
│   ├── likhadb-store/     # Collection, CollectionManager, MetaStore
│   ├── likhadb-persist/   # Snapshot + WAL  [NEW — Tier B]
│   ├── likhadb-server/    # axum HTTP server + shared state  [NEW — Tier D]
│   └── likhadb-bench/     # Criterion benchmarks
```

---

## Build order summary

```
A1 → A2 → A3          (library — no new crates)
         ↓
    B1 → B2            (likhadb-persist)
         ↓
         C1            (shared state in likhadb-server)
         ↓
         D1 → E1 → E2  (likhadb-server complete)
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
```
