# LikhaDB Architecture

## Crate Structure

LikhaDB is a workspace of six crates with a strict layered dependency order:

```
likhadb-core
    └── likhadb-index
            └── likhadb-store
                    └── likhadb-persist
                            └── likhadb-server
likhadb-bench  (depends on likhadb-index directly)
```

| Crate | Responsibility |
|---|---|
| `likhadb-core` | Shared primitives: `VecId`, `Vector`, `Metric`, `ScoredResult`, `LikhaDbError`, distance functions |
| `likhadb-index` | Index implementations (`FlatIndex`, `IvfIndex`, `HnswIndex`) and the `VectorIndex` trait |
| `likhadb-store` | `Collection` (index + metadata) and `CollectionManager` (named-collection registry) |
| `likhadb-persist` | WAL + snapshot durability layer wrapping `CollectionManager` |
| `likhadb-server` | HTTP/REST (Axum, port 8080) and gRPC (Tonic, port 50051) servers |
| `likhadb-bench` | Criterion benchmarks for index search performance |

---

## Index Types

All index implementations live in `likhadb-index` and satisfy the `VectorIndex` trait
(`crates/likhadb-index/src/traits.rs`), which is the sole coupling point between the
store layer and any index implementation:

```rust
pub trait VectorIndex: Send + Sync {
    fn insert(&mut self, id: VecId, vec: Vector) -> Result<()>;
    fn delete(&mut self, id: VecId) -> bool;
    fn search(&self, query: &[f32], k: usize, filter: Option<FilterFn<'_>>) -> Result<Vec<ScoredResult>>;
    fn get(&self, id: VecId) -> Option<Vector>;
    fn len(&self) -> usize;
    fn dim(&self) -> usize;
    fn index_type(&self) -> &'static str;
}
```

### FlatIndex (`crates/likhadb-index/src/flat.rs`)

Brute-force exact nearest-neighbour search.

- **Storage layout**: a single contiguous `Vec<f32>` slab (`data[i*dim..(i+1)*dim]` owns
  vector `ids[i]`). Eliminates per-vector heap allocations; enables sequential hardware
  prefetching during search.
- **Distance**: `simsimd` hardware-accelerated kernels (NEON on aarch64/M2, AVX-512 on
  x86_64) with scalar fallback for unsupported targets or empty slices.
- **Search**: Rayon parallel fold+reduce. Each thread maintains a local top-k max-heap
  of size `k`; heaps are merged in the reduce step (O(T·k) total allocation, no shared
  mutable state).
- **Delete**: swap-remove — the deleted slot is filled with the last element's data,
  then the slab is truncated. O(1) and avoids shifts.

### IvfIndex (`crates/likhadb-index/src/ivf.rs`)

Approximate nearest-neighbour using an Inverted File (IVF) structure.

- **Clustering**: Lloyd's k-means with up to 25 iterations and a `1e-4` convergence
  tolerance. Assignment and accumulation steps are Rayon-parallelised.
- **Staging buffer**: before `nlist` vectors have been inserted, all vectors live in a
  flat staging slab and searches fall back to brute-force. This makes the index always
  queryable. Training fires automatically on the insert that brings the count to `nlist`.
- **Post-training layout**: `nlist` `PostingList` buckets (each a flat slab). An
  `id_to_list: HashMap<VecId, usize>` provides O(1) cluster lookup for deletes and
  overwrites.
- **Search**: find `nprobe` nearest centroids (sequential, `nlist` is small), then
  parallel fold+reduce over the probed posting lists.
- **SQ8 variant** (`IvfIndex::new_sq8`): after training, each vector is compressed
  from `dim × 4` bytes (f32) to `dim × 1` byte (u8) — a 4× reduction. Query distance
  is computed asymmetrically: the query stays in f32 while stored codes are decoded
  on-the-fly using a per-dimension `(min, scale)` pair. Decoding uses a thread-local
  buffer to avoid per-vector heap allocations.
- **Key parameters**: `nlist` (cluster count / training threshold), `nprobe` (clusters
  searched per query; `nprobe == nlist` gives exact recall).

### HnswIndex (`crates/likhadb-index/src/hnsw.rs`)

Approximate nearest-neighbour using Hierarchical Navigable Small World graphs
(Malkov & Yashunin, 2018).

- **Graph structure**: multi-layer proximity graph. Layer 0 holds every node; each
  higher layer is an exponentially smaller random subset. `m` controls max edges per
  node per layer (layer 0 uses `2*m`).
- **Insert**: random level sampled from geometric distribution with multiplier
  `1/ln(m)`. Phase 1 greedily descends from `max_level` to `level+1` with `ef=1`.
  Phase 2 runs beam search (ef=`ef_construction`) from `min(level, max_level)` down
  to layer 0, connecting bidirectional edges and pruning over-full neighbour lists.
- **Delete**: tombstoning. Deleted nodes remain in the graph as valid traversal
  stepping-stones but are excluded from search results. `len()` returns
  `id_to_node.len() - deleted.len()`. If the entry point is deleted, a replacement is
  found by scanning nodes in reverse-insertion order.
- **Search**: greedy descent with `ef=1` from `max_level` to layer 1, then beam search
  at layer 0 with `ef = max(ef_search, k)`. Deleted nodes and user-filter misses are
  excluded from the result set.
- **Key parameters**: `m` (graph connectivity), `ef_construction` (build quality vs.
  speed; must be ≥ `m`), `ef_search` (query recall vs. latency).

---

## Distance Metrics

Three metrics are supported across all index types, defined in `likhadb-core`:

| Metric | Score semantics | Formula |
|---|---|---|
| `L2` | lower = more similar | Euclidean distance (`sqrt(Σ(aᵢ−bᵢ)²)`) |
| `Cosine` | lower = more similar | `1 − cosine_similarity` |
| `Dot` | lower = more similar | negated dot product (`−Σaᵢbᵢ`) |

The unified "lower is better" convention means all three metrics can share the same
max-heap top-k logic. `simsimd` is used for hardware-accelerated kernels; scalar
implementations in `likhadb-core/src/distance.rs` serve as fallbacks.

---

## Store Layer

### Collection (`crates/likhadb-store/src/collection.rs`)

Pairs a `Box<dyn VectorIndex>` with a `MetaStore` (JSON payload storage). All DML
routes through `Collection`:

- `insert(id, vec, payload)` — delegates to the index, stores payload separately.
- `delete(id)` — removes from the index and from `MetaStore`.
- `search(query, k, predicate, include_payload)` — `MetaStore::make_filter` builds a
  `FilterFn` from the JSON predicate; the filter is passed into `VectorIndex::search`
  so candidates are excluded before entering the result set. Payloads are attached
  after search if `include_payload` is true.

### CollectionManager (`crates/likhadb-store/src/manager.rs`)

A `HashMap<String, Collection>` registry. Factory methods create collections backed
by the chosen index type (`FlatIndex` default, `IvfIndex`, `IvfIndex+SQ8`, `HnswIndex`).

---

## Persistence Layer (`crates/likhadb-persist`)

### Data Directory Layout

```
<dir>/
  snapshot.bin       ← full serialised CollectionManager (written on checkpoint)
  snapshot.bin.tmp   ← temporary; renamed atomically over snapshot.bin
  wal.log            ← append-only Write-Ahead Log
```

### WAL Frame Format

Each WAL entry is wrapped in a length-prefixed, CRC32-checksummed frame:

```
[payload_len: u32 LE][crc32: u32 LE][payload: payload_len bytes]
```

Payloads are `bincode`-serialised `WalEntry { lsn: u64, op: WalOp }`. `WalOp`
variants cover `CreateCollection`, `DropCollection`, `Insert`, and `Delete`.
`serde_json::Value` payloads are serialised as strings inside `bincode` to work
around bincode's lack of `deserialize_any`.

### WalManager

Wraps `CollectionManager` and durably logs every mutation before applying it in
memory via `log_and_apply`. A monotonically increasing LSN is assigned to each entry.

### Recovery Flow (`WalManager::open`)

```
1. If snapshot.bin exists:
   a. Deserialise CollectionManager from snapshot.
   b. Read snapshot_lsn from the snapshot header.
2. If wal.log exists:
   a. Iterate frames via FrameIter.
   b. CRC-check each frame:
      - Mismatch with no trailing bytes → crash-truncated tail; discard and stop.
      - Mismatch with trailing bytes → mid-log corruption; surface as hard error.
   c. Skip entries with LSN ≤ snapshot_lsn.
   d. Apply remaining ops to the in-memory CollectionManager.
3. Open wal.log in append mode for new writes.
```

### Checkpoint Flow (`WalManager::checkpoint`)

```
1. Serialise CollectionManager (with current last_lsn) to snapshot.bin.tmp.
2. Atomic rename: snapshot.bin.tmp → snapshot.bin.
3. Truncate wal.log and reopen for appending.
```

The server spawns a periodic checkpoint task every 300 seconds
(`likhadb_server::spawn_checkpoint_task`).

---

## Server Layer (`crates/likhadb-server`)

### AppState

```rust
type AppState = Arc<RwLock<WalManager>>;
```

Concurrent reads (search, get, list) hold a read lock. Exclusive writes (insert,
delete, DDL) hold a write lock. Both are async RwLock from Tokio.

### REST API (Axum, port 8080)

| Method | Path | Operation |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/collections` | List collections |
| `POST` | `/collections` | Create collection |
| `GET` | `/collections/:name` | Get collection info |
| `DELETE` | `/collections/:name` | Drop collection |
| `POST` | `/collections/:name/vectors` | Insert vector |
| `GET` | `/collections/:name/vectors/:id` | Get vector by ID |
| `DELETE` | `/collections/:name/vectors/:id` | Delete vector |
| `POST` | `/collections/:name/query` | k-NN search |

### gRPC API (Tonic, port 50051)

Defined in `likhadb.proto`. Mirrors the REST surface plus a `QueryStream` RPC that
streams results one-by-one over a Tokio `mpsc::channel(32)` rather than returning a
single batch response.

### Startup

Both servers run concurrently under `tokio::select!`. The process exits with code 1
if either server terminates.

---

## Query Flows

### Flat index — insert then search

```
Client (REST/gRPC)
  │  write lock
  ▼
WalManager::insert
  ├─ WalWriter::append(WalEntry { lsn, WalOp::Insert { ... } })
  └─ CollectionManager::get_mut → Collection::insert
       ├─ FlatIndex::insert  (overwrite in-place or append to flat slab)
       └─ MetaStore::set      (store JSON payload)

Client (REST/gRPC)
  │  read lock
  ▼
WalManager::get → Collection::search
  ├─ MetaStore::make_filter   (compile JSON predicate → FilterFn)
  └─ FlatIndex::search
       ├─ Rayon par_iter over flat slab
       ├─ simd_distance (simsimd or scalar fallback)
       ├─ per-thread top-k max-heap
       └─ merge heaps → sort ascending → Vec<ScoredResult>
```

### IVF — insert path (pre-training → training → post-training)

```
IvfIndex::insert
  ├─ [pre-training]  append id + vec to staging_ids / staging_data
  │     └─ if staging_ids.len() >= nlist → IvfIndex::train()
  │           ├─ kmeans(staging_data, n, dim, nlist, metric)  [Rayon parallel]
  │           ├─ Sq8Quantizer::fit(staging_data) if quantize=true
  │           ├─ for each staged vector: nearest_centroid → PostingList::push / push_codes
  │           └─ staging cleared, trained = true
  └─ [post-training] nearest_centroid → PostingList::push / push_codes
                     id_to_list.insert(id, cluster)
```

### IVF — search path

```
IvfIndex::search
  ├─ [pre-training]  search_staging  (brute-force over staging slab)
  └─ [post-training] search_trained
       ├─ rank all nlist centroids by distance → take nprobe nearest
       └─ Rayon parallel fold+reduce over nprobe PostingLists
            ├─ f32 path:  simd_distance(query, chunk)
            └─ SQ8 path:  Sq8Quantizer::asym_distance (decode via thread-local buffer)
            → merge heaps → sort ascending → Vec<ScoredResult>
```

### HNSW — insert path

```
HnswIndex::insert(id, vec)
  ├─ if id already exists: tombstone old node (deleted.insert(old_id))
  ├─ push new HnswNode { id, layers: vec![vec![]; level+1] } + extend data slab
  ├─ id_to_node.insert(id, node_idx); deleted.remove(id)
  ├─ Phase 1 (greedy, ef=1): descend from max_level to level+1
  │     search_layer(ef=1, lc) → update entry point
  └─ Phase 2 (beam, ef=ef_construction): descend from min(level, max_level) to 0
        for each layer lc:
          candidates = search_layer(ef=ef_construction, lc)
          neighbours = select_neighbors(candidates, m_at(lc))
          connect bidirectional edges + prune over-full neighbour lists
```

### HNSW — search path

```
HnswIndex::search(query, k, filter)
  ├─ greedy descent (ef=1) from max_level down to layer 1
  │     → update entry point at each layer
  └─ beam search at layer 0 (ef = max(ef_search, k))
       → candidates heap
       → filter: skip deleted nodes; apply user FilterFn
       → sort ascending by distance
       → truncate to k
       → Vec<ScoredResult>
```

### WAL recovery

```
WalManager::open(dir)
  ├─ load snapshot.bin → CollectionManager (or fresh manager if absent)
  ├─ FrameIter over wal.log
  │     for each frame:
  │       CRC32 check
  │         bad CRC + no trailing bytes → discard tail (crash-truncated), stop
  │         bad CRC + trailing bytes    → hard PersistError::Crc
  │       bincode::deserialize → WalEntry
  │       skip if entry.lsn ≤ snapshot_lsn
  │       apply_op(mgr, entry.op)
  └─ WalWriter::open_append(wal.log) → ready for new writes
```

---

## Notable Design Decisions

- **Flat slab layout** (`FlatIndex`, `IvfIndex`, `HnswIndex`): all vector data lives in
  a single contiguous `Vec<f32>` indexed by slot position. This eliminates N separate
  heap allocations and makes the hardware prefetcher effective during sequential scans.

- **Swap-remove on delete** (`FlatIndex`, `IvfIndex`): the deleted slot is filled with
  the last element's data block, then the slab is truncated. O(1) and preserves slab
  contiguity without shifting.

- **IVF always queryable**: the staging buffer means searches work correctly at any
  point — before, during, and after training — without requiring the caller to manage
  index lifecycle.

- **HNSW tombstone deletion**: removing a node from a navigable small world graph while
  preserving search correctness is complex. Tombstoning keeps deleted nodes as valid
  traversal stepping-stones, so the graph topology remains intact and recall is not
  degraded.

- **SQ8 asymmetric distance**: the query vector stays in f32 precision; only the
  stored codes are compressed. This avoids quantisation error accumulating on both
  sides of the distance computation.

- **Atomic snapshot write**: the checkpoint writes to `snapshot.bin.tmp` first and
  then renames it over `snapshot.bin`. If the process crashes during the write, the
  old snapshot remains intact and WAL replay recovers the gap.

- **Single WAL entry format for all index types**: `IndexKind` is embedded in
  `WalOp::CreateCollection` so replay can reconstruct the correct index type without
  storing per-index schema separately.
