# likhadb

A progressively-layered, in-memory vector database written in Rust.

<p align="center">
  <img src="images/vecdb_tier_overview.svg" alt="Tier overview — Tier 1 through Tier 4 roadmap" width="720" />
</p>

likhadb stores float vectors alongside arbitrary JSON payloads, searches them with
k-nearest-neighbour queries, and filters candidates using a simple JSON predicate language.
The internal design is a clean stack of crates with one extension seam — the `VectorIndex`
trait — so index implementations slot in without changing the store or API layers.

For a deep dive into crate structure, index algorithms, query flows, and persistence
design, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Getting started

**Prerequisites:** Rust stable toolchain.

```sh
# Run all tests
cargo test --workspace

# Run benchmarks
cargo bench -p likhadb-bench

# Lint (zero warnings enforced)
cargo clippy --workspace -- -D warnings
```

---

## Index types

| Index | Type | When to use |
|---|---|---|
| `FlatIndex` | Exact brute-force | Small datasets or when precision matters most |
| `IvfIndex` | Approximate (IVF k-means) | Large datasets, latency-sensitive workloads |
| `IvfIndex` + SQ8 | Approximate + quantized | Memory-constrained deployments (4× smaller) |
| `HnswIndex` | Approximate (graph) | Sub-millisecond recall on large datasets |

---

## Quick usage

### Exact search (FlatIndex)

```rust
use likhadb_core::Metric;
use likhadb_store::CollectionManager;
use serde_json::json;

let mut mgr = CollectionManager::new();
mgr.create_collection("documents", 384, Metric::Cosine).unwrap();

let col = mgr.get_mut("documents").unwrap();
col.insert(1, vec![0.1; 384], Some(json!({"category": "news"}))).unwrap();
col.insert(2, vec![0.9; 384], Some(json!({"category": "sports"}))).unwrap();
col.insert(3, vec![0.5; 384], Some(json!({"category": "news"}))).unwrap();

let predicate = json!({"field": "category", "op": "eq", "value": "news"});
let results = col.search(&vec![0.15; 384], 5, Some(&predicate), false).unwrap();
```

### Approximate search (IvfIndex)

```rust
use likhadb_core::Metric;
use likhadb_store::CollectionManager;

let mut mgr = CollectionManager::new();
// nlist=1024: k-means clusters (also the training trigger threshold)
// nprobe=16:  clusters searched per query — higher = better recall, slower queries
mgr.create_ivf_collection("docs", 384, Metric::L2, 1024, 16).unwrap();

let col = mgr.get_mut("docs").unwrap();
for i in 0..100_000u64 {
    col.insert(i, vec![i as f32 / 100_000.0; 384], None).unwrap();
}
let results = col.search(&vec![0.5; 384], 10, None, false).unwrap();
```

**nlist / nprobe guidance:**
- `nlist`: typically `sqrt(N)` to `4 * sqrt(N)`. For 100 k vectors, 256–1024 is a good range.
- `nprobe`: start at `nlist / 64` for speed, increase toward `nlist / 8` for higher recall.
- `nprobe == nlist` gives exact recall identical to `FlatIndex`.

### Approximate search + 4× memory reduction (IvfIndex + SQ8)

```rust
// Same parameters as IvfIndex — just swap the constructor.
// After training, each vector is stored as dim × u8 instead of dim × f32.
mgr.create_ivf_sq8_collection("docs_sq8", 384, Metric::L2, 1024, 16).unwrap();
```

Memory: 100 k × d384 goes from ~146 MB (f32) to ~36 MB (u8). Distance computation
uses asymmetric evaluation — the query stays in f32 while stored codes are decoded
on-the-fly.

### Graph-based approximate search (HnswIndex)

```rust
use likhadb_core::Metric;
use likhadb_store::CollectionManager;

let mut mgr = CollectionManager::new();
// m=16: graph density · ef_construction=200: build quality · ef_search=50: query recall
mgr.create_hnsw_collection("docs", 384, Metric::Cosine, 16, 200, 50).unwrap();

let col = mgr.get_mut("docs").unwrap();
for i in 0..100_000u64 {
    col.insert(i, vec![i as f32 / 100_000.0; 384], None).unwrap();
}
let results = col.search(&vec![0.5; 384], 10, None, false).unwrap();
```

**m / ef_construction / ef_search guidance:**
- `m`: typically 8–32. Higher `m` increases memory and improves recall. 16 is a good default.
- `ef_construction`: must be ≥ `m`. 200 is a good default.
- `ef_search`: must be ≥ 1. Increase to trade latency for recall. Start at 50.

### Snapshot persistence

```rust
use std::path::Path;
use likhadb_persist::PersistExt;
use likhadb_store::CollectionManager;

mgr.save(Path::new("snapshot.bin")).unwrap();

// On next startup — all collections, index state, and payloads are restored.
let mgr = CollectionManager::load(Path::new("snapshot.bin")).unwrap();
```

### Write-Ahead Log

```rust
use std::path::Path;
use likhadb_core::Metric;
use likhadb_persist::WalManager;

// On restart after a crash: loads last snapshot then replays uncommitted WAL entries.
let mut wal = WalManager::open(Path::new("/data/mydb")).unwrap();

wal.create_collection("docs", 384, Metric::Cosine).unwrap();
wal.insert("docs", 1, vec![0.1; 384], Some(serde_json::json!({"title": "hello"}))).unwrap();
wal.delete("docs", 1).unwrap();

let results = wal.get("docs").unwrap()
    .search(&[0.5; 384], 10, None, false).unwrap();

// Collapse WAL into a fresh snapshot and truncate wal.log.
wal.checkpoint().unwrap();
```

---

## Distance metrics

| Metric | Formula | Best for |
|---|---|---|
| `Metric::L2` | `sqrt(Σ(aᵢ − bᵢ)²)` | General-purpose, unnormalised embeddings |
| `Metric::Cosine` | `1 − dot(a,b) / (‖a‖·‖b‖)` | Semantic similarity, text embeddings |
| `Metric::Dot` | `−Σ(aᵢ·bᵢ)` (negated so lower = better) | Pre-normalised vectors, recommendation |

---

## Benchmark results

Measured on Apple M2 (aarch64). SIMD kernels via [`simsimd`](https://github.com/ashvardanian/SimSIMD) (NEON).
Rayon uses the default thread pool (all available cores).

### FlatIndex (exact search)

| Benchmark | Vectors | Dim | k | Scalar | SIMD (1 thread) | SIMD + rayon | vs scalar |
|---|---|---|---|---|---|---|---|
| `1k_d128`   |   1 000 | 128 | 10 |  70.2 µs |  45.7 µs |  54.8 µs | **1.3×** |
| `10k_d384`  |  10 000 | 384 | 10 |  2.55 ms | 0.883 ms | 0.342 ms | **7.5×** |
| `100k_d384` | 100 000 | 384 | 10 | 26.9 ms  |  8.50 ms |  2.67 ms | **10×** |

### IvfIndex (approximate search)

| Vectors | Dim | nlist | nprobe | Training (one-time) | Query latency | vs FlatIndex SIMD+rayon |
|---|---|---|---|---|---|---|
|  10 000 | 384 |  256 |  8 | 20.8 ms |  91.7 µs | **3.7×** |
|  10 000 | 384 |  256 | 32 | 20.8 ms | 124 µs   | **2.8×** |
| 100 000 | 384 | 1024 | 16 | 309 ms  | 268 µs   | **10×**  |
| 100 000 | 384 | 1024 | 64 | 309 ms  | 512 µs   | **5.2×** |

### IvfIndex + SQ8 (approximate, 4× smaller posting lists)

| Vectors | Dim | nlist | nprobe | Query latency | vs IvfIndex (f32) |
|---|---|---|---|---|---|
|  10 000 | 384 |  256 |  8 | 295 µs | 0.31× |
|  10 000 | 384 |  256 | 32 | 436 µs | 0.28× |
| 100 000 | 384 | 1024 | 16 | 765 µs | 0.35× |
| 100 000 | 384 | 1024 | 64 | 1.82 ms | 0.28× |

**Notes:**
- `nprobe=16` on 100 k vectors (1.6% of clusters) delivers **10× speedup** over exact SIMD+rayon search.
- SQ8 reduces posting-list memory 4× but is slower per query due to asymmetric decode overhead; best for memory-constrained deployments.
- At 1 k vectors, Rayon dispatch overhead exceeds the parallelism benefit — SIMD alone is faster.

---

## Roadmap

| Tier | Status | Description |
|---|---|---|
| **Tier 1** | Done | Exact brute-force search, in-memory, JSON metadata filtering |
| **Tier 2** | Done | IVF approximate k-NN with k-means clustering + SQ8 quantization |
| **Tier 3** | Done | HNSW graph-based approximate search |
| **Tier 4 — B1** | Done | Snapshot persistence |
| **Tier 4 — B2** | Done | WAL crash durability — logs every mutation, replays on restart, atomic checkpoint |
| **Tier 4 — C1** | Done | Concurrent shared state — `AppState` (`Arc<RwLock<WalManager>>`), background checkpoint task |
| **Tier 4 — D+** | Done | HTTP REST + gRPC API |

---

## License

MIT
