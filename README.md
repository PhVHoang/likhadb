# likhadb

**The hybrid vector database built for the data lakehouse.**
Fast Rust-native search (HNSW, IVF, BM25 + RRF fusion) that reads and writes directly from Parquet, S3/GCS, and Delta tables — no ETL pipeline required.

<p align="center">
  <img src="images/vecdb_tier_overview.svg" alt="Tier overview — Tier 1 through Tier 4 roadmap" width="720" />
</p>

likhadb stores float vectors alongside arbitrary JSON payloads, searches them with
k-nearest-neighbour queries, and filters candidates using a simple JSON predicate language.
Collections can optionally enable a Tantivy-backed full-text index over payload string fields.
The internal design is a clean stack of crates with two extension seams — the `VectorIndex`
and `FtsIndex` traits — so implementations slot in without changing the store or API layers.

For a deep dive into crate structure, index algorithms, query flows, and persistence
design, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Getting started

**Prerequisites:** Rust stable toolchain.

```sh
# Run all tests
cargo test --workspace

# Run FTS tests (requires the fts feature)
cargo test -p likhadb-store --features fts
cargo test -p likhadb-fts

# Run benchmarks
cargo bench -p likhadb-bench

# Lint (zero warnings enforced)
cargo clippy --workspace -- -D warnings
cargo clippy -p likhadb-store --features fts -- -D warnings
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

### Full-text search (FtsIndex)

Enable per-collection with the `fts` Cargo feature. All string values in the JSON payload
(including nested objects and arrays) are indexed automatically. Scores are BM25.

```toml
# Cargo.toml
likhadb-store = { path = "crates/likhadb-store", features = ["fts"] }
```

```rust
use likhadb_core::Metric;
use likhadb_store::CollectionManager;
use serde_json::json;

let mut mgr = CollectionManager::new();
mgr.create_collection("articles", 384, Metric::Cosine).unwrap();

let col = mgr.get_mut("articles").unwrap();
col.enable_fts().unwrap();   // activates the Tantivy in-memory index

col.insert(1, vec![0.1; 384], Some(json!({"title": "Rust async runtime", "body": "tokio and async-std"}))).unwrap();
col.insert(2, vec![0.2; 384], Some(json!({"title": "Python data science", "body": "numpy pandas sklearn"}))).unwrap();
col.insert(3, vec![0.3; 384], Some(json!({"title": "Rust memory model", "body": "ownership borrowing lifetimes"}))).unwrap();

// BM25 full-text search — returns top-k results ranked by relevance
let results = col.fts_search("Rust ownership", 5).unwrap();
// results[0].id == 3  (highest BM25 score for "ownership")
// results[1].id == 1  (matches "Rust")
```

Deletions are propagated automatically: `col.delete(id)` removes the vector, the payload, and the FTS document in one call.

### Hybrid search (vector + BM25 via RRF)

Pass `enable_fts: true` at collection creation. Hybrid search fuses vector similarity ranks and BM25 text ranks using Reciprocal Rank Fusion:

```
rrf_score(id) = 1/(rrf_k + rank_vec) + 1/(rrf_k + rank_fts)    // default rrf_k = 60
```

A document that ranks 2nd by vector similarity and 3rd by keyword relevance beats a document that is top-1 in only one modality.

```rust
use likhadb_core::Metric;
use likhadb_store::CollectionManager;
use serde_json::json;

let mut mgr = CollectionManager::new();
mgr.create_collection("articles", 384, Metric::Cosine).unwrap();

let col = mgr.get_mut("articles").unwrap();
col.enable_fts().unwrap();   // activates Tantivy BM25 index

col.insert(1, vec![0.1; 384], Some(json!({"title": "Rust async runtime", "body": "tokio"}))).unwrap();
col.insert(2, vec![0.5; 384], Some(json!({"title": "Python ML", "body": "numpy sklearn"}))).unwrap();
col.insert(3, vec![0.2; 384], Some(json!({"title": "Rust memory model", "body": "ownership lifetimes"}))).unwrap();

// Returns top-5 fusing semantic + keyword signals
let results = col.hybrid_search(
    &vec![0.15; 384],  // query embedding
    "Rust ownership",  // keyword query
    5,                 // k
    60,                // rrf_k
    None,              // metadata filter
    true,              // include_payload
).unwrap();
```

**REST API:**
```sh
# Create collection with FTS enabled
curl -X POST localhost:8080/collections \
  -H 'Content-Type: application/json' \
  -d '{"name":"articles","dim":384,"metric":"cosine","enable_fts":true}'

# Hybrid query
curl -X POST localhost:8080/collections/articles/hybrid-query \
  -H 'Content-Type: application/json' \
  -d '{"vector":[...],"text":"Rust ownership","k":5,"include_payload":true}'
```

---

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

### Apple M2

Measured on Apple M2 (aarch64). SIMD kernels via [`simsimd`](https://github.com/ashvardanian/SimSIMD) (NEON).
Rayon uses the default thread pool (all available cores).

#### FlatIndex (exact search)

| Benchmark | Vectors | Dim | k | Scalar | SIMD (1 thread) | SIMD + rayon | vs scalar |
|---|---|---|---|---|---|---|---|
| `1k_d128`   |   1 000 | 128 | 10 | 80.5 µs | 55.3 µs | 70.3 µs | **1.1×** |
| `10k_d384`  |  10 000 | 384 | 10 | 2.80 ms | 0.888 ms | 0.396 ms | **7.1×** |
| `100k_d384` | 100 000 | 384 | 10 | 26.5 ms | 8.84 ms | 2.82 ms | **9.4×** |

#### IvfIndex (approximate search)

| Vectors | Dim | nlist | nprobe | Training (one-time) | Query latency | vs FlatIndex SIMD+rayon |
|---|---|---|---|---|---|---|
|  10 000 | 384 |  256 |  8 | 21.6 ms |  93.1 µs | **4.2×** |
|  10 000 | 384 |  256 | 32 | 21.6 ms | 141 µs   | **2.8×** |
| 100 000 | 384 | 1024 | 16 | 320 ms  | 272 µs   | **10.4×** |
| 100 000 | 384 | 1024 | 64 | 320 ms  | 554 µs   | **5.1×** |

#### IvfIndex + SQ8 (approximate, 4× smaller posting lists)

| Vectors | Dim | nlist | nprobe | Query latency | vs IvfIndex (f32) |
|---|---|---|---|---|---|
|  10 000 | 384 |  256 |  8 | 342 µs | 0.27× |
|  10 000 | 384 |  256 | 32 | 648 µs | 0.22× |
| 100 000 | 384 | 1024 | 16 | 848 µs | 0.32× |
| 100 000 | 384 | 1024 | 64 | 1.92 ms | 0.29× |

#### HnswIndex (graph-based approximate search)

| Vectors | Dim | m | ef_construction | ef_search | Query latency | vs FlatIndex SIMD+rayon |
|---|---|---|---|---|---|---|
|  10 000 | 384 | 16 | 200 |  50 | 146 µs | **2.7×** |
|  10 000 | 384 | 16 | 200 | 100 | 233 µs | **1.7×** |
| 100 000 | 384 | 16 | 200 |  50 | 167 µs | **16.9×** |
| 100 000 | 384 | 16 | 200 | 100 | 320 µs | **8.8×** |

**Build time** (one-time, amortised across all queries):

| Vectors | Dim | m | ef_construction | Build time |
|---|---|---|---|---|
| 10 000 | 384 | 16 | 200 | 4.57 s |

**Notes:**
- `nprobe=16` on 100 k vectors (1.6% of clusters) delivers **10.4× speedup** over exact SIMD+rayon search.
- SQ8 reduces posting-list memory 4× but is slower per query due to asymmetric decode overhead; best for memory-constrained deployments.
- At 1 k vectors, Rayon dispatch overhead exceeds the parallelism benefit — SIMD alone is faster.
- HNSW at `ef_search=50` on 100 k vectors achieves **16.9× speedup** vs exact SIMD+rayon with sub-200 µs latency.

---

### Apple M4 Mac Mini (16 GB RAM)

Measured on Apple M4 Mac Mini, 16 GB RAM (aarch64). SIMD kernels via [`simsimd`](https://github.com/ashvardanian/SimSIMD) (NEON).
Rayon uses the default thread pool (all available cores).

#### FlatIndex (exact search)

| Benchmark | Vectors | Dim | k | Scalar | SIMD (1 thread) | SIMD + rayon | vs scalar |
|---|---|---|---|---|---|---|---|
| `1k_d128`   |   1 000 | 128 | 10 | 34.6 µs | 27.2 µs | 55.4 µs | 0.6× |
| `10k_d384`  |  10 000 | 384 | 10 | 1.30 ms | 0.603 ms | 0.230 ms | **5.6×** |
| `100k_d384` | 100 000 | 384 | 10 | 13.9 ms | 5.72 ms | 1.41 ms | **9.8×** |

#### IvfIndex (approximate search)

| Vectors | Dim | nlist | nprobe | Training (one-time) | Query latency | vs FlatIndex SIMD+rayon |
|---|---|---|---|---|---|---|
|  10 000 | 384 |  256 |  8 | 13.5 ms |  84.5 µs | **2.7×** |
|  10 000 | 384 |  256 | 32 | 13.5 ms |  95.5 µs | **2.4×** |
| 100 000 | 384 | 1024 | 16 | 193 ms  | 197 µs   | **7.2×** |
| 100 000 | 384 | 1024 | 64 | 193 ms  | 335 µs   | **4.2×** |

#### IvfIndex + SQ8 (approximate, 4× smaller posting lists)

| Vectors | Dim | nlist | nprobe | Query latency | vs IvfIndex (f32) |
|---|---|---|---|---|---|
|  10 000 | 384 |  256 |  8 | 222 µs | 0.38× |
|  10 000 | 384 |  256 | 32 | 286 µs | 0.33× |
| 100 000 | 384 | 1024 | 16 | 568 µs | 0.35× |
| 100 000 | 384 | 1024 | 64 | 1.16 ms | 0.29× |

#### HnswIndex (graph-based approximate search)

| Vectors | Dim | m | ef_construction | ef_search | Query latency | vs FlatIndex SIMD+rayon |
|---|---|---|---|---|---|---|
|  10 000 | 384 | 16 | 200 |  50 | 103 µs | **2.2×** |
|  10 000 | 384 | 16 | 200 | 100 | 178 µs | **1.3×** |
| 100 000 | 384 | 16 | 200 |  50 | 128 µs | **11.0×** |
| 100 000 | 384 | 16 | 200 | 100 | 225 µs | **6.3×** |

**Build time** (one-time, amortised across all queries):

| Vectors | Dim | m | ef_construction | Build time |
|---|---|---|---|---|
| 10 000 | 384 | 16 | 200 | 3.07 s |

**Notes:**
- `nprobe=16` on 100 k vectors (1.6% of clusters) delivers **7.2× speedup** over exact SIMD+rayon search.
- SQ8 reduces posting-list memory 4× but is slower per query due to asymmetric decode overhead; best for memory-constrained deployments.
- At 1 k vectors, Rayon dispatch overhead exceeds the parallelism benefit — SIMD alone is faster.
- HNSW at `ef_search=50` on 100 k vectors achieves **11.0× speedup** vs exact SIMD+rayon with sub-130 µs latency.
- IVF training is ~40% faster than M2 (13.5 ms vs 21.6 ms at 10 k vectors), HNSW build is ~33% faster (3.07 s vs 4.57 s at 10 k vectors).

---

## Roadmap

| Item | Status | Description |
|---|---|---|
| **A — Foundation** | Done | Exact brute-force search, in-memory, JSON metadata filtering |
| **B — Approximate k-NN** | Done | IVF (k-means + SQ8 quantization) + HNSW graph-based search |
| **C — Persistence** | Done | Snapshot + WAL crash durability, atomic checkpoint |
| **D — Concurrency** | Done | `Arc<RwLock<WalManager>>`, background checkpoint task |
| **E — API** | Done | HTTP REST (axum) + gRPC (tonic) |
| **F — Observability** | Done | Prometheus metrics (`/metrics`) + structured JSON tracing |
| **F1 — Full-text search** | Done | Tantivy BM25 index per collection, opt-in via `fts` feature |
| **F2 — Hybrid search** | Done | RRF fusion of vector similarity + BM25 scores |
| **L — Lakehouse I/O** | Planned | Parquet import/export, object storage (S3/GCS), Delta Lake |
| **T — Vector transforms** | Planned | Insert-time L2 normalisation, scalar scaling |

---

## License

MIT
