# likhadb

A progressively-layered, in-memory vector database written in Rust.
Tier 1 is an exact brute-force search MVP. Tier 2 adds an IVF approximate index.
Tier 3 (HNSW) slots in behind the same `VectorIndex` trait without touching the public API or store layer.

<p align="center">
  <img src="images/vecdb_tier_overview.svg" alt="Tier overview — Tier 1 through Tier 4 roadmap" width="720" />
</p>

## Overview

likhadb stores float vectors alongside arbitrary JSON payloads, searches them with
k-nearest-neighbour queries, and filters candidates using a simple JSON predicate language.
The internal design is a clean stack of crates with one extension seam — the `VectorIndex`
trait — so index implementations (FlatIndex, IvfIndex, future HNSW) slot in without changing
the store or API layers.

### MVP internals

<p align="center">
  <img src="images/vecdb_mvp_internals.svg" alt="MVP internal architecture — CollectionManager → Collection → FlatIndex + MetaStore → distance kernels" width="720" />
</p>

## Workspace layout

```
likhadb/
├── crates/
│   ├── likhadb-core/    # Primitives: VecId, Vector, ScoredResult, Metric, distance kernels
│   ├── likhadb-index/   # VectorIndex trait + FlatIndex + IvfIndex
│   ├── likhadb-store/   # Collection, CollectionManager, MetaStore (JSON filtering)
│   └── likhadb-bench/   # Criterion benchmarks
└── images/
```

| Crate | Role |
|---|---|
| `likhadb-core` | Shared primitives and error types — no index logic |
| `likhadb-index` | `VectorIndex` trait (the extension seam) + `FlatIndex` + `IvfIndex` |
| `likhadb-store` | `Collection` wraps an index + metadata; `CollectionManager` names them |
| `likhadb-bench` | Criterion benchmarks for 1 k / 10 k / 100 k vectors |

## Features

### Tier 1 — Exact brute-force (`FlatIndex`)

- **Exact k-NN search** via brute-force over all stored vectors
- **Three distance metrics** — Cosine, Dot product, L2 (Euclidean)
- **JSON metadata payloads** stored alongside each vector
- **Metadata filtering** — `eq`, `ne`, `exists` predicates evaluated at query time
- **Serde-ready result types** — `ScoredResult` serialises/deserialises out of the box
- **SIMD-accelerated search** via `simsimd` (NEON on M2/aarch64, AVX-512 on x86) with scalar fallback
- **Parallel search** via `rayon` — each thread builds a local top-k heap; heaps are merged at the end
- **No unsafe code**, no `unwrap()` in library paths

### Tier 2 — Approximate search (`IvfIndex`)

- **IVF (Inverted File Index)** — vectors clustered into `nlist` buckets via k-means
- **Configurable recall/speed tradeoff** via `nprobe` (buckets searched per query)
- **Automatic training** — k-means fires once `nlist` vectors have been inserted; searches before that fall back to brute-force
- **Exact recall mode** — set `nprobe == nlist` to search all buckets (equivalent to brute-force)
- **Same API** — drop-in replacement for `FlatIndex` via `create_ivf_collection`

## Getting started

**Prerequisites:** Rust stable toolchain, macOS aarch64 (M-series) recommended.

```sh
# Run all tests
cargo test --workspace

# Run benchmarks
cargo bench -p likhadb-bench

# Lint (zero warnings enforced)
cargo clippy --workspace -- -D warnings
```

## Quick usage

### Tier 1 — Exact search (FlatIndex)

```rust
use likhadb_core::Metric;
use likhadb_store::CollectionManager;
use serde_json::json;

fn main() {
    let mut mgr = CollectionManager::new();

    // Create a collection: 384-dimensional vectors, cosine distance
    mgr.create_collection("documents", 384, Metric::Cosine).unwrap();

    let col = mgr.get_mut("documents").unwrap();

    // Insert vectors with JSON payloads
    col.insert(1, vec![0.1; 384], Some(json!({"category": "news"}))).unwrap();
    col.insert(2, vec![0.9; 384], Some(json!({"category": "sports"}))).unwrap();
    col.insert(3, vec![0.5; 384], Some(json!({"category": "news"}))).unwrap();

    // Search top-5, filtered to "news" category only
    let predicate = json!({"field": "category", "op": "eq", "value": "news"});
    let query = vec![0.15; 384];
    let results = col.search(&query, 5, Some(&predicate)).unwrap();

    for r in &results {
        println!("id={} score={:.4}", r.id, r.score);
    }
}
```

### Tier 2 — Approximate search (IvfIndex)

```rust
use likhadb_core::Metric;
use likhadb_store::CollectionManager;

fn main() {
    let mut mgr = CollectionManager::new();

    // nlist=1024: number of k-means clusters (also the training trigger threshold)
    // nprobe=16:  clusters searched per query — higher = better recall, slower queries
    mgr.create_ivf_collection("docs", 384, Metric::L2, 1024, 16).unwrap();

    let col = mgr.get_mut("docs").unwrap();

    // Insert vectors — training fires automatically when the 1024th vector is added
    for i in 0..100_000u64 {
        col.insert(i, vec![i as f32 / 100_000.0; 384], None).unwrap();
    }

    // Search — only probes 16 of 1024 clusters (~10× faster than brute-force)
    let query = vec![0.5; 384];
    let results = col.search(&query, 10, None).unwrap();

    for r in &results {
        println!("id={} score={:.4}", r.id, r.score);
    }
}
```

**nlist / nprobe guidance:**
- `nlist`: typically `sqrt(N)` to `4 * sqrt(N)`. For 100 k vectors, 256–1024 is a good range.
- `nprobe`: start at `nlist / 64` for speed, increase toward `nlist / 8` for higher recall.
- `nprobe == nlist` gives exact recall identical to `FlatIndex`.

## Distance metrics

| Metric | Formula | Best for |
|---|---|---|
| `Metric::L2` | `sqrt(Σ(aᵢ - bᵢ)²)` | General-purpose, unnormalised embeddings |
| `Metric::Cosine` | `1 - dot(a,b) / (‖a‖·‖b‖)` | Semantic similarity, text embeddings |
| `Metric::Dot` | `-Σ(aᵢ·bᵢ)` (negated so lower = better) | Pre-normalised vectors, recommendation |

## Benchmark results

Measured on Apple M2 (aarch64). SIMD kernels via [`simsimd`](https://github.com/ashvardanian/SimSIMD) (NEON on aarch64).
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
|  10 000 | 384 |  256 |  8 | 16.8 ms |  87.7 µs | **3.9×** |
|  10 000 | 384 |  256 | 32 | 16.8 ms | 122.6 µs | **2.8×** |
| 100 000 | 384 | 1024 | 16 | 261 ms  |   263 µs | **10×**  |
| 100 000 | 384 | 1024 | 64 | 261 ms  |   626 µs | **4.3×** |

**Notes:**
- Training is a one-time amortised cost per collection. At 100 k × d384 with nlist=1024 it takes ~260 ms.
- `nprobe=16` on 100 k vectors (1.6% of clusters) delivers **10× speedup** over exact SIMD+rayon search.
- Increasing `nprobe` improves recall at the cost of latency — the `nprobe=64` row searches 4× more clusters.
- At 1 k vectors, rayon's dispatch overhead exceeds the parallelism benefit — SIMD alone is faster.

---

## Roadmap

| Tier | Status | Description |
|---|---|---|
| **Tier 1** | Done | Exact brute-force search, in-memory, JSON metadata filtering |
| **Tier 2** | Done | IVF (Inverted File Index) — approximate k-NN with k-means clustering |
| **Tier 3** | Planned | HNSW (Hierarchical Navigable Small World graphs) |
| **Tier 4** | Future | Persistence / WAL, HTTP + gRPC API, vector quantisation |

All future tiers implement `VectorIndex` — the store layer is unchanged.

## License

MIT
