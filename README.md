# likhadb

A progressively-layered, in-memory vector database written in Rust.
Tier 1 is an exact brute-force search MVP. Tier 2 (IVF) and Tier 3 (HNSW) slot in
behind the same `VectorIndex` trait without touching the public API or store layer.

<p align="center">
  <img src="images/vecdb_tier_overview.svg" alt="Tier overview — Tier 1 through Tier 4 roadmap" width="720" />
</p>

---

## Overview

likhadb stores float vectors alongside arbitrary JSON payloads, searches them with
exact k-nearest-neighbour queries, and filters candidates using a simple JSON
predicate language. The internal design is a clean stack of crates with one
extension seam — the `VectorIndex` trait — so future index implementations (IVF,
HNSW) can be dropped in without changing the store or API layers.

### MVP internals

<p align="center">
  <img src="images/vecdb_mvp_internals.svg" alt="MVP internal architecture — CollectionManager → Collection → FlatIndex + MetaStore → distance kernels" width="720" />
</p>

---

## Workspace layout

```
likhadb/
├── crates/
│   ├── likhadb-core/    # Primitives: VecId, Vector, ScoredResult, Metric, distance kernels
│   ├── likhadb-index/   # VectorIndex trait + FlatIndex (brute-force, BinaryHeap)
│   ├── likhadb-store/   # Collection, CollectionManager, MetaStore (JSON filtering)
│   └── likhadb-bench/   # Criterion benchmarks
└── images/
```

| Crate | Role |
|---|---|
| `likhadb-core` | Shared primitives and error types — no index logic |
| `likhadb-index` | `VectorIndex` trait (the Tier 2/3 seam) + `FlatIndex` |
| `likhadb-store` | `Collection` wraps an index + metadata; `CollectionManager` names them |
| `likhadb-bench` | Criterion benchmarks for 1 k / 10 k / 100 k vectors |

---

## Features (Tier 1)

- **Exact k-NN search** via brute-force over all stored vectors
- **Three distance metrics** — Cosine, Dot product, L2 (Euclidean)
- **JSON metadata payloads** stored alongside each vector
- **Metadata filtering** — `eq`, `ne`, `exists` predicates evaluated at query time
- **Serde-ready result types** — `ScoredResult` serialises/deserialises out of the box
- **No unsafe code**, no SIMD, no `unwrap()` in library paths

---

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

---

## Quick usage

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

---

## Distance metrics

| Metric | Formula | Best for |
|---|---|---|
| `Metric::L2` | `sqrt(Σ(aᵢ - bᵢ)²)` | General-purpose, unnormalised embeddings |
| `Metric::Cosine` | `1 - dot(a,b) / (‖a‖·‖b‖)` | Semantic similarity, text embeddings |
| `Metric::Dot` | `-Σ(aᵢ·bᵢ)` (negated so lower = better) | Pre-normalised vectors, recommendation |

---

## Benchmark results

Measured on Apple M2 (aarch64), scalar kernels, no SIMD.

| Benchmark | Vectors | Dim | k | Result | Target |
|---|---|---|---|---|---|
| `flat_search_1k_d128` | 1 000 | 128 | 10 | 65 µs | — |
| `flat_search_10k_d384` | 10 000 | 384 | 10 | 2.4 ms | < 50 ms |
| `flat_search_100k_d384` | 100 000 | 384 | 10 | 24 ms | < 500 ms |

SIMD distance kernels are Tier 2 scope.

---

## Roadmap

| Tier | Status | Description |
|---|---|---|
| **Tier 1** | Done | Exact brute-force search, in-memory, JSON metadata filtering |
| **Tier 2** | Planned | IVF (Inverted File Index), SIMD distance kernels |
| **Tier 3** | Planned | HNSW (Hierarchical Navigable Small World graphs) |
| **Tier 4** | Future | Persistence / WAL, HTTP + gRPC API, vector quantisation |

All future tiers implement `VectorIndex` — the store layer is unchanged.

## License

MIT
