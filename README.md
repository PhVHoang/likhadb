# likhadb

**The hybrid vector database built for the data lakehouse.**
Fast Rust-native search (HNSW, IVF, BM25 + RRF fusion) that reads and writes directly from Parquet, S3/GCS, and Iceberg — no ETL pipeline required.

<p align="center">
  <img src="images/likhadb_logo.svg" alt="LikhaDB" width="720" />
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

## Query flow

End-to-end path from client request to ranked response, covering both the current ANN+RRF path and the planned DataFusion enrichment tier (Tier Q):

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Server as likhadb-server<br/>(axum / tonic)
    participant WAL as WalManager<br/>(likhadb-persist)
    participant Store as Collection<br/>(likhadb-store)
    participant ANN as VectorIndex<br/>(HNSW / IVF / Flat)
    participant FTS as FtsIndex<br/>(Tantivy / BM25)
    participant Pipeline as QueryPipeline<br/>(likhadb-query · Tier Q)
    participant DF as DataFusion<br/>SessionContext
    participant Parquet as Parquet tables<br/>(documents · authors · acl)

    Client->>Server: POST /collections/:name/query<br/>{ vector, text?, k, filter? }
    Server->>WAL: acquire read lock

    alt vector-only query
        WAL->>Store: search(vector, top_n, filter)
        Store->>ANN: knn(vector, top_n)
        ANN-->>Store: [(id, distance, rank)]
        Store-->>WAL: Vec<ScoredResult>
    else hybrid query (vector + text)
        WAL->>Store: hybrid_search(vector, text, 2k)
        Store->>ANN: knn(vector, 2k)
        ANN-->>Store: [(id, distance, rank_vec)]
        Store->>FTS: fts_search(text, 2k)
        FTS-->>Store: [(id, bm25_score, rank_fts)]
        Store->>Store: RRF fusion<br/>score = 1/(60+rank_vec) + 1/(60+rank_fts)
        Store-->>WAL: Vec<ScoredResult> top-k
    end

    WAL-->>Server: candidates (id, distance, rank)
    Server->>Server: drop read lock

    rect rgb(230, 245, 255)
        note over Pipeline,Parquet: Tier Q — DataFusion enrichment (planned, likhadb-query)
        Server->>Pipeline: run(candidates, k)
        Pipeline->>DF: register candidates MemTable<br/>(id, ann_distance, ann_rank)
        Pipeline->>DF: Stage 3 — enrichment SQL<br/>JOIN documents, authors<br/>WHERE acl allows + sensitivity != restricted
        DF->>Parquet: predicate-pushdown reads
        Parquet-->>DF: filtered row groups
        DF-->>Pipeline: enriched RecordBatch

        Pipeline->>DF: Stage 4a — score fusion SQL<br/>fusion = w_vec×norm(ann_dist) + w_rec×recency_decay
        DF-->>Pipeline: top-M ranked by fusion_score

        Pipeline->>Pipeline: Stage 4b — bi-encoder<br/>materialize top-M → batched model call → zip bi_scores
        Pipeline->>Pipeline: Stage 4c — cross-encoder<br/>materialize top-P → batched model call → zip scores
        Pipeline-->>Server: Vec<ScoredResult> top-K
    end

    Server-->>Client: { results: [{ id, score, payload }] }
```

> **Tier Q note:** stages inside the blue box are implemented by the `likhadb-query` crate (currently in progress — Q0 config/error done; Q1–Q4 planned). Without Tier Q the server returns the ANN/RRF result directly.

For a deeper walkthrough of each stage see [`docs/rfc_datafusion_integration.md`](docs/rfc_datafusion_integration.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

See [`docs/quick-usages.md`](docs/quick-usages.md) for Rust API and REST usage examples.

---

## Python SDK

A typed Python client ships under `sdk/python/`. It supports both sync and async usage and covers the full REST API surface.

**Install (development):**

```sh
cd sdk/python
pip install -e ".[dev]"
```

**Sync usage:**

```python
from likhadb import LikhaDB

with LikhaDB("http://localhost:8080") as db:
    db.create_collection("docs", dim=384, metric="cosine")
    col = db.collection("docs")
    col.insert(1, vector=[0.1] * 384, payload={"title": "hello"})
    results = col.search([0.1] * 384, k=5, include_payload=True)
```

**Async usage:**

```python
from likhadb import AsyncLikhaDB

async with AsyncLikhaDB("http://localhost:8080") as db:
    await db.create_collection("docs", dim=384, metric="cosine")
    col = db.collection("docs")
    await col.insert(1, vector=[0.1] * 384, payload={"title": "hello"})
    results = await col.search([0.1] * 384, k=5, include_payload=True)
```

Index types (`flat`, `ivf`, `ivf_sq8`, `hnsw`), hybrid search, Parquet import/export, and per-request payload filters are all supported. See [`sdk/python/`](sdk/python/) for the full API.

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
| **L — Lakehouse I/O** | Planned | Parquet import/export, object storage (S3/GCS), Iceberg |
| **Q — DataFusion pipeline** | In Progress | Post-ANN enrichment, ACL enforcement, multi-signal score fusion, reranking (`likhadb-query` crate) |
| **T — Vector transforms** | Planned | Insert-time L2 normalisation, scalar scaling |

---

## License

MIT
