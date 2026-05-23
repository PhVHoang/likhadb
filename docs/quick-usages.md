# Quick usage

## Exact search (FlatIndex)

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

## Approximate search (IvfIndex)

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

## Approximate search + 4× memory reduction (IvfIndex + SQ8)

```rust
// Same parameters as IvfIndex — just swap the constructor.
// After training, each vector is stored as dim × u8 instead of dim × f32.
mgr.create_ivf_sq8_collection("docs_sq8", 384, Metric::L2, 1024, 16).unwrap();
```

Memory: 100 k × d384 goes from ~146 MB (f32) to ~36 MB (u8). Distance computation
uses asymmetric evaluation — the query stays in f32 while stored codes are decoded
on-the-fly.

## Graph-based approximate search (HnswIndex)

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

## Full-text search (FtsIndex)

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

## Hybrid search (vector + BM25 via RRF)

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

## Snapshot persistence

```rust
use std::path::Path;
use likhadb_persist::PersistExt;
use likhadb_store::CollectionManager;

mgr.save(Path::new("snapshot.bin")).unwrap();

// On next startup — all collections, index state, and payloads are restored.
let mgr = CollectionManager::load(Path::new("snapshot.bin")).unwrap();
```

## Write-Ahead Log

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
