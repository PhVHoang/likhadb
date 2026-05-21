# LikhaDB Stress Test

`likhadb-stress` is a local workload tool that demonstrates LikhaDB's performance
characteristics across its three index types and hybrid search mode. It drives the
live HTTP API with concurrent workers, then prints a comparison table of throughput
and latency percentiles.

---

## Prerequisites

| Requirement | Details |
|---|---|
| LikhaDB server running | `./dev.sh` (or `./dev.sh -c` for a clean slate) |
| Rust toolchain | `rustup show` — any stable 1.75+ |

Ports used by default: **8080** (REST). The stress tool talks HTTP only.

---

## Quick start

```bash
# Terminal 1 — start the server
./dev.sh

# Terminal 2 — run the stress test (default workload)
cargo run --release -p likhadb-stress
```

The release build is strongly recommended: it enables SIMD and compiler
optimisations that make the server-side number representative of real deployment.

---

## What it tests

The tool runs four phases sequentially, each with fully concurrent HTTP workers.

### Phase 1 — Flat (brute-force SIMD)

A `FlatIndex` collection. Every query scans all vectors using SIMD-accelerated
distance kernels (NEON on Apple Silicon, AVX2 on x86-64) via Rayon. Throughput
scales with vector count, so this phase establishes the **exact-recall baseline**.

### Phase 2 — IVF (inverted-file)

An `IvfIndex` collection. k-means clusters auto-train after `nlist` (default: 100)
inserts; subsequent inserts and all queries use the trained index. Only `nprobe`
(default: 10) of the `nlist` buckets are probed per query — roughly **10× faster**
than Flat at 100k+ vectors with ~98% recall.

### Phase 3 — HNSW (proximity graph)

An `HnswIndex` collection. The Hierarchical Navigable Small World graph is built
incrementally with each insert. At query time, `ef_search` (default: 50) candidate
nodes are explored. HNSW typically delivers the **lowest query latency** at scale.

### Phase 4 — Hybrid (vector + BM25, RRF fusion)

A Flat+FTS collection using 1/10 the vector count. Inserts include a text `body`
payload (20 tech-domain sentences). Queries combine a vector search and a BM25
full-text search fused with Reciprocal Rank Fusion (RRF). This phase shows the
cost of FTS indexing and demonstrates the hybrid retrieval path.

---

## Reading the output
1st sample run:

```
  index       ins/s        p50      p95      p99       qry/s        p50      p95      p99
  ──────────────────────────────────────────────────────────────────────────────
  flat        8.2k/s    0.82ms   1.10ms   1.80ms    1.3k/s     6.10ms   7.40ms   9.20ms
  ivf         7.8k/s    0.88ms   1.15ms   1.90ms    3.9k/s     1.80ms   2.30ms   3.10ms
  hnsw        6.5k/s    1.05ms   1.40ms   2.10ms    5.2k/s     1.40ms   1.80ms   2.50ms
  hybrid      4.1k/s    1.60ms   2.20ms   3.40ms      680/s    9.20ms  11.50ms  14.10ms
```

2nd sample run: cargo run --release -p likhadb-stress -- --vectors 1000 --queries 100

```
index           ins/s       p50       p95       p99        qry/s       p50       p95       p99
──────────────────────────────────────────────────────────────────────────
flat          23.4k/s     339µs     448µs     580µs       1.7k/s    1.91ms   10.36ms   51.06ms
ivf           26.9k/s     282µs     393µs     471µs      14.6k/s     513µs     791µs     919µs
hnsw           1.3k/s    6.76ms    7.81ms    8.16ms      12.2k/s     564µs    1.09ms    1.26ms
hybrid          108/s   65.49ms  174.93ms  291.90ms       4.6k/s    1.25ms    3.18ms    4.16ms
```

3rd sample run: cargo run --release -p likhadb-stress -- --vectors 100000 --queries 2000

```
index           ins/s       p50       p95       p99        qry/s       p50       p95       p99
──────────────────────────────────────────────────────────────────────────
flat           6.0k/s    1.36ms    2.30ms    2.38ms        412/s    7.64ms   36.47ms  147.61ms
ivf           26.9k/s     286µs     396µs     462µs       5.5k/s    1.38ms    2.38ms    2.85ms
hnsw            705/s   11.77ms   14.44ms   15.21ms       8.6k/s     843µs    1.47ms    1.70ms
hybrid          106/s   70.59ms   75.12ms  186.12ms       1.2k/s    2.91ms   14.27ms   45.55ms
```

| Column | Meaning |
|---|---|
| `ins/s` | Wall-clock insert throughput (all workers combined) |
| `p50/p95/p99` | Insert latency percentiles (single HTTP request) |
| `qry/s` | Wall-clock query throughput |
| `p50/p95/p99` | Query latency percentiles |

**Key things to notice:**

- `ins/s` for HNSW is slightly lower than Flat/IVF — graph link maintenance is
  more expensive per insert than a flat append.
- `qry/s` grows from Flat → IVF → HNSW as the index structure narrows the search.
- Hybrid `qry/s` includes BM25 scoring + RRF merge, so it is slower than pure
  vector search but unlocks keyword-relevance ranking.

---

## CLI flags

```
cargo run --release -p likhadb-stress -- [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `--host` | `http://localhost:8080` | Server base URL |
| `--dim` | `128` | Vector dimension |
| `--vectors` | `10 000` | Vectors inserted per index type |
| `--queries` | `500` | Query iterations per index type |
| `--concurrency` | `8` | Concurrent HTTP workers |
| `--k` | `10` | Top-k results per query |
| `--no-cleanup` | off | Keep test collections for inspection |

### Presets

**Quick smoke test** (~10 seconds, just checking things work):
```bash
cargo run --release -p likhadb-stress -- --vectors 1000 --queries 100
```

**Default local demo** (~60 seconds, clear index comparison):
```bash
cargo run --release -p likhadb-stress
```

**Heavier throughput test** (~5–10 minutes, IVF/HNSW advantage really shows):
```bash
cargo run --release -p likhadb-stress -- --vectors 100000 --queries 2000
```

**High-concurrency test** (model more simultaneous clients):
```bash
cargo run --release -p likhadb-stress -- --concurrency 32
```

---

## Inspect live metrics during the run

Open a second terminal while the stress test is running:

```bash
# Prometheus-format metrics (histograms, gauges, counters)
curl -s http://localhost:8080/metrics

# Collection state
curl -s http://localhost:8080/collections | jq .
```

The `likhadb_search_duration_seconds` histogram in `/metrics` gives you
server-side latency independent of network overhead.

---

## Interpreting abnormal results

| Symptom | Likely cause |
|---|---|
| Very high p99 on inserts (>50ms) | WAL checkpoint running; normal under sustained writes |
| Flat and HNSW query latency nearly identical | Dataset too small (try `--vectors 50000+`) |
| IVF query latency matches Flat | Training not triggered yet — `nlist` not reached |
| Hybrid queries fail / skip | Server not built with `fts` feature; rebuild with `cargo build -p likhadb-server` |
| All latencies spike under `--concurrency 32+` | tokio worker thread contention; expected on <8-core machines |

---

## How collections are named

The tool creates collections named `stress_flat`, `stress_ivf`, `stress_hnsw`,
and `stress_hybrid`. They are deleted at the end unless `--no-cleanup` is passed.
The tool also drops any collection with those names at the **start** of each run,
so runs are always idempotent.
