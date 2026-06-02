use clap::Parser;
use rand::{rngs::StdRng, Rng, SeedableRng};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::{task::JoinSet, time::timeout};

// ─── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "likhadb-stress",
    about = "Breaking-point stress test for LikhaDB",
    long_about = "Pushes LikhaDB beyond its normal operational limits to discover breaking\n\
                  points, validate error handling under pressure, and verify SLO compliance.\n\n\
                  Phases (all enabled by default):\n\
                  1. Baseline  — flat / IVF / HNSW / hybrid throughput benchmarks\n\
                  2. Ramp      — doubles concurrency until error threshold or p99 SLO breach\n\
                  3. Spike     — sudden traffic surge followed by recovery measurement\n\
                  4. Soak      — sustained load across time windows; detects latency drift\n\
                  5. Chaos     — mixed valid/invalid operations; verifies graceful error handling"
)]
struct Args {
    /// Base URL of the running LikhaDB server
    #[arg(long, default_value = "http://localhost:8080")]
    host: String,

    /// Vector dimension
    #[arg(long, default_value_t = 128)]
    dim: usize,

    /// Vectors to insert per index type during the baseline phase
    #[arg(long, default_value_t = 10_000)]
    vectors: usize,

    /// Query iterations per index type during the baseline phase
    #[arg(long, default_value_t = 500)]
    queries: usize,

    /// Base concurrent HTTP workers
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Top-k results per query
    #[arg(long, default_value_t = 10)]
    k: usize,

    /// Keep test collections after the run
    #[arg(long)]
    no_cleanup: bool,

    // ── Stress controls ───────────────────────────────────────────────────────

    /// Per-request timeout in milliseconds (0 = disabled)
    #[arg(long, default_value_t = 5_000)]
    timeout_ms: u64,

    /// Maximum concurrency ceiling for the ramp phase
    #[arg(long, default_value_t = 64)]
    max_concurrency: usize,

    /// Error rate (%) that declares a breaking point during the ramp phase
    #[arg(long, default_value_t = 5.0)]
    error_threshold: f64,

    /// p99 latency SLO in milliseconds — ramp breaks here; final verdict uses this
    #[arg(long, default_value_t = 500)]
    p99_slo_ms: u64,

    /// Concurrency multiplier for the spike phase (e.g. 4 → 4× base concurrency)
    #[arg(long, default_value_t = 4)]
    spike_factor: usize,

    /// Duration of the soak phase in seconds
    #[arg(long, default_value_t = 30)]
    soak_secs: u64,

    /// Number of random operations in the chaos phase
    #[arg(long, default_value_t = 1_000)]
    chaos_ops: usize,

    // ── Phase toggles ─────────────────────────────────────────────────────────
    #[arg(long)]
    skip_baseline: bool,

    #[arg(long)]
    skip_ramp: bool,

    #[arg(long)]
    skip_spike: bool,

    #[arg(long)]
    skip_soak: bool,

    #[arg(long)]
    skip_chaos: bool,
}

// ─── Vector helpers ───────────────────────────────────────────────────────────

fn make_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect()
}

// Query seeds live in a different space so they never coincide with stored vectors.
fn make_query(seed: u64, dim: usize) -> Vec<f32> {
    make_vec(seed.wrapping_add(u64::MAX / 2), dim)
}

// ─── Phase result ─────────────────────────────────────────────────────────────

struct PhaseResult {
    latencies_us: Vec<u64>, // successful ops only, sorted
    errors: u64,            // HTTP non-2xx responses
    timeouts: u64,
    total: u64,
    elapsed: Duration,
}

impl PhaseResult {
    fn new(mut latencies_us: Vec<u64>, errors: u64, timeouts: u64, elapsed: Duration) -> Self {
        let total = latencies_us.len() as u64 + errors + timeouts;
        latencies_us.sort_unstable();
        Self {
            latencies_us,
            errors,
            timeouts,
            total,
            elapsed,
        }
    }

    fn success(&self) -> u64 {
        self.latencies_us.len() as u64
    }

    fn error_rate(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.errors + self.timeouts) as f64 / self.total as f64 * 100.0
    }

    fn percentile(&self, p: usize) -> u64 {
        if self.latencies_us.is_empty() {
            return 0;
        }
        let idx = (self.latencies_us.len() * p / 100).min(self.latencies_us.len() - 1);
        self.latencies_us[idx]
    }

    fn throughput(&self) -> f64 {
        self.total as f64 / self.elapsed.as_secs_f64()
    }

    fn meets_p99_slo(&self, slo_ms: u64) -> bool {
        self.percentile(99) <= slo_ms * 1_000
    }
}

// ─── HTTP outcome ─────────────────────────────────────────────────────────────

enum Outcome {
    Success(u64), // latency µs
    HttpError(u16),
    Timeout,
    NetworkError,
}

async fn send_timed(req: reqwest::RequestBuilder, timeout_ms: u64) -> Outcome {
    let t = Instant::now();
    let resp = if timeout_ms > 0 {
        match timeout(Duration::from_millis(timeout_ms), req.send()).await {
            Err(_) => return Outcome::Timeout,
            Ok(r) => r,
        }
    } else {
        req.send().await
    };
    let us = t.elapsed().as_micros() as u64;
    match resp {
        Err(_) => Outcome::NetworkError,
        Ok(r) if r.status().is_success() => Outcome::Success(us),
        Ok(r) => Outcome::HttpError(r.status().as_u16()),
    }
}

// ─── Server helpers ───────────────────────────────────────────────────────────

async fn health_check(client: &Client, host: &str) -> Result<(), String> {
    client
        .get(format!("{host}/health"))
        .send()
        .await
        .map_err(|e| format!("request error: {e}"))?
        .error_for_status()
        .map_err(|e| format!("server error: {e}"))?;
    Ok(())
}

async fn create_collection(
    client: &Client,
    host: &str,
    name: &str,
    dim: usize,
    index: Option<Value>,
    fts: bool,
) -> Result<(), String> {
    let mut body = json!({
        "name": name,
        "dim": dim,
        "metric": "cosine",
        "enable_fts": fts,
    });
    if let Some(idx) = index {
        body["index"] = idx;
    }
    let res = client
        .post(format!("{host}/collections"))
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        return Err(format!("HTTP {status}: {text}"));
    }
    Ok(())
}

async fn drop_collection(client: &Client, host: &str, name: &str) {
    let _ = client
        .delete(format!("{host}/collections/{name}"))
        .send()
        .await;
}

// ─── Shared worker collectors ─────────────────────────────────────────────────

// Returns (latencies, errors, timeouts) collected by one worker task.
type WorkerStats = (Vec<u64>, u64, u64);

fn merge_workers(collected: Vec<WorkerStats>) -> (Vec<u64>, u64, u64) {
    let mut lats = Vec::new();
    let mut errs = 0u64;
    let mut tos = 0u64;
    for (l, e, t) in collected {
        lats.extend(l);
        errs += e;
        tos += t;
    }
    (lats, errs, tos)
}

// ─── Baseline insert / query ──────────────────────────────────────────────────

async fn insert_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    n: usize,
    concurrency: usize,
    timeout_ms: u64,
) -> PhaseResult {
    let per = n.div_ceil(concurrency);
    let wall = Instant::now();
    let mut set: JoinSet<WorkerStats> = JoinSet::new();

    for w in 0..concurrency {
        let base = w * per;
        let count = per.min(n.saturating_sub(base));
        if count == 0 {
            break;
        }
        let c = client.clone();
        let h = host.clone();
        let col = collection.clone();
        set.spawn(async move {
            let mut lats = Vec::with_capacity(count);
            let mut errors = 0u64;
            let mut tos = 0u64;
            for i in 0..count {
                let id = (base + i) as u64;
                let vec = make_vec(id, dim);
                let req = c
                    .post(format!("{h}/collections/{col}/vectors"))
                    .json(&json!({"id": id, "vector": vec, "payload": {"seq": id}}));
                match send_timed(req, timeout_ms).await {
                    Outcome::Success(us) => lats.push(us),
                    Outcome::HttpError(_) | Outcome::NetworkError => errors += 1,
                    Outcome::Timeout => tos += 1,
                }
            }
            (lats, errors, tos)
        });
    }

    let mut collected = Vec::new();
    while let Some(Ok(stats)) = set.join_next().await {
        collected.push(stats);
    }
    let (lats, errs, tos) = merge_workers(collected);
    PhaseResult::new(lats, errs, tos, wall.elapsed())
}

async fn query_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    q: usize,
    k: usize,
    concurrency: usize,
    timeout_ms: u64,
) -> PhaseResult {
    let per = q.div_ceil(concurrency);
    let wall = Instant::now();
    let mut set: JoinSet<WorkerStats> = JoinSet::new();

    for w in 0..concurrency {
        let base = w * per;
        let count = per.min(q.saturating_sub(base));
        if count == 0 {
            break;
        }
        let c = client.clone();
        let h = host.clone();
        let col = collection.clone();
        set.spawn(async move {
            let mut lats = Vec::with_capacity(count);
            let mut errors = 0u64;
            let mut tos = 0u64;
            for i in 0..count {
                let seed = (base + i) as u64;
                let vec = make_query(seed, dim);
                let req = c
                    .post(format!("{h}/collections/{col}/query"))
                    .json(&json!({"vector": vec, "k": k}));
                match send_timed(req, timeout_ms).await {
                    Outcome::Success(us) => lats.push(us),
                    Outcome::HttpError(_) | Outcome::NetworkError => errors += 1,
                    Outcome::Timeout => tos += 1,
                }
            }
            (lats, errors, tos)
        });
    }

    let mut collected = Vec::new();
    while let Some(Ok(stats)) = set.join_next().await {
        collected.push(stats);
    }
    let (lats, errs, tos) = merge_workers(collected);
    PhaseResult::new(lats, errs, tos, wall.elapsed())
}

// ─── Hybrid (baseline sub-phase) ─────────────────────────────────────────────

const CORPUS: &[&str] = &[
    "vector database semantic similarity embeddings retrieval augmented",
    "approximate nearest neighbor graph HNSW index performance",
    "full text search BM25 ranking relevance scoring",
    "rust programming systems performance zero cost abstractions",
    "machine learning transformer model embedding generation inference",
    "parquet columnar storage lakehouse architecture iceberg delta",
    "reciprocal rank fusion hybrid retrieval pipeline",
    "inverted file index clustering k-means centroids training",
    "cosine similarity dot product euclidean distance metrics",
    "tokio async runtime concurrent parallel task scheduling",
    "write ahead log durability crash recovery snapshot bincode",
    "scalar quantization compression memory efficiency SQ8",
    "prometheus metrics observability latency histogram percentile",
    "axum web framework HTTP REST JSON API handlers",
    "tantivy full text search engine indexing analysis",
    "rayon parallel iterator thread pool compute simd",
    "SIMD intrinsics NEON AVX2 distance kernel acceleration",
    "data warehouse analytics OLAP columnar scan pushdown",
    "sentence transformer BERT language model fine tuning",
    "metadata filtering predicate pushdown index scan filter",
];

const SEARCH_TERMS: &[&str] = &[
    "vector", "search", "rust", "index", "embedding", "retrieval", "HNSW", "BM25",
];

async fn insert_hybrid_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    n: usize,
    concurrency: usize,
    timeout_ms: u64,
) -> PhaseResult {
    let per = n.div_ceil(concurrency);
    let wall = Instant::now();
    let mut set: JoinSet<WorkerStats> = JoinSet::new();

    for w in 0..concurrency {
        let base = w * per;
        let count = per.min(n.saturating_sub(base));
        if count == 0 {
            break;
        }
        let c = client.clone();
        let h = host.clone();
        let col = collection.clone();
        set.spawn(async move {
            let mut lats = Vec::with_capacity(count);
            let mut errors = 0u64;
            let mut tos = 0u64;
            for i in 0..count {
                let id = (base + i) as u64;
                let vec = make_vec(id, dim);
                let text = CORPUS[id as usize % CORPUS.len()];
                let req = c.post(format!("{h}/collections/{col}/vectors")).json(&json!({
                    "id": id,
                    "vector": vec,
                    "payload": {"body": text, "seq": id},
                }));
                match send_timed(req, timeout_ms).await {
                    Outcome::Success(us) => lats.push(us),
                    Outcome::HttpError(_) | Outcome::NetworkError => errors += 1,
                    Outcome::Timeout => tos += 1,
                }
            }
            (lats, errors, tos)
        });
    }

    let mut collected = Vec::new();
    while let Some(Ok(stats)) = set.join_next().await {
        collected.push(stats);
    }
    let (lats, errs, tos) = merge_workers(collected);
    PhaseResult::new(lats, errs, tos, wall.elapsed())
}

async fn hybrid_query_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    q: usize,
    k: usize,
    concurrency: usize,
    timeout_ms: u64,
) -> PhaseResult {
    let per = q.div_ceil(concurrency);
    let wall = Instant::now();
    let mut set: JoinSet<WorkerStats> = JoinSet::new();

    for w in 0..concurrency {
        let base = w * per;
        let count = per.min(q.saturating_sub(base));
        if count == 0 {
            break;
        }
        let c = client.clone();
        let h = host.clone();
        let col = collection.clone();
        set.spawn(async move {
            let mut lats = Vec::with_capacity(count);
            let mut errors = 0u64;
            let mut tos = 0u64;
            for i in 0..count {
                let seed = (base + i) as u64;
                let vec = make_query(seed, dim);
                let term = SEARCH_TERMS[seed as usize % SEARCH_TERMS.len()];
                let req = c
                    .post(format!("{h}/collections/{col}/hybrid-query"))
                    .json(&json!({"vector": vec, "text": term, "k": k}));
                match send_timed(req, timeout_ms).await {
                    Outcome::Success(us) => lats.push(us),
                    Outcome::HttpError(_) | Outcome::NetworkError => errors += 1,
                    Outcome::Timeout => tos += 1,
                }
            }
            (lats, errors, tos)
        });
    }

    let mut collected = Vec::new();
    while let Some(Ok(stats)) = set.join_next().await {
        collected.push(stats);
    }
    let (lats, errs, tos) = merge_workers(collected);
    PhaseResult::new(lats, errs, tos, wall.elapsed())
}

// ─── Stress phase: Ramp ───────────────────────────────────────────────────────
//
// Doubles concurrency from 1 → max_concurrency, stopping when error rate exceeds
// the threshold or p99 breaches the SLO. The last step is the breaking point.

struct RampStep {
    concurrency: usize,
    result: PhaseResult,
    is_breaking_point: bool,
}

async fn ramp_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    k: usize,
    queries_per_step: usize,
    max_concurrency: usize,
    error_threshold: f64,
    p99_slo_ms: u64,
    timeout_ms: u64,
) -> Vec<RampStep> {
    // Build doubling ramp: 1, 2, 4, … up to max_concurrency.
    let mut levels = Vec::new();
    let mut c = 1usize;
    loop {
        levels.push(c);
        if c >= max_concurrency {
            break;
        }
        c = (c * 2).min(max_concurrency);
    }

    let mut steps = Vec::new();
    for &concurrency in &levels {
        let result = query_phase(
            client.clone(),
            host.clone(),
            collection.clone(),
            dim,
            queries_per_step,
            k,
            concurrency,
            timeout_ms,
        )
        .await;

        let is_breaking_point =
            result.error_rate() > error_threshold || !result.meets_p99_slo(p99_slo_ms);

        steps.push(RampStep {
            concurrency,
            result,
            is_breaking_point,
        });

        if is_breaking_point {
            break;
        }
    }
    steps
}

// ─── Stress phase: Spike ──────────────────────────────────────────────────────
//
// Models a sudden traffic surge: warm-up at base load, spike to spike_factor×
// concurrency with proportionally more queries, then recover to base load.
// Measures the degradation ratio and whether the system recovers cleanly.

struct SpikeResult {
    warmup: PhaseResult,
    spike: PhaseResult,
    recovery: PhaseResult,
}

async fn spike_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    k: usize,
    base_concurrency: usize,
    spike_factor: usize,
    queries: usize,
    timeout_ms: u64,
) -> SpikeResult {
    let spike_concurrency = (base_concurrency * spike_factor).max(base_concurrency + 1);
    let spike_queries = queries * spike_factor;

    let warmup = query_phase(
        client.clone(),
        host.clone(),
        collection.clone(),
        dim,
        queries,
        k,
        base_concurrency,
        timeout_ms,
    )
    .await;

    let spike = query_phase(
        client.clone(),
        host.clone(),
        collection.clone(),
        dim,
        spike_queries,
        k,
        spike_concurrency,
        timeout_ms,
    )
    .await;

    let recovery = query_phase(
        client.clone(),
        host.clone(),
        collection.clone(),
        dim,
        queries,
        k,
        base_concurrency,
        timeout_ms,
    )
    .await;

    SpikeResult {
        warmup,
        spike,
        recovery,
    }
}

// ─── Stress phase: Soak ───────────────────────────────────────────────────────
//
// Runs sustained load for `total_duration`, split into `n_windows` equal windows.
// Windowed reporting reveals latency drift (memory leaks, GC pressure, lock contention).
// Within each window, workers loop continuously until the window deadline.

async fn soak_window(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    k: usize,
    concurrency: usize,
    window_dur: Duration,
    seed_base: u64,
    timeout_ms: u64,
) -> PhaseResult {
    let deadline = Instant::now() + window_dur;
    let wall = Instant::now();
    let mut set: JoinSet<WorkerStats> = JoinSet::new();

    for w in 0..concurrency {
        let c = client.clone();
        let h = host.clone();
        let col = collection.clone();
        // Spread seeds across workers so they don't repeat the same query vectors.
        let mut seed = seed_base + w as u64 * 100_000;
        set.spawn(async move {
            let mut lats = Vec::new();
            let mut errors = 0u64;
            let mut tos = 0u64;
            loop {
                if Instant::now() >= deadline {
                    break;
                }
                let vec = make_query(seed, dim);
                seed = seed.wrapping_add(1);
                let req = c
                    .post(format!("{h}/collections/{col}/query"))
                    .json(&json!({"vector": vec, "k": k}));
                match send_timed(req, timeout_ms).await {
                    Outcome::Success(us) => lats.push(us),
                    Outcome::HttpError(_) | Outcome::NetworkError => errors += 1,
                    Outcome::Timeout => tos += 1,
                }
            }
            (lats, errors, tos)
        });
    }

    let mut collected = Vec::new();
    while let Some(Ok(stats)) = set.join_next().await {
        collected.push(stats);
    }
    let (lats, errs, tos) = merge_workers(collected);
    PhaseResult::new(lats, errs, tos, wall.elapsed())
}

async fn soak_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    k: usize,
    concurrency: usize,
    total_duration: Duration,
    n_windows: usize,
    timeout_ms: u64,
) -> Vec<PhaseResult> {
    let window_dur = total_duration / n_windows as u32;
    let mut results = Vec::with_capacity(n_windows);
    for w in 0..n_windows {
        let seed_base = w as u64 * 10_000_000;
        let r = soak_window(
            client.clone(),
            host.clone(),
            collection.clone(),
            dim,
            k,
            concurrency,
            window_dur,
            seed_base,
            timeout_ms,
        )
        .await;
        results.push(r);
    }
    results
}

// ─── Stress phase: Chaos ──────────────────────────────────────────────────────
//
// Fires a mix of five operation types at full concurrency:
//   0 — valid query (expect 200)
//   1 — valid insert with fresh IDs (expect 204)
//   2 — query against a nonexistent collection (expect 404)
//   3 — insert with wrong vector dimension (expect 400)
//   4 — GET a nonexistent vector ID (expect 404)
//
// Unexpected errors: any 5xx from op 0/1, or network failures from any op.
// Expected errors: 4xx from op 2/3/4 (correct error handling, not a fault).
// After the chaos, a health check confirms the server survived.

struct ChaosResult {
    total: u64,
    valid_successes: u64,
    expected_errors: u64,
    unexpected_errors: u64,
    timeouts: u64,
    server_healthy: bool,
}

async fn chaos_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    n_ops: usize,
    concurrency: usize,
    timeout_ms: u64,
) -> ChaosResult {
    struct WorkerResult {
        valid_ok: u64,
        expected_err: u64,
        unexpected_err: u64,
        timeouts: u64,
    }

    let per = n_ops.div_ceil(concurrency);
    let mut set: JoinSet<WorkerResult> = JoinSet::new();

    for w in 0..concurrency {
        let base = w * per;
        let count = per.min(n_ops.saturating_sub(base));
        if count == 0 {
            break;
        }
        let c = client.clone();
        let h = host.clone();
        let col = collection.clone();
        set.spawn(async move {
            let mut wr = WorkerResult {
                valid_ok: 0,
                expected_err: 0,
                unexpected_err: 0,
                timeouts: 0,
            };
            for i in 0..count {
                let id = (base + i) as u64;
                let op = id % 5;

                let outcome = match op {
                    0 => {
                        // Valid query against the populated collection.
                        let vec = make_query(id, dim);
                        send_timed(
                            c.post(format!("{h}/collections/{col}/query"))
                                .json(&json!({"vector": vec, "k": 5})),
                            timeout_ms,
                        )
                        .await
                    }
                    1 => {
                        // Valid insert using IDs well above the pre-seeded range.
                        let vec = make_vec(id + 10_000_000, dim);
                        send_timed(
                            c.post(format!("{h}/collections/{col}/vectors"))
                                .json(&json!({"id": id + 10_000_000, "vector": vec})),
                            timeout_ms,
                        )
                        .await
                    }
                    2 => {
                        // Query a collection that does not exist (expect 404).
                        let vec = make_query(id, dim);
                        send_timed(
                            c.post(format!("{h}/collections/ghost_chaos_{id}/query"))
                                .json(&json!({"vector": vec, "k": 5})),
                            timeout_ms,
                        )
                        .await
                    }
                    3 => {
                        // Insert with one extra dimension — server must reject with 400.
                        let wrong: Vec<f32> = vec![0.5_f32; dim + 1];
                        send_timed(
                            c.post(format!("{h}/collections/{col}/vectors"))
                                .json(&json!({"id": id + 20_000_000, "vector": wrong})),
                            timeout_ms,
                        )
                        .await
                    }
                    _ => {
                        // Fetch a vector ID that was never inserted (expect 404).
                        send_timed(
                            c.get(format!(
                                "{h}/collections/{col}/vectors/{}",
                                id + 9_000_000
                            )),
                            timeout_ms,
                        )
                        .await
                    }
                };

                match (op, outcome) {
                    // Valid ops succeeded as expected.
                    (0 | 1, Outcome::Success(_)) => wr.valid_ok += 1,
                    // Invalid ops got the expected 4xx rejection.
                    (2 | 3 | 4, Outcome::HttpError(s)) if s >= 400 && s < 500 => {
                        wr.expected_err += 1
                    }
                    // Timeouts — server may be overloaded but not crashed.
                    (_, Outcome::Timeout) => wr.timeouts += 1,
                    // Anything else: unexpected 5xx, network error, or wrong status.
                    _ => wr.unexpected_err += 1,
                }
            }
            wr
        });
    }

    let mut valid_successes = 0u64;
    let mut expected_errors = 0u64;
    let mut unexpected_errors = 0u64;
    let mut timeouts = 0u64;
    while let Some(Ok(wr)) = set.join_next().await {
        valid_successes += wr.valid_ok;
        expected_errors += wr.expected_err;
        unexpected_errors += wr.unexpected_err;
        timeouts += wr.timeouts;
    }

    let server_healthy = health_check(&client, &host).await.is_ok();

    ChaosResult {
        total: n_ops as u64,
        valid_successes,
        expected_errors,
        unexpected_errors,
        timeouts,
        server_healthy,
    }
}

// ─── Display helpers ──────────────────────────────────────────────────────────

fn fmt_us(us: u64) -> String {
    if us < 1_000 {
        format!("{us}µs")
    } else {
        format!("{:.2}ms", us as f64 / 1_000.0)
    }
}

fn fmt_tput(tput: f64) -> String {
    if tput >= 1_000.0 {
        format!("{:.1}k/s", tput / 1_000.0)
    } else {
        format!("{:.0}/s", tput)
    }
}

fn separator() {
    println!("  {}", "─".repeat(76));
}

fn print_baseline_row(label: &str, tag: &str, r: &PhaseResult) {
    println!(
        "  [{label:6}] {tag:<10}  tput={:>8}  p50={:>8}  p95={:>8}  p99={:>8}  err={:.1}%",
        fmt_tput(r.throughput()),
        fmt_us(r.percentile(50)),
        fmt_us(r.percentile(95)),
        fmt_us(r.percentile(99)),
        r.error_rate(),
    );
}

fn slo_mark(passes: bool) -> &'static str {
    if passes {
        "PASS"
    } else {
        "FAIL"
    }
}

// ─── Summary ──────────────────────────────────────────────────────────────────

fn print_baseline_summary(results: &[(&str, PhaseResult, PhaseResult)], args: &Args) {
    println!(
        "  ══════════════════════════════════════════════════════════════════════════════"
    );
    println!(
        "  BASELINE SUMMARY  dim={}  vectors={}  queries={}  concurrency={}",
        args.dim, args.vectors, args.queries, args.concurrency
    );
    println!(
        "  {:<10}  {:>9}  {:>8}  {:>8}  {:>8}  {:>5}    {:>9}  {:>8}  {:>8}  {:>8}  {:>5}",
        "index",
        "ins/s",
        "p50",
        "p95",
        "p99",
        "err%",
        "qry/s",
        "p50",
        "p95",
        "p99",
        "err%"
    );
    println!("  {}", "─".repeat(78));
    for (tag, ins, qry) in results {
        println!(
            "  {:<10}  {:>9}  {:>8}  {:>8}  {:>8}  {:>4.1}%    {:>9}  {:>8}  {:>8}  {:>8}  {:>4.1}%",
            tag,
            fmt_tput(ins.throughput()),
            fmt_us(ins.percentile(50)),
            fmt_us(ins.percentile(95)),
            fmt_us(ins.percentile(99)),
            ins.error_rate(),
            fmt_tput(qry.throughput()),
            fmt_us(qry.percentile(50)),
            fmt_us(qry.percentile(95)),
            fmt_us(qry.percentile(99)),
            qry.error_rate(),
        );
    }
    println!(
        "  ══════════════════════════════════════════════════════════════════════════════"
    );
    println!();
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = Client::new();

    println!();
    println!("  ╔══════════════════════════════════════════════════════════════════════╗");
    println!("  ║         LikhaDB Stress Test  —  Breaking-Point Analysis            ║");
    println!("  ╚══════════════════════════════════════════════════════════════════════╝");
    println!();
    println!(
        "  host={}  dim={}  vectors={}  queries={}  concurrency={}  k={}",
        args.host, args.dim, args.vectors, args.queries, args.concurrency, args.k
    );
    println!(
        "  timeout={}ms  error_threshold={:.0}%  p99_slo={}ms  spike={}×  soak={}s  chaos_ops={}",
        args.timeout_ms,
        args.error_threshold,
        args.p99_slo_ms,
        args.spike_factor,
        args.soak_secs,
        args.chaos_ops,
    );
    println!();

    // ── Preflight ─────────────────────────────────────────────────────────────
    print!("  Checking server health... ");
    match health_check(&client, &args.host).await {
        Ok(()) => println!("OK"),
        Err(e) => {
            eprintln!("FAILED\n\n  Error: {e}");
            eprintln!("  Is the server running?  Try: ./dev.sh");
            std::process::exit(1);
        }
    }
    println!();

    let mut all_collections: Vec<String> = Vec::new();
    let mut baseline_results: Vec<(&str, PhaseResult, PhaseResult)> = Vec::new();

    // ── Phase 1: Baseline ─────────────────────────────────────────────────────
    if !args.skip_baseline {
        struct Config {
            tag: &'static str,
            index: Option<Value>,
        }

        let configs = [
            Config {
                tag: "flat",
                index: None,
            },
            Config {
                tag: "ivf",
                index: Some(json!({"type": "ivf", "nlist": 100, "nprobe": 10})),
            },
            Config {
                tag: "hnsw",
                index: Some(
                    json!({"type": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 50}),
                ),
            },
        ];

        for cfg in &configs {
            let name = format!("stress_{}", cfg.tag);
            println!(
                "  ── BASELINE: {} index  ({})",
                cfg.tag.to_uppercase(),
                name
            );
            separator();

            drop_collection(&client, &args.host, &name).await;
            all_collections.push(name.clone());

            print!("  Creating collection... ");
            match create_collection(
                &client,
                &args.host,
                &name,
                args.dim,
                cfg.index.clone(),
                false,
            )
            .await
            {
                Ok(()) => println!("OK"),
                Err(e) => {
                    eprintln!("FAILED: {e}");
                    continue;
                }
            }

            print!(
                "  Inserting {} vectors ({} workers)... ",
                args.vectors, args.concurrency
            );
            let ins = insert_phase(
                client.clone(),
                args.host.clone(),
                name.clone(),
                args.dim,
                args.vectors,
                args.concurrency,
                args.timeout_ms,
            )
            .await;
            println!("done ({:.2?})", ins.elapsed);
            print_baseline_row("Insert", cfg.tag, &ins);

            print!(
                "\n  Querying (k={}, {} queries, {} workers)... ",
                args.k, args.queries, args.concurrency
            );
            let qry = query_phase(
                client.clone(),
                args.host.clone(),
                name.clone(),
                args.dim,
                args.queries,
                args.k,
                args.concurrency,
                args.timeout_ms,
            )
            .await;
            println!("done ({:.2?})", qry.elapsed);
            print_baseline_row("Query", cfg.tag, &qry);
            println!();

            baseline_results.push((cfg.tag, ins, qry));
        }

        // Hybrid sub-phase
        {
            let n_hybrid = (args.vectors / 10).max(500);
            let q_hybrid = (args.queries / 5).max(50);
            let name = "stress_hybrid";

            println!("  ── BASELINE: HYBRID (flat + BM25, RRF)");
            separator();

            drop_collection(&client, &args.host, name).await;

            print!("  Creating collection (enable_fts=true)... ");
            match create_collection(&client, &args.host, name, args.dim, None, true).await {
                Ok(()) => {
                    println!("OK");
                    all_collections.push(name.to_string());

                    print!(
                        "  Inserting {} vectors with text payloads ({} workers)... ",
                        n_hybrid, args.concurrency
                    );
                    let ins = insert_hybrid_phase(
                        client.clone(),
                        args.host.clone(),
                        name.to_string(),
                        args.dim,
                        n_hybrid,
                        args.concurrency,
                        args.timeout_ms,
                    )
                    .await;
                    println!("done ({:.2?})", ins.elapsed);
                    print_baseline_row("Insert", "hybrid", &ins);

                    print!(
                        "\n  Hybrid queries (k={}, {} queries, {} workers)... ",
                        args.k, q_hybrid, args.concurrency
                    );
                    let qry = hybrid_query_phase(
                        client.clone(),
                        args.host.clone(),
                        name.to_string(),
                        args.dim,
                        q_hybrid,
                        args.k,
                        args.concurrency,
                        args.timeout_ms,
                    )
                    .await;
                    println!("done ({:.2?})", qry.elapsed);
                    print_baseline_row("Hybrid", "hybrid", &qry);
                    println!();

                    baseline_results.push(("hybrid", ins, qry));
                }
                Err(e) => {
                    eprintln!("FAILED: {e}");
                    eprintln!(
                        "  (Compiled without fts feature? Skipping hybrid sub-phase.)"
                    );
                    println!();
                }
            }
        }

        print_baseline_summary(&baseline_results, &args);
    }

    // ── Stress collection setup ───────────────────────────────────────────────
    // A flat collection pre-seeded with `vectors` entries is shared across
    // ramp, spike, soak, and chaos phases so each phase begins with a warm index.

    let any_stress =
        !args.skip_ramp || !args.skip_spike || !args.skip_soak || !args.skip_chaos;

    let stress_col = "stress_main".to_string();

    if any_stress {
        println!("  ── STRESS COLLECTION SETUP");
        separator();
        drop_collection(&client, &args.host, &stress_col).await;
        all_collections.push(stress_col.clone());

        print!("  Creating stress_main (flat)... ");
        match create_collection(&client, &args.host, &stress_col, args.dim, None, false).await {
            Ok(()) => println!("OK"),
            Err(e) => {
                eprintln!("FAILED: {e}  — aborting stress phases.");
                finalize(&args, &all_collections, &client).await;
                return;
            }
        }

        print!(
            "  Seeding {} vectors ({} workers)... ",
            args.vectors, args.concurrency
        );
        let seed_result = insert_phase(
            client.clone(),
            args.host.clone(),
            stress_col.clone(),
            args.dim,
            args.vectors,
            args.concurrency,
            args.timeout_ms,
        )
        .await;
        println!(
            "done ({:.2?})  err={:.1}%",
            seed_result.elapsed,
            seed_result.error_rate()
        );
        println!();
    }

    // ── Phase 2: Ramp ─────────────────────────────────────────────────────────
    if !args.skip_ramp {
        println!("  ── PHASE 2: RAMP  (concurrency doubles until breaking point)");
        separator();
        println!(
            "  {:<14}  {:>9}  {:>8}  {:>8}  {:>8}  {:>6}  {:>5}",
            "concurrency", "tput", "p50", "p95", "p99", "err%", "p99 SLO"
        );
        println!("  {}", "─".repeat(68));

        let queries_per_step = args.queries.max(100);
        let steps = ramp_phase(
            client.clone(),
            args.host.clone(),
            stress_col.clone(),
            args.dim,
            args.k,
            queries_per_step,
            args.max_concurrency,
            args.error_threshold,
            args.p99_slo_ms,
            args.timeout_ms,
        )
        .await;

        let mut breaking_concurrency: Option<usize> = None;
        for step in &steps {
            let slo = if step.result.meets_p99_slo(args.p99_slo_ms) {
                "✓"
            } else {
                "✗ BREAK"
            };
            let err_marker = if step.result.error_rate() > args.error_threshold {
                " ← err"
            } else {
                ""
            };
            println!(
                "  {:<14}  {:>9}  {:>8}  {:>8}  {:>8}  {:>5.1}%  {}{} ",
                step.concurrency,
                fmt_tput(step.result.throughput()),
                fmt_us(step.result.percentile(50)),
                fmt_us(step.result.percentile(95)),
                fmt_us(step.result.percentile(99)),
                step.result.error_rate(),
                slo,
                err_marker,
            );
            if step.is_breaking_point {
                breaking_concurrency = Some(step.concurrency);
            }
        }

        println!();
        match breaking_concurrency {
            Some(bp) => println!(
                "  ⚠ Breaking point at concurrency={bp}  \
                 (err>{:.0}% or p99>{}ms)",
                args.error_threshold, args.p99_slo_ms
            ),
            None => println!(
                "  ✓ No breaking point found up to concurrency={}",
                args.max_concurrency
            ),
        }
        println!();
    }

    // ── Phase 3: Spike ────────────────────────────────────────────────────────
    if !args.skip_spike {
        let spike_concurrency = args.concurrency * args.spike_factor;
        println!(
            "  ── PHASE 3: SPIKE  ({}→{}→{} workers)",
            args.concurrency, spike_concurrency, args.concurrency
        );
        separator();

        let sr = spike_phase(
            client.clone(),
            args.host.clone(),
            stress_col.clone(),
            args.dim,
            args.k,
            args.concurrency,
            args.spike_factor,
            args.queries,
            args.timeout_ms,
        )
        .await;

        let warmup_p99 = sr.warmup.percentile(99).max(1);
        let spike_p99 = sr.spike.percentile(99);
        let recovery_p99 = sr.recovery.percentile(99);
        let degradation = spike_p99 as f64 / warmup_p99 as f64;
        let recovered =
            recovery_p99 <= (warmup_p99 as f64 * 1.5) as u64; // within 1.5× warm-up is "recovered"

        println!(
            "  {:<12}  {:>9}  {:>8}  {:>8}  {:>8}  {:>5.1}%",
            "phase", "tput", "p50", "p95", "p99", "err%"
        );
        println!("  {}", "─".repeat(60));
        for (label, r) in [("warm-up", &sr.warmup), ("spike", &sr.spike), ("recovery", &sr.recovery)] {
            println!(
                "  {:<12}  {:>9}  {:>8}  {:>8}  {:>8}  {:>5.1}%",
                label,
                fmt_tput(r.throughput()),
                fmt_us(r.percentile(50)),
                fmt_us(r.percentile(95)),
                fmt_us(r.percentile(99)),
                r.error_rate(),
            );
        }
        println!();
        println!(
            "  p99 degradation during spike: {:.1}×   recovery: {}",
            degradation,
            if recovered { "✓ clean" } else { "✗ still elevated" }
        );
        println!();
    }

    // ── Phase 4: Soak ─────────────────────────────────────────────────────────
    if !args.skip_soak {
        const N_WINDOWS: usize = 5;
        println!(
            "  ── PHASE 4: SOAK  ({}s sustained, {} windows, {} workers)",
            args.soak_secs, N_WINDOWS, args.concurrency
        );
        separator();

        let windows = soak_phase(
            client.clone(),
            args.host.clone(),
            stress_col.clone(),
            args.dim,
            args.k,
            args.concurrency,
            Duration::from_secs(args.soak_secs),
            N_WINDOWS,
            args.timeout_ms,
        )
        .await;

        println!(
            "  {:<10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>6}  {:>8}",
            "window", "ops", "p50", "p95", "p99", "err%", "tput"
        );
        println!("  {}", "─".repeat(68));
        for (i, w) in windows.iter().enumerate() {
            println!(
                "  {:<10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>5.1}%  {:>8}",
                format!("{}/{}", i + 1, N_WINDOWS),
                w.success(),
                fmt_us(w.percentile(50)),
                fmt_us(w.percentile(95)),
                fmt_us(w.percentile(99)),
                w.error_rate(),
                fmt_tput(w.throughput()),
            );
        }

        if windows.len() >= 2 {
            let first_p95 = windows.first().map(|w| w.percentile(95)).unwrap_or(0).max(1);
            let last_p95 = windows.last().map(|w| w.percentile(95)).unwrap_or(0);
            let drift_pct = (last_p95 as f64 - first_p95 as f64) / first_p95 as f64 * 100.0;
            println!();
            println!(
                "  Latency drift (p95 first→last window): {}{:.1}%  {}",
                if drift_pct >= 0.0 { "+" } else { "" },
                drift_pct,
                if drift_pct.abs() <= 20.0 {
                    "✓ stable"
                } else {
                    "⚠ drifting"
                }
            );
        }
        println!();
    }

    // ── Phase 5: Chaos ────────────────────────────────────────────────────────
    if !args.skip_chaos {
        let chaos_concurrency = (args.concurrency * 2).max(4);
        println!(
            "  ── PHASE 5: CHAOS  ({} ops, {} workers, mixed valid/invalid)",
            args.chaos_ops, chaos_concurrency
        );
        separator();
        println!("  Op mix: valid-query (20%) · valid-insert (20%) · ghost-collection (20%)");
        println!("          wrong-dimension (20%) · nonexistent-vector (20%)");
        println!();

        let cr = chaos_phase(
            client.clone(),
            args.host.clone(),
            stress_col.clone(),
            args.dim,
            args.chaos_ops,
            chaos_concurrency,
            args.timeout_ms,
        )
        .await;

        println!("  Results  ({} total ops):", cr.total);
        println!("    valid successes  : {}", cr.valid_successes);
        println!("    expected errors  : {} (correct 4xx from invalid ops)", cr.expected_errors);
        println!("    timeouts         : {}", cr.timeouts);
        println!(
            "    unexpected errors: {}  {}",
            cr.unexpected_errors,
            if cr.unexpected_errors == 0 {
                "✓"
            } else {
                "✗ (5xx or wrong status from valid ops)"
            }
        );
        println!(
            "    server health    : {}",
            if cr.server_healthy { "OK ✓" } else { "FAILED ✗" }
        );
        println!();
    }

    // ── Final SLO verdict ─────────────────────────────────────────────────────
    println!(
        "  ══════════════════════════════════════════════════════════════════════════════"
    );
    println!("  SLO VERDICT  (p99 threshold = {}ms)", args.p99_slo_ms);
    println!("  {}", "─".repeat(78));

    if !baseline_results.is_empty() {
        for (tag, _, qry) in &baseline_results {
            println!(
                "  baseline/{:<8}  p99={}  {}",
                tag,
                fmt_us(qry.percentile(99)),
                slo_mark(qry.meets_p99_slo(args.p99_slo_ms))
            );
        }
    }

    println!(
        "  ══════════════════════════════════════════════════════════════════════════════"
    );
    println!();

    finalize(&args, &all_collections, &client).await;
}

async fn finalize(args: &Args, all_collections: &[String], client: &Client) {
    if !args.no_cleanup {
        print!("  Cleaning up test collections... ");
        for name in all_collections {
            drop_collection(client, &args.host, name).await;
        }
        println!("done");
    } else {
        println!(
            "  Collections retained (--no-cleanup). Inspect via GET /collections."
        );
    }
    println!();
}
