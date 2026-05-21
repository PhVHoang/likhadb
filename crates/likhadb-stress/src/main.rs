use clap::Parser;
use rand::{rngs::StdRng, Rng, SeedableRng};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

// ─── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "likhadb-stress",
    about = "Stress / workload demonstration for LikhaDB",
    long_about = "Runs concurrent insert + query workloads against a live LikhaDB server.\n\
                  Compares Flat (brute-force), IVF, and HNSW indexes, then runs a\n\
                  hybrid vector+BM25 phase to show full-text fusion."
)]
struct Args {
    /// Base URL of the running LikhaDB server
    #[arg(long, default_value = "http://localhost:8080")]
    host: String,

    /// Vector dimension (should match your embedding model)
    #[arg(long, default_value_t = 128)]
    dim: usize,

    /// Vectors to insert per index type
    #[arg(long, default_value_t = 10_000)]
    vectors: usize,

    /// Query iterations per index type
    #[arg(long, default_value_t = 500)]
    queries: usize,

    /// Concurrent HTTP workers
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Top-k results returned per query
    #[arg(long, default_value_t = 10)]
    k: usize,

    /// Keep test collections after the run (useful for inspection)
    #[arg(long)]
    no_cleanup: bool,
}

// ─── Vector helpers ───────────────────────────────────────────────────────────

fn make_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect()
}

// Query seeds live in a different space from insert seeds so queries never
// coincide with stored vectors (which would trivially score 1.0).
fn make_query(seed: u64, dim: usize) -> Vec<f32> {
    make_vec(seed.wrapping_add(u64::MAX / 2), dim)
}

// ─── Latency stats ────────────────────────────────────────────────────────────

struct Timing {
    us: Vec<u64>, // per-operation microseconds, sorted after construction
    elapsed: Duration,
}

impl Timing {
    fn new(mut us: Vec<u64>, elapsed: Duration) -> Self {
        us.sort_unstable();
        Self { us, elapsed }
    }

    fn percentile(&self, p: usize) -> u64 {
        if self.us.is_empty() {
            return 0;
        }
        let idx = (self.us.len() * p / 100).min(self.us.len() - 1);
        self.us[idx]
    }

    fn throughput(&self) -> f64 {
        self.us.len() as f64 / self.elapsed.as_secs_f64()
    }
}

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

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
    // Best-effort; ignore errors (collection may not exist yet).
    let _ = client
        .delete(format!("{host}/collections/{name}"))
        .send()
        .await;
}

// ─── Workload phases ──────────────────────────────────────────────────────────

/// Split `n` items across `concurrency` workers; each worker runs sequentially
/// to model a realistic multi-client scenario.
async fn insert_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    n: usize,
    concurrency: usize,
) -> Timing {
    let per = n.div_ceil(concurrency);
    let wall = Instant::now();
    let mut set: JoinSet<Vec<u64>> = JoinSet::new();

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
            let mut us = Vec::with_capacity(count);
            for i in 0..count {
                let id = (base + i) as u64;
                let vec = make_vec(id, dim);
                let t = Instant::now();
                let _ = c
                    .post(format!("{h}/collections/{col}/vectors"))
                    .json(&json!({"id": id, "vector": vec, "payload": {"seq": id}}))
                    .send()
                    .await;
                us.push(t.elapsed().as_micros() as u64);
            }
            us
        });
    }

    let mut all = Vec::with_capacity(n);
    while let Some(Ok(chunk)) = set.join_next().await {
        all.extend(chunk);
    }
    Timing::new(all, wall.elapsed())
}

async fn query_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    q: usize,
    k: usize,
    concurrency: usize,
) -> Timing {
    let per = q.div_ceil(concurrency);
    let wall = Instant::now();
    let mut set: JoinSet<Vec<u64>> = JoinSet::new();

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
            let mut us = Vec::with_capacity(count);
            for i in 0..count {
                let seed = (base + i) as u64;
                let vec = make_query(seed, dim);
                let t = Instant::now();
                let _ = c
                    .post(format!("{h}/collections/{col}/query"))
                    .json(&json!({"vector": vec, "k": k}))
                    .send()
                    .await;
                us.push(t.elapsed().as_micros() as u64);
            }
            us
        });
    }

    let mut all = Vec::with_capacity(q);
    while let Some(Ok(chunk)) = set.join_next().await {
        all.extend(chunk);
    }
    Timing::new(all, wall.elapsed())
}

// 20 tech-domain sentences give BM25 meaningful term frequencies to rank.
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

async fn insert_hybrid_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    n: usize,
    concurrency: usize,
) -> Timing {
    let per = n.div_ceil(concurrency);
    let wall = Instant::now();
    let mut set: JoinSet<Vec<u64>> = JoinSet::new();

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
            let mut us = Vec::with_capacity(count);
            for i in 0..count {
                let id = (base + i) as u64;
                let vec = make_vec(id, dim);
                let text = CORPUS[id as usize % CORPUS.len()];
                let t = Instant::now();
                let _ = c
                    .post(format!("{h}/collections/{col}/vectors"))
                    .json(&json!({
                        "id": id,
                        "vector": vec,
                        "payload": {"body": text, "seq": id},
                    }))
                    .send()
                    .await;
                us.push(t.elapsed().as_micros() as u64);
            }
            us
        });
    }

    let mut all = Vec::with_capacity(n);
    while let Some(Ok(chunk)) = set.join_next().await {
        all.extend(chunk);
    }
    Timing::new(all, wall.elapsed())
}

const SEARCH_TERMS: &[&str] = &[
    "vector",
    "search",
    "rust",
    "index",
    "embedding",
    "retrieval",
    "HNSW",
    "BM25",
];

async fn hybrid_query_phase(
    client: Client,
    host: String,
    collection: String,
    dim: usize,
    q: usize,
    k: usize,
    concurrency: usize,
) -> Timing {
    let per = q.div_ceil(concurrency);
    let wall = Instant::now();
    let mut set: JoinSet<Vec<u64>> = JoinSet::new();

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
            let mut us = Vec::with_capacity(count);
            for i in 0..count {
                let seed = (base + i) as u64;
                let vec = make_query(seed, dim);
                let term = SEARCH_TERMS[seed as usize % SEARCH_TERMS.len()];
                let t = Instant::now();
                let _ = c
                    .post(format!("{h}/collections/{col}/hybrid-query"))
                    .json(&json!({"vector": vec, "text": term, "k": k}))
                    .send()
                    .await;
                us.push(t.elapsed().as_micros() as u64);
            }
            us
        });
    }

    let mut all = Vec::with_capacity(q);
    while let Some(Ok(chunk)) = set.join_next().await {
        all.extend(chunk);
    }
    Timing::new(all, wall.elapsed())
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

fn print_row(label: &str, tag: &str, t: &Timing) {
    println!(
        "  [{label:6}] {tag:<10}  tput={:>8}  p50={:>8}  p95={:>8}  p99={:>8}",
        fmt_tput(t.throughput()),
        fmt_us(t.percentile(50)),
        fmt_us(t.percentile(95)),
        fmt_us(t.percentile(99)),
    );
}

fn separator() {
    println!("  {}", "─".repeat(72));
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = Client::new();

    println!();
    println!("  ╔══════════════════════════════════════════════════════════════╗");
    println!("  ║              LikhaDB Stress Test                            ║");
    println!("  ╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  host={}", args.host);
    println!("  dim={dim}  vectors={v}  queries={q}  concurrency={c}  k={k}",
        dim = args.dim, v = args.vectors, q = args.queries,
        c = args.concurrency, k = args.k);
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

    // ── Index phases ──────────────────────────────────────────────────────────

    struct Config {
        tag: &'static str,
        index: Option<Value>,
    }

    let configs = [
        Config {
            tag: "flat",
            index: None, // uses server default (brute-force SIMD)
        },
        Config {
            tag: "ivf",
            // Auto-trains after nlist=100 inserts; nprobe=10 searches 10% of clusters.
            index: Some(json!({"type": "ivf", "nlist": 100, "nprobe": 10})),
        },
        Config {
            tag: "hnsw",
            // m=16 balances graph fan-out vs. memory; ef_search=50 for good recall.
            index: Some(json!({"type": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 50})),
        },
    ];

    let mut results: Vec<(&str, Timing, Timing)> = Vec::new();

    for cfg in &configs {
        let name = format!("stress_{}", cfg.tag);
        println!("  ── {} index ({name})", cfg.tag.to_uppercase());
        separator();

        // Idempotent: delete any leftover collection from a previous run.
        drop_collection(&client, &args.host, &name).await;

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

        // Insert
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
        )
        .await;
        println!("done ({:.2?})", ins.elapsed);
        print_row("Insert", cfg.tag, &ins);

        // Query
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
        )
        .await;
        println!("done ({:.2?})", qry.elapsed);
        print_row("Query", cfg.tag, &qry);

        println!();
        results.push((cfg.tag, ins, qry));
    }

    // ── Hybrid phase ──────────────────────────────────────────────────────────

    {
        // Smaller dataset so FTS indexing doesn't dominate demo time.
        let n_hybrid = (args.vectors / 10).max(500);
        let q_hybrid = (args.queries / 5).max(50);
        let name = "stress_hybrid";

        println!("  ── HYBRID (flat + BM25, RRF fusion)");
        separator();

        drop_collection(&client, &args.host, name).await;

        print!("  Creating collection (enable_fts=true)... ");
        match create_collection(
            &client,
            &args.host,
            name,
            args.dim,
            None, // flat index under the hood
            true,
        )
        .await
        {
            Ok(()) => println!("OK"),
            Err(e) => {
                eprintln!("FAILED: {e}");
                eprintln!("  (Compiled without fts feature? Skipping hybrid phase.)");
                goto_summary(&results, &args);
                return;
            }
        }

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
        )
        .await;
        println!("done ({:.2?})", ins.elapsed);
        print_row("Insert", "hybrid", &ins);

        print!(
            "\n  Running {} hybrid queries (k={}, {} workers)... ",
            q_hybrid, args.k, args.concurrency
        );
        let qry = hybrid_query_phase(
            client.clone(),
            args.host.clone(),
            name.to_string(),
            args.dim,
            q_hybrid,
            args.k,
            args.concurrency,
        )
        .await;
        println!("done ({:.2?})", qry.elapsed);
        print_row("Hybrid", "hybrid", &qry);
        println!();

        results.push(("hybrid", ins, qry));
    }

    goto_summary(&results, &args);

    // ── Cleanup ───────────────────────────────────────────────────────────────
    if !args.no_cleanup {
        print!("  Cleaning up test collections... ");
        for (tag, _, _) in &results {
            let name = if *tag == "hybrid" {
                "stress_hybrid".to_string()
            } else {
                format!("stress_{tag}")
            };
            drop_collection(&client, &args.host, &name).await;
        }
        println!("done");
    } else {
        println!("  Collections retained (--no-cleanup). Inspect via GET /collections.");
    }
    println!();
}

fn goto_summary(results: &[(&str, Timing, Timing)], args: &Args) {
    println!(
        "  ══════════════════════════════════════════════════════════════════════════"
    );
    println!(
        "  SUMMARY  dim={}  vectors={}  queries={}  concurrency={}",
        args.dim, args.vectors, args.queries, args.concurrency
    );
    println!(
        "  {:<10}  {:>9}  {:>8}  {:>8}  {:>8}    {:>9}  {:>8}  {:>8}  {:>8}",
        "index", "ins/s", "p50", "p95", "p99", "qry/s", "p50", "p95", "p99"
    );
    println!("  {}", "─".repeat(74));
    for (tag, ins, qry) in results {
        println!(
            "  {:<10}  {:>9}  {:>8}  {:>8}  {:>8}    {:>9}  {:>8}  {:>8}  {:>8}",
            tag,
            fmt_tput(ins.throughput()),
            fmt_us(ins.percentile(50)),
            fmt_us(ins.percentile(95)),
            fmt_us(ins.percentile(99)),
            fmt_tput(qry.throughput()),
            fmt_us(qry.percentile(50)),
            fmt_us(qry.percentile(95)),
            fmt_us(qry.percentile(99)),
        );
    }
    println!(
        "  ══════════════════════════════════════════════════════════════════════════"
    );
    println!();
}
