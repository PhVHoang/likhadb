use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use likhadb_core::Metric;
use likhadb_index::{IvfIndex, VectorIndex};
use likhadb_store::CollectionManager;
use likhadb_index::HnswIndex;
use rand::{rngs::StdRng, Rng, SeedableRng};

fn random_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen::<f32>()).collect()
}

/// Brute-force L2 search with no SIMD and no rayon — establishes the scalar baseline.
struct ScalarIndex {
    dim: usize,
    ids: Vec<u64>,
    data: Vec<f32>,
}

impl ScalarIndex {
    fn new(dim: usize) -> Self {
        Self { dim, ids: Vec::new(), data: Vec::new() }
    }

    fn insert(&mut self, id: u64, v: &[f32]) {
        self.ids.push(id);
        self.data.extend_from_slice(v);
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(f32, u64)> {
        let mut all: Vec<(f32, u64)> = self
            .ids
            .iter()
            .zip(self.data.chunks_exact(self.dim))
            .map(|(&id, chunk)| {
                let d: f32 = query
                    .iter()
                    .zip(chunk.iter())
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum::<f32>()
                    .sqrt();
                (d, id)
            })
            .collect();
        all.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        all.truncate(k);
        all
    }
}

fn bench_scalar(c: &mut Criterion, label: &str, n: usize, dim: usize, k: usize) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut idx = ScalarIndex::new(dim);
    let vecs: Vec<Vec<f32>> = (0..n).map(|_| random_vec(&mut rng, dim)).collect();
    for (i, v) in vecs.iter().enumerate() {
        idx.insert(i as u64, v);
    }
    let query = random_vec(&mut rng, dim);
    drop(vecs);

    c.bench_with_input(BenchmarkId::new("scalar", label), &query, |b, q| {
        b.iter(|| {
            let results = idx.search(black_box(q), k);
            black_box(results);
        });
    });
}

/// SIMD-only: FlatIndex (uses simsimd kernels) but forced onto a single rayon thread
/// so we isolate the SIMD benefit from the parallelism benefit.
fn bench_simd(c: &mut Criterion, label: &str, n: usize, dim: usize, k: usize) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut mgr = CollectionManager::new();
    mgr.create_collection("bench_simd", dim, Metric::L2).unwrap();
    let col = mgr.get_mut("bench_simd").unwrap();
    for i in 0..n as u64 {
        col.insert(i, random_vec(&mut rng, dim), None).unwrap();
    }
    let query = random_vec(&mut rng, dim);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();

    c.bench_with_input(BenchmarkId::new("simd", label), &query, |b, q| {
        let col = mgr.get("bench_simd").unwrap();
        b.iter(|| {
            pool.install(|| {
                let results = col.search(black_box(q), k, None, false).unwrap();
                black_box(results);
            });
        });
    });
}

/// SIMD + rayon: FlatIndex on the default thread pool.
fn bench_simd_rayon(c: &mut Criterion, label: &str, n: usize, dim: usize, k: usize) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut mgr = CollectionManager::new();
    mgr.create_collection("bench_rayon", dim, Metric::L2).unwrap();
    let col = mgr.get_mut("bench_rayon").unwrap();
    for i in 0..n as u64 {
        col.insert(i, random_vec(&mut rng, dim), None).unwrap();
    }
    let query = random_vec(&mut rng, dim);

    c.bench_with_input(BenchmarkId::new("simd_rayon", label), &query, |b, q| {
        let col = mgr.get("bench_rayon").unwrap();
        b.iter(|| {
            let results = col.search(black_box(q), k, None, false).unwrap();
            black_box(results);
        });
    });
}

/// Measures k-means training latency in isolation.
/// Setup builds an IvfIndex with nlist-1 vectors already inserted; the
/// measured call inserts the nth vector, which triggers training.
fn bench_ivf_training(
    c: &mut Criterion,
    label: &str,
    n: usize,
    dim: usize,
    nlist: usize,
) {
    let mut rng = StdRng::seed_from_u64(42);
    // Pre-generate all vectors so setup overhead is deterministic.
    let vecs: Vec<Vec<f32>> = (0..n).map(|_| random_vec(&mut rng, dim)).collect();

    c.bench_with_input(
        BenchmarkId::new("ivf_training", label),
        &vecs,
        |b, vecs| {
            b.iter_batched(
                || {
                    // Setup: insert nlist-1 vectors into a fresh index.
                    let mut idx =
                        IvfIndex::new(dim, Metric::L2, nlist, nlist / 4).unwrap();
                    for i in 0..(nlist - 1) {
                        idx.insert(i as u64, vecs[i].clone()).unwrap();
                    }
                    idx
                },
                |mut idx| {
                    // Measured: the nlist-th insert triggers k-means.
                    idx.insert(black_box((nlist - 1) as u64), black_box(vecs[nlist - 1].clone()))
                        .unwrap();
                    black_box(idx);
                },
                BatchSize::SmallInput,
            );
        },
    );
}

/// Measures post-training query latency for an IVF-SQ8 collection at a given nprobe.
fn bench_ivf_sq8_search(
    c: &mut Criterion,
    label: &str,
    n: usize,
    dim: usize,
    nlist: usize,
    nprobe: usize,
) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut mgr = CollectionManager::new();
    let col_name = format!("ivf_sq8_{label}_np{nprobe}");
    mgr.create_ivf_sq8_collection(&col_name, dim, Metric::L2, nlist, nprobe)
        .unwrap();
    let col = mgr.get_mut(&col_name).unwrap();
    for i in 0..n as u64 {
        col.insert(i, random_vec(&mut rng, dim), None).unwrap();
    }
    let query = random_vec(&mut rng, dim);

    let bench_label = format!("ivf_sq8_search/np{nprobe}");
    c.bench_with_input(BenchmarkId::new(bench_label, label), &query, |b, q| {
        let col = mgr.get(&col_name).unwrap();
        b.iter(|| {
            let results = col.search(black_box(q), 10, None, false).unwrap();
            black_box(results);
        });
    });
}

/// Measures post-training query latency at a given nprobe.
/// Training is amortised before the bench loop begins.
fn bench_ivf_search(
    c: &mut Criterion,
    label: &str,
    n: usize,
    dim: usize,
    nlist: usize,
    nprobe: usize,
) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut mgr = CollectionManager::new();
    let col_name = format!("ivf_{label}_np{nprobe}");
    mgr.create_ivf_collection(&col_name, dim, Metric::L2, nlist, nprobe)
        .unwrap();
    let col = mgr.get_mut(&col_name).unwrap();
    for i in 0..n as u64 {
        col.insert(i, random_vec(&mut rng, dim), None).unwrap();
    }
    let query = random_vec(&mut rng, dim);

    // Warm up: first search triggers nothing (training already happened at insert time).
    let bench_label = format!("ivf_search/np{nprobe}");
    c.bench_with_input(BenchmarkId::new(bench_label, label), &query, |b, q| {
        let col = mgr.get(&col_name).unwrap();
        b.iter(|| {
            let results = col.search(black_box(q), 10, None, false).unwrap();
            black_box(results);
        });
    });
}

/// Measures cumulative HNSW build time: inserts n vectors one by one, timed as a whole.
fn bench_hnsw_build(c: &mut Criterion, label: &str, n: usize, dim: usize, m: usize, ef_construction: usize) {
    let mut rng = StdRng::seed_from_u64(42);
    let vecs: Vec<Vec<f32>> = (0..n).map(|_| random_vec(&mut rng, dim)).collect();

    c.bench_with_input(
        BenchmarkId::new("hnsw_build", label),
        &vecs,
        |b, vecs| {
            b.iter_batched(
                || (),
                |()| {
                    let mut idx =
                        HnswIndex::new(dim, Metric::L2, m, ef_construction, 50).unwrap();
                    for (i, v) in vecs.iter().enumerate() {
                        idx.insert(i as u64, v.clone()).unwrap();
                    }
                    black_box(idx);
                },
                BatchSize::SmallInput,
            );
        },
    );
}

/// Measures post-build HNSW query latency at a given ef_search.
fn bench_hnsw_search(
    c: &mut Criterion,
    label: &str,
    n: usize,
    dim: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut mgr = CollectionManager::new();
    let col_name = format!("hnsw_{label}_ef{ef_search}");
    mgr.create_hnsw_collection(&col_name, dim, Metric::L2, m, ef_construction, ef_search)
        .unwrap();
    let col = mgr.get_mut(&col_name).unwrap();
    for i in 0..n as u64 {
        col.insert(i, random_vec(&mut rng, dim), None).unwrap();
    }
    let query = random_vec(&mut rng, dim);

    let bench_label = format!("hnsw_search/ef{ef_search}");
    c.bench_with_input(BenchmarkId::new(bench_label, label), &query, |b, q| {
        let col = mgr.get(&col_name).unwrap();
        b.iter(|| {
            let results = col.search(black_box(q), 10, None, false).unwrap();
            black_box(results);
        });
    });
}

fn benchmarks(c: &mut Criterion) {
    // FlatIndex baselines (scalar / SIMD / SIMD+rayon)
    for &(label, n, dim) in &[
        ("1k_d128", 1_000usize, 128usize),
        ("10k_d384", 10_000, 384),
        ("100k_d384", 100_000, 384),
    ] {
        bench_scalar(c, label, n, dim, 10);
        bench_simd(c, label, n, dim, 10);
        bench_simd_rayon(c, label, n, dim, 10);
    }

    // IVF training cost
    for &(label, n, dim, nlist) in &[
        ("10k_d384_nl256",  10_000usize, 384usize, 256usize),
        ("100k_d384_nl1024", 100_000, 384, 1024),
    ] {
        bench_ivf_training(c, label, n, dim, nlist);
    }

    // IVF search at varying nprobe
    for &(label, n, dim, nlist, nprobe) in &[
        ("10k_d384_nl256",    10_000usize, 384usize, 256usize,   8usize),
        ("10k_d384_nl256",    10_000, 384, 256,  32),
        ("100k_d384_nl1024", 100_000, 384, 1024,  16),
        ("100k_d384_nl1024", 100_000, 384, 1024,  64),
    ] {
        bench_ivf_search(c, label, n, dim, nlist, nprobe);
    }

    // IVF-SQ8 search at varying nprobe (same matrix for direct comparison)
    for &(label, n, dim, nlist, nprobe) in &[
        ("10k_d384_nl256",    10_000usize, 384usize, 256usize,   8usize),
        ("10k_d384_nl256",    10_000, 384, 256,  32),
        ("100k_d384_nl1024", 100_000, 384, 1024,  16),
        ("100k_d384_nl1024", 100_000, 384, 1024,  64),
    ] {
        bench_ivf_sq8_search(c, label, n, dim, nlist, nprobe);
    }

    // HNSW build cost (one-time construction)
    for &(label, n, dim) in &[
        ("10k_d384", 10_000usize, 384usize),
        ("100k_d384", 100_000, 384),
    ] {
        bench_hnsw_build(c, label, n, dim, 16, 200);
    }

    // HNSW search at varying ef_search
    for &(label, n, dim, ef_search) in &[
        ("10k_d384",  10_000usize, 384usize,  50usize),
        ("10k_d384",  10_000, 384, 100),
        ("100k_d384", 100_000, 384,  50),
        ("100k_d384", 100_000, 384, 100),
    ] {
        bench_hnsw_search(c, label, n, dim, 16, 200, ef_search);
    }
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
