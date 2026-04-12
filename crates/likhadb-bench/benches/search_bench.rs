use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use likhadb_core::Metric;
use likhadb_store::CollectionManager;
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
                let results = col.search(black_box(q), k, None).unwrap();
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
            let results = col.search(black_box(q), k, None).unwrap();
            black_box(results);
        });
    });
}

fn benchmarks(c: &mut Criterion) {
    for &(label, n, dim) in &[
        ("1k_d128", 1_000usize, 128usize),
        ("10k_d384", 10_000, 384),
        ("100k_d384", 100_000, 384),
    ] {
        bench_scalar(c, label, n, dim, 10);
        bench_simd(c, label, n, dim, 10);
        bench_simd_rayon(c, label, n, dim, 10);
    }
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
