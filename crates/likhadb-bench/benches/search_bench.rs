use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use likhadb_core::Metric;
use likhadb_store::CollectionManager;
use rand::{rngs::StdRng, Rng, SeedableRng};

fn random_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen::<f32>()).collect()
}

fn bench_flat_search(c: &mut Criterion, label: &str, n: usize, dim: usize, k: usize) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut mgr = CollectionManager::new();
    mgr.create_collection("bench", dim, Metric::L2).unwrap();
    let col = mgr.get_mut("bench").unwrap();

    for i in 0..n as u64 {
        col.insert(i, random_vec(&mut rng, dim), None).unwrap();
    }

    let query = random_vec(&mut rng, dim);

    c.bench_with_input(BenchmarkId::new("flat_search", label), &query, |b, q| {
        let col = mgr.get("bench").unwrap();
        b.iter(|| {
            let results = col.search(black_box(q), k, None).unwrap();
            black_box(results);
        });
    });
}

fn benchmarks(c: &mut Criterion) {
    bench_flat_search(c, "1k_d128", 1_000, 128, 10);
    bench_flat_search(c, "10k_d384", 10_000, 384, 10);
    bench_flat_search(c, "100k_d384", 100_000, 384, 10);
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
