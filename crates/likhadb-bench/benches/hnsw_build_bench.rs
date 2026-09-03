use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use likhadb_core::Metric;
use likhadb_index::{HnswIndex, VectorIndex};
use rand::{rngs::StdRng, Rng, SeedableRng};

fn random_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen::<f32>()).collect()
}

/// Compare cumulative serial and parallel build time for greedy and
/// diversity-based neighbour selection.
fn bench_hnsw_build(c: &mut Criterion, use_heuristic: bool, parallel: bool) {
    const N: usize = 10_000;
    const DIM: usize = 384;
    const M: usize = 16;
    const EF_CONSTRUCTION: usize = 200;

    let mut rng = StdRng::seed_from_u64(42);
    let vecs: Vec<(u64, Vec<f32>)> = (0..N)
        .map(|id| (id as u64, random_vec(&mut rng, DIM)))
        .collect();
    let selector = if use_heuristic { "heuristic" } else { "greedy" };
    let benchmark = if parallel {
        "hnsw_build_parallel"
    } else {
        "hnsw_build_serial"
    };

    c.bench_with_input(BenchmarkId::new(benchmark, selector), &vecs, |b, vecs| {
        b.iter_batched(
            || (),
            |()| {
                let mut idx = HnswIndex::new(DIM, Metric::L2, M, EF_CONSTRUCTION, 50)
                    .unwrap()
                    .with_heuristic(use_heuristic);
                if parallel {
                    idx.build_from_vecs(vecs).unwrap();
                } else {
                    for (id, vector) in vecs {
                        idx.insert(*id, vector.clone()).unwrap();
                    }
                }
                black_box(idx);
            },
            BatchSize::LargeInput,
        );
    });
}

fn hnsw_build_benchmarks(c: &mut Criterion) {
    bench_hnsw_build(c, false, false);
    bench_hnsw_build(c, false, true);
    bench_hnsw_build(c, true, false);
    bench_hnsw_build(c, true, true);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(15));
    targets = hnsw_build_benchmarks
}
criterion_main!(benches);
