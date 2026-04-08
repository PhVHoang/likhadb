## Tier 1 implementation

Benchmark results (M2, scalar, no SIMD):

┌───────────────────────┬────────┬────────────┐
│       Benchmark       │ Result │   Target   │
├───────────────────────┼────────┼────────────┤
│ flat_search_1k_d128   │ 65 µs  │ —          │
├───────────────────────┼────────┼────────────┤
│ flat_search_10k_d384  │ 2.4 ms │ < 50 ms ✓  │
├───────────────────────┼────────┼────────────┤
│ flat_search_100k_d384 │ 24 ms  │ < 500 ms ✓ │
└───────────────────────┴────────┴────────────┘

Workspace layout created as specified:
- likhadb-core — VecId, Vector, FilterFn, ScoredResult, LikhaDbError, Metric, scalar distance kernels
- likhadb-index — VectorIndex trait (the Tier 2/3 extension seam), FlatIndex with BinaryHeap<OrderedFloat> search
- likhadb-store — MetaStore (JSON predicate filters: eq/ne/exists), Collection, CollectionManager
- likhadb-bench — Criterion benchmarks for 1k/10k/100k vectors