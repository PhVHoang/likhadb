use std::collections::BinaryHeap;

use ordered_float::OrderedFloat;
use rayon::prelude::*;

use likhadb_core::{
    cosine_distance, dot_product, l2_distance, FilterFn, LikhaDbError, Metric, Result,
    ScoredResult, VecId, Vector,
};
use simsimd::SpatialSimilarity;

use crate::traits::VectorIndex;

/// Hardware-accelerated distance via `simsimd`, with scalar fallback.
///
/// Returns a value where **lower = more similar** across all metrics:
/// - `Cosine`  → cosine distance (1 − similarity)
/// - `Dot`     → negated dot product
/// - `L2`      → Euclidean distance (sqrt of squared Euclidean from simsimd)
#[inline]
pub(crate) fn simd_distance(metric: Metric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        Metric::Dot => {
            let v = <f32 as SpatialSimilarity>::dot(a, b)
                .unwrap_or_else(|| dot_product(a, b) as f64);
            -(v as f32)
        }
        Metric::Cosine => {
            <f32 as SpatialSimilarity>::cosine(a, b)
                .unwrap_or_else(|| cosine_distance(a, b) as f64) as f32
        }
        Metric::L2 => {
            let sq = <f32 as SpatialSimilarity>::sqeuclidean(a, b)
                .unwrap_or_else(|| l2_distance(a, b).powi(2) as f64);
            (sq as f32).sqrt()
        }
    }
}

/// Brute-force exact index with a cache-friendly flat buffer layout and SIMD distance kernels.
///
/// All vector data lives in a single contiguous `Vec<f32>` slab:
///   `data[i * dim .. (i + 1) * dim]`  ←→  vector for `ids[i]`
///
/// This eliminates the N separate heap allocations of the old `Vec<(VecId, Vec<f32>)>`
/// layout, allowing the hardware prefetcher to stream through the entire dataset
/// sequentially during search.
///
/// Distance computation uses `simsimd` for hardware-accelerated kernels (NEON on
/// aarch64/M2, AVX-512 on x86). A scalar fallback is used when `simsimd` returns
/// `None` (empty slices or unsupported targets).
pub struct FlatIndex {
    dim: usize,
    metric: Metric,
    /// Parallel to `data` blocks: `ids[i]` owns `data[i*dim..(i+1)*dim]`.
    ids: Vec<VecId>,
    /// Flat slab: all vectors concatenated. Length is always `ids.len() * dim`.
    data: Vec<f32>,
}

impl FlatIndex {
    pub fn new(dim: usize, metric: Metric) -> Self {
        Self {
            dim,
            metric,
            ids: Vec::new(),
            data: Vec::new(),
        }
    }

    /// Return the slot index for `id`, if present.
    fn position(&self, id: VecId) -> Option<usize> {
        self.ids.iter().position(|&eid| eid == id)
    }

}

impl VectorIndex for FlatIndex {
    fn insert(&mut self, id: VecId, vec: Vector) -> Result<()> {
        if vec.len() != self.dim {
            return Err(LikhaDbError::DimMismatch {
                expected: self.dim,
                got: vec.len(),
            });
        }
        if let Some(pos) = self.position(id) {
            // Overwrite in-place — no allocation needed.
            self.data[pos * self.dim..(pos + 1) * self.dim].copy_from_slice(&vec);
        } else {
            self.ids.push(id);
            self.data.extend_from_slice(&vec);
        }
        Ok(())
    }

    fn delete(&mut self, id: VecId) -> bool {
        let Some(pos) = self.position(id) else {
            return false;
        };
        let last = self.ids.len() - 1;

        // Swap-remove the id.
        self.ids.swap_remove(pos);

        // Move the last vector's data into the vacated slot, then truncate.
        if pos != last {
            let (lo, hi) = self.data.split_at_mut(last * self.dim);
            lo[pos * self.dim..(pos + 1) * self.dim].copy_from_slice(&hi[..self.dim]);
        }
        self.data.truncate(last * self.dim);

        true
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<FilterFn<'_>>,
    ) -> Result<Vec<ScoredResult>> {
        if query.len() != self.dim {
            return Err(LikhaDbError::DimMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }
        if k == 0 || self.ids.is_empty() {
            return Ok(vec![]);
        }

        // Each rayon thread maintains a local top-k max-heap, then the per-thread
        // heaps are merged in the reduce step. This keeps allocations at O(T·k)
        // rather than O(N) and avoids any shared mutable state.
        let heap = self
            .ids
            .par_iter()
            .zip(self.data.par_chunks_exact(self.dim))
            .fold(
                || BinaryHeap::with_capacity(k + 1),
                |mut local, (id, chunk)| {
                    if filter.is_none_or(|f| f(*id)) {
                        let dist = OrderedFloat(simd_distance(self.metric, query, chunk));
                        if local.len() < k {
                            local.push((dist, *id));
                        } else if let Some(&(worst, _)) = local.peek() {
                            if dist < worst {
                                local.pop();
                                local.push((dist, *id));
                            }
                        }
                    }
                    local
                },
            )
            .reduce(
                || BinaryHeap::with_capacity(k + 1),
                |mut a, b| {
                    for item in b {
                        if a.len() < k {
                            a.push(item);
                        } else if let Some(&(worst, _)) = a.peek() {
                            if item.0 < worst {
                                a.pop();
                                a.push(item);
                            }
                        }
                    }
                    a
                },
            );

        let mut results: Vec<ScoredResult> = heap
            .into_iter()
            .map(|(d, id)| ScoredResult {
                id,
                score: d.into_inner(),
                payload: None,
            })
            .collect();

        // Sort ascending by score (best first).
        results.sort_by(|a, b| {
            OrderedFloat(a.score)
                .partial_cmp(&OrderedFloat(b.score))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(results)
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn index_type(&self) -> &'static str {
        "FlatIndex"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use likhadb_core::Metric;

    fn make_index() -> FlatIndex {
        FlatIndex::new(4, Metric::L2)
    }

    fn insert_n(index: &mut FlatIndex, n: usize) {
        for i in 0..n as u64 {
            let v = vec![i as f32, 0.0, 0.0, 0.0];
            index.insert(i, v).unwrap();
        }
    }

    #[test]
    fn search_returns_correct_ids() {
        let mut idx = make_index();
        insert_n(&mut idx, 10);

        let query = [0.1_f32, 0.0, 0.0, 0.0];
        let results = idx.search(&query, 3, None).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 0);
        for w in results.windows(2) {
            assert!(w[0].score <= w[1].score);
        }
    }

    #[test]
    fn delete_removes_vector_from_search() {
        let mut idx = make_index();
        insert_n(&mut idx, 10);

        assert!(idx.delete(0));

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = idx.search(&query, 3, None).unwrap();
        assert!(results.iter().all(|r| r.id != 0));
    }

    #[test]
    fn delete_nonexistent_returns_false() {
        let mut idx = make_index();
        assert!(!idx.delete(99));
    }

    #[test]
    fn delete_swap_preserves_remaining_vectors() {
        // Delete the first element (not the last) to exercise the swap-copy path.
        let mut idx = make_index();
        insert_n(&mut idx, 5); // ids 0..4, data=[0,0,0,0, 1,0,0,0, ...]

        assert!(idx.delete(0));
        assert_eq!(idx.len(), 4);

        // All ids 1..4 must still be findable with correct data.
        let query = [3.0_f32, 0.0, 0.0, 0.0];
        let results = idx.search(&query, 1, None).unwrap();
        assert_eq!(results[0].id, 3);
        assert!(results[0].score < 1e-4);
    }

    #[test]
    fn delete_last_element() {
        let mut idx = make_index();
        insert_n(&mut idx, 3);
        assert!(idx.delete(2)); // delete the last slot directly
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.data.len(), 2 * idx.dim);
    }

    #[test]
    fn insert_dim_mismatch_returns_error() {
        let mut idx = make_index();
        let result = idx.insert(1, vec![1.0, 2.0]);
        assert!(matches!(result, Err(LikhaDbError::DimMismatch { .. })));
    }

    #[test]
    fn filter_excludes_ids() {
        let mut idx = make_index();
        insert_n(&mut idx, 10);

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = idx
            .search(&query, 5, Some(&|id: VecId| id % 2 == 0))
            .unwrap();
        assert!(results.iter().all(|r| r.id % 2 == 0));
    }

    #[test]
    fn search_empty_index_returns_empty() {
        let idx = make_index();
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        assert!(idx.search(&query, 3, None).unwrap().is_empty());
    }

    #[test]
    fn search_k_larger_than_len_returns_all() {
        let mut idx = make_index();
        insert_n(&mut idx, 3);
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        assert_eq!(idx.search(&query, 10, None).unwrap().len(), 3);
    }

    #[test]
    fn insert_overwrites_existing_id() {
        let mut idx = make_index();
        idx.insert(1, vec![10.0, 0.0, 0.0, 0.0]).unwrap();
        idx.insert(1, vec![0.1, 0.0, 0.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.data.len(), idx.dim); // still one slot

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = idx.search(&query, 1, None).unwrap();
        assert!((results[0].score - 0.1).abs() < 1e-4);
    }

    #[test]
    fn data_invariant_len_equals_ids_times_dim() {
        let mut idx = make_index();
        insert_n(&mut idx, 7);
        assert_eq!(idx.data.len(), idx.ids.len() * idx.dim);
        idx.delete(3);
        assert_eq!(idx.data.len(), idx.ids.len() * idx.dim);
        idx.delete(0);
        assert_eq!(idx.data.len(), idx.ids.len() * idx.dim);
    }
}
