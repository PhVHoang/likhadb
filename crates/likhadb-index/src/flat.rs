use std::collections::BinaryHeap;

use ordered_float::OrderedFloat;

use likhadb_core::{distance, FilterFn, LikhaDbError, Metric, Result, ScoredResult, VecId, Vector};

use crate::traits::VectorIndex;

pub struct FlatIndex {
    dim: usize,
    metric: Metric,
    vectors: Vec<(VecId, Vector)>, // ordered by insertion; never sorted
}

impl FlatIndex {
    pub fn new(dim: usize, metric: Metric) -> Self {
        Self {
            dim,
            metric,
            vectors: Vec::new(),
        }
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
        // Overwrite if id already exists
        if let Some(entry) = self.vectors.iter_mut().find(|(eid, _)| *eid == id) {
            entry.1 = vec;
        } else {
            self.vectors.push((id, vec));
        }
        Ok(())
    }

    fn delete(&mut self, id: VecId) -> bool {
        if let Some(pos) = self.vectors.iter().position(|(eid, _)| *eid == id) {
            self.vectors.swap_remove(pos);
            true
        } else {
            false
        }
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
        if k == 0 || self.vectors.is_empty() {
            return Ok(vec![]);
        }

        // Max-heap keyed by distance so we can pop the worst (largest distance) candidate.
        // Capacity bounded to k+1 to keep it tight.
        let mut heap: BinaryHeap<(OrderedFloat<f32>, VecId)> = BinaryHeap::with_capacity(k + 1);

        for (id, vec) in &self.vectors {
            if let Some(f) = filter {
                if !f(*id) {
                    continue;
                }
            }
            let dist = distance(self.metric, query, vec);
            let dist_of = OrderedFloat(dist);

            if heap.len() < k {
                heap.push((dist_of, *id));
            } else if let Some(&(worst, _)) = heap.peek() {
                if dist_of < worst {
                    heap.pop();
                    heap.push((dist_of, *id));
                }
            }
        }

        let mut results: Vec<ScoredResult> = heap
            .into_iter()
            .map(|(d, id)| ScoredResult {
                id,
                score: d.into_inner(),
            })
            .collect();

        // Sort ascending by score (best first)
        results.sort_by(|a, b| {
            OrderedFloat(a.score)
                .partial_cmp(&OrderedFloat(b.score))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(results)
    }

    fn len(&self) -> usize {
        self.vectors.len()
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

        // Query closest to id=0 => [0,0,0,0]
        let query = [0.1_f32, 0.0, 0.0, 0.0];
        let results = idx.search(&query, 3, None).unwrap();
        assert_eq!(results.len(), 3);
        // id 0 should be closest
        assert_eq!(results[0].id, 0);
        // results must be sorted ascending
        for w in results.windows(2) {
            assert!(w[0].score <= w[1].score);
        }
    }

    #[test]
    fn delete_removes_vector_from_search() {
        let mut idx = make_index();
        insert_n(&mut idx, 10);

        let deleted = idx.delete(0);
        assert!(deleted);

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
    fn insert_dim_mismatch_returns_error() {
        let mut idx = make_index();
        let result = idx.insert(1, vec![1.0, 2.0]); // wrong dim
        assert!(matches!(result, Err(LikhaDbError::DimMismatch { .. })));
    }

    #[test]
    fn filter_excludes_ids() {
        let mut idx = make_index();
        insert_n(&mut idx, 10);

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        // Only allow even ids
        let results = idx
            .search(&query, 5, Some(&|id: VecId| id % 2 == 0))
            .unwrap();
        assert!(results.iter().all(|r| r.id % 2 == 0));
    }

    #[test]
    fn search_empty_index_returns_empty() {
        let idx = make_index();
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = idx.search(&query, 3, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn search_k_larger_than_len_returns_all() {
        let mut idx = make_index();
        insert_n(&mut idx, 3);
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = idx.search(&query, 10, None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn insert_overwrites_existing_id() {
        let mut idx = make_index();
        idx.insert(1, vec![10.0, 0.0, 0.0, 0.0]).unwrap();
        idx.insert(1, vec![0.1, 0.0, 0.0, 0.0]).unwrap(); // overwrite
        assert_eq!(idx.len(), 1);

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = idx.search(&query, 1, None).unwrap();
        // The overwritten vector [0.1, 0, 0, 0] should be closer than [10, 0, 0, 0]
        assert!((results[0].score - 0.1).abs() < 1e-4);
    }
}
