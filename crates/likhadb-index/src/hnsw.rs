use std::cell::{Cell, RefCell};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};

// Per-thread visited tracking: each thread maintains its own epoch counter and
// stamp array so concurrent `search` calls on different threads never interfere.
// Epoch wraps at u64::MAX (18 quintillion searches per thread before collision).
thread_local! {
    static SEARCH_EPOCH: Cell<u64> = Cell::new(0);
    static VISIT_STAMPS: RefCell<Vec<u64>> = RefCell::new(Vec::new());
}

use ordered_float::OrderedFloat;

use likhadb_core::{FilterFn, LikhaDbError, Metric, Result, ScoredResult, VecId, Vector};

use crate::flat::simd_distance;
use crate::traits::VectorIndex;

const MAX_LEVEL: usize = 16;

// ---------------------------------------------------------------------------
// Internal node type
// ---------------------------------------------------------------------------

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone)]
struct HnswNode {
    id: VecId,
    /// `layers[l]` holds the indices into `HnswIndex::nodes` of neighbours at level `l`.
    layers: Vec<Vec<usize>>,
}

// ---------------------------------------------------------------------------
// HnswIndex
// ---------------------------------------------------------------------------

/// Approximate nearest-neighbour index using Hierarchical Navigable Small World
/// (HNSW) graphs (Malkov & Yashunin, 2018).
///
/// Vectors are organised into a multi-layer proximity graph.  Layer 0 contains
/// every node; each higher layer contains an exponentially smaller random subset.
/// Search starts at the highest layer's single entry point and descends greedily,
/// using a beam-search at layer 0 to collect the final candidates.
///
/// **Deletion** uses tombstoning: deleted nodes remain in the graph as traversal
/// stepping-stones but are excluded from search results.  `len()` reports the
/// number of live (non-deleted) vectors.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone)]
pub struct HnswIndex {
    dim: usize,
    metric: Metric,
    m: usize,               // max edges per node per layer (l > 0)
    m0: usize,              // max edges at layer 0 = 2 * m
    ef_construction: usize, // beam width during graph construction
    ef_search: usize,       // beam width during query
    ml: f64,                // level multiplier = 1 / ln(m)

    nodes: Vec<HnswNode>,
    data: Vec<f32>, // flat slab; nodes[i] → data[i*dim..(i+1)*dim]
    id_to_node: HashMap<VecId, usize>,
    deleted: HashSet<VecId>,

    entry_point: Option<usize>, // node index of the current highest-level entry point
    max_level: usize,
}

impl HnswIndex {
    /// Create a new HNSW index.
    ///
    /// - `m`: maximum number of bidirectional links per node per layer (`m0 = 2 * m`
    ///   for layer 0). Must be ≥ 2. Typical: 16.
    /// - `ef_construction`: candidate list size during graph construction. Must be
    ///   ≥ `m`. Larger values improve graph quality at the cost of build time. Typical: 200.
    /// - `ef_search`: candidate list size at query time. Must be ≥ 1; will be
    ///   automatically raised to `k` when `k > ef_search`. Typical: 50.
    pub fn new(
        dim: usize,
        metric: Metric,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Result<Self> {
        if dim == 0 {
            return Err(LikhaDbError::InvalidArgument("dim must be > 0".into()));
        }
        if m < 2 {
            return Err(LikhaDbError::InvalidArgument("m must be >= 2".into()));
        }
        if ef_construction < m {
            return Err(LikhaDbError::InvalidArgument(format!(
                "ef_construction must be >= m ({m}), got {ef_construction}"
            )));
        }
        if ef_search < 1 {
            return Err(LikhaDbError::InvalidArgument(
                "ef_search must be >= 1".into(),
            ));
        }
        Ok(Self {
            dim,
            metric,
            m,
            m0: 2 * m,
            ef_construction,
            ef_search,
            ml: 1.0 / (m as f64).ln(),
            nodes: Vec::new(),
            data: Vec::new(),
            id_to_node: HashMap::new(),
            deleted: HashSet::new(),
            entry_point: None,
            max_level: 0,
        })
    }

    /// Sample a random level for a new node using the geometric distribution
    /// `floor(-ln(uniform) * ml)`, capped at `MAX_LEVEL`.
    fn random_level(&self) -> usize {
        let r: f64 = rand::random();
        // Avoid -inf when r == 0 (practically impossible but safe)
        if r == 0.0 {
            return 0;
        }
        ((-r.ln()) * self.ml).floor() as usize
    }

    /// Return the vector data slice for a node index.
    #[inline]
    fn vec_of(&self, node_idx: usize) -> &[f32] {
        &self.data[node_idx * self.dim..(node_idx + 1) * self.dim]
    }

    /// Distance from `query` to node `node_idx`.
    #[inline]
    fn dist(&self, query: &[f32], node_idx: usize) -> f32 {
        simd_distance(self.metric, query, self.vec_of(node_idx))
    }

    /// Maximum allowed neighbours at a given layer.
    #[inline]
    fn m_at(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }

    // -----------------------------------------------------------------------
    // Core HNSW primitives
    // -----------------------------------------------------------------------

    /// Beam search within a single graph layer.
    ///
    /// Returns a **max-heap** of `(distance, node_idx)` with at most `ef` entries
    /// (the `ef` closest nodes found).  Deleted nodes are NOT filtered here — the
    /// caller decides what to do with them (they serve as valid traversal stepping-
    /// stones during construction and search).
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[usize],
        ef: usize,
        level: usize,
    ) -> BinaryHeap<(OrderedFloat<f32>, usize)> {
        // Bump the per-thread epoch so every node looks unvisited at the start.
        // No allocation: we reuse the thread-local stamps Vec across calls.
        let epoch = SEARCH_EPOCH.with(|e| {
            let next = e.get().wrapping_add(1);
            e.set(next);
            next
        });

        VISIT_STAMPS.with(|cell| {
            let mut stamps = cell.borrow_mut();
            if stamps.len() < self.nodes.len() {
                stamps.resize(self.nodes.len(), 0);
            }

            // W: result set (max-heap, worst/farthest at top, size ≤ ef)
            let mut w: BinaryHeap<(OrderedFloat<f32>, usize)> = BinaryHeap::new();
            // C: candidates to expand (min-heap, nearest at top)
            let mut c: BinaryHeap<Reverse<(OrderedFloat<f32>, usize)>> = BinaryHeap::new();

            for &ep in entry_points {
                let d = OrderedFloat(self.dist(query, ep));
                w.push((d, ep));
                c.push(Reverse((d, ep)));
                stamps[ep] = epoch;
            }

            while let Some(Reverse((c_dist, c_idx))) = c.pop() {
                let f_dist = w.peek().map(|&(d, _)| d).unwrap_or(OrderedFloat(f32::MAX));
                if c_dist > f_dist {
                    break; // every remaining candidate is farther than the worst result
                }

                // Borrow neighbours as a slice to avoid a Vec allocation per hop.
                let neighbours: &[usize] = self.nodes[c_idx]
                    .layers
                    .get(level)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);

                for &nbr in neighbours {
                    if stamps[nbr] == epoch {
                        continue;
                    }
                    stamps[nbr] = epoch;
                    let d = OrderedFloat(self.dist(query, nbr));
                    let f_dist = w.peek().map(|&(d, _)| d).unwrap_or(OrderedFloat(f32::MAX));
                    if d < f_dist || w.len() < ef {
                        c.push(Reverse((d, nbr)));
                        w.push((d, nbr));
                        if w.len() > ef {
                            w.pop(); // evict worst
                        }
                    }
                }
            }

            w
        })
    }

    /// Select the `m_max` closest candidates from a max-heap, returning their node indices.
    fn select_neighbors(
        mut candidates: BinaryHeap<(OrderedFloat<f32>, usize)>,
        m_max: usize,
    ) -> Vec<usize> {
        // Max-heap: pop removes the farthest. Trim until only m_max closest remain.
        while candidates.len() > m_max {
            candidates.pop();
        }
        candidates.into_iter().map(|(_, idx)| idx).collect()
    }

    /// Prune node `node_idx`'s neighbour list at `level` to at most `m_max` entries,
    /// keeping the `m_max` closest.
    fn prune_connections(&mut self, node_idx: usize, level: usize, m_max: usize) {
        let nbrs = self.nodes[node_idx].layers[level].clone();
        if nbrs.len() <= m_max {
            return;
        }
        let query = self.vec_of(node_idx).to_vec();
        let heap: BinaryHeap<(OrderedFloat<f32>, usize)> = nbrs
            .into_iter()
            .map(|n| {
                (
                    OrderedFloat(simd_distance(self.metric, &query, self.vec_of(n))),
                    n,
                )
            })
            .collect();
        self.nodes[node_idx].layers[level] = Self::select_neighbors(heap, m_max);
    }

    /// Find the closest non-deleted node in a candidate heap.  Returns `None` if
    /// the heap is empty or all candidates are deleted.
    fn closest_live(&self, heap: &BinaryHeap<(OrderedFloat<f32>, usize)>) -> Option<usize> {
        heap.iter()
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal))
            .map(|&(_, idx)| idx)
    }
}

// ---------------------------------------------------------------------------
// VectorIndex implementation
// ---------------------------------------------------------------------------

impl VectorIndex for HnswIndex {
    fn get(&self, id: VecId) -> Option<Vector> {
        if self.deleted.contains(&id) {
            return None;
        }
        let &node_idx = self.id_to_node.get(&id)?;
        Some(self.data[node_idx * self.dim..(node_idx + 1) * self.dim].to_vec())
    }

    fn insert(&mut self, id: VecId, vec: Vector) -> Result<()> {
        if vec.len() != self.dim {
            return Err(LikhaDbError::DimMismatch {
                expected: self.dim,
                got: vec.len(),
            });
        }

        // Overwrite: tombstone the old entry (keep graph edges; old node_idx becomes a
        // ghost).  The new node gets a fresh node_idx and is the canonical entry for `id`.
        if self.id_to_node.contains_key(&id) {
            self.deleted.insert(id);
        }

        let node_idx = self.nodes.len();
        let level = self.random_level().min(MAX_LEVEL);

        self.nodes.push(HnswNode {
            id,
            layers: vec![vec![]; level + 1],
        });
        self.data.extend_from_slice(&vec);
        self.id_to_node.insert(id, node_idx);
        self.deleted.remove(&id); // ensure live after overwrite

        // First node: just set the entry point.
        if self.entry_point.is_none() {
            self.entry_point = Some(node_idx);
            self.max_level = level;
            return Ok(());
        }

        let mut ep = self.entry_point.unwrap();

        // Phase 1: greedy descent from max_level down to level+1 (ef=1 per layer).
        for lc in (level + 1..=self.max_level).rev() {
            let w = self.search_layer(&vec, &[ep], 1, lc);
            if let Some(closest) = self.closest_live(&w) {
                ep = closest;
            }
        }

        // Phase 2: beam search from min(level, max_level) down to 0 and connect.
        for lc in (0..=level.min(self.max_level)).rev() {
            let candidates = self.search_layer(&vec, &[ep], self.ef_construction, lc);

            // Update ep for the next (lower) layer.
            if let Some(closest) = self.closest_live(&candidates) {
                ep = closest;
            }

            let m_max = self.m_at(lc);
            let neighbours = Self::select_neighbors(candidates, m_max);

            self.nodes[node_idx].layers[lc] = neighbours.clone();

            // Add reverse edges and prune if over-full.
            for &nbr in &neighbours {
                if nbr >= self.nodes.len() {
                    continue;
                }
                if lc < self.nodes[nbr].layers.len() {
                    self.nodes[nbr].layers[lc].push(node_idx);
                    if self.nodes[nbr].layers[lc].len() > m_max {
                        self.prune_connections(nbr, lc, m_max);
                    }
                }
            }
        }

        // Promote entry point if this node has a higher level.
        if level > self.max_level {
            self.entry_point = Some(node_idx);
            self.max_level = level;
        }

        Ok(())
    }

    fn delete(&mut self, id: VecId) -> bool {
        if !self.id_to_node.contains_key(&id) || self.deleted.contains(&id) {
            return false;
        }
        self.deleted.insert(id);

        // If the entry point was just tombstoned, find a replacement.
        let ep_deleted = self
            .entry_point
            .map(|ep| self.nodes[ep].id == id)
            .unwrap_or(false);
        if ep_deleted {
            // Walk nodes in reverse-insertion order: higher-level nodes were inserted later
            // (statistically) so we prefer them as entry points.
            let new_ep = self.nodes.iter().enumerate().rev().find_map(|(i, n)| {
                if !self.deleted.contains(&n.id) {
                    Some(i)
                } else {
                    None
                }
            });
            self.entry_point = new_ep;
            self.max_level = new_ep
                .map(|ep| self.nodes[ep].layers.len().saturating_sub(1))
                .unwrap_or(0);
        }

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
        if k == 0 || self.entry_point.is_none() {
            return Ok(vec![]);
        }

        let mut ep = self.entry_point.unwrap();

        // Greedy descent from max_level to layer 1.
        for lc in (1..=self.max_level).rev() {
            let w = self.search_layer(query, &[ep], 1, lc);
            if let Some(closest) = self.closest_live(&w) {
                ep = closest;
            }
        }

        // Beam search at layer 0 with ef = max(ef_search, k).
        let ef = self.ef_search.max(k);
        let candidates = self.search_layer(query, &[ep], ef, 0);

        // Collect results, skipping deleted and applying user filter.
        let mut results: Vec<ScoredResult> = candidates
            .into_iter()
            .filter(|&(_, idx)| {
                let node_id = self.nodes[idx].id;
                !self.deleted.contains(&node_id) && filter.is_none_or(|f| f(node_id))
            })
            .map(|(d, idx)| ScoredResult {
                id: self.nodes[idx].id,
                score: d.into_inner(),
                payload: None,
            })
            .collect();

        results.sort_by(|a, b| {
            OrderedFloat(a.score)
                .partial_cmp(&OrderedFloat(b.score))
                .unwrap_or(Ordering::Equal)
        });
        results.truncate(k);
        Ok(results)
    }

    fn len(&self) -> usize {
        self.id_to_node.len().saturating_sub(self.deleted.len())
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn index_type(&self) -> &'static str {
        "HnswIndex"
    }

    #[cfg(feature = "serde")]
    fn to_snapshot(&self) -> crate::snapshot::IndexSnapshot {
        crate::snapshot::IndexSnapshot::Hnsw(self.clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use likhadb_core::Metric;

    fn make_hnsw(m: usize, ef_construction: usize, ef_search: usize) -> HnswIndex {
        HnswIndex::new(4, Metric::L2, m, ef_construction, ef_search).unwrap()
    }

    fn insert_n(idx: &mut HnswIndex, n: usize) {
        for i in 0..n as u64 {
            idx.insert(i, vec![i as f32, 0.0, 0.0, 0.0]).unwrap();
        }
    }

    // --- Construction ---

    #[test]
    fn construction_invalid_params() {
        assert!(HnswIndex::new(0, Metric::L2, 16, 200, 50).is_err(), "dim=0");
        assert!(HnswIndex::new(4, Metric::L2, 1, 200, 50).is_err(), "m<2");
        assert!(
            HnswIndex::new(4, Metric::L2, 16, 10, 50).is_err(),
            "ef_construction<m"
        );
        assert!(
            HnswIndex::new(4, Metric::L2, 16, 200, 0).is_err(),
            "ef_search=0"
        );
    }

    #[test]
    fn construction_valid() {
        let idx = make_hnsw(16, 200, 50);
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.dim(), 4);
        assert_eq!(idx.index_type(), "HnswIndex");
        assert!(idx.is_empty());
    }

    // --- Basic insert and search ---

    #[test]
    fn insert_and_search_basic() {
        let mut idx = make_hnsw(4, 8, 10);
        insert_n(&mut idx, 20);
        assert_eq!(idx.len(), 20);

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 5, None).unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res[0].id, 0, "nearest to origin should be id=0");
        for w in res.windows(2) {
            assert!(w[0].score <= w[1].score, "results not sorted");
        }
    }

    #[test]
    fn insert_overwrites() {
        let mut idx = make_hnsw(4, 8, 10);
        idx.insert(1, vec![100.0, 0.0, 0.0, 0.0]).unwrap();
        idx.insert(1, vec![0.1, 0.0, 0.0, 0.0]).unwrap(); // overwrite
        assert_eq!(idx.len(), 1, "overwrite must not increase len");

        let res = idx.search(&[0.0_f32; 4], 1, None).unwrap();
        assert_eq!(res.len(), 1);
        assert!((res[0].score - 0.1).abs() < 0.5, "new value should win");
    }

    // --- Delete ---

    #[test]
    fn delete_removes_from_results() {
        let mut idx = make_hnsw(4, 16, 20);
        insert_n(&mut idx, 20);

        let query = [0.0_f32; 4];
        let before = idx.search(&query, 1, None).unwrap();
        let nearest_id = before[0].id;

        assert!(idx.delete(nearest_id));
        assert_eq!(idx.len(), 19);

        let after = idx.search(&query, 5, None).unwrap();
        assert!(
            after.iter().all(|r| r.id != nearest_id),
            "deleted id must not appear"
        );
    }

    #[test]
    fn delete_nonexistent() {
        let mut idx = make_hnsw(4, 8, 10);
        assert!(!idx.delete(99));
    }

    #[test]
    fn delete_already_deleted() {
        let mut idx = make_hnsw(4, 8, 10);
        insert_n(&mut idx, 5);
        assert!(idx.delete(2));
        assert!(!idx.delete(2), "second delete must return false");
    }

    #[test]
    fn delete_entry_point_still_searchable() {
        let mut idx = make_hnsw(4, 8, 10);
        insert_n(&mut idx, 10);

        // Delete the entry point.
        let ep_id = idx.entry_point.map(|ep| idx.nodes[ep].id).unwrap();
        assert!(idx.delete(ep_id));

        // Index must still be searchable.
        let res = idx.search(&[0.0_f32; 4], 5, None).unwrap();
        assert!(!res.is_empty());
        assert!(res.iter().all(|r| r.id != ep_id));
    }

    // --- Edge cases ---

    #[test]
    fn search_k_zero() {
        let idx = make_hnsw(4, 8, 10);
        assert!(idx.search(&[0.0_f32; 4], 0, None).unwrap().is_empty());
    }

    #[test]
    fn search_empty_index() {
        let idx = make_hnsw(4, 8, 10);
        assert!(idx.search(&[0.0_f32; 4], 5, None).unwrap().is_empty());
    }

    #[test]
    fn search_k_larger_than_len() {
        let mut idx = make_hnsw(4, 8, 10);
        insert_n(&mut idx, 5);
        let res = idx.search(&[0.0_f32; 4], 100, None).unwrap();
        assert_eq!(res.len(), 5);
    }

    #[test]
    fn dim_mismatch_insert() {
        let mut idx = make_hnsw(4, 8, 10);
        assert!(matches!(
            idx.insert(1, vec![1.0, 2.0]),
            Err(LikhaDbError::DimMismatch { .. })
        ));
    }

    #[test]
    fn dim_mismatch_search() {
        let idx = make_hnsw(4, 8, 10);
        assert!(matches!(
            idx.search(&[1.0_f32, 2.0], 1, None),
            Err(LikhaDbError::DimMismatch { .. })
        ));
    }

    // --- Filter ---

    #[test]
    fn filter_applied() {
        let mut idx = make_hnsw(4, 16, 20);
        insert_n(&mut idx, 30);
        let res = idx
            .search(&[0.0_f32; 4], 10, Some(&|id: VecId| id.is_multiple_of(2)))
            .unwrap();
        assert!(!res.is_empty());
        assert!(res.iter().all(|r| r.id % 2 == 0), "filter not applied");
    }

    // --- Recall ---

    #[test]
    fn high_recall() {
        // With generous ef_search, HNSW should match FlatIndex on ≥90% of top-10.
        use crate::flat::FlatIndex;

        let n = 500usize;
        let dim = 4;
        let k = 10;

        let mut hnsw = HnswIndex::new(dim, Metric::L2, 16, 100, 200).unwrap();
        let mut flat = FlatIndex::new(dim, Metric::L2);

        for i in 0..n as u64 {
            let v = vec![i as f32, (i % 17) as f32, (i % 7) as f32, (i % 3) as f32];
            hnsw.insert(i, v.clone()).unwrap();
            flat.insert(i, v).unwrap();
        }

        let query = [50.0_f32, 3.0, 2.0, 1.0];
        let hnsw_res = hnsw.search(&query, k, None).unwrap();
        let flat_res = flat.search(&query, k, None).unwrap();

        let hnsw_ids: HashSet<u64> = hnsw_res.iter().map(|r| r.id).collect();
        let flat_ids: HashSet<u64> = flat_res.iter().map(|r| r.id).collect();
        let overlap = hnsw_ids.intersection(&flat_ids).count();
        assert!(
            overlap * 10 >= k * 9,
            "HNSW recall too low: {overlap}/{k} (expected ≥90%)"
        );
    }

    // --- All three metrics ---

    #[test]
    fn all_three_metrics() {
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let mut idx = HnswIndex::new(4, metric, 4, 8, 10).unwrap();
            for i in 0..20u64 {
                idx.insert(i, vec![i as f32 + 1.0, 1.0, 1.0, 1.0]).unwrap();
            }
            let res = idx.search(&[1.0_f32, 1.0, 1.0, 1.0], 5, None).unwrap();
            assert!(!res.is_empty(), "metric={metric:?}");
            for w in res.windows(2) {
                assert!(
                    w[0].score <= w[1].score,
                    "unsorted results for metric={metric:?}"
                );
            }
        }
    }

    // --- Len invariant ---

    #[test]
    fn len_invariant() {
        let mut idx = make_hnsw(4, 8, 10);
        assert_eq!(idx.len(), 0);
        insert_n(&mut idx, 5);
        assert_eq!(idx.len(), 5);
        idx.insert(10, vec![10.0, 0.0, 0.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 6);
        idx.insert(0, vec![0.5, 0.0, 0.0, 0.0]).unwrap(); // overwrite id=0
        assert_eq!(idx.len(), 6, "overwrite must not change len");
        idx.delete(1);
        assert_eq!(idx.len(), 5);
        idx.delete(999); // nonexistent
        assert_eq!(idx.len(), 5);
    }

    #[test]
    fn get_returns_vector_for_existing_id() {
        let mut idx = make_hnsw(4, 16, 10);
        idx.insert(3, vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        assert_eq!(idx.get(3).unwrap(), vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn get_returns_none_for_missing_id() {
        let idx = make_hnsw(4, 16, 10);
        assert!(idx.get(99).is_none());
    }

    #[test]
    fn get_returns_none_after_delete() {
        let mut idx = make_hnsw(4, 16, 10);
        idx.insert(5, vec![0.0, 1.0, 0.0, 0.0]).unwrap();
        idx.delete(5);
        assert!(idx.get(5).is_none());
    }
}
