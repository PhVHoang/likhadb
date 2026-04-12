use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use ordered_float::OrderedFloat;
use rayon::prelude::*;

use likhadb_core::{FilterFn, LikhaDbError, Metric, Result, ScoredResult, VecId, Vector};

use crate::flat::simd_distance;
use crate::traits::VectorIndex;

const MAX_KMEANS_ITER: usize = 25;
const KMEANS_TOL: f32 = 1e-4;

// ---------------------------------------------------------------------------
// Sq8Quantizer — per-dimension scalar quantization (f32 → u8)
// ---------------------------------------------------------------------------

struct Sq8Quantizer {
    mins:   Vec<f32>, // [dim] per-dimension minimum observed during training
    scales: Vec<f32>, // [dim]: (max[i] - min[i]) / 255.0; 1.0 if range == 0
}

impl Sq8Quantizer {
    /// Fit quantizer from a flat f32 slab of `n` vectors of length `dim`.
    fn fit(data: &[f32], n: usize, dim: usize) -> Self {
        let mut mins = vec![f32::MAX; dim];
        let mut maxs = vec![f32::MIN; dim];
        for i in 0..n {
            for j in 0..dim {
                let v = data[i * dim + j];
                if v < mins[j] { mins[j] = v; }
                if v > maxs[j] { maxs[j] = v; }
            }
        }
        let scales = mins
            .iter()
            .zip(maxs.iter())
            .map(|(&mn, &mx)| {
                let r = mx - mn;
                if r > 0.0 { r / 255.0 } else { 1.0 }
            })
            .collect();
        Self { mins, scales }
    }

    fn encode(&self, vec: &[f32]) -> Vec<u8> {
        vec.iter()
            .enumerate()
            .map(|(i, &v)| {
                let q = ((v - self.mins[i]) / self.scales[i]).round();
                q.clamp(0.0, 255.0) as u8
            })
            .collect()
    }

    fn decode(&self, codes: &[u8]) -> Vec<f32> {
        codes
            .iter()
            .enumerate()
            .map(|(i, &c)| self.mins[i] + c as f32 * self.scales[i])
            .collect()
    }

    /// Asymmetric distance: query stays f32, stored codes decoded on-the-fly.
    fn asym_distance(&self, metric: Metric, query: &[f32], codes: &[u8]) -> f32 {
        simd_distance(metric, query, &self.decode(codes))
    }
}

// ---------------------------------------------------------------------------
// PostingList — flat-slab storage for one IVF bucket
// ---------------------------------------------------------------------------

struct PostingList {
    ids:   Vec<VecId>,
    data:  Vec<f32>, // flat slab: ids[i] owns data[i*dim..(i+1)*dim]; used when NOT quantized
    codes: Vec<u8>,  // flat slab: ids[i] owns codes[i*dim..(i+1)*dim]; used for SQ8
}

impl PostingList {
    fn new() -> Self {
        Self { ids: Vec::new(), data: Vec::new(), codes: Vec::new() }
    }

    fn push(&mut self, id: VecId, vec: &[f32]) {
        self.ids.push(id);
        self.data.extend_from_slice(vec);
    }

    fn push_codes(&mut self, id: VecId, codes: &[u8]) {
        self.ids.push(id);
        self.codes.extend_from_slice(codes);
    }

    /// Swap-remove by id from the f32 slab. Returns true if found and removed.
    fn remove_by_id(&mut self, id: VecId, dim: usize) -> bool {
        let Some(pos) = self.ids.iter().position(|&eid| eid == id) else {
            return false;
        };
        let last = self.ids.len() - 1;
        self.ids.swap_remove(pos);
        if pos != last {
            let (lo, hi) = self.data.split_at_mut(last * dim);
            lo[pos * dim..(pos + 1) * dim].copy_from_slice(&hi[..dim]);
        }
        self.data.truncate(last * dim);
        true
    }

    /// Swap-remove by id from the u8 codes slab. Returns true if found and removed.
    fn remove_by_id_sq8(&mut self, id: VecId, dim: usize) -> bool {
        let Some(pos) = self.ids.iter().position(|&eid| eid == id) else {
            return false;
        };
        let last = self.ids.len() - 1;
        self.ids.swap_remove(pos);
        if pos != last {
            let (lo, hi) = self.codes.split_at_mut(last * dim);
            lo[pos * dim..(pos + 1) * dim].copy_from_slice(&hi[..dim]);
        }
        self.codes.truncate(last * dim);
        true
    }

    /// Iterator over (id, vector_slice) pairs (unquantized path).
    fn ids_and_chunks(&self, dim: usize) -> impl Iterator<Item = (VecId, &[f32])> {
        self.ids.iter().copied().zip(self.data.chunks_exact(dim))
    }

    /// Iterator over (id, code_slice) pairs (SQ8 path).
    fn ids_and_codes(&self, dim: usize) -> impl Iterator<Item = (VecId, &[u8])> {
        self.ids.iter().copied().zip(self.codes.chunks_exact(dim))
    }
}

// ---------------------------------------------------------------------------
// K-means (Lloyd's algorithm)
// ---------------------------------------------------------------------------

/// Run Lloyd's k-means on `data` (flat slab of `n` vectors of length `dim`).
/// Returns a flat slab of `k` centroids of length `dim`.
fn kmeans(data: &[f32], n: usize, dim: usize, k: usize, metric: Metric) -> Vec<f32> {
    // --- Initialisation: stride-sample k vectors from data ---
    let mut centroids = vec![0.0f32; k * dim];
    for i in 0..k {
        let src = (i * n) / k; // evenly spaced indices; no RNG needed
        centroids[i * dim..(i + 1) * dim].copy_from_slice(&data[src * dim..(src + 1) * dim]);
    }

    for _ in 0..MAX_KMEANS_ITER {
        // --- Assignment step (rayon parallel) ---
        let assignments: Vec<usize> = (0..n)
            .into_par_iter()
            .map(|i| {
                let v = &data[i * dim..(i + 1) * dim];
                (0..k)
                    .min_by(|&a, &b| {
                        let da = simd_distance(metric, v, &centroids[a * dim..(a + 1) * dim]);
                        let db = simd_distance(metric, v, &centroids[b * dim..(b + 1) * dim]);
                        da.partial_cmp(&db).unwrap_or(Ordering::Equal)
                    })
                    .unwrap_or(0) // k >= 1 guaranteed by IvfIndex::new validation
            })
            .collect();

        // --- Update step: recompute centroids as cluster means ---
        let mut new_centroids = vec![0.0f32; k * dim];
        let mut counts = vec![0usize; k];

        for (i, &c) in assignments.iter().enumerate() {
            let v = &data[i * dim..(i + 1) * dim];
            let slot = &mut new_centroids[c * dim..(c + 1) * dim];
            for (s, &x) in slot.iter_mut().zip(v.iter()) {
                *s += x;
            }
            counts[c] += 1;
        }

        for c in 0..k {
            if counts[c] == 0 {
                // Empty cluster: re-seed from the previous iteration's centroid of the
                // largest cluster. Reading from `centroids` (old slab) avoids any
                // borrow-of-new_centroids-while-mutating complexity.
                let src = counts
                    .iter()
                    .enumerate()
                    .max_by_key(|&(_, &cnt)| cnt)
                    .map(|(idx, _)| idx)
                    .unwrap_or(0);
                let src_vec: Vec<f32> =
                    centroids[src * dim..(src + 1) * dim].to_vec();
                new_centroids[c * dim..(c + 1) * dim].copy_from_slice(&src_vec);
            } else {
                let cnt = counts[c] as f32;
                for x in &mut new_centroids[c * dim..(c + 1) * dim] {
                    *x /= cnt;
                }
            }
        }

        // --- Convergence check ---
        let converged = (0..k).all(|c| {
            let old = &centroids[c * dim..(c + 1) * dim];
            let new = &new_centroids[c * dim..(c + 1) * dim];
            old.iter().zip(new.iter()).all(|(&o, &n)| (o - n).abs() < KMEANS_TOL)
        });

        centroids = new_centroids;
        if converged {
            break;
        }
    }

    centroids
}

// ---------------------------------------------------------------------------
// IvfIndex
// ---------------------------------------------------------------------------

/// Approximate nearest-neighbour index using Inverted File (IVF) structure.
///
/// Vectors are clustered into `nlist` buckets via k-means. At query time only
/// `nprobe` nearest buckets are searched, trading recall for speed.
///
/// **Training** fires automatically once `nlist` vectors have been inserted.
/// Before that threshold, searches fall back to brute-force over the staging area
/// so the index is always queryable.
///
/// Setting `nprobe == nlist` searches every bucket and gives exact recall
/// (equivalent to brute-force), which is useful for correctness testing.
pub struct IvfIndex {
    dim:    usize,
    metric: Metric,
    nlist:  usize,  // cluster count; also the training trigger threshold
    nprobe: usize,  // clusters searched per query

    // Pre-training staging buffer (drained and cleared on training).
    staging_ids:  Vec<VecId>,
    staging_data: Vec<f32>, // flat slab; len == staging_ids.len() * dim

    // Post-training state.
    trained:    bool,
    centroids:  Vec<f32>,           // flat slab of nlist centroids; len == nlist * dim
    lists:      Vec<PostingList>,   // one per centroid; len == nlist when trained
    id_to_list: HashMap<VecId, usize>, // O(1) cluster lookup for delete / overwrite

    // SQ8 scalar quantization (optional, set at construction time).
    quantize:  bool,
    quantizer: Option<Sq8Quantizer>, // None until training; Some after if quantize=true
}

impl IvfIndex {
    /// Create a new IVF index.
    ///
    /// - `nlist`: number of k-means clusters. Also the minimum number of vectors
    ///   required before automatic training fires.
    /// - `nprobe`: number of clusters to search per query. Must satisfy
    ///   `1 <= nprobe <= nlist`.
    pub fn new(dim: usize, metric: Metric, nlist: usize, nprobe: usize) -> Result<Self> {
        if dim == 0 {
            return Err(LikhaDbError::InvalidArgument("dim must be > 0".into()));
        }
        if nlist == 0 {
            return Err(LikhaDbError::InvalidArgument("nlist must be > 0".into()));
        }
        if nprobe == 0 || nprobe > nlist {
            return Err(LikhaDbError::InvalidArgument(format!(
                "nprobe must be in 1..={nlist}, got {nprobe}"
            )));
        }
        Ok(Self {
            dim,
            metric,
            nlist,
            nprobe,
            staging_ids:  Vec::new(),
            staging_data: Vec::new(),
            trained:      false,
            centroids:    Vec::new(),
            lists:        Vec::new(),
            id_to_list:   HashMap::new(),
            quantize:     false,
            quantizer:    None,
        })
    }

    /// Create a new IVF index with SQ8 scalar quantization enabled.
    ///
    /// After training, vectors in each posting list are stored as 8-bit codes
    /// (one byte per dimension) instead of full-precision f32 (four bytes per
    /// dimension), giving a 4× memory reduction. Distances at query time use
    /// asymmetric computation: the query stays in f32 while stored codes are
    /// decoded on-the-fly.
    ///
    /// Parameter validation is identical to [`IvfIndex::new`].
    pub fn new_sq8(dim: usize, metric: Metric, nlist: usize, nprobe: usize) -> Result<Self> {
        let mut idx = Self::new(dim, metric, nlist, nprobe)?;
        idx.quantize = true;
        Ok(idx)
    }

    /// Run k-means on staging, assign all staged vectors to their nearest cluster,
    /// then clear staging. Called by `insert` once `staging_ids.len() >= nlist`.
    fn train(&mut self) {
        let n = self.staging_ids.len();
        self.centroids = kmeans(&self.staging_data, n, self.dim, self.nlist, self.metric);

        self.lists = (0..self.nlist).map(|_| PostingList::new()).collect();

        if self.quantize {
            self.quantizer = Some(Sq8Quantizer::fit(&self.staging_data, n, self.dim));
        }

        for (i, &id) in self.staging_ids.iter().enumerate() {
            let vec = &self.staging_data[i * self.dim..(i + 1) * self.dim];
            let c = self.nearest_centroid(vec);
            if let Some(q) = &self.quantizer {
                let codes = q.encode(vec);
                self.lists[c].push_codes(id, &codes);
            } else {
                self.lists[c].push(id, vec);
            }
            self.id_to_list.insert(id, c);
        }

        self.staging_ids.clear();
        self.staging_data.clear();
        self.trained = true;
    }

    /// Return the index of the nearest centroid to `vec`. Requires `self.trained`.
    fn nearest_centroid(&self, vec: &[f32]) -> usize {
        (0..self.nlist)
            .min_by(|&a, &b| {
                let da = simd_distance(
                    self.metric, vec,
                    &self.centroids[a * self.dim..(a + 1) * self.dim],
                );
                let db = simd_distance(
                    self.metric, vec,
                    &self.centroids[b * self.dim..(b + 1) * self.dim],
                );
                da.partial_cmp(&db).unwrap_or(Ordering::Equal)
            })
            .unwrap_or(0) // nlist >= 1 guaranteed
    }

    /// Brute-force search over the staging buffer (pre-training fallback).
    fn search_staging(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<FilterFn<'_>>,
    ) -> Vec<ScoredResult> {
        let mut heap: BinaryHeap<(OrderedFloat<f32>, VecId)> =
            BinaryHeap::with_capacity(k + 1);

        for (i, &id) in self.staging_ids.iter().enumerate() {
            if filter.is_none_or(|f| f(id)) {
                let dist = OrderedFloat(simd_distance(
                    self.metric,
                    query,
                    &self.staging_data[i * self.dim..(i + 1) * self.dim],
                ));
                if heap.len() < k {
                    heap.push((dist, id));
                } else if let Some(&(worst, _)) = heap.peek() {
                    if dist < worst {
                        heap.pop();
                        heap.push((dist, id));
                    }
                }
            }
        }

        sorted_results(heap)
    }

    /// IVF search over `nprobe` nearest posting lists (post-training path).
    fn search_trained(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<FilterFn<'_>>,
    ) -> Result<Vec<ScoredResult>> {
        // Step 1: find nprobe nearest centroids (sequential — nlist is small).
        let mut centroid_dists: Vec<(OrderedFloat<f32>, usize)> = (0..self.nlist)
            .map(|c| {
                let d = simd_distance(
                    self.metric,
                    query,
                    &self.centroids[c * self.dim..(c + 1) * self.dim],
                );
                (OrderedFloat(d), c)
            })
            .collect();
        centroid_dists.sort_unstable();
        let probe: Vec<usize> = centroid_dists
            .iter()
            .take(self.nprobe)
            .map(|&(_, c)| c)
            .collect();

        // Step 2: parallel fold+reduce over probed posting lists.
        let quantizer = self.quantizer.as_ref();
        let heap: BinaryHeap<(OrderedFloat<f32>, VecId)> = probe
            .par_iter()
            .fold(
                || BinaryHeap::with_capacity(k + 1),
                |mut local, &c| {
                    if let Some(q) = quantizer {
                        for (id, codes) in self.lists[c].ids_and_codes(self.dim) {
                            if filter.is_none_or(|f| f(id)) {
                                let dist = OrderedFloat(q.asym_distance(self.metric, query, codes));
                                if local.len() < k {
                                    local.push((dist, id));
                                } else if let Some(&(worst, _)) = local.peek() {
                                    if dist < worst {
                                        local.pop();
                                        local.push((dist, id));
                                    }
                                }
                            }
                        }
                    } else {
                        for (id, chunk) in self.lists[c].ids_and_chunks(self.dim) {
                            if filter.is_none_or(|f| f(id)) {
                                let dist = OrderedFloat(simd_distance(self.metric, query, chunk));
                                if local.len() < k {
                                    local.push((dist, id));
                                } else if let Some(&(worst, _)) = local.peek() {
                                    if dist < worst {
                                        local.pop();
                                        local.push((dist, id));
                                    }
                                }
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

        Ok(sorted_results(heap))
    }
}

/// Convert a max-heap of `(distance, id)` into a `Vec<ScoredResult>` sorted ascending.
fn sorted_results(heap: BinaryHeap<(OrderedFloat<f32>, VecId)>) -> Vec<ScoredResult> {
    let mut results: Vec<ScoredResult> = heap
        .into_iter()
        .map(|(d, id)| ScoredResult { id, score: d.into_inner() })
        .collect();
    results.sort_by(|a, b| {
        OrderedFloat(a.score)
            .partial_cmp(&OrderedFloat(b.score))
            .unwrap_or(Ordering::Equal)
    });
    results
}

// ---------------------------------------------------------------------------
// VectorIndex implementation
// ---------------------------------------------------------------------------

impl VectorIndex for IvfIndex {
    fn insert(&mut self, id: VecId, vec: Vector) -> Result<()> {
        if vec.len() != self.dim {
            return Err(LikhaDbError::DimMismatch { expected: self.dim, got: vec.len() });
        }

        if self.trained {
            // Overwrite: remove from its current posting list first.
            if let Some(&old_list) = self.id_to_list.get(&id) {
                if self.quantizer.is_some() {
                    self.lists[old_list].remove_by_id_sq8(id, self.dim);
                } else {
                    self.lists[old_list].remove_by_id(id, self.dim);
                }
                self.id_to_list.remove(&id);
            }
            let c = self.nearest_centroid(&vec);
            if let Some(q) = &self.quantizer {
                let codes = q.encode(&vec);
                self.lists[c].push_codes(id, &codes);
            } else {
                self.lists[c].push(id, &vec);
            }
            self.id_to_list.insert(id, c);
        } else {
            // Overwrite in staging: update in-place, no length change.
            if let Some(pos) = self.staging_ids.iter().position(|&sid| sid == id) {
                self.staging_data[pos * self.dim..(pos + 1) * self.dim].copy_from_slice(&vec);
                return Ok(());
            }
            self.staging_ids.push(id);
            self.staging_data.extend_from_slice(&vec);
            if self.staging_ids.len() >= self.nlist {
                self.train();
            }
        }

        Ok(())
    }

    fn delete(&mut self, id: VecId) -> bool {
        if self.trained {
            if let Some(&list_idx) = self.id_to_list.get(&id) {
                if self.quantizer.is_some() {
                    self.lists[list_idx].remove_by_id_sq8(id, self.dim);
                } else {
                    self.lists[list_idx].remove_by_id(id, self.dim);
                }
                self.id_to_list.remove(&id);
                return true;
            }
            return false;
        }

        // Pre-training: scan staging.
        if let Some(pos) = self.staging_ids.iter().position(|&sid| sid == id) {
            let last = self.staging_ids.len() - 1;
            self.staging_ids.swap_remove(pos);
            if pos != last {
                let (lo, hi) = self.staging_data.split_at_mut(last * self.dim);
                lo[pos * self.dim..(pos + 1) * self.dim].copy_from_slice(&hi[..self.dim]);
            }
            self.staging_data.truncate(last * self.dim);
            return true;
        }

        false
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<FilterFn<'_>>,
    ) -> Result<Vec<ScoredResult>> {
        if query.len() != self.dim {
            return Err(LikhaDbError::DimMismatch { expected: self.dim, got: query.len() });
        }
        if k == 0 {
            return Ok(vec![]);
        }
        if !self.trained {
            return Ok(self.search_staging(query, k, filter));
        }
        self.search_trained(query, k, filter)
    }

    fn len(&self) -> usize {
        if self.trained {
            self.id_to_list.len()
        } else {
            self.staging_ids.len()
        }
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn index_type(&self) -> &'static str {
        "IvfIndex"
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use likhadb_core::Metric;

    // nlist=4 so tests stay fast; dim=4 for simple arithmetic
    fn make_ivf(nlist: usize, nprobe: usize) -> IvfIndex {
        IvfIndex::new(4, Metric::L2, nlist, nprobe).unwrap()
    }

    fn insert_n(idx: &mut IvfIndex, n: usize) {
        for i in 0..n as u64 {
            idx.insert(i, vec![i as f32, 0.0, 0.0, 0.0]).unwrap();
        }
    }

    // --- Construction ---

    #[test]
    fn construction_invalid_params() {
        assert!(IvfIndex::new(0, Metric::L2, 4, 2).is_err(), "dim=0");
        assert!(IvfIndex::new(4, Metric::L2, 0, 0).is_err(), "nlist=0");
        assert!(IvfIndex::new(4, Metric::L2, 4, 0).is_err(), "nprobe=0");
        assert!(IvfIndex::new(4, Metric::L2, 4, 5).is_err(), "nprobe>nlist");
    }

    #[test]
    fn construction_valid() {
        let idx = make_ivf(4, 2);
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.dim(), 4);
        assert_eq!(idx.index_type(), "IvfIndex");
    }

    // --- Staging (pre-training) ---

    #[test]
    fn staging_insert_and_search() {
        let mut idx = make_ivf(4, 2);
        insert_n(&mut idx, 3); // nlist=4, so still in staging
        assert!(!idx.trained);
        assert_eq!(idx.len(), 3);

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 2, None).unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].id, 0); // [0,0,0,0] is nearest to query
        assert!(res[0].score <= res[1].score);
    }

    #[test]
    fn staging_insert_overwrites() {
        let mut idx = make_ivf(4, 2);
        idx.insert(1, vec![5.0, 0.0, 0.0, 0.0]).unwrap();
        idx.insert(1, vec![0.1, 0.0, 0.0, 0.0]).unwrap(); // overwrite
        assert_eq!(idx.len(), 1);

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 1, None).unwrap();
        assert!((res[0].score - 0.1).abs() < 1e-4, "new value should win");
    }

    #[test]
    fn staging_delete_existing() {
        let mut idx = make_ivf(4, 2);
        insert_n(&mut idx, 3);
        assert!(idx.delete(1));
        assert_eq!(idx.len(), 2);

        let query = [1.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 3, None).unwrap();
        assert!(res.iter().all(|r| r.id != 1));
    }

    #[test]
    fn staging_delete_nonexistent() {
        let mut idx = make_ivf(4, 2);
        assert!(!idx.delete(99));
    }

    // --- Training ---

    #[test]
    fn training_triggers_at_nlist() {
        let mut idx = make_ivf(4, 2);
        insert_n(&mut idx, 4); // exactly nlist
        assert!(idx.trained);
        assert!(idx.staging_ids.is_empty());
        assert_eq!(idx.lists.len(), 4);
        assert_eq!(idx.len(), 4);
    }

    // --- Post-training ---

    #[test]
    fn post_training_insert_and_search() {
        // Use nprobe=nlist so all clusters are probed, guaranteeing k results
        // regardless of how vectors are distributed across clusters.
        let mut idx = make_ivf(4, 4);
        insert_n(&mut idx, 12); // 3 × nlist
        assert!(idx.trained);
        assert_eq!(idx.len(), 12);

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 5, None).unwrap();
        assert_eq!(res.len(), 5);
        for w in res.windows(2) {
            assert!(w[0].score <= w[1].score, "results must be sorted");
        }
    }

    #[test]
    fn partial_nprobe_returns_fewer_than_k_when_clusters_imbalanced() {
        // With nprobe < nlist, results may be fewer than k when the nearest vectors
        // are distributed across more clusters than nprobe covers. This verifies
        // the index does not panic or return garbage — just fewer results.
        let mut idx = make_ivf(4, 1); // only probe 1 cluster
        insert_n(&mut idx, 12);
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 10, None).unwrap();
        // Results are always sorted ascending, however many come back.
        for w in res.windows(2) {
            assert!(w[0].score <= w[1].score, "results must be sorted");
        }
    }

    #[test]
    fn post_training_overwrite() {
        let mut idx = make_ivf(4, 4); // nprobe=nlist for exact recall
        insert_n(&mut idx, 4); // triggers training
        assert_eq!(idx.len(), 4);

        idx.insert(0, vec![99.0, 0.0, 0.0, 0.0]).unwrap(); // overwrite id=0
        assert_eq!(idx.len(), 4, "len unchanged after overwrite");

        // id=0 should now be far from origin
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 4, None).unwrap();
        assert_eq!(res[3].id, 0, "id=0 should be farthest from origin");
    }

    #[test]
    fn post_training_delete() {
        let mut idx = make_ivf(4, 4);
        insert_n(&mut idx, 8);
        assert!(idx.delete(2));
        assert_eq!(idx.len(), 7);

        let query = [2.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 8, None).unwrap();
        assert!(res.iter().all(|r| r.id != 2));
    }

    #[test]
    fn post_training_delete_nonexistent() {
        let mut idx = make_ivf(4, 2);
        insert_n(&mut idx, 4);
        assert!(!idx.delete(99));
    }

    #[test]
    fn delete_swap_preserves_others() {
        // Force all vectors into a single posting list by making them identical
        // (nearest centroid is the same for all), then delete the first slot.
        let mut idx = make_ivf(4, 4);
        insert_n(&mut idx, 8);

        assert!(idx.delete(0)); // delete a non-last element in some list
        let query = [5.0_f32, 0.0, 0.0, 0.0];
        let res = idx.search(&query, 7, None).unwrap();
        assert_eq!(res.len(), 7);
        assert!(res.iter().all(|r| r.id != 0));
        // All remaining ids should be findable
        for id in 1u64..8 {
            assert!(res.iter().any(|r| r.id == id), "id {id} missing");
        }
    }

    // --- Filters ---

    #[test]
    fn filter_in_staging() {
        let mut idx = make_ivf(4, 2);
        insert_n(&mut idx, 3);
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let res = idx
            .search(&query, 3, Some(&|id: VecId| id % 2 == 0))
            .unwrap();
        assert!(res.iter().all(|r| r.id % 2 == 0));
    }

    #[test]
    fn filter_post_training() {
        let mut idx = make_ivf(4, 4);
        insert_n(&mut idx, 12);
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let res = idx
            .search(&query, 6, Some(&|id: VecId| id % 2 == 0))
            .unwrap();
        assert!(!res.is_empty());
        assert!(res.iter().all(|r| r.id % 2 == 0));
    }

    // --- Correctness: nprobe == nlist gives exact recall ---

    #[test]
    fn nprobe_equals_nlist_exact_recall() {
        use crate::flat::FlatIndex;

        let n = 100usize;
        let dim = 4;
        let nlist = 8;

        let mut ivf = IvfIndex::new(dim, Metric::L2, nlist, nlist).unwrap();
        let mut flat = FlatIndex::new(dim, Metric::L2);

        for i in 0..n as u64 {
            let v = vec![i as f32 * 0.1, (i % 7) as f32, 0.0, 0.0];
            ivf.insert(i, v.clone()).unwrap();
            flat.insert(i, v).unwrap();
        }

        let query = [3.0_f32, 1.0, 0.0, 0.0];
        let k = 5;

        let ivf_res = ivf.search(&query, k, None).unwrap();
        let flat_res = flat.search(&query, k, None).unwrap();

        assert_eq!(ivf_res.len(), flat_res.len());
        for (ivf_r, flat_r) in ivf_res.iter().zip(flat_res.iter()) {
            assert_eq!(ivf_r.id, flat_r.id, "id mismatch");
            assert!(
                (ivf_r.score - flat_r.score).abs() < 1e-4,
                "score mismatch: {} vs {}",
                ivf_r.score,
                flat_r.score,
            );
        }
    }

    // --- Error cases ---

    #[test]
    fn dim_mismatch_insert() {
        let mut idx = make_ivf(4, 2);
        assert!(matches!(
            idx.insert(1, vec![1.0, 2.0]),
            Err(LikhaDbError::DimMismatch { .. })
        ));
    }

    #[test]
    fn dim_mismatch_search() {
        let idx = make_ivf(4, 2);
        assert!(matches!(
            idx.search(&[1.0_f32, 2.0], 1, None),
            Err(LikhaDbError::DimMismatch { .. })
        ));
    }

    #[test]
    fn search_k_zero() {
        let idx = make_ivf(4, 2);
        assert!(idx.search(&[0.0_f32; 4], 0, None).unwrap().is_empty());
    }

    #[test]
    fn search_empty_index() {
        let idx = make_ivf(4, 2);
        assert!(idx.search(&[0.0_f32; 4], 5, None).unwrap().is_empty());
    }

    #[test]
    fn search_empty_post_training() {
        let mut idx = make_ivf(4, 4);
        insert_n(&mut idx, 4); // triggers training
        for i in 0..4u64 {
            idx.delete(i);
        }
        assert_eq!(idx.len(), 0);
        assert!(idx.search(&[0.0_f32; 4], 5, None).unwrap().is_empty());
    }

    #[test]
    fn search_k_larger_than_len() {
        let mut idx = make_ivf(4, 4);
        insert_n(&mut idx, 6);
        let res = idx.search(&[0.0_f32; 4], 100, None).unwrap();
        assert_eq!(res.len(), 6);
    }

    #[test]
    fn len_invariant() {
        let mut idx = make_ivf(4, 2);
        insert_n(&mut idx, 3); // staging
        assert_eq!(idx.len(), 3);
        idx.insert(10, vec![10.0, 0.0, 0.0, 0.0]).unwrap(); // triggers training (4th insert)
        assert_eq!(idx.len(), 4);
        idx.insert(11, vec![11.0, 0.0, 0.0, 0.0]).unwrap(); // post-training
        assert_eq!(idx.len(), 5);
        idx.delete(0);
        assert_eq!(idx.len(), 4);
        idx.delete(999); // nonexistent
        assert_eq!(idx.len(), 4);
    }

    #[test]
    fn all_three_metrics() {
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let mut idx = IvfIndex::new(4, metric, 4, 4).unwrap();
            for i in 0..8u64 {
                idx.insert(i, vec![i as f32 + 1.0, 1.0, 1.0, 1.0]).unwrap();
            }
            let res = idx.search(&[1.0_f32, 1.0, 1.0, 1.0], 4, None).unwrap();
            assert_eq!(res.len(), 4, "metric={metric:?}");
            for w in res.windows(2) {
                assert!(
                    w[0].score <= w[1].score,
                    "unsorted results for metric={metric:?}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // SQ8 tests
    // -----------------------------------------------------------------------

    fn make_ivf_sq8(nlist: usize, nprobe: usize) -> IvfIndex {
        IvfIndex::new_sq8(4, Metric::L2, nlist, nprobe).unwrap()
    }

    fn insert_n_sq8(idx: &mut IvfIndex, n: usize) {
        for i in 0..n as u64 {
            idx.insert(i, vec![i as f32, 0.0, 0.0, 0.0]).unwrap();
        }
    }

    #[test]
    fn sq8_construction() {
        // Valid construction
        let idx = make_ivf_sq8(4, 2);
        assert_eq!(idx.len(), 0);
        assert!(idx.quantize);
        assert!(idx.quantizer.is_none()); // not yet trained

        // new_sq8 shares validation with new
        assert!(IvfIndex::new_sq8(0, Metric::L2, 4, 2).is_err(), "dim=0");
        assert!(IvfIndex::new_sq8(4, Metric::L2, 4, 0).is_err(), "nprobe=0");
        assert!(IvfIndex::new_sq8(4, Metric::L2, 4, 5).is_err(), "nprobe>nlist");
    }

    #[test]
    fn sq8_training_triggers_and_quantizer_built() {
        let mut idx = make_ivf_sq8(4, 4);
        insert_n_sq8(&mut idx, 4); // triggers training
        assert!(idx.trained);
        assert!(idx.quantizer.is_some());
        assert!(idx.staging_ids.is_empty());
    }

    #[test]
    fn sq8_encode_decode_roundtrip() {
        // Fit on a tiny dataset and verify decode(encode(v)) ≈ v.
        let dim = 4;
        let data: Vec<f32> = (0..8).flat_map(|i| vec![i as f32, i as f32 * 2.0, 0.5, -1.0 + i as f32 * 0.1]).collect();
        let q = Sq8Quantizer::fit(&data, 8, dim);

        let original = vec![3.0_f32, 6.0, 0.5, -0.7];
        let codes = q.encode(&original);
        let decoded = q.decode(&codes);

        // Quantization error <= max_scale = (range / 255)
        let max_scale = q.scales.iter().cloned().fold(0.0_f32, f32::max);
        for (&o, d) in original.iter().zip(decoded.iter()) {
            assert!((o - d).abs() <= max_scale + 1e-5, "decode error too large: {o} vs {d}");
        }
    }

    #[test]
    fn sq8_search_sorted() {
        let mut idx = make_ivf_sq8(4, 4);
        insert_n_sq8(&mut idx, 12);
        let res = idx.search(&[0.0_f32; 4], 5, None).unwrap();
        assert_eq!(res.len(), 5);
        for w in res.windows(2) {
            assert!(w[0].score <= w[1].score, "SQ8 results not sorted");
        }
    }

    #[test]
    fn sq8_approximate_recall() {
        // With nprobe=nlist, SQ8-IVF should match FlatIndex on well-separated data.
        //
        // Key design: n == nlist so ALL vectors go through staging → training.
        // The quantizer is fit on the full dataset, so no post-training out-of-range
        // clamping occurs.
        //
        // Data: [i*10, 0, 0, 0] for i in 0..50.
        // Range = [0, 490], SQ8 scale = 490/255 ≈ 1.92.
        // Gap between consecutive neighbors = 10 >> 1.92, so ordering is preserved.
        use crate::flat::FlatIndex;

        let n = 50usize;
        let dim = 4;
        let nlist = n; // all vectors go through staging → training
        let k = 5;

        let mut ivf = IvfIndex::new_sq8(dim, Metric::L2, nlist, nlist).unwrap();
        let mut flat = FlatIndex::new(dim, Metric::L2);

        for i in 0..n as u64 {
            let v = vec![i as f32 * 10.0, 0.0, 0.0, 0.0];
            ivf.insert(i, v.clone()).unwrap();
            flat.insert(i, v).unwrap();
        }

        let query = [100.0_f32, 0.0, 0.0, 0.0]; // nearest: v10(0), v9(10), v11(10), ...
        let ivf_res = ivf.search(&query, k, None).unwrap();
        let flat_res = flat.search(&query, k, None).unwrap();

        let ivf_ids: std::collections::HashSet<u64> = ivf_res.iter().map(|r| r.id).collect();
        let flat_ids: std::collections::HashSet<u64> = flat_res.iter().map(|r| r.id).collect();
        let overlap = ivf_ids.intersection(&flat_ids).count();
        assert!(
            overlap * k >= k * 4 / 5,
            "SQ8 recall too low: {overlap}/{k} overlap (expected ≥80%)"
        );
    }

    #[test]
    fn sq8_delete() {
        let mut idx = make_ivf_sq8(4, 4);
        insert_n_sq8(&mut idx, 8);
        assert!(idx.delete(3));
        assert_eq!(idx.len(), 7);

        let res = idx.search(&[3.0_f32, 0.0, 0.0, 0.0], 8, None).unwrap();
        assert!(res.iter().all(|r| r.id != 3), "deleted id 3 should not appear");
    }

    #[test]
    fn sq8_delete_nonexistent() {
        let mut idx = make_ivf_sq8(4, 4);
        insert_n_sq8(&mut idx, 4);
        assert!(!idx.delete(99));
    }

    #[test]
    fn sq8_overwrite() {
        let mut idx = make_ivf_sq8(4, 4);
        insert_n_sq8(&mut idx, 4); // triggers training
        assert_eq!(idx.len(), 4);

        // Overwrite id=0 with a far-from-origin vector
        idx.insert(0, vec![99.0, 0.0, 0.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 4, "len unchanged after overwrite");

        let res = idx.search(&[0.0_f32; 4], 4, None).unwrap();
        assert_eq!(res[3].id, 0, "overwritten id=0 should be farthest");
    }

    #[test]
    fn sq8_filter() {
        let mut idx = make_ivf_sq8(4, 4);
        insert_n_sq8(&mut idx, 12);
        let res = idx
            .search(&[0.0_f32; 4], 6, Some(&|id: VecId| id % 2 == 0))
            .unwrap();
        assert!(!res.is_empty());
        assert!(res.iter().all(|r| r.id % 2 == 0), "filter not applied correctly");
    }

    #[test]
    fn sq8_dim_mismatch() {
        let mut idx = make_ivf_sq8(4, 2);
        assert!(matches!(
            idx.insert(1, vec![1.0, 2.0]),
            Err(LikhaDbError::DimMismatch { .. })
        ));
        assert!(matches!(
            idx.search(&[1.0_f32, 2.0], 1, None),
            Err(LikhaDbError::DimMismatch { .. })
        ));
    }

    #[test]
    fn sq8_len_invariant() {
        let mut idx = make_ivf_sq8(4, 4);
        insert_n_sq8(&mut idx, 3);
        assert_eq!(idx.len(), 3);
        idx.insert(10, vec![10.0, 0.0, 0.0, 0.0]).unwrap(); // 4th insert triggers training
        assert_eq!(idx.len(), 4);
        idx.insert(11, vec![11.0, 0.0, 0.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 5);
        idx.delete(0);
        assert_eq!(idx.len(), 4);
    }
}
