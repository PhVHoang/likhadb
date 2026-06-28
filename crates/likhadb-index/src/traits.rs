use likhadb_core::{FilterFn, Result, ScoredResult, VecId, Vector};

/// The sole coupling point between the store layer and any index implementation.
/// Tier 2 (IVF) and Tier 3 (HNSW) implement this trait; the store layer is unchanged.
pub trait VectorIndex: Send + Sync {
    /// Insert or overwrite a vector. Must validate dimension.
    fn insert(&mut self, id: VecId, vec: Vector) -> Result<()>;

    /// Remove a vector. Returns true if it existed.
    fn delete(&mut self, id: VecId) -> bool;

    /// Return the k nearest neighbours. Optional filter excludes candidates
    /// before they enter the result set (not before distance is computed).
    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<FilterFn<'_>>,
    ) -> Result<Vec<ScoredResult>>;

    /// Retrieve a stored vector by ID. Returns `None` if the ID does not exist
    /// (or has been deleted). For SQ8-quantized indices the returned vector is
    /// decoded from 8-bit codes and is therefore an approximation of the original.
    fn get(&self, id: VecId) -> Option<Vector>;

    fn len(&self) -> usize;
    fn dim(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return all live vector IDs stored in this index.
    fn list_ids(&self) -> Vec<VecId>;

    /// Index type name — used for observability/logging only.
    fn index_type(&self) -> &'static str;

    /// Fraction of physical nodes that are tombstoned (dead but still resident).
    /// `0.0` for indexes that delete in place (IVF/Flat). Graph indexes (HNSW)
    /// accumulate tombstones from deletes and overwrites; this drives the
    /// compaction trigger.
    fn tombstone_ratio(&self) -> f32 {
        0.0
    }

    /// Rebuild compactly from live entries, dropping tombstones and refreshing
    /// derived structures (e.g. retraining IVF centroids). Returns a fresh index,
    /// or `None` for in-place indexes that have nothing to compact.
    fn compact(&self) -> Option<Box<dyn VectorIndex>> {
        None
    }

    #[cfg(feature = "serde")]
    fn to_snapshot(&self) -> crate::snapshot::IndexSnapshot;
}
