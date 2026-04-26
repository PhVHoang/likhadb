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

    /// Index type name — used for observability/logging only.
    fn index_type(&self) -> &'static str;

    #[cfg(feature = "serde")]
    fn to_snapshot(&self) -> crate::snapshot::IndexSnapshot;
}
