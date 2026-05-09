pub type VecId = u64;
pub type Vector = Vec<f32>;

/// A filter predicate over VecId. Evaluated against MetaStore at query time.
pub type FilterFn<'a> = &'a (dyn Fn(VecId) -> bool + Send + Sync);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScoredResult {
    pub id: VecId,
    pub score: f32, // lower = better for L2/cosine distance; higher = better for dot
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum LikhaDbError {
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimMismatch { expected: usize, got: usize },
    #[error("collection not found: {0}")]
    CollectionNotFound(String),
    #[error("vector not found: {0}")]
    VectorNotFound(VecId),
    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("fts error: {0}")]
    Fts(String),
}

pub type Result<T> = std::result::Result<T, LikhaDbError>;
