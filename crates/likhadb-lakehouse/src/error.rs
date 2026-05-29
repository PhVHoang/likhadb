use thiserror::Error;

#[derive(Debug, Error)]
pub enum LakehouseError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("column not found: '{0}'")]
    ColumnNotFound(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("dimension mismatch: collection expects {expected}, Parquet vector has {got}")]
    DimMismatch { expected: usize, got: usize },

    #[error("type mismatch for column '{col}': expected {expected}, got {got}")]
    TypeMismatch {
        col: String,
        expected: String,
        got: String,
    },

    #[error("store error: {0}")]
    Store(#[from] likhadb_core::LikhaDbError),

    #[cfg(feature = "minio")]
    #[error("object store error: {0}")]
    ObjectStore(object_store::Error),
}
