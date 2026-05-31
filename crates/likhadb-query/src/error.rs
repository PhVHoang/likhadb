/// Errors produced by the DataFusion post-ANN query pipeline.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// A configuration invariant was violated.
    ///
    /// The service must refuse to start when this error is returned.
    #[error("configuration error: {0}")]
    Config(String),

    #[cfg(feature = "datafusion")]
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[cfg(feature = "datafusion")]
    #[error("iceberg error: {0}")]
    Iceberg(iceberg::Error),

    #[cfg(feature = "datafusion")]
    #[error("arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[cfg(feature = "datafusion")]
    #[error("schema error: {0}")]
    Schema(String),
}
