/// Errors produced by the DataFusion post-ANN query pipeline.
///
/// Variants are added as pipeline stages are implemented:
/// - `Config` — config validation (Step 1, this file)
/// - Arrow / DataFusion / IO variants will be added in Steps 3-4
///   when those dependencies are introduced.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// A configuration invariant was violated.
    ///
    /// The service must refuse to start when this error is returned.
    #[error("configuration error: {0}")]
    Config(String),
}
