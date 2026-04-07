pub mod distance;
pub mod metric;
pub mod types;

pub use distance::{cosine_distance, distance, dot_product, l2_distance};
pub use metric::Metric;
pub use types::{FilterFn, LikhaDbError, Result, ScoredResult, VecId, Vector};
