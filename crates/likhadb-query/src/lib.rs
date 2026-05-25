//! DataFusion post-ANN query pipeline for LikhaDB.
//!
//! This crate implements the Tier Q (DataFusion Query Layer) described in
//! `rfc/rfc_datafusion_integration.md`. It takes the small candidate set
//! returned by the ANN index and runs it through a sequence of DataFusion
//! stages: metadata enrichment, access control enforcement, multi-signal
//! score fusion, and (in future steps) model-based reranking.
//!
//! ## Implementation status
//!
//! | Step | Stage | Status |
//! |------|-------|--------|
//! | 1 | Config + error type | ✅ Done |
//! | 2 | Candidate MemTable | Planned |
//! | 3 | Sync distance UDFs | Planned |
//! | 4 | SessionContext + Parquet tables | Planned |
//! | 5 | Enrichment SQL (Stage 3) | Planned |
//! | 6 | Score fusion SQL (Stage 4a) | Planned |
//! | 7 | Pipeline orchestration | Planned |
//! | 8 | Server integration | Planned |

pub mod config;
mod error;

pub use error::QueryError;

/// Convenience alias — all fallible operations in this crate return this type.
pub type Result<T> = std::result::Result<T, QueryError>;
