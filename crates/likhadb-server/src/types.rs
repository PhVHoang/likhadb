use likhadb_core::{Metric, ScoredResult, VecId, Vector};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;

// ── Collection DDL ────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub dim: usize,
    pub metric: String,
    #[serde(default)]
    pub index: IndexConfig,
    #[serde(default)]
    pub enable_fts: bool,
    /// Optional binding to an externally-written Iceberg source table. When set,
    /// the collection will (in a later phase) reflect source-table snapshot deltas.
    #[serde(default)]
    pub source_binding: Option<likhadb_core::SourceBinding>,
}

#[derive(Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IndexConfig {
    #[default]
    Flat,
    Ivf {
        nlist: usize,
        nprobe: usize,
    },
    IvfSq8 {
        nlist: usize,
        nprobe: usize,
    },
    Hnsw {
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    },
}

#[derive(Serialize)]
pub struct CollectionInfo {
    pub name: String,
    pub dim: usize,
    pub metric: String,
    pub count: usize,
    pub index_type: String,
}

// ── Vector DML ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct InsertRequest {
    pub id: VecId,
    pub vector: Vector,
    pub payload: Option<Value>,
}

#[derive(Serialize)]
pub struct VectorResponse {
    pub id: VecId,
    pub vector: Vector,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

// ── Query ─────────────────────────────────────────────────────────────────────

/// Upper bound on `k` for any search. Caps result-set size so an open query
/// endpoint can't be used to scan an entire collection in one request.
pub const MAX_K: usize = 1024;

pub fn validate_k(k: usize) -> Result<usize, ApiError> {
    match k {
        0 => Err(ApiError::bad_request("k must be >= 1")),
        k if k > MAX_K => Err(ApiError::bad_request(format!("k={k} exceeds max {MAX_K}"))),
        k => Ok(k),
    }
}

#[derive(Deserialize)]
pub struct QueryRequest {
    pub vector: Vector,
    pub k: usize,
    pub filter: Option<Value>,
    #[serde(default)]
    pub include_payload: bool,
    /// Team identifiers for Tier Q ACL enforcement.
    #[cfg(feature = "enriched-search")]
    #[serde(default)]
    pub allowed_teams: Vec<String>,
    /// Query text for Tier Q bi-encoder / cross-encoder reranking.
    #[cfg(feature = "enriched-search")]
    #[serde(default)]
    pub query_text: Option<String>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub results: Vec<ScoredResult>,
}

// ── Hybrid Query ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct HybridQueryRequest {
    pub vector: Vector,
    pub text: String,
    pub k: usize,
    #[serde(default = "default_rrf_k")]
    pub rrf_k: u32,
    pub filter: Option<Value>,
    #[serde(default)]
    pub include_payload: bool,
    /// Team identifiers for Tier Q ACL enforcement.
    #[cfg(feature = "enriched-search")]
    #[serde(default)]
    pub allowed_teams: Vec<String>,
}

fn default_rrf_k() -> u32 {
    60
}

#[derive(Serialize)]
pub struct HybridQueryResponse {
    pub results: Vec<ScoredResult>,
}

// ── Tier Q ranked response ────────────────────────────────────────────────────

#[cfg(feature = "enriched-search")]
#[derive(Serialize)]
pub struct RankedQueryResponse {
    pub results: Vec<likhadb_query::pipeline::PipelineResult>,
}

// ── Lakehouse I/O ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ImportParquetRequest {
    pub path: String,
    pub id_col: String,
    pub vector_col: String,
    #[serde(default)]
    pub payload_cols: Vec<String>,
}

#[derive(Serialize)]
pub struct ImportParquetResponse {
    pub imported: usize,
}

#[derive(Deserialize)]
pub struct ExportParquetRequest {
    pub path: String,
}

// ── Metric helpers ────────────────────────────────────────────────────────────

pub fn parse_metric(s: &str) -> Result<Metric, ApiError> {
    match s.to_lowercase().as_str() {
        "l2" => Ok(Metric::L2),
        "cosine" => Ok(Metric::Cosine),
        "dot" => Ok(Metric::Dot),
        _ => Err(ApiError::bad_request(format!(
            "unknown metric '{s}': expected l2, cosine, or dot"
        ))),
    }
}

pub fn metric_str(m: Metric) -> &'static str {
    match m {
        Metric::L2 => "l2",
        Metric::Cosine => "cosine",
        Metric::Dot => "dot",
    }
}
