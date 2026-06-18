//! Wire the Tier Q DataFusion pipeline into server startup.
//!
//! Reads enrichment configuration from environment variables and constructs a
//! `Pipeline` whose enrichment session is backed by the same Iceberg REST
//! catalog used by the WAL flusher. Returns `None` on missing config or any
//! init failure — the server keeps running, just without Tier Q ranking.

use std::path::PathBuf;
use std::sync::Arc;

use likhadb_lakehouse::iceberg_io::{build_rest_catalog, IcebergConfig};
use likhadb_lakehouse::NamespaceIdent;
use likhadb_query::config::{
    AnnConfig, DataFusionRuntimeConfig, QueryConfig, RecencyConfig, ScoringConfig, ScoringWeights,
};
use likhadb_query::pipeline::Pipeline;
use likhadb_query::session::DataFusionSession;

const DEFAULT_VECTOR_WEIGHT: f32 = 0.7;
const DEFAULT_RECENCY_WEIGHT: f32 = 0.3;
const DEFAULT_GRACE_DAYS: i64 = 30;
const DEFAULT_DECAY_LAMBDA: f64 = 0.01;
const DEFAULT_ANN_TOP_N: usize = 500;
const DEFAULT_TOP_M: usize = 50;

/// Build a Tier Q `Pipeline` from environment variables and the existing
/// `IcebergConfig`. Returns `None` when the operator has not opted in
/// (`LIKHADB_ENRICH_NAMESPACE` unset) or when initialization fails for any
/// reason — failures are logged and the caller continues without Tier Q.
pub async fn try_build_pipeline_from_env(iceberg_config: &IcebergConfig) -> Option<Arc<Pipeline>> {
    let enrich_ns = match std::env::var("LIKHADB_ENRICH_NAMESPACE") {
        Ok(s) if !s.is_empty() => s,
        _ => return None,
    };

    let query_config = match build_query_config_from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "tier Q config invalid; pipeline disabled");
            return None;
        }
    };

    let catalog = match build_rest_catalog(iceberg_config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "tier Q catalog init failed");
            return None;
        }
    };

    let ns = NamespaceIdent::new(enrich_ns.clone());
    let session = match DataFusionSession::try_new_with_iceberg(
        &query_config,
        Arc::new(catalog),
        &[ns],
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e,
                namespace = %enrich_ns,
                "tier Q session init failed"
            );
            return None;
        }
    };

    let top_n = query_config.ann.top_n;
    let top_m = query_config.top_m;

    let pipeline = Pipeline::new(&query_config, Arc::new(session), None, None);
    tracing::info!(
        namespace = %enrich_ns,
        top_n,
        top_m,
        "tier Q pipeline initialised"
    );
    Some(Arc::new(pipeline))
}

fn build_query_config_from_env() -> Result<QueryConfig, String> {
    let vector = parse_env_f32("LIKHADB_SCORING_VECTOR_WEIGHT", DEFAULT_VECTOR_WEIGHT)?;
    let recency = parse_env_f32("LIKHADB_SCORING_RECENCY_WEIGHT", DEFAULT_RECENCY_WEIGHT)?;
    let weights = ScoringWeights::new(vector, recency).map_err(|e| e.to_string())?;

    let grace = parse_env_i64("LIKHADB_RECENCY_GRACE_DAYS", DEFAULT_GRACE_DAYS)?;
    let lambda = parse_env_f64("LIKHADB_RECENCY_DECAY_LAMBDA", DEFAULT_DECAY_LAMBDA)?;
    let recency_cfg = RecencyConfig::new(grace, lambda).map_err(|e| e.to_string())?;

    let top_n = parse_env_usize("LIKHADB_ANN_TOP_N", DEFAULT_ANN_TOP_N)?;
    let top_m = parse_env_usize("LIKHADB_TOP_M", DEFAULT_TOP_M)?;

    if top_n == 0 {
        return Err("LIKHADB_ANN_TOP_N must be ≥ 1".to_string());
    }
    if top_m == 0 || top_m > top_n {
        return Err(format!("LIKHADB_TOP_M must be in 1..={top_n}, got {top_m}"));
    }

    Ok(QueryConfig {
        // parquet_dir is unused on the Iceberg path. session::base_context reads
        // only the datafusion-runtime fields, and try_new_with_iceberg does not
        // touch parquet_dir.
        parquet_dir: PathBuf::new(),
        datafusion: DataFusionRuntimeConfig::default(),
        ann: AnnConfig { top_n },
        scoring: ScoringConfig {
            weights,
            recency: recency_cfg,
        },
        top_m,
        bi_encoder: None,
        cross_encoder: None,
    })
}

fn parse_env_f32(key: &str, default: f32) -> Result<f32, String> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(s) => s.parse().map_err(|e| format!("{key}={s:?}: {e}")),
    }
}

fn parse_env_f64(key: &str, default: f64) -> Result<f64, String> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(s) => s.parse().map_err(|e| format!("{key}={s:?}: {e}")),
    }
}

fn parse_env_i64(key: &str, default: i64) -> Result<i64, String> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(s) => s.parse().map_err(|e| format!("{key}={s:?}: {e}")),
    }
}

fn parse_env_usize(key: &str, default: usize) -> Result<usize, String> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(s) => s.parse().map_err(|e| format!("{key}={s:?}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // Env vars are process-wide; tests in this module mutate them and must run
    // serially. A local mutex avoids pulling in the `serial_test` crate.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const KEYS: &[&str] = &[
        "LIKHADB_ENRICH_NAMESPACE",
        "LIKHADB_SCORING_VECTOR_WEIGHT",
        "LIKHADB_SCORING_RECENCY_WEIGHT",
        "LIKHADB_RECENCY_GRACE_DAYS",
        "LIKHADB_RECENCY_DECAY_LAMBDA",
        "LIKHADB_ANN_TOP_N",
        "LIKHADB_TOP_M",
    ];

    /// Acquires the env lock and clears all LIKHADB_* keys this module reads,
    /// restoring any pre-existing values when dropped.
    struct EnvGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
        saved: HashMap<&'static str, String>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let mut saved = HashMap::new();
            for k in KEYS {
                if let Ok(v) = std::env::var(k) {
                    saved.insert(*k, v);
                }
                std::env::remove_var(k);
            }
            Self { _guard, saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for k in KEYS {
                std::env::remove_var(k);
            }
            for (k, v) in self.saved.drain() {
                std::env::set_var(k, v);
            }
        }
    }

    fn dummy_iceberg_config() -> IcebergConfig {
        IcebergConfig {
            catalog_uri: "http://127.0.0.1:1".to_string(),
            s3_endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            region: "us-east-1".to_string(),
            warehouse: String::new(),
            extra_properties: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn unset_namespace_returns_none() {
        let _g = EnvGuard::new();
        // No LIKHADB_ENRICH_NAMESPACE — helper must short-circuit before any
        // catalog work and not panic even with a bogus IcebergConfig.
        let result = try_build_pipeline_from_env(&dummy_iceberg_config()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn invalid_weight_sum_returns_none() {
        let _g = EnvGuard::new();
        std::env::set_var("LIKHADB_ENRICH_NAMESPACE", "enrich");
        std::env::set_var("LIKHADB_SCORING_VECTOR_WEIGHT", "0.7");
        std::env::set_var("LIKHADB_SCORING_RECENCY_WEIGHT", "0.5"); // sum 1.2
        let result = try_build_pipeline_from_env(&dummy_iceberg_config()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn negative_grace_days_returns_none() {
        let _g = EnvGuard::new();
        std::env::set_var("LIKHADB_ENRICH_NAMESPACE", "enrich");
        std::env::set_var("LIKHADB_RECENCY_GRACE_DAYS", "-1");
        let result = try_build_pipeline_from_env(&dummy_iceberg_config()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn zero_top_n_returns_none() {
        let _g = EnvGuard::new();
        std::env::set_var("LIKHADB_ENRICH_NAMESPACE", "enrich");
        std::env::set_var("LIKHADB_ANN_TOP_N", "0");
        let result = try_build_pipeline_from_env(&dummy_iceberg_config()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn top_m_greater_than_top_n_returns_none() {
        let _g = EnvGuard::new();
        std::env::set_var("LIKHADB_ENRICH_NAMESPACE", "enrich");
        std::env::set_var("LIKHADB_ANN_TOP_N", "10");
        std::env::set_var("LIKHADB_TOP_M", "20");
        let result = try_build_pipeline_from_env(&dummy_iceberg_config()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn unparseable_value_returns_none() {
        let _g = EnvGuard::new();
        std::env::set_var("LIKHADB_ENRICH_NAMESPACE", "enrich");
        std::env::set_var("LIKHADB_SCORING_VECTOR_WEIGHT", "not-a-float");
        let result = try_build_pipeline_from_env(&dummy_iceberg_config()).await;
        assert!(result.is_none());
    }

    #[test]
    fn defaults_build_a_valid_config() {
        let _g = EnvGuard::new();
        let cfg = build_query_config_from_env().expect("defaults must validate");
        assert_eq!(cfg.ann.top_n, DEFAULT_ANN_TOP_N);
        assert_eq!(cfg.top_m, DEFAULT_TOP_M);
        assert_eq!(cfg.scoring.weights.vector_score, DEFAULT_VECTOR_WEIGHT);
        assert_eq!(cfg.scoring.weights.recency_score, DEFAULT_RECENCY_WEIGHT);
    }
}
