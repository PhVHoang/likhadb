use std::path::PathBuf;

use crate::{QueryError, Result};

/// Tolerance for floating-point weight sum validation.
const WEIGHT_SUM_TOLERANCE: f32 = 1e-5;

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

/// Top-level configuration for the DataFusion post-ANN query pipeline.
///
/// Constructed once at service startup. Individual sub-configs validate their
/// own invariants at construction time; the service must refuse to start if
/// any sub-config constructor returns an error.
pub struct QueryConfig {
    /// Root directory containing the enrichment Parquet tables.
    ///
    /// Expected files: `embeddings.parquet`, `documents.parquet`, `authors.parquet`.
    /// File existence is verified by [`crate::session::DataFusionSession`] at startup,
    /// not here — this struct is pure configuration, not I/O.
    pub parquet_dir: PathBuf,

    /// DataFusion runtime tuning (batch size, parallelism, session strategy).
    pub datafusion: DataFusionRuntimeConfig,

    /// ANN retrieval parameters.
    pub ann: AnnConfig,

    /// Score fusion signal weights and recency decay parameters.
    pub scoring: ScoringConfig,

    /// Number of candidates emitted by Stage 4a (score fusion).
    /// Must be ≤ `ann.top_n`. Passed to Stage 4b (bi-encoder) when implemented.
    pub top_m: usize,
}

// ---------------------------------------------------------------------------
// DataFusion runtime
// ---------------------------------------------------------------------------

/// DataFusion `SessionContext` runtime parameters.
pub struct DataFusionRuntimeConfig {
    /// `RecordBatch` row count. Tune for candidate set cardinality.
    ///
    /// The candidate set is typically 100–500 rows. A `batch_size` equal to
    /// `ann.top_n` ensures each stage processes a single batch.
    pub batch_size: usize,

    /// Number of executor threads (DataFusion `target_partitions`).
    ///
    /// Defaults to the number of logical CPUs. For a candidate-set workload
    /// (hundreds of rows) a single partition is often sufficient; increase only
    /// if profiling shows executor parallelism is a bottleneck.
    pub target_partitions: usize,

    /// How per-request `SessionContext` isolation is achieved.
    pub session_strategy: SessionStrategy,
}

/// Per-request `SessionContext` isolation strategy.
///
/// The shared `SessionContext` holds the catalog and UDF registrations and must
/// not be mutated after startup. Per-request `candidates` MemTable registration
/// requires an isolated context — this enum controls how that isolation is achieved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStrategy {
    /// Clone the shared context for each request (Strategy B from the RFC).
    ///
    /// The clone is shallow: catalog references and UDF registrations are shared;
    /// only the table registry is isolated. This is the recommended starting point.
    /// Switch to `Pool` if clone latency is measurable under production load.
    Child,

    /// Pre-allocate a fixed pool of `SessionContext` instances (Strategy C from the RFC).
    ///
    /// Eliminates per-request clone cost at the expense of higher idle memory.
    /// Each pool slot is acquired exclusively for the duration of one request.
    Pool { size: usize },
}

impl Default for DataFusionRuntimeConfig {
    fn default() -> Self {
        Self {
            batch_size: 512,
            target_partitions: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            session_strategy: SessionStrategy::Child,
        }
    }
}

// ---------------------------------------------------------------------------
// ANN retrieval
// ---------------------------------------------------------------------------

/// ANN retrieval parameters.
pub struct AnnConfig {
    /// Number of ANN candidates retrieved from the index per query.
    ///
    /// These candidates form the input to the DataFusion enrichment pipeline.
    /// After ACL filtering (Stage 3) and score fusion (Stage 4a), the candidate
    /// count is reduced to `top_m`. Setting `top_n` ≥ 5 × final top-K is a
    /// safe starting point; tune against a recall evaluation set.
    pub top_n: usize,
}

impl Default for AnnConfig {
    fn default() -> Self {
        Self { top_n: 500 }
    }
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

/// Score fusion configuration (Stage 4a).
pub struct ScoringConfig {
    /// Signal weights. Must sum to 1.0 — validated at construction.
    pub weights: ScoringWeights,

    /// Recency decay parameters for the temporal scoring signal.
    pub recency: RecencyConfig,
}

/// Per-signal weights for Stage 4a score fusion.
///
/// The MVP pipeline uses two signals: vector distance and document recency.
/// Additional signals (author reputation, content quality, etc.) are added by
/// introducing new weight fields and extending the fusion SQL in Stage 4a.
///
/// # Invariants
///
/// - All weights are non-negative.
/// - `vector_score + recency_score` must equal `1.0` within [`WEIGHT_SUM_TOLERANCE`].
///
/// The service must refuse to start if these invariants are violated.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoringWeights {
    /// Weight for the ANN distance signal (inverted and normalised: lower distance → higher score).
    pub vector_score: f32,

    /// Weight for the recency decay signal (exponential decay from `created_at`).
    pub recency_score: f32,
}

impl ScoringWeights {
    /// Construct and validate scoring weights.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] if:
    /// - Any weight is negative.
    /// - The sum of all weights does not equal `1.0` within [`WEIGHT_SUM_TOLERANCE`] (`1e-5`).
    pub fn new(vector_score: f32, recency_score: f32) -> Result<Self> {
        if vector_score < 0.0 {
            return Err(QueryError::Config(format!(
                "vector_score must be non-negative, got {vector_score}"
            )));
        }
        if recency_score < 0.0 {
            return Err(QueryError::Config(format!(
                "recency_score must be non-negative, got {recency_score}"
            )));
        }
        let sum = vector_score + recency_score;
        if (sum - 1.0_f32).abs() > WEIGHT_SUM_TOLERANCE {
            return Err(QueryError::Config(format!(
                "scoring weights must sum to 1.0, got {sum:.6} \
                 (vector_score={vector_score}, recency_score={recency_score})"
            )));
        }
        Ok(Self {
            vector_score,
            recency_score,
        })
    }
}

/// Recency decay parameters for the temporal scoring signal.
///
/// The recency score for a document of age `d` days is:
///
/// ```text
/// recency_score = exp(-decay_lambda × max(0, d - grace_period_days))
/// ```
///
/// Within `grace_period_days` the score is `1.0` (no penalty).
/// Beyond `grace_period_days` the score decays exponentially at rate `decay_lambda`.
#[derive(Debug)]
pub struct RecencyConfig {
    /// Flat scoring window in days before exponential decay begins.
    ///
    /// Documents newer than `grace_period_days` receive a recency score of `1.0`.
    /// Must be non-negative.
    pub grace_period_days: i64,

    /// Exponential decay rate `λ`. Higher values cause faster score decay.
    ///
    /// Example: `λ = 0.01` decays to `~0.37` after 100 days beyond the grace period.
    /// Must be strictly positive.
    pub decay_lambda: f64,
}

impl RecencyConfig {
    /// Construct and validate recency decay parameters.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] if:
    /// - `grace_period_days` is negative.
    /// - `decay_lambda` is not strictly positive.
    pub fn new(grace_period_days: i64, decay_lambda: f64) -> Result<Self> {
        if grace_period_days < 0 {
            return Err(QueryError::Config(format!(
                "grace_period_days must be non-negative, got {grace_period_days}"
            )));
        }
        if decay_lambda <= 0.0 {
            return Err(QueryError::Config(format!(
                "decay_lambda must be strictly positive, got {decay_lambda}"
            )));
        }
        Ok(Self {
            grace_period_days,
            decay_lambda,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- ScoringWeights ---

    #[test]
    fn weights_valid_sum_to_one() {
        let w = ScoringWeights::new(0.6, 0.4).unwrap();
        assert_eq!(w.vector_score, 0.6);
        assert_eq!(w.recency_score, 0.4);
    }

    #[test]
    fn weights_valid_extreme_cases() {
        // All weight on one signal is allowed.
        assert!(ScoringWeights::new(1.0, 0.0).is_ok());
        assert!(ScoringWeights::new(0.0, 1.0).is_ok());
    }

    #[test]
    fn weights_sum_below_one_errors() {
        let err = ScoringWeights::new(0.6, 0.3).unwrap_err();
        assert!(err.to_string().contains("sum to 1.0"));
        assert!(err.to_string().contains("0.900000"));
    }

    #[test]
    fn weights_sum_above_one_errors() {
        let err = ScoringWeights::new(0.6, 0.5).unwrap_err();
        assert!(err.to_string().contains("sum to 1.0"));
    }

    #[test]
    fn weights_negative_vector_score_errors() {
        let err = ScoringWeights::new(-0.1, 1.1).unwrap_err();
        assert!(err.to_string().contains("vector_score"));
        assert!(err.to_string().contains("non-negative"));
    }

    #[test]
    fn weights_negative_recency_score_errors() {
        let err = ScoringWeights::new(1.1, -0.1).unwrap_err();
        assert!(err.to_string().contains("recency_score"));
        assert!(err.to_string().contains("non-negative"));
    }

    #[test]
    fn weights_within_tolerance_accepted() {
        // 0.6 + 0.4000001 = 1.0000001, within 1e-5 tolerance.
        assert!(ScoringWeights::new(0.6, 0.400_000_1).is_ok());
    }

    #[test]
    fn weights_outside_tolerance_rejected() {
        // 0.6 + 0.401 = 1.001, exceeds 1e-5 tolerance.
        assert!(ScoringWeights::new(0.6, 0.401).is_err());
    }

    // --- RecencyConfig ---

    #[test]
    fn recency_valid() {
        let r = RecencyConfig::new(30, 0.01).unwrap();
        assert_eq!(r.grace_period_days, 30);
        assert_eq!(r.decay_lambda, 0.01);
    }

    #[test]
    fn recency_zero_grace_period_valid() {
        assert!(RecencyConfig::new(0, 0.01).is_ok());
    }

    #[test]
    fn recency_negative_grace_period_errors() {
        let err = RecencyConfig::new(-1, 0.01).unwrap_err();
        assert!(err.to_string().contains("grace_period_days"));
        assert!(err.to_string().contains("non-negative"));
    }

    #[test]
    fn recency_zero_lambda_errors() {
        let err = RecencyConfig::new(30, 0.0).unwrap_err();
        assert!(err.to_string().contains("decay_lambda"));
        assert!(err.to_string().contains("strictly positive"));
    }

    #[test]
    fn recency_negative_lambda_errors() {
        let err = RecencyConfig::new(30, -0.01).unwrap_err();
        assert!(err.to_string().contains("decay_lambda"));
        assert!(err.to_string().contains("strictly positive"));
    }

    // --- SessionStrategy ---

    #[test]
    fn session_strategy_variants_are_distinct() {
        assert_ne!(SessionStrategy::Child, SessionStrategy::Pool { size: 4 });
        assert_eq!(
            SessionStrategy::Pool { size: 4 },
            SessionStrategy::Pool { size: 4 }
        );
    }

    // --- DataFusionRuntimeConfig default ---

    #[test]
    fn runtime_config_default_is_sane() {
        let cfg = DataFusionRuntimeConfig::default();
        assert!(cfg.batch_size > 0);
        assert!(cfg.target_partitions > 0);
        assert_eq!(cfg.session_strategy, SessionStrategy::Child);
    }

    // --- AnnConfig default ---

    #[test]
    fn ann_config_default_top_n() {
        assert_eq!(AnnConfig::default().top_n, 500);
    }
}
