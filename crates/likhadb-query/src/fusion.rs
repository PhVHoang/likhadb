//! Q3 — Stage 4a: multi-signal score fusion via DataFusion SQL window functions.
//!
//! Takes the enriched `DataFrame` from Stage 3 and computes a single `fusion_score`
//! by normalising each signal to `[0, 1]` via min-max windowing and combining them
//! with a configured weighted sum. Output is ordered descending and limited to top-M.
//!
//! All weights and decay parameters are substituted from config as numeric literals
//! — they are never derived from user input.

use datafusion::prelude::{DataFrame, SessionContext};

use crate::config::{RecencyConfig, ScoringWeights};
use crate::{QueryError, Result};

/// Run Stage 4a score fusion over the enriched candidate set.
///
/// Registers `enriched` as a temporary view `enriched_candidates`, then executes
/// a two-CTE SQL query that:
/// 1. Inverts `ann_distance` and computes recency decay to produce raw signals.
/// 2. Min-max normalises each signal within the candidate window.
/// 3. Computes `fusion_score = Σ(weight_i × norm_signal_i)`.
/// 4. Orders descending by `fusion_score` and limits to `top_m`.
///
/// # Parameters
///
/// - `ctx`: per-request child `SessionContext` (already has enrichment tables and
///   `candidates` registered).
/// - `enriched`: `DataFrame` from [`crate::enrich::enrich`] — must contain columns
///   `id`, `ann_distance`, `ann_rank`, `chunk_text`, `created_at`.
/// - `weights`: signal weights from [`crate::config::ScoringConfig`].
/// - `recency`: recency decay parameters.
/// - `top_m`: output cardinality limit.
pub async fn fuse_scores(
    ctx: &SessionContext,
    enriched: DataFrame,
    weights: &ScoringWeights,
    recency: &RecencyConfig,
    top_m: usize,
) -> Result<DataFrame> {
    ctx.register_table("enriched_candidates", enriched.into_view())
        .map_err(QueryError::DataFusion)?;

    let lambda = recency.decay_lambda;
    let grace = recency.grace_period_days;
    let w_vector = weights.vector_score;
    let w_recency = weights.recency_score;

    // created_at is stored as Float64 (Unix epoch seconds) in the test schema and
    // as TIMESTAMP in production Iceberg tables. We compute age in days as:
    //   (now_epoch_seconds - created_at_epoch_seconds) / 86400
    // Using extract(epoch from now()) gives a Float64 in DataFusion.
    let sql = format!(
        "WITH normalized AS (
    SELECT
        id,
        ann_distance,
        ann_rank,
        chunk_text,
        created_at,
        1.0 / (1.0 + ann_distance) AS raw_vector_score,
        exp(-{lambda} * CASE
            WHEN (extract(epoch from now()) - CAST(created_at AS DOUBLE)) / 86400.0 - {grace}.0 > 0.0
            THEN (extract(epoch from now()) - CAST(created_at AS DOUBLE)) / 86400.0 - {grace}.0
            ELSE 0.0
        END) AS raw_recency_score
    FROM enriched_candidates
),
minmax AS (
    SELECT
        id,
        ann_distance,
        ann_rank,
        chunk_text,
        created_at,
        raw_vector_score,
        raw_recency_score,
        (raw_vector_score  - MIN(raw_vector_score)  OVER ())
            / NULLIF(MAX(raw_vector_score)  OVER () - MIN(raw_vector_score)  OVER (), 0)
            AS norm_vector_score,
        (raw_recency_score - MIN(raw_recency_score) OVER ())
            / NULLIF(MAX(raw_recency_score) OVER () - MIN(raw_recency_score) OVER (), 0)
            AS norm_recency_score
    FROM normalized
)
SELECT
    id,
    ann_distance,
    ann_rank,
    chunk_text,
    created_at,
    ({w_vector} * COALESCE(norm_vector_score, 0.0)
     + {w_recency} * COALESCE(norm_recency_score, 0.0)) AS fusion_score
FROM minmax
ORDER BY fusion_score DESC
LIMIT {top_m}"
    );

    ctx.sql(&sql).await.map_err(QueryError::DataFusion)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RecencyConfig, ScoringWeights};
    use datafusion::arrow::array::Float64Array as ArrowF64;
    use datafusion::arrow::array::{
        Float32Array, Float64Array, RecordBatch, StringArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn default_weights() -> ScoringWeights {
        ScoringWeights::new(0.7, 0.3).unwrap()
    }

    fn default_recency() -> RecencyConfig {
        RecencyConfig::new(0, 0.001).unwrap()
    }

    /// Build an enriched DataFrame with `n` rows.
    ///
    /// All rows have the same `created_at` (far in the past so recency decay is
    /// non-trivial) and distinct `ann_distance` values so ordering is deterministic.
    async fn make_enriched_df(ctx: &SessionContext, n: usize) -> DataFrame {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ann_distance", DataType::Float32, false),
            Field::new("ann_rank", DataType::UInt64, false),
            Field::new("chunk_text", DataType::Utf8, true),
            Field::new("created_at", DataType::Float64, true),
        ]));

        let ids: Vec<&str> = (0..n)
            .map(|i| Box::leak(format!("c{i}").into_boxed_str()) as &str)
            .collect();
        let distances: Vec<f32> = (0..n).map(|i| i as f32 * 0.1).collect();
        let ranks: Vec<u64> = (0..n).map(|i| i as u64 + 1).collect();
        let texts: Vec<&str> = ids.iter().map(|_| "text").collect();
        // created_at = 1_600_000_000.0 (well in the past)
        let created_ats: Vec<f64> = (0..n).map(|_| 1_600_000_000.0f64).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Float32Array::from(distances)),
                Arc::new(UInt64Array::from(ranks)),
                Arc::new(StringArray::from(texts)),
                Arc::new(ArrowF64::from(created_ats)),
            ],
        )
        .unwrap();

        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.read_table(Arc::new(table)).unwrap()
    }

    #[tokio::test]
    async fn fusion_scores_in_range() {
        let ctx = SessionContext::new();
        let df = make_enriched_df(&ctx, 5).await;
        let result_df = fuse_scores(&ctx, df, &default_weights(), &default_recency(), 10)
            .await
            .unwrap();
        let batches = result_df.collect().await.unwrap();
        // fusion_score is the last column
        for batch in &batches {
            let col_idx = batch.schema().index_of("fusion_score").unwrap();
            let scores = batch
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..scores.len() {
                let s = scores.value(i);
                assert!(
                    s >= -1e-6 && s <= 1.0 + 1e-6,
                    "fusion_score {s} out of [0, 1]"
                );
            }
        }
    }

    #[tokio::test]
    async fn fusion_scores_ordered_descending() {
        let ctx = SessionContext::new();
        let df = make_enriched_df(&ctx, 5).await;
        let result_df = fuse_scores(&ctx, df, &default_weights(), &default_recency(), 10)
            .await
            .unwrap();
        let batches = result_df.collect().await.unwrap();
        let col_idx = batches[0].schema().index_of("fusion_score").unwrap();
        let scores = batches[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 1..scores.len() {
            assert!(
                scores.value(i - 1) >= scores.value(i) - 1e-9,
                "scores not descending at index {i}"
            );
        }
    }

    #[tokio::test]
    async fn top_m_limits_output() {
        let ctx = SessionContext::new();
        let df = make_enriched_df(&ctx, 10).await;
        let result_df = fuse_scores(&ctx, df, &default_weights(), &default_recency(), 3)
            .await
            .unwrap();
        let batches = result_df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn single_row_returns_zero_score() {
        // With one row, MIN == MAX → NULLIF denominator is 0 → COALESCE returns 0.0.
        let ctx = SessionContext::new();
        let df = make_enriched_df(&ctx, 1).await;
        let result_df = fuse_scores(&ctx, df, &default_weights(), &default_recency(), 10)
            .await
            .unwrap();
        let batches = result_df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        let col_idx = batches[0].schema().index_of("fusion_score").unwrap();
        let scores = batches[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((scores.value(0) - 0.0).abs() < 1e-6);
    }
}
