//! Q4 — Stages 4b and 4c: model-based reranking via materialize-then-call.
//!
//! Both stages collect the candidate set out of DataFusion into memory, make a
//! **single batched** HTTP call to the model service, zip scores back to IDs, and
//! sort descending. At top-100 → top-20 → top-K cardinalities this pattern is
//! simpler and faster than an `AsyncScalarUDF` — the DataFusion operator overhead
//! is not justified for a few dozen rows.
//!
//! # Feature gate
//!
//! The HTTP implementation types (`HttpBiEncoderClient`, `HttpCrossEncoderClient`)
//! require the `rerank` feature. The traits and data types are always available so
//! that callers (including the `pipeline` module) can compile without the feature.

use async_trait::async_trait;
use datafusion::arrow::array::{Float64Array, StringArray};
use datafusion::prelude::DataFrame;
#[cfg(feature = "rerank")]
use serde::Serialize;

use crate::{QueryError, Result};

// ---------------------------------------------------------------------------
// Model client traits
// ---------------------------------------------------------------------------

/// Batched bi-encoder similarity client.
///
/// A single call receives the query and the full candidate text slice, and must
/// return one score per element in `texts`. Implementors must **not** iterate
/// per-row — the whole batch is passed in one call.
#[async_trait]
pub trait BiEncoderClient: Send + Sync {
    async fn score(&self, query: &str, texts: &[String]) -> Result<Vec<f32>>;
}

/// Batched cross-encoder relevance client.
///
/// A single call receives the query and the full candidate passage slice, and must
/// return one score per element in `passages`.
#[async_trait]
pub trait CrossEncoderClient: Send + Sync {
    async fn score(&self, query: &str, passages: &[String]) -> Result<Vec<f32>>;
}

// ---------------------------------------------------------------------------
// HTTP implementations (reqwest)
// ---------------------------------------------------------------------------

#[cfg(feature = "rerank")]
#[derive(Serialize)]
struct ScoreRequest<'a> {
    query: &'a str,
    texts: &'a [String],
}

/// HTTP bi-encoder client sending `POST {endpoint}` with JSON body `{query, texts}`.
#[cfg(feature = "rerank")]
pub struct HttpBiEncoderClient {
    pub endpoint: String,
    pub client: reqwest::Client,
}

#[cfg(feature = "rerank")]
#[async_trait]
impl BiEncoderClient for HttpBiEncoderClient {
    async fn score(&self, query: &str, texts: &[String]) -> Result<Vec<f32>> {
        let resp = self
            .client
            .post(&self.endpoint)
            .json(&ScoreRequest { query, texts })
            .send()
            .await?;
        resp.json::<Vec<f32>>().await.map_err(QueryError::Http)
    }
}

/// HTTP cross-encoder client — same wire format as the bi-encoder.
#[cfg(feature = "rerank")]
pub struct HttpCrossEncoderClient {
    pub endpoint: String,
    pub client: reqwest::Client,
}

#[cfg(feature = "rerank")]
#[async_trait]
impl CrossEncoderClient for HttpCrossEncoderClient {
    async fn score(&self, query: &str, passages: &[String]) -> Result<Vec<f32>> {
        let resp = self
            .client
            .post(&self.endpoint)
            .json(&ScoreRequest {
                query,
                texts: passages,
            })
            .send()
            .await?;
        resp.json::<Vec<f32>>().await.map_err(QueryError::Http)
    }
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Stage 4b output: a candidate scored by the bi-encoder and a combined score.
#[derive(Debug, Clone)]
pub struct BiRankedCandidate {
    pub id: String,
    pub fusion_score: f64,
    pub bi_score: f32,
    /// `α × bi_score + (1 - α) × fusion_score`
    pub combined_score: f64,
    pub chunk_text: String,
}

/// Stage 4c output: final ranked result with all three scores.
#[derive(Debug, Clone)]
pub struct CrossRankedResult {
    pub id: String,
    pub fusion_score: f64,
    pub bi_score: Option<f32>,
    pub cross_score: f32,
    pub chunk_text: String,
}

// ---------------------------------------------------------------------------
// Stage 4b — Bi-encoder reranking
// ---------------------------------------------------------------------------

/// Stage 4b: materialize top-M `DataFrame` → single batched bi-encoder call → top-P.
///
/// Steps:
/// 1. Collect the `DataFrame` into memory (one `collect()` call).
/// 2. Extract `id`, `fusion_score`, `chunk_text` columns.
/// 3. Call `client.score(query, &chunk_texts)` — **one call for the whole batch**.
/// 4. Zip scores: `combined = α × bi_score + (1-α) × fusion_score`.
/// 5. Sort descending by `combined_score`, truncate to `top_p`.
pub async fn rerank_biencoder(
    fused: DataFrame,
    query: &str,
    client: &dyn BiEncoderClient,
    alpha: f32,
    top_p: usize,
) -> Result<Vec<BiRankedCandidate>> {
    let batches = fused.collect().await.map_err(QueryError::DataFusion)?;

    let mut ids: Vec<String> = Vec::new();
    let mut fusion_scores: Vec<f64> = Vec::new();
    let mut chunk_texts: Vec<String> = Vec::new();

    for batch in &batches {
        let schema = batch.schema();
        let id_idx = schema
            .index_of("id")
            .map_err(|e| QueryError::Schema(e.to_string()))?;
        let score_idx = schema
            .index_of("fusion_score")
            .map_err(|e| QueryError::Schema(e.to_string()))?;
        let text_idx = schema
            .index_of("chunk_text")
            .map_err(|e| QueryError::Schema(e.to_string()))?;

        let id_col = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| QueryError::Schema("id column is not Utf8".to_string()))?;
        let score_col = batch
            .column(score_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| QueryError::Schema("fusion_score column is not Float64".to_string()))?;
        let text_col = batch
            .column(text_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| QueryError::Schema("chunk_text column is not Utf8".to_string()))?;

        for i in 0..batch.num_rows() {
            ids.push(id_col.value(i).to_string());
            fusion_scores.push(score_col.value(i));
            chunk_texts.push(text_col.value(i).to_string());
        }
    }

    if ids.is_empty() {
        return Ok(vec![]);
    }

    // Single batched call — not per-row.
    let bi_scores = client.score(query, &chunk_texts).await?;
    if bi_scores.len() != ids.len() {
        return Err(QueryError::Rerank(format!(
            "bi-encoder returned {} scores for {} candidates",
            bi_scores.len(),
            ids.len()
        )));
    }

    let alpha_f64 = alpha as f64;
    let mut candidates: Vec<BiRankedCandidate> = ids
        .into_iter()
        .zip(fusion_scores)
        .zip(bi_scores)
        .zip(chunk_texts)
        .map(|(((id, fs), bs), ct)| {
            let combined = alpha_f64 * bs as f64 + (1.0 - alpha_f64) * fs;
            BiRankedCandidate {
                id,
                fusion_score: fs,
                bi_score: bs,
                combined_score: combined,
                chunk_text: ct,
            }
        })
        .collect();

    candidates.sort_by(|a, b| b.combined_score.partial_cmp(&a.combined_score).unwrap());
    candidates.truncate(top_p);
    Ok(candidates)
}

// ---------------------------------------------------------------------------
// Stage 4c — Cross-encoder reranking
// ---------------------------------------------------------------------------

/// Stage 4c: top-P `BiRankedCandidate`s → single batched cross-encoder call → top-K.
///
/// Steps:
/// 1. Extract `chunk_text` from each candidate (already in memory — no DataFusion needed).
/// 2. Call `client.score(query, &passages)` — **one call for the whole batch**.
/// 3. Zip scores back to IDs.
/// 4. Sort descending by `cross_score`, truncate to `top_k`.
pub async fn rerank_crossencoder(
    candidates: Vec<BiRankedCandidate>,
    query: &str,
    client: &dyn CrossEncoderClient,
    top_k: usize,
) -> Result<Vec<CrossRankedResult>> {
    if candidates.is_empty() {
        return Ok(vec![]);
    }

    let passages: Vec<String> = candidates.iter().map(|c| c.chunk_text.clone()).collect();

    // Single batched call.
    let cross_scores = client.score(query, &passages).await?;
    if cross_scores.len() != candidates.len() {
        return Err(QueryError::Rerank(format!(
            "cross-encoder returned {} scores for {} candidates",
            cross_scores.len(),
            candidates.len()
        )));
    }

    let mut results: Vec<CrossRankedResult> = candidates
        .into_iter()
        .zip(cross_scores)
        .map(|(c, cs)| CrossRankedResult {
            id: c.id,
            fusion_score: c.fusion_score,
            bi_score: Some(c.bi_score),
            cross_score: cs,
            chunk_text: c.chunk_text,
        })
        .collect();

    results.sort_by(|a, b| b.cross_score.partial_cmp(&a.cross_score).unwrap());
    results.truncate(top_k);
    Ok(results)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, RecordBatch, StringArray, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // --- Mock clients ---

    struct MockBiEncoder {
        call_count: Arc<AtomicUsize>,
        scores: Vec<f32>,
    }

    #[async_trait]
    impl BiEncoderClient for MockBiEncoder {
        async fn score(&self, _query: &str, texts: &[String]) -> Result<Vec<f32>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                texts.len(),
                self.scores.len(),
                "mock received wrong number of texts"
            );
            Ok(self.scores.clone())
        }
    }

    struct MockCrossEncoder {
        call_count: Arc<AtomicUsize>,
        scores: Vec<f32>,
    }

    #[async_trait]
    impl CrossEncoderClient for MockCrossEncoder {
        async fn score(&self, _query: &str, passages: &[String]) -> Result<Vec<f32>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                passages.len(),
                self.scores.len(),
                "mock received wrong number of passages"
            );
            Ok(self.scores.clone())
        }
    }

    // --- Helpers ---

    /// Build a `DataFrame` that looks like Stage 4a output (has fusion_score column).
    async fn make_fused_df(n: usize) -> DataFrame {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ann_rank", DataType::UInt64, false),
            Field::new("chunk_text", DataType::Utf8, true),
            Field::new("fusion_score", DataType::Float64, false),
        ]));
        let ids: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let ranks: Vec<u64> = (0..n).map(|i| i as u64 + 1).collect();
        let texts: Vec<String> = (0..n).map(|i| format!("text{i}")).collect();
        // Descending fusion scores: 0.9, 0.8, 0.7, ...
        let scores: Vec<f64> = (0..n).map(|i| 0.9 - i as f64 * 0.1).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|s| s.as_str()),
                )),
                Arc::new(UInt64Array::from(ranks)),
                Arc::new(StringArray::from_iter_values(
                    texts.iter().map(|s| s.as_str()),
                )),
                Arc::new(Float64Array::from(scores)),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.read_table(Arc::new(table)).unwrap()
    }

    // --- bi-encoder tests ---

    #[tokio::test]
    async fn biencoder_single_batched_call() {
        let n = 5;
        let call_count = Arc::new(AtomicUsize::new(0));
        let client = MockBiEncoder {
            call_count: call_count.clone(),
            scores: vec![0.5; n],
        };
        let df = make_fused_df(n).await;
        rerank_biencoder(df, "query", &client, 0.5, n)
            .await
            .unwrap();
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "must be exactly 1 call"
        );
    }

    #[tokio::test]
    async fn biencoder_results_ordered_by_combined_score() {
        // bi_scores: [0.9, 0.1, 0.5, 0.3, 0.7] — should reorder results.
        let n = 5;
        let bi_scores = vec![0.9f32, 0.1, 0.5, 0.3, 0.7];
        let client = MockBiEncoder {
            call_count: Arc::new(AtomicUsize::new(0)),
            scores: bi_scores,
        };
        let df = make_fused_df(n).await;
        let results = rerank_biencoder(df, "q", &client, 0.5, n).await.unwrap();
        for i in 1..results.len() {
            assert!(
                results[i - 1].combined_score >= results[i].combined_score - 1e-9,
                "not descending at {i}"
            );
        }
    }

    #[tokio::test]
    async fn biencoder_truncates_to_top_p() {
        let n = 5;
        let client = MockBiEncoder {
            call_count: Arc::new(AtomicUsize::new(0)),
            scores: vec![0.5; n],
        };
        let df = make_fused_df(n).await;
        let results = rerank_biencoder(df, "q", &client, 0.5, 2).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn biencoder_empty_input_returns_empty() {
        let df = make_fused_df(0).await;
        let client = MockBiEncoder {
            call_count: Arc::new(AtomicUsize::new(0)),
            scores: vec![],
        };
        let results = rerank_biencoder(df, "q", &client, 0.5, 5).await.unwrap();
        assert!(results.is_empty());
    }

    // --- cross-encoder tests ---

    fn make_bi_candidates(n: usize) -> Vec<BiRankedCandidate> {
        (0..n)
            .map(|i| BiRankedCandidate {
                id: format!("c{i}"),
                fusion_score: 0.9 - i as f64 * 0.1,
                bi_score: 0.5,
                combined_score: 0.7 - i as f64 * 0.05,
                chunk_text: format!("text{i}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn crossencoder_single_batched_call() {
        let n = 4;
        let call_count = Arc::new(AtomicUsize::new(0));
        let client = MockCrossEncoder {
            call_count: call_count.clone(),
            scores: vec![0.5; n],
        };
        rerank_crossencoder(make_bi_candidates(n), "q", &client, n)
            .await
            .unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn crossencoder_ordered_by_cross_score() {
        let n = 4;
        let cross_scores = vec![0.2f32, 0.9, 0.5, 0.1];
        let client = MockCrossEncoder {
            call_count: Arc::new(AtomicUsize::new(0)),
            scores: cross_scores,
        };
        let results = rerank_crossencoder(make_bi_candidates(n), "q", &client, n)
            .await
            .unwrap();
        for i in 1..results.len() {
            assert!(
                results[i - 1].cross_score >= results[i].cross_score - 1e-6,
                "not descending at {i}"
            );
        }
    }

    #[tokio::test]
    async fn crossencoder_truncates_to_top_k() {
        let n = 4;
        let client = MockCrossEncoder {
            call_count: Arc::new(AtomicUsize::new(0)),
            scores: vec![0.5; n],
        };
        let results = rerank_crossencoder(make_bi_candidates(n), "q", &client, 2)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn crossencoder_empty_input_returns_empty() {
        let client = MockCrossEncoder {
            call_count: Arc::new(AtomicUsize::new(0)),
            scores: vec![],
        };
        let results = rerank_crossencoder(vec![], "q", &client, 5).await.unwrap();
        assert!(results.is_empty());
    }
}
