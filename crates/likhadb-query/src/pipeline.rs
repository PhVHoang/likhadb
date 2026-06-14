//! Q5 — Pipeline orchestration: stages 2 → 3 → 4a → 4b → 4c.
//!
//! `Pipeline` sequences the DataFusion enrichment and scoring stages and the
//! optional model-based reranking stages into a single callable unit. It owns a
//! shared [`DataFusionSession`] and optional reranking clients, and produces a
//! `Vec<PipelineResult>` for each incoming request.
//!
//! Reranking (stages 4b and 4c) is opt-in: when `bi_encoder` or `cross_encoder`
//! is `None`, that stage is skipped and the pipeline returns the Stage 4a results
//! truncated to `top_k`.

use std::sync::Arc;

use datafusion::arrow::array::{Float32Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use crate::config::{BiEncoderConfig, CrossEncoderConfig, QueryConfig, ScoringConfig};
use crate::enrich::enrich;
use crate::fusion::fuse_scores;
use crate::rerank::{
    rerank_biencoder, rerank_crossencoder, BiEncoderClient, BiRankedCandidate, CrossEncoderClient,
};
use crate::session::{register_candidates_in, DataFusionSession};
use crate::{QueryError, Result};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A single ANN candidate, as returned by the vector index.
///
/// The pipeline converts this into an Arrow `RecordBatch` for DataFusion registration.
/// `ann_distance` is the raw distance metric (lower = closer for L2/cosine).
/// `ann_rank` is the 1-based position in the sorted result list.
pub struct Candidate {
    pub id: u64,
    pub ann_distance: f32,
    pub ann_rank: u64,
}

/// Per-request pipeline input.
pub struct PipelineRequest {
    /// ANN candidates from the vector index. Sorted ascending by distance (rank order).
    pub candidates: Vec<Candidate>,
    /// Original query text — passed to bi-encoder and cross-encoder if enabled.
    pub query_text: String,
    /// Team identifiers from the authenticated request context.
    /// Used for ACL predicate injection in Stage 3.
    pub allowed_teams: Vec<String>,
    /// Final result count. Applied after all reranking stages.
    pub top_k: usize,
}

/// A single ranked result from the pipeline.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PipelineResult {
    pub id: String,
    pub fusion_score: f64,
    pub bi_score: Option<f32>,
    pub cross_score: Option<f32>,
    pub chunk_text: Option<String>,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Composed Tier Q pipeline.
///
/// Construct once at service startup via [`Pipeline::new`] and share across
/// requests via `Arc<Pipeline>`. Each call to [`Pipeline::execute`] acquires a
/// child `SessionContext` for per-request isolation.
pub struct Pipeline {
    session: Arc<DataFusionSession>,
    scoring: ScoringConfig,
    top_m: usize,
    bi_encoder: Option<(Arc<dyn BiEncoderClient>, BiEncoderConfig)>,
    cross_encoder: Option<(Arc<dyn CrossEncoderClient>, CrossEncoderConfig)>,
}

impl Pipeline {
    /// Construct a `Pipeline` from config and a pre-initialised session.
    ///
    /// `bi` and `cross` must be `Some` when the corresponding config fields are
    /// `Some`, and `None` otherwise. The constructor does not validate this
    /// correspondence — callers are responsible for pairing config with clients.
    pub fn new(
        config: &QueryConfig,
        session: Arc<DataFusionSession>,
        bi: Option<Arc<dyn BiEncoderClient>>,
        cross: Option<Arc<dyn CrossEncoderClient>>,
    ) -> Self {
        let bi_encoder = config
            .bi_encoder
            .as_ref()
            .zip(bi)
            .map(|(cfg, client)| (client, cfg.clone()));
        let cross_encoder = config
            .cross_encoder
            .as_ref()
            .zip(cross)
            .map(|(cfg, client)| (client, cfg.clone()));
        Self {
            session,
            scoring: ScoringConfig {
                weights: config.scoring.weights.clone(),
                recency: crate::config::RecencyConfig {
                    grace_period_days: config.scoring.recency.grace_period_days,
                    decay_lambda: config.scoring.recency.decay_lambda,
                },
            },
            top_m: config.top_m,
            bi_encoder,
            cross_encoder,
        }
    }

    /// Execute the full pipeline for one request.
    ///
    /// Returns an empty `Vec` when `req.candidates` is empty.
    pub async fn execute(&self, req: PipelineRequest) -> Result<Vec<PipelineResult>> {
        if req.candidates.is_empty() {
            return Ok(vec![]);
        }

        // Stage 2 — register candidates as Arrow MemTable in a per-request child context.
        let batch = candidates_to_batch(&req.candidates)?;
        let child_ctx = self.session.child_context();
        register_candidates_in(&child_ctx, batch)?;

        // Stage 3 — enrichment SQL (joins + ACL).
        let include_embedding = false; // dot_product UDF not implemented; bi-encoder uses text
        let enriched = enrich(&child_ctx, &req.allowed_teams, include_embedding).await?;

        // Stage 4a — score fusion.
        let fused = fuse_scores(
            &child_ctx,
            enriched,
            &self.scoring.weights,
            &self.scoring.recency,
            self.top_m,
        )
        .await?;

        // Stage 4b — bi-encoder reranking (optional).
        let (bi_candidates, effective_top_k): (Vec<BiRankedCandidate>, usize) =
            if let Some((client, cfg)) = &self.bi_encoder {
                let ranked = rerank_biencoder(
                    fused,
                    &req.query_text,
                    client.as_ref(),
                    cfg.alpha,
                    cfg.top_p,
                )
                .await?;
                let top_k = self
                    .cross_encoder
                    .as_ref()
                    .map(|(_, c)| c.top_k)
                    .unwrap_or(req.top_k);
                (ranked, top_k)
            } else {
                // No bi-encoder — materialize Stage 4a output directly.
                let batches = fused.collect().await.map_err(QueryError::DataFusion)?;
                let results = materialize_fused(batches, req.top_k)?;
                return Ok(results);
            };

        // Stage 4c — cross-encoder reranking (optional).
        if let Some((client, _cfg)) = &self.cross_encoder {
            let final_results = rerank_crossencoder(
                bi_candidates,
                &req.query_text,
                client.as_ref(),
                effective_top_k,
            )
            .await?;
            Ok(final_results
                .into_iter()
                .map(|r| PipelineResult {
                    id: r.id,
                    fusion_score: r.fusion_score,
                    bi_score: r.bi_score,
                    cross_score: Some(r.cross_score),
                    chunk_text: Some(r.chunk_text),
                })
                .collect())
        } else {
            // Bi-encoder only — truncate to top_k.
            Ok(bi_candidates
                .into_iter()
                .take(req.top_k)
                .map(|c| PipelineResult {
                    id: c.id,
                    fusion_score: c.fusion_score,
                    bi_score: Some(c.bi_score),
                    cross_score: None,
                    chunk_text: Some(c.chunk_text),
                })
                .collect())
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert `Vec<Candidate>` into an Arrow `RecordBatch` with schema
/// `id: Utf8, ann_distance: Float32, ann_rank: UInt64`.
fn candidates_to_batch(candidates: &[Candidate]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("ann_distance", DataType::Float32, false),
        Field::new("ann_rank", DataType::UInt64, false),
    ]));
    let ids: Vec<String> = candidates.iter().map(|c| c.id.to_string()).collect();
    let distances: Vec<f32> = candidates.iter().map(|c| c.ann_distance).collect();
    let ranks: Vec<u64> = candidates.iter().map(|c| c.ann_rank).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|s| s.as_str()),
            )),
            Arc::new(Float32Array::from(distances)),
            Arc::new(UInt64Array::from(ranks)),
        ],
    )
    .map_err(QueryError::Arrow)
}

/// Materialize Stage 4a batches into `PipelineResult`s when reranking is disabled.
fn materialize_fused(batches: Vec<RecordBatch>, top_k: usize) -> Result<Vec<PipelineResult>> {
    use datafusion::arrow::array::Float64Array;

    let mut results = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let id_idx = schema
            .index_of("id")
            .map_err(|e| QueryError::Schema(e.to_string()))?;
        let score_idx = schema
            .index_of("fusion_score")
            .map_err(|e| QueryError::Schema(e.to_string()))?;
        let text_idx = schema.index_of("chunk_text").ok();

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

        for i in 0..batch.num_rows() {
            let chunk_text = text_idx.and_then(|idx| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|a| a.value(i).to_string())
            });

            results.push(PipelineResult {
                id: id_col.value(i).to_string(),
                fusion_score: score_col.value(i),
                bi_score: None,
                cross_score: None,
                chunk_text,
            });
        }
    }
    results.truncate(top_k);
    Ok(results)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AnnConfig, DataFusionRuntimeConfig, RecencyConfig, ScoringConfig, ScoringWeights,
    };
    use crate::session::DataFusionSession;
    use datafusion::arrow::array::{
        BooleanArray, Float64Array, ListArray, RecordBatch, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    // --- Test helpers ---

    fn test_config() -> QueryConfig {
        QueryConfig {
            parquet_dir: "/tmp/nonexistent".into(),
            datafusion: DataFusionRuntimeConfig::default(),
            ann: AnnConfig::default(),
            scoring: ScoringConfig {
                weights: ScoringWeights::new(0.7, 0.3).unwrap(),
                recency: RecencyConfig::new(0, 0.001).unwrap(),
            },
            top_m: 10,
            bi_encoder: None,
            cross_encoder: None,
        }
    }

    /// Register the five enrichment tables so Stage 3 can run.
    fn register_enrichment_tables(ctx: &SessionContext, candidate_ids: &[&str]) {
        let n = candidate_ids.len();

        // embeddings: one row per candidate
        let emb_schema = Arc::new(Schema::new(vec![
            Field::new("chunk_id", DataType::Utf8, false),
            Field::new("doc_id", DataType::Utf8, false),
            Field::new("chunk_text", DataType::Utf8, false),
        ]));
        let doc_ids: Vec<String> = (0..n).map(|i| format!("d{i}")).collect();
        let doc_id_refs: Vec<&str> = doc_ids.iter().map(|s| s.as_str()).collect();
        let texts: Vec<String> = (0..n).map(|i| format!("text{i}")).collect();
        let text_refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        ctx.register_table(
            "embeddings",
            Arc::new(
                MemTable::try_new(
                    emb_schema.clone(),
                    vec![vec![RecordBatch::try_new(
                        emb_schema,
                        vec![
                            Arc::new(StringArray::from(candidate_ids.to_vec())),
                            Arc::new(StringArray::from(doc_id_refs.clone())),
                            Arc::new(StringArray::from(text_refs)),
                        ],
                    )
                    .unwrap()]],
                )
                .unwrap(),
            ),
        )
        .unwrap();

        // documents
        let doc_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("author_id", DataType::Utf8, false),
            Field::new("created_at", DataType::Float64, true),
        ]));
        let created_ats: Vec<f64> = (0..n).map(|_| 1_700_000_000.0f64).collect();
        ctx.register_table(
            "documents",
            Arc::new(
                MemTable::try_new(
                    doc_schema.clone(),
                    vec![vec![RecordBatch::try_new(
                        doc_schema,
                        vec![
                            Arc::new(StringArray::from(doc_id_refs.clone())),
                            Arc::new(StringArray::from(vec!["a0"; n])),
                            Arc::new(Float64Array::from(created_ats)),
                        ],
                    )
                    .unwrap()]],
                )
                .unwrap(),
            ),
        )
        .unwrap();

        // authors
        let auth_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("reputation_score", DataType::Float64, false),
            Field::new("is_verified", DataType::Boolean, false),
        ]));
        ctx.register_table(
            "authors",
            Arc::new(
                MemTable::try_new(
                    auth_schema.clone(),
                    vec![vec![RecordBatch::try_new(
                        auth_schema,
                        vec![
                            Arc::new(StringArray::from(vec!["a0"])),
                            Arc::new(Float64Array::from(vec![0.9f64])),
                            Arc::new(BooleanArray::from(vec![true])),
                        ],
                    )
                    .unwrap()]],
                )
                .unwrap(),
            ),
        )
        .unwrap();

        // classifications — all "public"
        let cls_schema = Arc::new(Schema::new(vec![
            Field::new("doc_id", DataType::Utf8, false),
            Field::new("sensitivity_label", DataType::Utf8, false),
        ]));
        ctx.register_table(
            "classifications",
            Arc::new(
                MemTable::try_new(
                    cls_schema.clone(),
                    vec![vec![RecordBatch::try_new(
                        cls_schema,
                        vec![
                            Arc::new(StringArray::from(doc_id_refs.clone())),
                            Arc::new(StringArray::from(vec!["public"; n])),
                        ],
                    )
                    .unwrap()]],
                )
                .unwrap(),
            ),
        )
        .unwrap();

        // access_control — all teams ["eng"]
        let values = datafusion::arrow::array::StringArray::from(
            vec!["eng"; n].into_iter().collect::<Vec<_>>(),
        );
        let offsets =
            datafusion::arrow::buffer::OffsetBuffer::new((0..=n as i32).collect::<Vec<_>>().into());
        let list_arr = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            offsets,
            Arc::new(values),
            None,
        );
        let acl_schema = Arc::new(Schema::new(vec![
            Field::new("doc_id", DataType::Utf8, false),
            Field::new(
                "allowed_teams",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ]));
        ctx.register_table(
            "access_control",
            Arc::new(
                MemTable::try_new(
                    acl_schema.clone(),
                    vec![vec![RecordBatch::try_new(
                        acl_schema,
                        vec![Arc::new(StringArray::from(doc_id_refs)), Arc::new(list_arr)],
                    )
                    .unwrap()]],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }

    fn make_candidates(n: usize) -> Vec<Candidate> {
        (0..n)
            .map(|i| Candidate {
                id: i as u64,
                ann_distance: i as f32 * 0.1,
                ann_rank: i as u64 + 1,
            })
            .collect()
    }

    // Integration test: pipeline without reranking
    #[tokio::test]
    async fn pipeline_no_reranking_returns_fused_results() {
        // Build a child context with all tables registered directly for this test.
        let cfg = test_config();
        let session = Arc::new(DataFusionSession::try_new(&cfg).await.unwrap());
        let n = 3;
        let ids: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        let id_strs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();

        // Use pipeline but bypass the shared session — test the materialize path
        // by creating a child ctx and registering tables there.
        let child = session.child_context();
        register_enrichment_tables(&child, &id_strs);

        // Manually run stages to test materialize_fused helper.
        let candidates = make_candidates(n);
        let batch = candidates_to_batch(&candidates).unwrap();
        register_candidates_in(&child, batch).unwrap();
        let enriched = enrich(&child, &[], false).await.unwrap();
        let fused = fuse_scores(
            &child,
            enriched,
            &ScoringWeights::new(0.7, 0.3).unwrap(),
            &RecencyConfig::new(0, 0.001).unwrap(),
            10,
        )
        .await
        .unwrap();
        let batches = fused.collect().await.unwrap();
        let results = materialize_fused(batches, 10).unwrap();
        assert_eq!(results.len(), n);
        assert!(results[0].bi_score.is_none());
        assert!(results[0].cross_score.is_none());
    }

    #[test]
    fn empty_candidates_returns_empty() {
        // candidates_to_batch with empty slice should still produce a valid batch
        // (but pipeline.execute returns early for empty candidates).
        let result = candidates_to_batch(&[]);
        assert!(result.is_ok());
    }

    #[test]
    fn candidates_to_batch_correct_schema() {
        let cands = make_candidates(3);
        let batch = candidates_to_batch(&cands).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "ann_distance");
        assert_eq!(batch.schema().field(2).name(), "ann_rank");
    }
}
