//! Q1 — DataFusion `SessionContext` wired up with enrichment tables.
//!
//! A `DataFusionSession` wraps a DataFusion [`SessionContext`] that has been
//! pre-loaded with enrichment tables sourced from either Parquet files on disk
//! or an Iceberg catalog backed by MinIO.  It also accepts an ANN candidate
//! [`RecordBatch`] and registers it as the `candidates` table so that
//! enrichment SQL can join against it.
//!
//! ## Table registration order
//!
//! 1. Enrichment tables (Parquet or Iceberg) are registered at construction.
//! 2. Per-request candidate batch is registered via [`DataFusionSession::register_candidates`].
//! 3. Enrichment / score-fusion SQL is executed via [`DataFusionSession::sql`].

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::config::QueryConfig;
use crate::{QueryError, Result};

/// DataFusion session pre-loaded with enrichment tables.
///
/// One `DataFusionSession` is shared across requests. Per-request isolation
/// (e.g. registering `candidates`) is achieved by cloning the inner context
/// for each request via [`DataFusionSession::child_context`].
pub struct DataFusionSession {
    ctx: SessionContext,
}

impl DataFusionSession {
    /// Build a session pre-loaded with Parquet enrichment tables from `config.parquet_dir`.
    ///
    /// Expected files: `embeddings.parquet`, `documents.parquet`, `authors.parquet`.
    /// Missing files are silently skipped so callers can populate tables selectively.
    pub async fn try_new(config: &QueryConfig) -> Result<Self> {
        let ctx = base_context(config);
        register_parquet_dir(&ctx, &config.parquet_dir).await?;
        Ok(Self { ctx })
    }

    /// Build a session pre-loaded with Iceberg tables from `catalog`.
    ///
    /// All tables in every listed `namespace` are loaded into memory and
    /// registered with the DataFusion context.  Tables that already exist
    /// (from a previous call) are overwritten.
    ///
    /// # When to use
    ///
    /// Use this constructor when enrichment data lives in an Iceberg catalog
    /// (e.g. Nessie or the Iceberg REST server) backed by MinIO.  For a
    /// Parquet-file-only setup use [`DataFusionSession::try_new`] instead.
    pub async fn try_new_with_iceberg(
        config: &QueryConfig,
        catalog: Arc<dyn Catalog>,
        namespaces: &[NamespaceIdent],
    ) -> Result<Self> {
        let ctx = base_context(config);
        for ns in namespaces {
            let table_idents = catalog
                .list_tables(ns)
                .await
                .map_err(QueryError::Iceberg)?;
            for ident in table_idents {
                register_iceberg_table(&ctx, catalog.as_ref(), &ident).await?;
            }
        }
        Ok(Self { ctx })
    }

    /// Create an isolated child context for a single request.
    ///
    /// The clone is shallow — catalog registrations and UDF registrations are
    /// shared; only the table registry is isolated.  Use the returned context
    /// to register the per-request `candidates` batch via
    /// [`register_candidates_in`].
    pub fn child_context(&self) -> SessionContext {
        self.ctx.clone()
    }

    /// Run SQL against the shared context (no per-request candidates).
    pub async fn sql(&self, query: &str) -> Result<datafusion::dataframe::DataFrame> {
        self.ctx.sql(query).await.map_err(QueryError::DataFusion)
    }
}

/// Register ANN candidates as a `"candidates"` MemTable in `ctx`.
///
/// Call this on the per-request child context returned by
/// [`DataFusionSession::child_context`].
pub fn register_candidates_in(
    ctx: &SessionContext,
    batch: RecordBatch,
) -> Result<()> {
    let schema: SchemaRef = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("candidates", Arc::new(table))
        .map_err(QueryError::DataFusion)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn base_context(config: &QueryConfig) -> SessionContext {
    let session_config = SessionConfig::new()
        .with_batch_size(config.datafusion.batch_size)
        .with_target_partitions(config.datafusion.target_partitions);
    SessionContext::new_with_config(session_config)
}

/// Register enrichment Parquet files from `dir` as DataFusion tables.
///
/// The table name is the file stem (e.g. `embeddings.parquet` → `embeddings`).
async fn register_parquet_dir(ctx: &SessionContext, dir: &Path) -> Result<()> {
    for name in ["embeddings", "documents", "authors"] {
        let path = dir.join(format!("{name}.parquet"));
        if path.exists() {
            ctx.register_parquet(
                name,
                path.to_str().expect("path is valid UTF-8"),
                Default::default(),
            )
            .await
            .map_err(QueryError::DataFusion)?;
        }
    }
    Ok(())
}

/// Scan all rows from an Iceberg table and register them as a DataFusion `MemTable`.
///
/// The table is registered under its short name (the `TableIdent::name` field).
///
/// # Scaling note
///
/// This materialises the whole table into memory at session startup.  For
/// large enrichment tables a lazy `TableProvider` backed by iceberg-datafusion
/// is the right upgrade path.
async fn register_iceberg_table(
    ctx: &SessionContext,
    catalog: &dyn Catalog,
    ident: &TableIdent,
) -> Result<()> {
    let table = catalog
        .load_table(ident)
        .await
        .map_err(QueryError::Iceberg)?;

    let scan = table.scan().build().map_err(QueryError::Iceberg)?;
    let mut stream = scan.to_arrow().await.map_err(QueryError::Iceberg)?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = stream.try_next().await.map_err(QueryError::Iceberg)? {
        batches.push(batch);
    }

    if batches.is_empty() {
        // Register an empty table using the Iceberg schema converted to Arrow.
        let arrow_schema: SchemaRef = Arc::new(
            table
                .metadata()
                .current_schema()
                .as_ref()
                .try_into()
                .map_err(|e: iceberg::Error| QueryError::Iceberg(e))?,
        );
        let mem_table = MemTable::try_new(arrow_schema, vec![])?;
        ctx.register_table(ident.name.as_str(), Arc::new(mem_table))
            .map_err(QueryError::DataFusion)?;
    } else {
        let schema = batches[0].schema();
        let mem_table = MemTable::try_new(schema, vec![batches])?;
        ctx.register_table(ident.name.as_str(), Arc::new(mem_table))
            .map_err(QueryError::DataFusion)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AnnConfig, DataFusionRuntimeConfig, QueryConfig, RecencyConfig, ScoringConfig, ScoringWeights};
    use datafusion::arrow::array::{Float32Array, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::path::PathBuf;

    fn test_config() -> QueryConfig {
        QueryConfig {
            parquet_dir: PathBuf::from("/tmp/nonexistent"),
            datafusion: DataFusionRuntimeConfig::default(),
            ann: AnnConfig::default(),
            scoring: ScoringConfig {
                weights: ScoringWeights::new(0.7, 0.3).unwrap(),
                recency: RecencyConfig::new(30, 0.01).unwrap(),
            },
            top_m: 20,
        }
    }

    #[tokio::test]
    async fn session_starts_with_empty_parquet_dir() {
        // parquet_dir doesn't exist — should not error, just register nothing.
        let cfg = test_config();
        let session = DataFusionSession::try_new(&cfg).await.unwrap();
        // Verify the context is usable.
        let df = session.sql("SELECT 1 + 1 AS answer").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn register_candidates_and_query() {
        let cfg = test_config();
        let session = DataFusionSession::try_new(&cfg).await.unwrap();
        let child = session.child_context();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("score", DataType::Float32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
                Arc::new(Float32Array::from(vec![0.9f32, 0.8, 0.7])),
            ],
        )
        .unwrap();

        register_candidates_in(&child, batch).unwrap();

        let df = child
            .sql("SELECT id, score FROM candidates ORDER BY score DESC")
            .await
            .unwrap();
        let result = df.collect().await.unwrap();
        assert_eq!(result[0].num_rows(), 3);

        let ids = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1); // highest score first
    }
}
