//! Q2 — Stage 3: metadata enrichment and ACL enforcement.
//!
//! Joins the `candidates` MemTable (already registered in the child context by
//! [`crate::session::register_candidates_in`]) against five Iceberg/Parquet enrichment
//! tables and applies access-control filtering before any scoring takes place.
//!
//! ACL enforcement is in the SQL `WHERE` clause — not in application code — so that
//! DataFusion's optimizer can push it down into the Parquet scan and eliminate
//! restricted rows before they touch the scoring pipeline.

use datafusion::prelude::{DataFrame, SessionContext};

use crate::{QueryError, Result};

/// Sanitised team identifier: only alphanumeric, `-`, `_`, `.` characters allowed.
fn validate_team_name(team: &str) -> Result<()> {
    if team.is_empty() {
        return Err(QueryError::Config(
            "team name must not be empty".to_string(),
        ));
    }
    if !team
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(QueryError::Config(format!(
            "team name contains invalid characters: {team:?}"
        )));
    }
    Ok(())
}

/// Build the ACL predicate fragment for SQL injection.
///
/// Returns `None` when `allowed_teams` is empty (open access — no ACL filter applied).
fn acl_predicate(allowed_teams: &[String]) -> Result<Option<String>> {
    if allowed_teams.is_empty() {
        return Ok(None);
    }
    for team in allowed_teams {
        validate_team_name(team)?;
    }
    let clauses: Vec<String> = allowed_teams
        .iter()
        .map(|t| format!("array_has(acl.allowed_teams, '{t}')"))
        .collect();
    Ok(Some(format!("({})", clauses.join(" OR "))))
}

/// Run Stage 3 enrichment on `ctx`.
///
/// **Precondition:** `ctx` must already have a `candidates` table registered
/// (via [`crate::session::register_candidates_in`]) and all enrichment tables
/// (`embeddings`, `documents`, `authors`, `classifications`, `access_control`).
///
/// # Parameters
///
/// - `allowed_teams`: team identifiers from the authenticated request context.
///   Injected as SQL literals after validation (alphanumeric/`-`/`_`/`.` only).
///   Empty slice means open access — ACL predicate is omitted.
/// - `include_embedding`: include the `embedding` column from `embeddings`.
///   Set to `true` only when Stage 4b uses a dot-product UDF over embeddings.
///   Omitting it saves ~6 KB per row in the candidate set.
pub async fn enrich(
    ctx: &SessionContext,
    allowed_teams: &[String],
    include_embedding: bool,
) -> Result<DataFrame> {
    let acl = acl_predicate(allowed_teams)?;

    let embedding_col = if include_embedding {
        ",\n    e.embedding"
    } else {
        ""
    };

    let where_clauses: Vec<&str> = {
        let mut clauses = vec!["cl.sensitivity_label != 'restricted'"];
        // acl_str lifetime must outlast the vec — bind to a local
        let acl_str_storage;
        if let Some(ref s) = acl {
            acl_str_storage = s.as_str();
            clauses.push(acl_str_storage);
        }
        clauses
    };
    let where_clause = where_clauses.join("\n  AND ");

    let sql = format!(
        "SELECT
    c.id,
    c.ann_distance,
    c.ann_rank,
    e.chunk_text,
    d.created_at,
    a.reputation_score,
    a.is_verified{embedding_col}
FROM candidates c
JOIN embeddings e       ON c.id = e.chunk_id
JOIN documents d        ON e.doc_id = d.id
JOIN authors a          ON d.author_id = a.id
JOIN classifications cl ON e.doc_id = cl.doc_id
JOIN access_control acl ON e.doc_id = acl.doc_id
WHERE {where_clause}"
    );

    ctx.sql(&sql).await.map_err(QueryError::DataFusion)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        BooleanArray, Float32Array, Float64Array, ListArray, RecordBatch, StringArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    /// Build a minimal `SessionContext` with all six tables pre-registered.
    ///
    /// Table contents:
    ///
    /// ```text
    /// candidates:      id=c1 (ann_distance=0.1), id=c2 (ann_distance=0.2)
    /// embeddings:      chunk_id=c1 doc_id=d1 chunk_text="hello"
    ///                  chunk_id=c2 doc_id=d2 chunk_text="world"
    /// documents:       id=d1 author_id=a1 | id=d2 author_id=a1
    /// authors:         id=a1 reputation_score=0.9 is_verified=true
    /// classifications: doc_id=d1 sensitivity_label="public"
    ///                  doc_id=d2 sensitivity_label="restricted"
    /// access_control:  doc_id=d1 allowed_teams=["eng","ml"]
    ///                  doc_id=d2 allowed_teams=["eng"]
    /// ```
    async fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();

        // --- candidates ---
        let cand_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ann_distance", DataType::Float32, false),
            Field::new("ann_rank", DataType::UInt64, false),
        ]));
        let cand_batch = RecordBatch::try_new(
            cand_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["c1", "c2"])),
                Arc::new(Float32Array::from(vec![0.1f32, 0.2])),
                Arc::new(UInt64Array::from(vec![1u64, 2])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "candidates",
            Arc::new(MemTable::try_new(cand_schema, vec![vec![cand_batch]]).unwrap()),
        )
        .unwrap();

        // --- embeddings ---
        let emb_schema = Arc::new(Schema::new(vec![
            Field::new("chunk_id", DataType::Utf8, false),
            Field::new("doc_id", DataType::Utf8, false),
            Field::new("chunk_text", DataType::Utf8, false),
        ]));
        let emb_batch = RecordBatch::try_new(
            emb_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["c1", "c2"])),
                Arc::new(StringArray::from(vec!["d1", "d2"])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "embeddings",
            Arc::new(MemTable::try_new(emb_schema, vec![vec![emb_batch]]).unwrap()),
        )
        .unwrap();

        // --- documents ---
        let doc_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("author_id", DataType::Utf8, false),
            // created_at as Float64 (Unix seconds) for simplicity in tests
            Field::new("created_at", DataType::Float64, true),
        ]));
        let doc_batch = RecordBatch::try_new(
            doc_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["d1", "d2"])),
                Arc::new(StringArray::from(vec!["a1", "a1"])),
                Arc::new(Float64Array::from(vec![
                    1_700_000_000.0f64,
                    1_700_000_000.0,
                ])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "documents",
            Arc::new(MemTable::try_new(doc_schema, vec![vec![doc_batch]]).unwrap()),
        )
        .unwrap();

        // --- authors ---
        let auth_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("reputation_score", DataType::Float64, false),
            Field::new("is_verified", DataType::Boolean, false),
        ]));
        let auth_batch = RecordBatch::try_new(
            auth_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a1"])),
                Arc::new(Float64Array::from(vec![0.9f64])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "authors",
            Arc::new(MemTable::try_new(auth_schema, vec![vec![auth_batch]]).unwrap()),
        )
        .unwrap();

        // --- classifications ---
        let cls_schema = Arc::new(Schema::new(vec![
            Field::new("doc_id", DataType::Utf8, false),
            Field::new("sensitivity_label", DataType::Utf8, false),
        ]));
        let cls_batch = RecordBatch::try_new(
            cls_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["d1", "d2"])),
                Arc::new(StringArray::from(vec!["public", "restricted"])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "classifications",
            Arc::new(MemTable::try_new(cls_schema, vec![vec![cls_batch]]).unwrap()),
        )
        .unwrap();

        // --- access_control (allowed_teams as List<Utf8>) ---
        let teams_field = Field::new("item", DataType::Utf8, true);
        let acl_schema = Arc::new(Schema::new(vec![
            Field::new("doc_id", DataType::Utf8, false),
            Field::new(
                "allowed_teams",
                DataType::List(Arc::new(teams_field)),
                false,
            ),
        ]));
        // Build a ListArray: d1 → ["eng","ml"], d2 → ["eng"]
        let values = StringArray::from(vec!["eng", "ml", "eng"]);
        let offsets = datafusion::arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 3].into());
        let list_arr = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            offsets,
            Arc::new(values),
            None,
        );
        let acl_batch = RecordBatch::try_new(
            acl_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["d1", "d2"])),
                Arc::new(list_arr),
            ],
        )
        .unwrap();
        ctx.register_table(
            "access_control",
            Arc::new(MemTable::try_new(acl_schema, vec![vec![acl_batch]]).unwrap()),
        )
        .unwrap();

        ctx
    }

    #[tokio::test]
    async fn restricted_doc_is_filtered() {
        // c2 maps to d2 which has sensitivity_label="restricted" → must be absent.
        let ctx = make_ctx().await;
        let df = enrich(&ctx, &[], false).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "only c1/d1 (public) should survive");
        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "c1");
    }

    #[tokio::test]
    async fn acl_team_filter_removes_non_matching_team() {
        // d1 has teams ["eng","ml"]. Requesting team "data" should match nothing.
        // c2/d2 is already restricted, so zero rows expected.
        let ctx = make_ctx().await;
        let df = enrich(&ctx, &["data".to_string()], false).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[tokio::test]
    async fn acl_team_filter_passes_matching_team() {
        // d1 has teams ["eng","ml"]. Requesting "ml" should return c1/d1.
        let ctx = make_ctx().await;
        let df = enrich(&ctx, &["ml".to_string()], false).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn open_access_returns_all_non_restricted() {
        // Empty allowed_teams → no ACL predicate → all non-restricted rows returned.
        let ctx = make_ctx().await;
        let df = enrich(&ctx, &[], false).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1); // only d1 is public
    }

    #[tokio::test]
    async fn embedding_column_absent_when_not_requested() {
        let ctx = make_ctx().await;
        let df = enrich(&ctx, &[], false).await.unwrap();
        let schema = df.schema().clone();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            !col_names.contains(&"embedding"),
            "embedding column must be absent when include_embedding=false"
        );
    }

    #[test]
    fn invalid_team_name_rejected() {
        let err = validate_team_name("team; DROP TABLE candidates;--").unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    #[test]
    fn empty_team_name_rejected() {
        let err = validate_team_name("").unwrap_err();
        assert!(err.to_string().contains("not be empty"));
    }

    #[test]
    fn valid_team_names_accepted() {
        assert!(validate_team_name("eng").is_ok());
        assert!(validate_team_name("ml-team").is_ok());
        assert!(validate_team_name("data_science.eu").is_ok());
    }
}
