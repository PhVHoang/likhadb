/// Binds a collection to an externally-written Iceberg source table so that
/// writes authored by other lakehouse engines (Spark, Trino, dbt) can be
/// reflected in the live index via snapshot-diff maintenance.
///
/// The source table is identified by its namespace path + name as plain strings
/// (rather than iceberg's `TableIdent`) so this type stays dependency-free and
/// serializable; the lakehouse layer resolves it to a `TableIdent` when it opens
/// the table. A collection with no binding behaves exactly as before — the
/// feature is opt-in and additive.
///
/// In this phase the binding is plumbed through create-collection and persisted,
/// but nothing consumes it yet: the background maintenance task that reads source
/// deltas lands in a later phase.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceBinding {
    /// Namespace path of the source table, e.g. `["lake", "embeddings"]`.
    pub source_namespace: Vec<String>,
    /// Source table name.
    pub source_table: String,
    /// Column mapped to `VecId` (integer ids only in v1).
    pub id_column: String,
    /// Column holding the embedding vector.
    pub vector_column: String,
    /// Columns carried into the index payload.
    #[serde(default)]
    pub payload_columns: Vec<String>,
}
