use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::TryStreamExt;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Struct, Type,
};
use iceberg::transaction::Transaction;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::ArrowWriter;
use serde_json::Value;

use crate::error::LakehouseError;

pub const STAGING_WATERMARK_PROP: &str = "last_wal_lsn";

pub struct StagingRow {
    pub id: u64,
    pub vector: Vec<f32>,
    pub payload: Option<Value>,
    pub lsn: u64,
    /// When `true` the row is a delete tombstone; `merge_status` is written as
    /// `"deleted"` and `vector_json` is stored as an empty array sentinel.
    pub is_tombstone: bool,
}

pub struct StagingBatch {
    pub collection_name: String,
    pub rows: Vec<StagingRow>,
}

pub struct PendingVector {
    pub id: u64,
    pub vector: Vec<f32>,
    pub payload: Option<Value>,
    pub lsn: u64,
    /// `true` means this row is a delete tombstone — apply as a delete, not an insert.
    pub is_delete: bool,
}

fn staging_iceberg_schema() -> Result<iceberg::spec::Schema, LakehouseError> {
    iceberg::spec::Schema::builder()
        .with_schema_id(0)
        .with_fields([
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                2,
                "vector_json",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "payload",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                4,
                "lsn",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                5,
                "merge_status",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                6,
                "ingested_at_ms",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .map_err(|e| LakehouseError::Schema(e.to_string()))
}

fn staging_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("vector_json", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
        Field::new("lsn", DataType::Int64, false),
        Field::new("merge_status", DataType::Utf8, false),
        Field::new("ingested_at_ms", DataType::Int64, false),
    ]))
}

pub(crate) fn staging_table_ident(namespace: &NamespaceIdent, collection_name: &str) -> TableIdent {
    TableIdent::new(
        namespace.clone(),
        format!("likhadb_staging_{collection_name}"),
    )
}

/// Open or create the per-collection staging table.
pub async fn get_or_create_staging_table<C: Catalog>(
    catalog: &C,
    namespace: &NamespaceIdent,
    collection_name: &str,
) -> Result<iceberg::table::Table, LakehouseError> {
    let ident = staging_table_ident(namespace, collection_name);

    let exists = catalog
        .table_exists(&ident)
        .await
        .map_err(LakehouseError::Iceberg)?;
    if exists {
        return catalog
            .load_table(&ident)
            .await
            .map_err(LakehouseError::Iceberg);
    }

    let ns_exists = catalog
        .namespace_exists(namespace)
        .await
        .map_err(LakehouseError::Iceberg)?;
    if !ns_exists {
        catalog
            .create_namespace(namespace, Default::default())
            .await
            .map_err(LakehouseError::Iceberg)?;
    }

    let schema = staging_iceberg_schema()?;
    let creation = TableCreation::builder()
        .name(ident.name().to_string())
        .schema(schema)
        .properties(HashMap::from([(
            STAGING_WATERMARK_PROP.to_string(),
            "0".to_string(),
        )]))
        .build();

    catalog
        .create_table(namespace, creation)
        .await
        .map_err(LakehouseError::Iceberg)
}

/// Append pending vectors to the staging table and atomically advance the
/// `last_wal_lsn` table property to `new_watermark`.
pub async fn append_to_staging<C: Catalog>(
    catalog: &C,
    table: &iceberg::table::Table,
    batch: &StagingBatch,
    new_watermark: u64,
) -> Result<(), LakehouseError> {
    if batch.rows.is_empty() {
        // No data rows — only advance the watermark.
        Transaction::new(table)
            .set_properties(HashMap::from([(
                STAGING_WATERMARK_PROP.to_string(),
                new_watermark.to_string(),
            )]))
            .map_err(LakehouseError::Iceberg)?
            .commit(catalog)
            .await
            .map_err(LakehouseError::Iceberg)?;
        return Ok(());
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let n = batch.rows.len();
    let mut ids: Vec<i64> = Vec::with_capacity(n);
    let mut vectors: Vec<String> = Vec::with_capacity(n);
    let mut payloads: Vec<Option<String>> = Vec::with_capacity(n);
    let mut lsns: Vec<i64> = Vec::with_capacity(n);

    let mut statuses: Vec<&str> = Vec::with_capacity(n);

    for row in &batch.rows {
        ids.push(row.id as i64);
        // Tombstone rows store an empty array sentinel; no real vector data needed.
        vectors.push(if row.is_tombstone {
            "[]".to_string()
        } else {
            serde_json::to_string(&row.vector).unwrap_or_default()
        });
        payloads.push(row.payload.as_ref().map(|v| v.to_string()));
        lsns.push(row.lsn as i64);
        statuses.push(if row.is_tombstone {
            "deleted"
        } else {
            "pending"
        });
    }

    let schema = staging_arrow_schema();
    let id_arr: ArrayRef = Arc::new(Int64Array::from(ids));
    let vec_arr: ArrayRef = Arc::new(StringArray::from(vectors));
    let pay_arr: ArrayRef = Arc::new(StringArray::from(payloads));
    let lsn_arr: ArrayRef = Arc::new(Int64Array::from(lsns));
    let status_arr: ArrayRef = Arc::new(StringArray::from(statuses));
    let ts_arr: ArrayRef = Arc::new(Int64Array::from(vec![now_ms; n]));

    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_arr, vec_arr, pay_arr, lsn_arr, status_arr, ts_arr],
    )
    .map_err(LakehouseError::Arrow)?;

    let parquet_bytes = batch_to_parquet_bytes(&record_batch, &schema)?;
    let byte_count = parquet_bytes.len() as u64;

    let file_path = format!(
        "{}/data/staging_{}_{}.parquet",
        table.metadata().location(),
        new_watermark,
        now_ms,
    );
    table
        .file_io()
        .new_output(&file_path)
        .map_err(LakehouseError::Iceberg)?
        .write(Bytes::from(parquet_bytes))
        .await
        .map_err(LakehouseError::Iceberg)?;

    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(file_path)
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .record_count(n as u64)
        .file_size_in_bytes(byte_count)
        .build()
        .map_err(|e| LakehouseError::Schema(format!("DataFile build: {e}")))?;

    // Atomically commit the data file and the watermark property.
    let mut append = Transaction::new(table)
        .set_properties(HashMap::from([(
            STAGING_WATERMARK_PROP.to_string(),
            new_watermark.to_string(),
        )]))
        .map_err(LakehouseError::Iceberg)?
        .fast_append(None, vec![])
        .map_err(LakehouseError::Iceberg)?;

    append
        .add_data_files([data_file])
        .map_err(LakehouseError::Iceberg)?;

    append
        .apply()
        .await
        .map_err(LakehouseError::Iceberg)?
        .commit(catalog)
        .await
        .map_err(LakehouseError::Iceberg)?;

    Ok(())
}

/// Read the current `last_wal_lsn` watermark from the staging table's properties.
/// Returns `0` if the property is missing (new table).
pub fn read_watermark(table: &iceberg::table::Table) -> u64 {
    table
        .metadata()
        .properties()
        .get(STAGING_WATERMARK_PROP)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Scan the staging table and return all actionable rows — both
/// `merge_status = 'pending'` (inserts) and `merge_status = 'deleted'`
/// (tombstones) — sorted by LSN ascending so recovery applies them in order.
///
/// Returns `(entries, max_lsn_seen)`.
pub async fn scan_pending(
    table: &iceberg::table::Table,
) -> Result<(Vec<PendingVector>, u64), LakehouseError> {
    let scan = table.scan().build().map_err(LakehouseError::Iceberg)?;
    let mut stream = scan.to_arrow().await.map_err(LakehouseError::Iceberg)?;

    let mut entries = Vec::new();
    let mut max_lsn: u64 = 0;

    while let Some(batch) = stream.try_next().await.map_err(LakehouseError::Iceberg)? {
        let schema = batch.schema();

        let id_idx = schema
            .index_of("id")
            .map_err(|_| LakehouseError::ColumnNotFound("id".to_string()))?;
        let vec_idx = schema
            .index_of("vector_json")
            .map_err(|_| LakehouseError::ColumnNotFound("vector_json".to_string()))?;
        let pay_idx = schema
            .index_of("payload")
            .map_err(|_| LakehouseError::ColumnNotFound("payload".to_string()))?;
        let lsn_idx = schema
            .index_of("lsn")
            .map_err(|_| LakehouseError::ColumnNotFound("lsn".to_string()))?;
        let status_idx = schema
            .index_of("merge_status")
            .map_err(|_| LakehouseError::ColumnNotFound("merge_status".to_string()))?;

        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| LakehouseError::Schema("id not Int64".to_string()))?;
        let vecs = batch
            .column(vec_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| LakehouseError::Schema("vector_json not Utf8".to_string()))?;
        let pays = batch
            .column(pay_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| LakehouseError::Schema("payload not Utf8".to_string()))?;
        let lsns = batch
            .column(lsn_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| LakehouseError::Schema("lsn not Int64".to_string()))?;
        let statuses = batch
            .column(status_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| LakehouseError::Schema("merge_status not Utf8".to_string()))?;

        for row in 0..batch.num_rows() {
            let status = statuses.value(row);
            let is_delete = match status {
                "pending" => false,
                "deleted" => true,
                _ => continue, // "merged" / "merging" rows are already incorporated
            };

            let lsn = lsns.value(row) as u64;
            max_lsn = max_lsn.max(lsn);

            let vector = if is_delete {
                Vec::new()
            } else {
                serde_json::from_str(vecs.value(row)).unwrap_or_default()
            };
            let payload = if is_delete || pays.is_null(row) {
                None
            } else {
                serde_json::from_str(pays.value(row)).ok()
            };

            entries.push(PendingVector {
                id: ids.value(row) as u64,
                vector,
                payload,
                lsn,
                is_delete,
            });
        }
    }

    // Sort by LSN so recovery applies inserts and deletes in the correct order.
    entries.sort_unstable_by_key(|e| e.lsn);

    Ok((entries, max_lsn))
}

fn batch_to_parquet_bytes(
    batch: &RecordBatch,
    schema: &Arc<ArrowSchema>,
) -> Result<Vec<u8>, LakehouseError> {
    let mut buf = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema.clone(), None).map_err(LakehouseError::Parquet)?;
    writer.write(batch).map_err(LakehouseError::Parquet)?;
    writer.close().map_err(LakehouseError::Parquet)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_zero_on_missing_property() {
        // Verify that read_watermark returns 0 when property is absent.
        // We test the parsing logic directly since we can't build a Table in unit tests.
        let raw: Option<&str> = None;
        let result = raw.and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        assert_eq!(result, 0);
    }

    #[test]
    fn vector_json_roundtrip() {
        let v = vec![0.1f32, 0.2, 0.3, 0.4];
        let s = serde_json::to_string(&v).unwrap();
        let back: Vec<f32> = serde_json::from_str(&s).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn staging_arrow_schema_columns() {
        let schema = staging_arrow_schema();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("vector_json").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("merge_status").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(schema.field_with_name("payload").unwrap().is_nullable());
    }
}
