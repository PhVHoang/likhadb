use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array, LargeBinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::TryStreamExt;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Struct, Type,
};
use iceberg::transaction::Transaction;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use likhadb_store::CollectionSnapshot;
use parquet::arrow::ArrowWriter;

use crate::error::LakehouseError;

fn iceberg_schema() -> Result<iceberg::spec::Schema, LakehouseError> {
    iceberg::spec::Schema::builder()
        .with_schema_id(0)
        .with_fields([
            Arc::new(NestedField::required(
                1,
                "collection_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                2,
                "snapshot_blob",
                Type::Primitive(PrimitiveType::Binary),
            )),
            Arc::new(NestedField::required(
                3,
                "written_at_ms",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .map_err(|e| LakehouseError::Schema(e.to_string()))
}

fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("collection_name", DataType::Utf8, false),
        Field::new("snapshot_blob", DataType::LargeBinary, false),
        Field::new("written_at_ms", DataType::Int64, false),
    ]))
}

async fn get_or_create_snapshots_table<C: Catalog>(
    catalog: &C,
    table_ident: &TableIdent,
) -> Result<iceberg::table::Table, LakehouseError> {
    let exists = catalog
        .table_exists(table_ident)
        .await
        .map_err(LakehouseError::Iceberg)?;

    if exists {
        return catalog
            .load_table(table_ident)
            .await
            .map_err(LakehouseError::Iceberg);
    }

    let ns = table_ident.namespace().clone();
    let ns_exists = catalog
        .namespace_exists(&ns)
        .await
        .map_err(LakehouseError::Iceberg)?;
    if !ns_exists {
        catalog
            .create_namespace(&ns, Default::default())
            .await
            .map_err(LakehouseError::Iceberg)?;
    }

    let schema = iceberg_schema()?;
    let creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .schema(schema)
        .build();

    catalog
        .create_table(&ns, creation)
        .await
        .map_err(LakehouseError::Iceberg)
}

/// Serialize a `CollectionSnapshot` as a single binary blob and append it to
/// the Iceberg index-snapshot table.  The latest row (by `written_at_ms`) per
/// collection is the authoritative snapshot.
pub async fn write_collection_snapshot<C: Catalog>(
    catalog: &C,
    table_ident: &TableIdent,
    col_snap: &CollectionSnapshot,
) -> Result<(), LakehouseError> {
    let table = get_or_create_snapshots_table(catalog, table_ident).await?;

    let blob: Vec<u8> = bincode::serialize(col_snap)
        .map_err(|e| LakehouseError::IndexBlob(format!("snapshot encode: {e}")))?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let schema = arrow_schema();
    let names: ArrayRef = Arc::new(StringArray::from(vec![col_snap.name.as_str()]));
    let blobs: ArrayRef = Arc::new(LargeBinaryArray::from_iter_values([blob.as_slice()]));
    let timestamps: ArrayRef = Arc::new(Int64Array::from(vec![now_ms]));

    let batch = RecordBatch::try_new(schema.clone(), vec![names, blobs, timestamps])
        .map_err(LakehouseError::Arrow)?;

    let parquet_bytes = batch_to_parquet_bytes(&batch, &schema)?;
    let byte_count = parquet_bytes.len() as u64;

    let file_path = format!(
        "{}/data/snapshot_{}_{}.parquet",
        table.metadata().location(),
        col_snap.name,
        now_ms,
    );
    let output = table
        .file_io()
        .new_output(&file_path)
        .map_err(LakehouseError::Iceberg)?;
    output
        .write(Bytes::from(parquet_bytes))
        .await
        .map_err(LakehouseError::Iceberg)?;

    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(file_path)
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .record_count(1)
        .file_size_in_bytes(byte_count)
        .build()
        .map_err(|e| LakehouseError::Schema(format!("DataFile build: {e}")))?;

    let mut append = Transaction::new(&table)
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

/// Load all collection snapshots from the Iceberg index-snapshot table.
///
/// Returns an empty `Vec` if the table does not exist yet — callers should
/// fall back to the WAL + bincode snapshot path in that case.
pub async fn load_collection_snapshots<C: Catalog>(
    catalog: &C,
    table_ident: &TableIdent,
) -> Result<Vec<CollectionSnapshot>, LakehouseError> {
    let exists = catalog
        .table_exists(table_ident)
        .await
        .map_err(LakehouseError::Iceberg)?;
    if !exists {
        return Ok(vec![]);
    }

    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(LakehouseError::Iceberg)?;

    let scan = table.scan().build().map_err(LakehouseError::Iceberg)?;
    let mut stream = scan.to_arrow().await.map_err(LakehouseError::Iceberg)?;

    // Keep the latest snapshot per collection (highest written_at_ms wins).
    let mut latest: std::collections::HashMap<String, (i64, CollectionSnapshot)> =
        std::collections::HashMap::new();

    while let Some(batch) = stream.try_next().await.map_err(LakehouseError::Iceberg)? {
        let schema = batch.schema();

        let name_idx = schema
            .index_of("collection_name")
            .map_err(|_| LakehouseError::ColumnNotFound("collection_name".to_string()))?;
        let blob_idx = schema
            .index_of("snapshot_blob")
            .map_err(|_| LakehouseError::ColumnNotFound("snapshot_blob".to_string()))?;
        let ts_idx = schema
            .index_of("written_at_ms")
            .map_err(|_| LakehouseError::ColumnNotFound("written_at_ms".to_string()))?;

        let names = batch
            .column(name_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| LakehouseError::Schema("collection_name not Utf8".to_string()))?;
        let blobs = batch
            .column(blob_idx)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| LakehouseError::Schema("snapshot_blob not LargeBinary".to_string()))?;
        let timestamps = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| LakehouseError::Schema("written_at_ms not Int64".to_string()))?;

        for row in 0..batch.num_rows() {
            let name = names.value(row).to_string();
            let ts = timestamps.value(row);

            if let Some((existing_ts, _)) = latest.get(&name) {
                if ts <= *existing_ts {
                    continue;
                }
            }

            let snap: CollectionSnapshot = bincode::deserialize(blobs.value(row))
                .map_err(|e| LakehouseError::IndexBlob(format!("snapshot decode '{name}': {e}")))?;

            latest.insert(name, (ts, snap));
        }
    }

    Ok(latest.into_values().map(|(_, s)| s).collect())
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

/// Canonical `TableIdent` for the shared index-snapshot table.
pub fn index_snapshot_table_ident(namespace: &NamespaceIdent) -> TableIdent {
    TableIdent::new(namespace.clone(), "likhadb_index_snapshots".to_string())
}

#[cfg(test)]
mod tests {
    use likhadb_core::Metric;
    use likhadb_index::IndexSnapshot;
    use likhadb_store::{CollectionManager, ManagerSnapshot};

    use super::*;

    fn make_snap(name: &str, dim: usize) -> CollectionSnapshot {
        let mut mgr = CollectionManager::new();
        mgr.create_collection(name, dim, Metric::L2).unwrap();
        let col = mgr.get_mut(name).unwrap();
        for i in 0..10u64 {
            col.insert(i, vec![i as f32, 0.0, 0.0, 0.0], None).unwrap();
        }
        let snap = mgr.to_snapshot_with_lsn(0);
        snap.collections.into_iter().next().unwrap()
    }

    fn make_hnsw_snap() -> CollectionSnapshot {
        let mut mgr = CollectionManager::new();
        mgr.create_hnsw_collection("hnsw", 4, Metric::L2, 4, 8, 10)
            .unwrap();
        let col = mgr.get_mut("hnsw").unwrap();
        for i in 0..20u64 {
            col.insert(i, vec![i as f32, 0.0, 0.0, 0.0], None).unwrap();
        }
        let snap = mgr.to_snapshot_with_lsn(0);
        snap.collections.into_iter().next().unwrap()
    }

    #[test]
    fn round_trip_flat_snapshot_bincode() {
        let snap = make_snap("flat", 4);
        let blob = bincode::serialize(&snap).unwrap();
        let recovered: CollectionSnapshot = bincode::deserialize(&blob).unwrap();

        let mgr = CollectionManager::from_snapshot(ManagerSnapshot {
            collections: vec![recovered],
            last_lsn: 0,
        });
        let result = mgr
            .get("flat")
            .unwrap()
            .search(&[0.0; 4], 1, None, false)
            .unwrap();
        assert_eq!(result[0].id, 0);
    }

    #[test]
    fn round_trip_hnsw_snapshot_bincode() {
        let snap = make_hnsw_snap();
        let blob = bincode::serialize(&snap).unwrap();
        let recovered: CollectionSnapshot = bincode::deserialize(&blob).unwrap();

        let mgr = CollectionManager::from_snapshot(ManagerSnapshot {
            collections: vec![recovered],
            last_lsn: 0,
        });
        let result = mgr
            .get("hnsw")
            .unwrap()
            .search(&[0.0; 4], 1, None, false)
            .unwrap();
        assert_eq!(result[0].id, 0);
    }
}
