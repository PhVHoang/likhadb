//! Background synchronization of bound collections from Iceberg snapshot deltas.

use std::sync::Arc;
use std::time::Duration;

use iceberg::Catalog;
use likhadb_core::SourceBinding;
use likhadb_persist::WalManager;
use tokio::sync::RwLock;

use crate::{load_source_table, scan_delta, LakehouseError, SnapshotDelta};

const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct BoundCollection {
    name: String,
    binding: SourceBinding,
    source_snapshot_id: Option<i64>,
}

/// Polls source Iceberg tables and applies committed snapshot deltas to the
/// corresponding live collections.
pub struct IndexMaintenanceTask {
    wal: Arc<RwLock<WalManager>>,
    catalog: Arc<dyn Catalog>,
    interval: Duration,
}

impl IndexMaintenanceTask {
    pub fn new(wal: Arc<RwLock<WalManager>>, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            wal,
            catalog,
            interval: DEFAULT_INTERVAL,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Spawn the periodic maintenance loop.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            self.run_once().await;
        }
    }

    /// Run one maintenance tick across every currently bound collection.
    ///
    /// Failures are isolated per collection so one unavailable or malformed
    /// source does not prevent other collections from advancing.
    pub async fn run_once(&self) {
        let collections = self.bound_collections().await;
        for collection in collections {
            if let Err(error) = self.maintain_collection(&collection).await {
                tracing::warn!(
                    collection = %collection.name,
                    error = %error,
                    "index maintenance tick failed"
                );
            }
        }
    }

    async fn bound_collections(&self) -> Vec<BoundCollection> {
        let guard = self.wal.read().await;
        guard
            .list()
            .into_iter()
            .filter_map(|name| {
                let collection = guard.get(name).ok()?;
                Some(BoundCollection {
                    name: name.to_owned(),
                    binding: collection.source_binding.clone()?,
                    source_snapshot_id: collection.source_snapshot_id,
                })
            })
            .collect()
    }

    async fn maintain_collection(
        &self,
        collection: &BoundCollection,
    ) -> Result<(), LakehouseError> {
        // First-bind full scans are deliberately handled by issue #105. Until
        // then, do not silently establish a baseline that omits existing rows.
        let Some(from_snapshot_id) = collection.source_snapshot_id else {
            tracing::debug!(
                collection = %collection.name,
                "source snapshot watermark is unset; waiting for first-bind scan"
            );
            return Ok(());
        };

        // Catalog and file I/O happen without holding the store lock.
        let table = load_source_table(self.catalog.as_ref(), &collection.binding).await?;
        let Some(to_snapshot_id) = table.metadata().current_snapshot_id() else {
            return Ok(());
        };
        if to_snapshot_id == from_snapshot_id {
            return Ok(());
        }

        let result = scan_delta(
            &table,
            SnapshotDelta {
                from_snapshot_id: Some(from_snapshot_id),
                to_snapshot_id,
            },
            &collection.binding,
        )
        .await?;
        let row_count = result.rows.len();

        // Revalidate the state observed before the scan while holding the same
        // write lock used to apply every row and advance the watermark.
        let applied = self.wal.write().await.apply_source_delta(
            &collection.name,
            &collection.binding,
            Some(from_snapshot_id),
            to_snapshot_id,
            result.rows,
        )?;
        if !applied {
            tracing::debug!(
                collection = %collection.name,
                from_snapshot_id,
                to_snapshot_id,
                "discarding stale source snapshot scan"
            );
            return Ok(());
        }

        tracing::info!(
            collection = %collection.name,
            from_snapshot_id,
            to_snapshot_id,
            rows_applied = row_count,
            unresolved_delete_files = result.unresolved_delete_files,
            "source snapshot delta applied"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use iceberg::arrow::arrow_schema_to_schema;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use likhadb_core::Metric;
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn default_poll_interval_is_sixty_seconds() {
        assert_eq!(DEFAULT_INTERVAL, Duration::from_secs(60));
    }

    fn field(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            id.to_string(),
        )]))
    }

    fn arrow_schema() -> Arc<ArrowSchema> {
        let vector_element = Arc::new(field("element", DataType::Float32, false, 3));
        Arc::new(ArrowSchema::new(vec![
            field("id", DataType::Int64, false, 1),
            field(
                "embedding",
                DataType::FixedSizeList(vector_element, 2),
                false,
                2,
            ),
        ]))
    }

    fn parquet_bytes(id: i64, vector: [f32; 2]) -> Vec<u8> {
        let schema = arrow_schema();
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![id]));
        let vectors: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(field("element", DataType::Float32, false, 3)),
                2,
                Arc::new(Float32Array::from(vector.to_vec())),
                None,
            )
            .unwrap(),
        );
        let batch = RecordBatch::try_new(schema.clone(), vec![ids, vectors]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    async fn append_row(
        catalog: &dyn Catalog,
        table: &iceberg::table::Table,
        file_name: &str,
        id: i64,
        vector: [f32; 2],
    ) -> iceberg::table::Table {
        let bytes = parquet_bytes(id, vector);
        let file_path = format!("{}/data/{file_name}.parquet", table.metadata().location());
        table
            .file_io()
            .new_output(&file_path)
            .unwrap()
            .write(Bytes::from(bytes.clone()))
            .await
            .unwrap();
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path)
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(1)
            .file_size_in_bytes(bytes.len() as u64)
            .build()
            .unwrap();
        let tx = Transaction::new(table);
        let tx = tx
            .fast_append()
            .add_data_files([data_file])
            .apply(tx)
            .unwrap();
        tx.commit(catalog).await.unwrap()
    }

    #[tokio::test]
    async fn tick_applies_external_append_and_advances_watermark() {
        let warehouse = TempDir::new().unwrap();
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "maintenance-test",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        format!("file://{}", warehouse.path().display()),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("source".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let table_ident = TableIdent::new(namespace.clone(), "vectors".to_string());
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(table_ident.name().to_string())
                    .schema(arrow_schema_to_schema(arrow_schema().as_ref()).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        let table = append_row(catalog.as_ref(), &table, "baseline", 1, [1.0, 0.0]).await;
        let baseline_snapshot = table.metadata().current_snapshot_id().unwrap();

        let data_dir = TempDir::new().unwrap();
        let mut wal = WalManager::open(data_dir.path()).unwrap();
        wal.create_hnsw_collection("documents", 2, Metric::L2, 4, 8, 10)
            .unwrap();
        let binding = SourceBinding {
            source_namespace: namespace.as_ref().clone(),
            source_table: table_ident.name().to_string(),
            id_column: "id".to_string(),
            vector_column: "embedding".to_string(),
            payload_columns: vec![],
        };
        wal.set_source_binding("documents", binding.clone())
            .unwrap();
        wal.apply_source_delta("documents", &binding, None, baseline_snapshot, [])
            .unwrap();

        // Reloading before the append models a writer independent from the
        // maintenance task's later catalog read.
        let writer_table = catalog.load_table(&table_ident).await.unwrap();
        let writer_table =
            append_row(catalog.as_ref(), &writer_table, "external", 2, [2.0, 0.0]).await;
        let external_snapshot = writer_table.metadata().current_snapshot_id().unwrap();

        let wal = Arc::new(RwLock::new(wal));
        let task = IndexMaintenanceTask::new(wal.clone(), catalog);
        task.run_once().await;

        let guard = wal.read().await;
        let collection = guard.get("documents").unwrap();
        assert_eq!(collection.source_snapshot_id, Some(external_snapshot));
        assert_eq!(
            collection.search(&[2.0, 0.0], 1, None, false).unwrap()[0].id,
            2
        );
        assert!(collection.get(1).unwrap().is_none());
    }
}
