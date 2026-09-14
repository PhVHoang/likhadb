//! Background synchronization of bound collections from Iceberg snapshot deltas.

use std::sync::Arc;
use std::time::Duration;

use iceberg::Catalog;
use likhadb_core::SourceBinding;
use likhadb_persist::WalManager;
use likhadb_store::DeltaRow;
use tokio::sync::RwLock;

use crate::{load_source_table, scan_delta, LakehouseError, MaintenanceConfig, SnapshotDelta};

const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct BoundCollection {
    name: String,
    binding: SourceBinding,
    source_snapshot_id: Option<i64>,
    tombstone_ratio: f32,
}

fn initialize_metrics(collection: &BoundCollection) {
    let name = collection.name.clone();
    metrics::gauge!("likhadb_source_snapshot_lag", "collection" => name.clone()).set(f64::NAN);
    metrics::gauge!("likhadb_index_tombstone_ratio", "collection" => name.clone())
        .set(collection.tombstone_ratio as f64);
    for op in ["upsert", "delete"] {
        metrics::counter!(
            "likhadb_delta_rows_applied_total",
            "collection" => name.clone(),
            "op" => op
        )
        .increment(0);
    }
    for result in ["success", "failure", "stale"] {
        metrics::counter!(
            "likhadb_index_compactions_total",
            "collection" => name.clone(),
            "result" => result
        )
        .increment(0);
    }
    metrics::counter!("likhadb_source_full_rescan_total", "collection" => name.clone())
        .increment(0);
    metrics::counter!(
        "likhadb_unresolved_delete_files_total",
        "collection" => name
    )
    .increment(0);
}

fn snapshot_lag_seconds(
    table: &iceberg::table::Table,
    from_snapshot_id: Option<i64>,
    to_snapshot_id: i64,
) -> f64 {
    let metadata = table.metadata();
    let Some(from) = from_snapshot_id.and_then(|id| metadata.snapshot_by_id(id)) else {
        return f64::NAN;
    };
    let Some(to) = metadata.snapshot_by_id(to_snapshot_id) else {
        return f64::NAN;
    };
    // Iceberg stores snapshot timestamps in milliseconds. Expose their
    // non-negative age difference in seconds while retaining the RFC's metric
    // name for compatibility.
    to.timestamp_ms().saturating_sub(from.timestamp_ms()).max(0) as f64 / 1_000.0
}

fn row_counts(rows: &[DeltaRow]) -> (u64, u64) {
    rows.iter()
        .fold((0, 0), |(upserts, deletes), row| match row {
            DeltaRow::Upsert { .. } => (upserts + 1, deletes),
            DeltaRow::Delete { .. } => (upserts, deletes + 1),
        })
}

fn record_compaction(collection: &str, result: &'static str) {
    metrics::counter!(
        "likhadb_index_compactions_total",
        "collection" => collection.to_owned(),
        "result" => result
    )
    .increment(1);
}

/// Polls source Iceberg tables and applies committed snapshot deltas to the
/// corresponding live collections.
pub struct IndexMaintenanceTask {
    wal: Arc<RwLock<WalManager>>,
    catalog: Arc<dyn Catalog>,
    interval: Duration,
    hnsw_compaction_tombstone_ratio: f32,
}

impl IndexMaintenanceTask {
    pub fn new(wal: Arc<RwLock<WalManager>>, catalog: Arc<dyn Catalog>) -> Self {
        let config = MaintenanceConfig::default();
        Self {
            wal,
            catalog,
            interval: DEFAULT_INTERVAL,
            hnsw_compaction_tombstone_ratio: config.hnsw_compaction_tombstone_ratio,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_hnsw_compaction_tombstone_ratio(mut self, threshold: f32) -> Self {
        self.hnsw_compaction_tombstone_ratio = threshold;
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
            initialize_metrics(&collection);
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
                    tombstone_ratio: collection.tombstone_ratio(),
                })
            })
            .collect()
    }

    async fn maintain_collection(
        &self,
        collection: &BoundCollection,
    ) -> Result<(), LakehouseError> {
        // Catalog and file I/O happen without holding the store lock.
        let table = load_source_table(self.catalog.as_ref(), &collection.binding).await?;
        let Some(to_snapshot_id) = table.metadata().current_snapshot_id() else {
            return Ok(());
        };
        let from_snapshot_id = collection.source_snapshot_id;
        metrics::gauge!(
            "likhadb_source_snapshot_lag",
            "collection" => collection.name.clone()
        )
        .set(snapshot_lag_seconds(
            &table,
            from_snapshot_id,
            to_snapshot_id,
        ));
        if from_snapshot_id == Some(to_snapshot_id) {
            metrics::gauge!(
                "likhadb_source_snapshot_lag",
                "collection" => collection.name.clone()
            )
            .set(0.0);
            return Ok(());
        }

        if from_snapshot_id.is_none() {
            tracing::info!(
                collection = %collection.name,
                to_snapshot_id,
                "source snapshot watermark is unset; running first-bind full scan"
            );
        }

        let mut full_rescan = from_snapshot_id.is_none();
        let result = match scan_delta(
            &table,
            SnapshotDelta {
                from_snapshot_id,
                to_snapshot_id,
            },
            &collection.binding,
        )
        .await
        {
            Ok(result) => result,
            Err(LakehouseError::NonAncestorSnapshot { from, to }) => {
                full_rescan = true;
                tracing::warn!(
                    collection = %collection.name,
                    from_snapshot_id = from,
                    to_snapshot_id = to,
                    "source snapshot watermark is not an ancestor; falling back to full scan"
                );
                scan_delta(
                    &table,
                    SnapshotDelta {
                        from_snapshot_id: None,
                        to_snapshot_id,
                    },
                    &collection.binding,
                )
                .await?
            }
            Err(error) => return Err(error),
        };
        let row_count = result.rows.len();
        let (upserts, deletes) = row_counts(&result.rows);
        let unresolved_delete_files = result.unresolved_delete_files;

        // Revalidate the state observed before the scan while holding the same
        // write lock used to apply every row and advance the watermark.
        let (applied, tombstone_ratio) = {
            let mut wal = self.wal.write().await;
            let applied = wal.apply_source_delta(
                &collection.name,
                &collection.binding,
                from_snapshot_id,
                to_snapshot_id,
                result.rows,
            )?;
            let tombstone_ratio = if applied {
                wal.get(&collection.name)?.tombstone_ratio()
            } else {
                collection.tombstone_ratio
            };
            (applied, tombstone_ratio)
        };
        if !applied {
            tracing::debug!(
                collection = %collection.name,
                ?from_snapshot_id,
                to_snapshot_id,
                "discarding stale source snapshot scan"
            );
            return Ok(());
        }

        metrics::counter!(
            "likhadb_delta_rows_applied_total",
            "collection" => collection.name.clone(),
            "op" => "upsert"
        )
        .increment(upserts);
        metrics::counter!(
            "likhadb_delta_rows_applied_total",
            "collection" => collection.name.clone(),
            "op" => "delete"
        )
        .increment(deletes);
        metrics::counter!(
            "likhadb_unresolved_delete_files_total",
            "collection" => collection.name.clone()
        )
        .increment(unresolved_delete_files as u64);
        if full_rescan {
            metrics::counter!(
                "likhadb_source_full_rescan_total",
                "collection" => collection.name.clone()
            )
            .increment(1);
        }
        metrics::gauge!(
            "likhadb_source_snapshot_lag",
            "collection" => collection.name.clone()
        )
        .set(0.0);
        metrics::gauge!(
            "likhadb_index_tombstone_ratio",
            "collection" => collection.name.clone()
        )
        .set(tombstone_ratio as f64);

        tracing::info!(
            collection = %collection.name,
            ?from_snapshot_id,
            to_snapshot_id,
            rows_applied = row_count,
            unresolved_delete_files,
            full_rescan,
            "source snapshot maintenance applied"
        );
        drop(self.enqueue_hnsw_compaction(&collection.name).await?);
        Ok(())
    }

    /// Capture a compaction plan under the store lock, then rebuild on a
    /// blocking worker. The worker reacquires the lock only to replay mutations
    /// recorded since the snapshot and swap the index pointer.
    async fn enqueue_hnsw_compaction(
        &self,
        collection: &str,
    ) -> Result<Option<tokio::task::JoinHandle<()>>, LakehouseError> {
        let prepared = match self
            .wal
            .write()
            .await
            .prepare_index_compaction(collection, self.hnsw_compaction_tombstone_ratio)
        {
            Ok(prepared) => prepared,
            Err(error) => {
                record_compaction(collection, "failure");
                return Err(error.into());
            }
        };
        let Some(prepared) = prepared else {
            return Ok(None);
        };

        let wal = self.wal.clone();
        let collection = collection.to_owned();
        let threshold = self.hnsw_compaction_tombstone_ratio;
        tracing::info!(
            collection = %collection,
            tombstone_threshold = threshold,
            "HNSW index compaction started"
        );

        Ok(Some(tokio::spawn(async move {
            let build = tokio::task::spawn_blocking(move || prepared.build()).await;
            match build {
                Ok(Ok(built)) => {
                    let finish = wal
                        .write()
                        .await
                        .finish_index_compaction(&collection, built);
                    match finish {
                        Ok(true) => {
                            record_compaction(&collection, "success");
                            let tombstone_ratio = wal
                                .read()
                                .await
                                .get(&collection)
                                .map(|value| value.tombstone_ratio())
                                .ok();
                            if let Some(tombstone_ratio) = tombstone_ratio {
                                metrics::gauge!(
                                    "likhadb_index_tombstone_ratio",
                                    "collection" => collection.clone()
                                )
                                .set(tombstone_ratio as f64);
                            }
                            tracing::info!(
                                collection = %collection,
                                "HNSW index compaction completed"
                            );
                        }
                        Ok(false) => {
                            record_compaction(&collection, "stale");
                            tracing::debug!(
                                collection = %collection,
                                "discarding stale HNSW index compaction"
                            );
                        }
                        Err(error) => {
                            record_compaction(&collection, "failure");
                            tracing::warn!(
                                collection = %collection,
                                error = %error,
                                "HNSW index compaction swap failed"
                            );
                        }
                    }
                }
                Ok(Err(error)) => {
                    record_compaction(&collection, "failure");
                    let cancel_error = wal.write().await.cancel_index_compaction(&collection);
                    tracing::warn!(
                        collection = %collection,
                        error = %error,
                        cancel_error = ?cancel_error.err(),
                        "HNSW index compaction build failed"
                    );
                }
                Err(error) => {
                    record_compaction(&collection, "failure");
                    let cancel_error = wal.write().await.cancel_index_compaction(&collection);
                    tracing::warn!(
                        collection = %collection,
                        error = %error,
                        cancel_error = ?cancel_error.err(),
                        "HNSW index compaction worker failed"
                    );
                }
            }
        })))
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

    async fn source_fixture(
        file_name: &str,
        id: i64,
        vector: [f32; 2],
    ) -> (
        TempDir,
        Arc<dyn Catalog>,
        SourceBinding,
        iceberg::table::Table,
    ) {
        let warehouse = TempDir::new().unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(
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
        let table = append_row(catalog.as_ref(), &table, file_name, id, vector).await;
        let binding = SourceBinding {
            source_namespace: namespace.as_ref().clone(),
            source_table: table_ident.name().to_string(),
            id_column: "id".to_string(),
            vector_column: "embedding".to_string(),
            payload_columns: vec![],
        };
        (warehouse, catalog, binding, table)
    }

    #[tokio::test]
    async fn first_bind_full_scan_ingests_current_source_snapshot() {
        let (_warehouse, catalog, binding, table) = source_fixture("baseline", 1, [1.0, 0.0]).await;
        let current_snapshot = table.metadata().current_snapshot_id().unwrap();

        let data_dir = TempDir::new().unwrap();
        let mut wal = WalManager::open(data_dir.path()).unwrap();
        wal.create_hnsw_collection("documents", 2, Metric::L2, 4, 8, 10)
            .unwrap();
        wal.set_source_binding("documents", binding).unwrap();

        let wal = Arc::new(RwLock::new(wal));
        let task = IndexMaintenanceTask::new(wal.clone(), catalog);
        task.run_once().await;

        let guard = wal.read().await;
        let collection = guard.get("documents").unwrap();
        assert_eq!(collection.source_snapshot_id, Some(current_snapshot));
        assert_eq!(
            collection.search(&[1.0, 0.0], 1, None, false).unwrap()[0].id,
            1
        );
    }

    #[tokio::test]
    async fn non_ancestor_watermark_falls_back_to_full_scan() {
        let (_warehouse, catalog, binding, table) =
            source_fixture("replacement", 7, [0.0, 1.0]).await;
        let current_snapshot = table.metadata().current_snapshot_id().unwrap();
        let expired_snapshot = i64::MAX;

        let data_dir = TempDir::new().unwrap();
        let mut wal = WalManager::open(data_dir.path()).unwrap();
        wal.create_hnsw_collection("documents", 2, Metric::L2, 4, 8, 10)
            .unwrap();
        wal.set_source_binding("documents", binding.clone())
            .unwrap();
        wal.apply_source_delta("documents", &binding, None, expired_snapshot, [])
            .unwrap();

        let wal = Arc::new(RwLock::new(wal));
        let task = IndexMaintenanceTask::new(wal.clone(), catalog);
        task.run_once().await;

        let guard = wal.read().await;
        let collection = guard.get("documents").unwrap();
        assert_eq!(collection.source_snapshot_id, Some(current_snapshot));
        assert_eq!(
            collection.search(&[0.0, 1.0], 1, None, false).unwrap()[0].id,
            7
        );
    }

    #[tokio::test]
    async fn tick_applies_external_append_and_compacts_hnsw() {
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
        for id in 10..20 {
            wal.insert("documents", id, vec![id as f32, 0.0], None)
                .unwrap();
        }
        for id in 10..13 {
            wal.delete("documents", id).unwrap();
        }
        assert!(wal.get("documents").unwrap().tombstone_ratio() > 0.2);

        // Reloading before the append models a writer independent from the
        // maintenance task's later catalog read.
        let writer_table = catalog.load_table(&table_ident).await.unwrap();
        let writer_table =
            append_row(catalog.as_ref(), &writer_table, "external", 2, [2.0, 0.0]).await;
        let external_snapshot = writer_table.metadata().current_snapshot_id().unwrap();

        let wal = Arc::new(RwLock::new(wal));
        let task = IndexMaintenanceTask::new(wal.clone(), catalog);
        task.run_once().await;

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if wal.read().await.get("documents").unwrap().tombstone_ratio() == 0.0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("HNSW compaction should finish without holding the store lock");

        let guard = wal.read().await;
        let collection = guard.get("documents").unwrap();
        assert_eq!(collection.source_snapshot_id, Some(external_snapshot));
        assert_eq!(collection.tombstone_ratio(), 0.0);
        assert_eq!(
            collection.search(&[2.0, 0.0], 1, None, false).unwrap()[0].id,
            2
        );
        assert!(collection.get(1).unwrap().is_none());
        for id in 10..13 {
            assert!(collection.get(id).unwrap().is_none());
        }
        for id in 13..20 {
            assert!(collection.get(id).unwrap().is_some());
        }
    }
}
