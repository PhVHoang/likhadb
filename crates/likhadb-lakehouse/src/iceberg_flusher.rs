use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iceberg::NamespaceIdent;
use likhadb_persist::wal::WalOp;
use likhadb_persist::WalManager;
use tokio::sync::RwLock;

use crate::error::LakehouseError;
use crate::iceberg_io::{build_rest_catalog, IcebergConfig};
use crate::staging_io::{append_to_staging, get_or_create_staging_table, StagingBatch, StagingRow};

pub struct IcebergFlusher {
    wal: Arc<RwLock<WalManager>>,
    config: IcebergConfig,
    namespace: NamespaceIdent,
    flush_interval: Duration,
    max_batch_entries: usize,
}

impl IcebergFlusher {
    pub fn new(
        wal: Arc<RwLock<WalManager>>,
        config: IcebergConfig,
        namespace: NamespaceIdent,
    ) -> Self {
        Self {
            wal,
            config,
            namespace,
            flush_interval: Duration::from_millis(100),
            max_batch_entries: 500,
        }
    }

    pub fn with_interval(mut self, d: Duration) -> Self {
        self.flush_interval = d;
        self
    }

    pub fn with_max_batch(mut self, n: usize) -> Self {
        self.max_batch_entries = n;
        self
    }

    /// Spawn a background tokio task that flushes WAL inserts to Iceberg staging
    /// at `flush_interval` cadence.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(self) {
        let mut ticker = tokio::time::interval(self.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            if let Err(e) = self.flush_once().await {
                metrics::counter!("likhadb_iceberg_flush_errors_total").increment(1);
                tracing::warn!(error = %e, "iceberg flush error");
            }
        }
    }

    async fn flush_once(&self) -> Result<(), LakehouseError> {
        let start = std::time::Instant::now();

        // 1. Drain pending entries under a brief read lock — no I/O while locked.
        let (entries, current_watermark) = {
            let guard = self.wal.read().await;
            let entries = guard
                .collect_unflushed()
                .into_iter()
                .take(self.max_batch_entries)
                .collect::<Vec<_>>();
            let watermark = guard.iceberg_watermark();
            (entries, watermark)
        };

        if entries.is_empty() {
            return Ok(());
        }

        // 2. Group Insert and Delete ops by collection name; track highest LSN
        //    and whether the batch contains any DDL ops (which are not yet
        //    mirrored to staging and therefore block safe WAL truncation).
        let mut batches: HashMap<String, (Vec<StagingRow>, u64)> = HashMap::new();
        let mut max_lsn: u64 = current_watermark;
        let mut has_ddl = false;

        for entry in &entries {
            max_lsn = max_lsn.max(entry.lsn);
            match &entry.op {
                WalOp::Insert {
                    collection,
                    id,
                    vector,
                    payload,
                } => {
                    let (rows, batch_max) = batches
                        .entry(collection.clone())
                        .or_insert_with(|| (Vec::new(), 0));
                    rows.push(StagingRow {
                        id: *id,
                        vector: vector.clone(),
                        payload: payload.clone(),
                        lsn: entry.lsn,
                        is_tombstone: false,
                    });
                    *batch_max = (*batch_max).max(entry.lsn);
                }
                WalOp::Delete { collection, id } => {
                    let (rows, batch_max) = batches
                        .entry(collection.clone())
                        .or_insert_with(|| (Vec::new(), 0));
                    rows.push(StagingRow {
                        id: *id,
                        vector: Vec::new(),
                        payload: None,
                        lsn: entry.lsn,
                        is_tombstone: true,
                    });
                    *batch_max = (*batch_max).max(entry.lsn);
                }
                // DDL ops (CreateCollection, DropCollection, EnableFts) are not
                // mirrored to staging yet — flag them so we skip WAL truncation.
                _ => {
                    has_ddl = true;
                }
            }
        }

        if batches.is_empty() {
            // DDL-only batch — advance watermark but skip WAL truncation.
            if max_lsn > current_watermark {
                self.wal.write().await.set_iceberg_watermark(max_lsn);
            }
            return Ok(());
        }

        // 3. Flush each collection's batch to Iceberg staging (no lock held).
        let catalog = build_rest_catalog(&self.config)
            .map_err(|e| LakehouseError::Schema(format!("catalog build: {e}")))?;

        let mut flush_errors = 0usize;
        for (collection_name, (rows, batch_max_lsn)) in batches {
            let table = match get_or_create_staging_table(
                &catalog,
                &self.namespace,
                &collection_name,
            )
            .await
            {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(
                        collection = %collection_name,
                        error = %e,
                        "failed to open staging table"
                    );
                    flush_errors += 1;
                    continue;
                }
            };

            let batch = StagingBatch {
                collection_name: collection_name.clone(),
                rows,
            };
            if let Err(e) = append_to_staging(&catalog, &table, &batch, batch_max_lsn).await {
                tracing::warn!(
                    collection = %collection_name,
                    error = %e,
                    "failed to append to staging"
                );
                flush_errors += 1;
            }
        }

        // 4. Advance watermark and, when safe, truncate the WAL.
        //    Truncation is safe when:
        //    (a) all collections flushed successfully (both inserts and tombstones), AND
        //    (b) the batch contained no DDL ops (CreateCollection / DropCollection /
        //        EnableFts are not yet mirrored to staging).
        if flush_errors == 0 {
            let mut guard = self.wal.write().await;
            guard.set_iceberg_watermark(max_lsn);
            if !has_ddl {
                if let Err(e) = guard.truncate_wal_up_to(max_lsn) {
                    metrics::counter!("likhadb_iceberg_flush_errors_total").increment(1);
                    tracing::warn!(error = %e, "wal truncation failed");
                }
            }
            drop(guard);
            metrics::counter!("likhadb_iceberg_flush_total").increment(1);
        }

        metrics::histogram!("likhadb_iceberg_flush_duration_seconds")
            .record(start.elapsed().as_secs_f64());

        Ok(())
    }
}
