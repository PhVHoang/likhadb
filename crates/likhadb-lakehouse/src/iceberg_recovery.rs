use std::path::Path;

use iceberg::NamespaceIdent;
use likhadb_persist::{PersistError, WalManager};
use likhadb_store::{CollectionManager, CollectionSnapshot, ManagerSnapshot};

use crate::error::LakehouseError;
use crate::iceberg_io::{build_rest_catalog, IcebergConfig};
use crate::index_snapshot_io::{index_snapshot_table_ident, load_collection_snapshots};
use crate::staging_io::{get_or_create_staging_table, read_watermark, scan_pending};

/// Unified error for the Iceberg recovery startup path.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("lakehouse: {0}")]
    Lakehouse(#[from] LakehouseError),
    #[error("persist: {0}")]
    Persist(#[from] PersistError),
}

/// Open a `WalManager` using Iceberg as the primary recovery source.
///
/// # Recovery sequence
/// 1. Load index snapshots from `likhadb_index_snapshots` → build `CollectionManager`.
///    If no snapshots exist yet, start from an empty manager.
/// 2. For each collection, scan the staging table for pending vectors and apply
///    them in-memory (bridging the gap between the last full snapshot and the
///    current staging watermark).
/// 3. Replay the WAL for any entries with LSN above the snapshot's `last_lsn`,
///    covering both inserts and deletes that have not yet been staged or snapshotted.
///
/// The WAL is NOT truncated here — that requires separate coordination with
/// delete tombstone mirroring in staging.
pub async fn open_with_iceberg(
    dir: &Path,
    config: &IcebergConfig,
    namespace: NamespaceIdent,
) -> Result<WalManager, RecoveryError> {
    let catalog = build_rest_catalog(config)
        .map_err(|e| LakehouseError::Schema(format!("catalog build: {e}")))?;

    // 1. Load index snapshots.
    let index_table = index_snapshot_table_ident(&namespace);
    let snapshots: Vec<CollectionSnapshot> =
        load_collection_snapshots(&catalog, &index_table).await?;

    if snapshots.is_empty() {
        tracing::info!("no Iceberg index snapshots found — falling back to WAL-only recovery");
        let wal = WalManager::open(dir)?;
        return Ok(wal);
    }

    tracing::info!(
        collections = snapshots.len(),
        "loaded Iceberg index snapshots"
    );

    let manager_snap = ManagerSnapshot {
        collections: snapshots,
        last_lsn: 0, // WAL replay from 0 covers full history; staging watermark guards progress.
    };
    let mut manager = CollectionManager::from_snapshot(manager_snap, None);

    // 2. Apply pending staging rows for each collection.
    let mut max_watermark: u64 = 0;
    for name in manager
        .list()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>()
    {
        let staging = get_or_create_staging_table(&catalog, &namespace, &name).await?;
        let watermark = read_watermark(&staging);
        max_watermark = max_watermark.max(watermark);

        let (entries, _) = scan_pending(&staging).await?;
        if entries.is_empty() {
            continue;
        }
        let inserts = entries.iter().filter(|e| !e.is_delete).count();
        let deletes = entries.iter().filter(|e| e.is_delete).count();
        tracing::info!(
            collection = %name,
            inserts,
            deletes,
            "applying pending staging entries"
        );
        let col = manager
            .get_mut(&name)
            .map_err(|e| LakehouseError::Schema(format!("get_mut '{name}': {e}")))?;
        // Entries are already sorted by LSN from scan_pending, so insert/delete
        // order is correct even when the same vector ID appears in both.
        for entry in entries {
            if entry.is_delete {
                let _ = col.delete(entry.id, u64::MAX);
            } else {
                let _ = col.insert(entry.id, entry.vector, entry.payload, u64::MAX);
            }
        }
    }

    // 3. Replay the full WAL from LSN=0 so deletes and DDL below the staging
    //    watermark are correctly applied (WAL truncation is not yet active).
    //    Then set the watermark from staging so the background flusher can
    //    track progress without re-sending already-staged entries.
    let mut wal = WalManager::open_from_iceberg_state(dir, manager, 0)?;
    wal.set_iceberg_watermark(max_watermark);
    Ok(wal)
}
