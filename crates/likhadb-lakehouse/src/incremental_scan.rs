//! Snapshot-diff scanning for incremental index maintenance (RFC §6.1).
//!
//! `iceberg-rs` 0.4 has no incremental scan and its high-level `TableScan`
//! errors on any delete file (see `spike_iceberg_incremental_scan.md`), so this
//! module hand-rolls a **manifest-level diff** from the public `spec` APIs:
//!
//! - Walk `parent_snapshot_id` from `to` back to `from` to bound the range.
//! - In `to`'s manifest list, select manifests whose `added_snapshot_id` falls
//!   in `(from, to]`. Within them, `Added` data files are new rows and `Deleted`
//!   data files are drops; rows are read directly via the parquet reader (not
//!   `table.scan()`).
//! - Row-level (position/equality) delete files are **not** resolved in v1 — they
//!   are counted in [`DeltaScanResult::unresolved_delete_files`] so a guardrail
//!   metric can alert, per the RFC's minimal cut (§13.3).

use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};
use iceberg::table::Table;
use likhadb_core::SourceBinding;
use likhadb_store::DeltaRow;

use crate::error::LakehouseError;
use crate::parquet_io::{batch_to_ids, batch_to_vectors};

/// The snapshot range to diff. `from_snapshot_id == None` requests a full scan
/// of `to` (first bind, or the expired-snapshot fallback).
#[derive(Debug, Clone, Copy)]
pub struct SnapshotDelta {
    pub from_snapshot_id: Option<i64>,
    pub to_snapshot_id: i64,
}

/// The ordered changes between two snapshots, plus a count of delete files we
/// could not resolve (always 0 today; non-zero means the source uses v2
/// merge-on-read deletes that v1 does not yet handle).
#[derive(Default)]
pub struct DeltaScanResult {
    pub rows: Vec<DeltaRow>,
    pub unresolved_delete_files: usize,
}

/// One change at data-file granularity, tagged with the snapshot that produced
/// it so the whole range can be applied in commit order.
struct FileChange {
    snapshot_id: i64,
    path: String,
    kind: ChangeKind,
}

enum ChangeKind {
    AddedData,
    DroppedData,
}

/// Diff `table` between the two snapshots in `delta` and materialise the rows.
pub async fn scan_delta(
    table: &Table,
    delta: SnapshotDelta,
    binding: &SourceBinding,
) -> Result<DeltaScanResult, LakehouseError> {
    let metadata = table.metadata();
    let file_io = table.file_io();

    let to_snapshot = metadata
        .snapshot_by_id(delta.to_snapshot_id)
        .ok_or_else(|| {
            LakehouseError::Schema(format!("to-snapshot {} not found", delta.to_snapshot_id))
        })?;

    // Bound the range. `None` ⇒ full scan (every live data file at `to`).
    // Otherwise collect the snapshot ids in `(from, to]` by walking parents; if
    // `from` is not an ancestor of `to` the range is unreconstructable and the
    // caller must fall back to a full rescan.
    let range: Option<std::collections::HashSet<i64>> = match delta.from_snapshot_id {
        None => None,
        Some(from) => {
            let mut ids = std::collections::HashSet::new();
            let mut cursor = Some(to_snapshot.clone());
            let mut reached = false;
            while let Some(snap) = cursor {
                if snap.snapshot_id() == from {
                    reached = true;
                    break;
                }
                ids.insert(snap.snapshot_id());
                cursor = snap
                    .parent_snapshot_id()
                    .and_then(|pid| metadata.snapshot_by_id(pid).cloned());
            }
            if !reached {
                return Err(LakehouseError::Schema(format!(
                    "from-snapshot {from} is not an ancestor of {}; full rescan required",
                    delta.to_snapshot_id
                )));
            }
            Some(ids)
        }
    };

    let manifest_list = to_snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(LakehouseError::Iceberg)?;

    let mut changes: Vec<FileChange> = Vec::new();
    let mut unresolved_delete_files = 0usize;

    for manifest_file in manifest_list.entries() {
        // Scope to the range (full scan keeps everything).
        let in_range = range
            .as_ref()
            .is_none_or(|ids| ids.contains(&manifest_file.added_snapshot_id));
        if !in_range {
            continue;
        }

        if manifest_file.content == ManifestContentType::Deletes {
            // v1 cannot resolve row-level deletes; surface them for alerting.
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(LakehouseError::Iceberg)?;
            unresolved_delete_files += manifest.entries().iter().filter(|e| e.is_alive()).count();
            continue;
        }

        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(LakehouseError::Iceberg)?;
        let owner = manifest_file.added_snapshot_id;

        for entry in manifest.entries() {
            if entry.content_type() != DataContentType::Data {
                unresolved_delete_files += 1;
                continue;
            }
            match (range.is_some(), entry.status()) {
                // Incremental: only entries changed within the range matter.
                (true, ManifestStatus::Added) => changes.push(FileChange {
                    snapshot_id: owner,
                    path: entry.data_file().file_path().to_string(),
                    kind: ChangeKind::AddedData,
                }),
                (true, ManifestStatus::Deleted) => changes.push(FileChange {
                    snapshot_id: owner,
                    path: entry.data_file().file_path().to_string(),
                    kind: ChangeKind::DroppedData,
                }),
                (true, ManifestStatus::Existing) => {}
                // Full scan: every live data file is an upsert.
                (false, _) if entry.is_alive() => changes.push(FileChange {
                    snapshot_id: owner,
                    path: entry.data_file().file_path().to_string(),
                    kind: ChangeKind::AddedData,
                }),
                (false, _) => {}
            }
        }
    }

    // Apply in commit order so a re-add after a drop (or vice versa) within the
    // range resolves last-writer-wins.
    changes.sort_by_key(|c| c.snapshot_id);

    let payload_cols: Vec<&str> = binding.payload_columns.iter().map(String::as_str).collect();
    let mut rows: Vec<DeltaRow> = Vec::new();

    for change in changes {
        let data = file_io
            .new_input(&change.path)
            .map_err(LakehouseError::Iceberg)?
            .read()
            .await
            .map_err(LakehouseError::Iceberg)?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data)?
            .build()?;
        for batch in reader {
            let batch = batch?;
            match change.kind {
                ChangeKind::AddedData => {
                    for (id, vector, payload) in batch_to_vectors(
                        &batch,
                        &binding.id_column,
                        &binding.vector_column,
                        &payload_cols,
                    )? {
                        rows.push(DeltaRow::Upsert {
                            id,
                            vector,
                            payload,
                        });
                    }
                }
                ChangeKind::DroppedData => {
                    for id in batch_to_ids(&batch, &binding.id_column)? {
                        rows.push(DeltaRow::Delete { id });
                    }
                }
            }
        }
    }

    Ok(DeltaScanResult {
        rows,
        unresolved_delete_files,
    })
}
