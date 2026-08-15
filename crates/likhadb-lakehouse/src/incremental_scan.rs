//! Snapshot-diff scanning for incremental index maintenance (RFC §6.1).
//!
//! `iceberg-rs` has no snapshot-range scan, so this module hand-rolls a
//! **manifest-level diff** from the public `spec` APIs:
//!
//! - Walk `parent_snapshot_id` from `to` back to `from` to bound the range.
//! - In `to`'s manifest list, select manifests whose `added_snapshot_id` falls
//!   in `(from, to]`. Within them, `Added` data files are new rows and `Deleted`
//!   data files are drops; rows are read directly via the parquet reader (not
//!   `table.scan()`).
//! - Commits containing position/equality delete files are resolved by comparing
//!   the parent and child snapshots through `iceberg-rs`' delete-aware
//!   `TableScan`, producing the same [`DeltaRow::Delete`] as dropped data files.

use std::collections::{HashMap, HashSet};

use futures_util::TryStreamExt;
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

/// The ordered changes between two snapshots, plus a guardrail count for any
/// delete files that could not be resolved.
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
    ResolvedDelete(u64),
}

impl ChangeKind {
    fn order(&self) -> u8 {
        match self {
            Self::DroppedData | Self::ResolvedDelete(_) => 0,
            Self::AddedData => 1,
        }
    }
}

async fn snapshot_ids(
    table: &Table,
    snapshot_id: i64,
    id_column: &str,
) -> Result<HashSet<u64>, LakehouseError> {
    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .select([id_column])
        .build()
        .map_err(LakehouseError::Iceberg)?;
    let mut stream = scan.to_arrow().await.map_err(LakehouseError::Iceberg)?;
    let mut ids = HashSet::new();
    while let Some(batch) = stream.try_next().await.map_err(LakehouseError::Iceberg)? {
        ids.extend(batch_to_ids(&batch, id_column)?);
    }
    Ok(ids)
}

fn removed_ids(before: &HashSet<u64>, after: &HashSet<u64>) -> Vec<u64> {
    before.difference(after).copied().collect()
}

fn sort_changes(changes: &mut [FileChange], commit_order: &HashMap<i64, usize>) {
    changes.sort_by_key(|change| {
        (
            commit_order
                .get(&change.snapshot_id)
                .copied()
                .unwrap_or(usize::MAX),
            change.kind.order(),
        )
    });
}

async fn scan_snapshot(
    table: &Table,
    snapshot_id: i64,
    binding: &SourceBinding,
) -> Result<Vec<DeltaRow>, LakehouseError> {
    let mut columns = vec![binding.id_column.as_str(), binding.vector_column.as_str()];
    columns.extend(binding.payload_columns.iter().map(String::as_str));
    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .select(columns)
        .build()
        .map_err(LakehouseError::Iceberg)?;
    let mut stream = scan.to_arrow().await.map_err(LakehouseError::Iceberg)?;
    let payload_cols: Vec<&str> = binding.payload_columns.iter().map(String::as_str).collect();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.map_err(LakehouseError::Iceberg)? {
        rows.extend(
            batch_to_vectors(
                &batch,
                &binding.id_column,
                &binding.vector_column,
                &payload_cols,
            )?
            .into_iter()
            .map(|(id, vector, payload)| DeltaRow::Upsert {
                id,
                vector,
                payload,
            }),
        );
    }
    Ok(rows)
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

    // A full rescan should represent the live child snapshot, including all
    // position/equality deletes. The 0.9 reader applies those delete files.
    if delta.from_snapshot_id.is_none() {
        return Ok(DeltaScanResult {
            rows: scan_snapshot(table, delta.to_snapshot_id, binding).await?,
            unresolved_delete_files: 0,
        });
    }

    // Bound the range. `None` ⇒ full scan (every live data file at `to`).
    // Otherwise collect the snapshot ids in `(from, to]` by walking parents; if
    // `from` is not an ancestor of `to` the range is unreconstructable and the
    // caller must fall back to a full rescan.
    let mut commit_order = HashMap::new();
    let range: Option<HashSet<i64>> = match delta.from_snapshot_id {
        None => unreachable!("full scans return above"),
        Some(from) => {
            let mut ids = Vec::new();
            let mut cursor = Some(to_snapshot.clone());
            let mut reached = false;
            while let Some(snap) = cursor {
                if snap.snapshot_id() == from {
                    reached = true;
                    break;
                }
                ids.push(snap.snapshot_id());
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
            ids.reverse();
            commit_order.extend(
                ids.iter()
                    .enumerate()
                    .map(|(position, snapshot_id)| (*snapshot_id, position)),
            );
            Some(ids.into_iter().collect())
        }
    };

    let manifest_list = to_snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(LakehouseError::Iceberg)?;

    let mut changes: Vec<FileChange> = Vec::new();
    let mut unresolved_delete_files = 0usize;
    let mut delete_snapshots = HashSet::new();

    for manifest_file in manifest_list.entries() {
        // Scope to the range (full scan keeps everything).
        let in_range = range
            .as_ref()
            .is_none_or(|ids| ids.contains(&manifest_file.added_snapshot_id));
        if !in_range {
            continue;
        }

        if manifest_file.content == ManifestContentType::Deletes {
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(LakehouseError::Iceberg)?;
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Added {
                    continue;
                }
                match entry.content_type() {
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                        // Entry snapshot ids are authoritative when a manifest is
                        // reused by a later snapshot.
                        delete_snapshots.insert(
                            entry
                                .snapshot_id()
                                .unwrap_or(manifest_file.added_snapshot_id),
                        );
                    }
                    DataContentType::Data => unresolved_delete_files += 1,
                }
            }
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

    // Resolve row-level deletes with the 0.9 delete-aware reader. Comparing the
    // parent and child live id sets supports both positional deletes and
    // equality deletes on arbitrary columns, while retaining the manifest diff
    // for snapshot-range attribution.
    let snapshots: HashMap<i64, _> = metadata
        .snapshots()
        .map(|snapshot| (snapshot.snapshot_id(), snapshot))
        .collect();
    // Snapshot summaries keep the signal even when a later commit rewrites the
    // manifest that originally contained the added delete-file entry.
    for (&snapshot_id, snapshot) in &snapshots {
        let summary = &snapshot.summary().additional_properties;
        let added_delete_file = [
            "added-delete-files",
            "added-position-delete-files",
            "added-equality-delete-files",
        ]
        .iter()
        .any(|key| {
            summary
                .get(*key)
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|count| count > 0)
        });
        if added_delete_file && range.as_ref().is_some_and(|ids| ids.contains(&snapshot_id)) {
            delete_snapshots.insert(snapshot_id);
        }
    }
    for snapshot_id in delete_snapshots {
        if !range.as_ref().is_some_and(|ids| ids.contains(&snapshot_id)) {
            continue;
        }
        let Some(snapshot) = snapshots.get(&snapshot_id) else {
            unresolved_delete_files += 1;
            continue;
        };
        let before = match snapshot.parent_snapshot_id() {
            Some(parent_id) => snapshot_ids(table, parent_id, &binding.id_column).await?,
            None => HashSet::new(),
        };
        let after = snapshot_ids(table, snapshot_id, &binding.id_column).await?;
        for id in removed_ids(&before, &after) {
            changes.push(FileChange {
                snapshot_id,
                path: String::new(),
                kind: ChangeKind::ResolvedDelete(id),
            });
        }
    }

    // Apply in commit order. Deletes precede additions within one commit so a
    // merge-on-read update of the same id remains present with its new value.
    sort_changes(&mut changes, &commit_order);

    let payload_cols: Vec<&str> = binding.payload_columns.iter().map(String::as_str).collect();
    let mut rows: Vec<DeltaRow> = Vec::new();

    for change in changes {
        if let ChangeKind::ResolvedDelete(id) = change.kind {
            rows.push(DeltaRow::Delete { id });
            continue;
        }
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
                ChangeKind::ResolvedDelete(_) => unreachable!("handled above"),
            }
        }
    }

    Ok(DeltaScanResult {
        rows,
        unresolved_delete_files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_diff_returns_only_ids_removed_from_child() {
        let before = HashSet::from([1, 2, 3, 4]);
        let after = HashSet::from([1, 3, 5]);
        let mut removed = removed_ids(&before, &after);
        removed.sort_unstable();
        assert_eq!(removed, vec![2, 4]);
    }

    #[test]
    fn changes_follow_commit_chain_and_delete_before_readd() {
        let mut changes = [
            FileChange {
                snapshot_id: 7,
                path: "new.parquet".to_string(),
                kind: ChangeKind::AddedData,
            },
            FileChange {
                snapshot_id: 7,
                path: String::new(),
                kind: ChangeKind::ResolvedDelete(42),
            },
            FileChange {
                snapshot_id: 99,
                path: "old.parquet".to_string(),
                kind: ChangeKind::AddedData,
            },
        ];
        // Snapshot IDs are not chronological; the ancestry walk is.
        sort_changes(&mut changes, &HashMap::from([(99, 0), (7, 1)]));
        assert_eq!(changes[0].snapshot_id, 99);
        assert!(matches!(changes[1].kind, ChangeKind::ResolvedDelete(42)));
        assert!(matches!(changes[2].kind, ChangeKind::AddedData));
    }
}
