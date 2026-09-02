mod entry;
mod frame;
mod recovery;

pub use entry::{IndexKind, WalEntry, WalOp};

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use bincode::Options as _;
use likhadb_core::{Metric, SourceBinding, VecId, Vector};
use likhadb_store::{Collection, CollectionManager, DeltaRow};
use serde_json::Value;

use crate::{bincode_opts, PersistError};
use frame::{checksum, write_frame, FrameIter};
use recovery::apply_op;

// ── WalWriter ──────────────────────────────────────────────────────────────

struct WalWriter {
    file: BufWriter<File>,
    bytes_written: u64,
}

impl WalWriter {
    fn open_append(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let bytes_written = file.metadata()?.len();
        Ok(Self {
            file: BufWriter::new(file),
            bytes_written,
        })
    }

    fn append(&mut self, entry: &WalEntry) -> Result<(), PersistError> {
        let payload = bincode_opts()
            .serialize(entry)
            .map_err(PersistError::Encode)?;
        // Frame layout: 4-byte length prefix + 4-byte CRC + payload
        let frame_bytes = 8u64 + payload.len() as u64;
        write_frame(&mut self.file, &payload).map_err(PersistError::Io)?;
        self.file.flush().map_err(PersistError::Io)?;
        self.file.get_mut().sync_data().map_err(PersistError::Io)?;
        self.bytes_written = self.bytes_written.saturating_add(frame_bytes);
        metrics::counter!("likhadb_wal_bytes_written_total").increment(frame_bytes);
        metrics::counter!("likhadb_wal_appends_total").increment(1);
        Ok(())
    }

    fn bytes_on_disk(&self) -> u64 {
        self.file
            .get_ref()
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(self.bytes_written)
    }

    fn truncate(path: &Path) -> std::io::Result<()> {
        File::create(path)?; // O_TRUNC
        Ok(())
    }
}

// ── WalManager ─────────────────────────────────────────────────────────────

/// Point-in-time statistics for a [`WalManager`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalStats {
    /// Total entries written since this `WalManager` was opened.
    pub entries_written: u64,
    /// Entries newer than the most recent snapshot and pending replay.
    pub entries_since_checkpoint: u64,
    /// Approximate byte size of `wal.log` on disk.
    pub wal_bytes: u64,
    /// LSN of the last committed entry.
    pub last_lsn: u64,
    /// LSN captured in the most recent snapshot (`0` if none exists).
    pub snapshot_lsn: u64,
}

/// Thresholds that control automatic WAL checkpoints.
///
/// A threshold of zero disables that trigger. When both thresholds are
/// enabled, a checkpoint runs as soon as either one is reached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalConfig {
    /// Trigger a checkpoint after this many WAL entries (`0` = disabled).
    pub checkpoint_every_n_entries: u64,
    /// Trigger a checkpoint after `wal.log` reaches this size (`0` = disabled).
    pub checkpoint_every_n_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            checkpoint_every_n_entries: 100_000,
            checkpoint_every_n_bytes: 256 * 1024 * 1024,
        }
    }
}

/// A `CollectionManager` wrapper that durably logs every mutation to a
/// Write-Ahead Log before applying it in memory.
///
/// # Data directory layout
/// ```text
/// <dir>/
///   snapshot.bin      ← full snapshot (written on checkpoint)
///   wal.log           ← append-only WAL
/// ```
///
/// # Recovery
/// On [`WalManager::open`], if a snapshot exists it is loaded first.  Then any
/// WAL entries with LSN greater than the snapshot's `last_lsn` are replayed in
/// order.  A truncated or CRC-corrupt tail frame (crash mid-write) is silently
/// discarded — it was never committed.
///
/// # Error type
/// All write methods return `Result<_, PersistError>` rather than
/// `likhadb_core::Result<_>` because WAL I/O errors are distinct from logic
/// errors and must be surfaced to the caller.
pub struct WalManager {
    inner: CollectionManager,
    wal: WalWriter,
    config: WalConfig,
    wal_entries: u64,
    next_lsn: u64,
    entries_written: u64,
    entries_since_checkpoint: u64,
    snapshot_lsn: u64,
    dir: PathBuf,
    /// Highest LSN confirmed durably committed to Iceberg staging.  Zero means
    /// none.  Only meaningful when the `iceberg-recovery` feature is active.
    #[cfg(feature = "iceberg-recovery")]
    iceberg_watermark: u64,
    /// In-memory buffer of entries written above `iceberg_watermark`.
    /// Stored as `(lsn, serialized_payload)` to avoid cloning large vectors.
    #[cfg(feature = "iceberg-recovery")]
    unflushed: Vec<(u64, Vec<u8>)>,
}

impl WalManager {
    const SNAPSHOT_FILE: &'static str = "snapshot.bin";
    const WAL_FILE: &'static str = "wal.log";

    /// Open (or create) a data directory, recovering from any existing
    /// snapshot + WAL using [`WalConfig::default`].
    pub fn open(dir: &Path) -> Result<Self, PersistError> {
        Self::open_with_config(dir, WalConfig::default())
    }

    /// Open (or create) a data directory with custom auto-checkpoint
    /// thresholds.
    pub fn open_with_config(dir: &Path, config: WalConfig) -> Result<Self, PersistError> {
        std::fs::create_dir_all(dir).map_err(PersistError::Io)?;

        let snapshot_path = dir.join(Self::SNAPSHOT_FILE);
        let wal_path = dir.join(Self::WAL_FILE);

        // 1. Load snapshot (if present), opening on-disk FTS indexes.
        let (mut inner, snapshot_lsn) = if snapshot_path.exists() {
            use likhadb_store::ManagerSnapshot;
            let file = File::open(&snapshot_path).map_err(PersistError::Io)?;
            let reader = BufReader::new(file);
            let snap: ManagerSnapshot = bincode_opts()
                .deserialize_from(reader)
                .map_err(PersistError::Decode)?;
            let lsn = snap.last_lsn;
            let mgr = CollectionManager::from_snapshot(snap, Some(dir));
            (mgr, lsn)
        } else {
            (CollectionManager::new(), 0)
        };

        // 2. Replay WAL entries newer than the snapshot.
        let mut next_lsn = snapshot_lsn + 1;
        let mut wal_entries = 0;
        let mut entries_since_checkpoint = 0;
        if wal_path.exists() {
            (next_lsn, wal_entries, entries_since_checkpoint) =
                Self::replay_wal(&wal_path, &mut inner, snapshot_lsn, dir)?;
        }

        // 3. Open WAL for appending.
        let wal = WalWriter::open_append(&wal_path).map_err(PersistError::Io)?;

        Ok(Self {
            inner,
            wal,
            config,
            wal_entries,
            next_lsn,
            entries_written: 0,
            entries_since_checkpoint,
            snapshot_lsn,
            dir: dir.to_path_buf(),
            #[cfg(feature = "iceberg-recovery")]
            iceberg_watermark: 0,
            #[cfg(feature = "iceberg-recovery")]
            unflushed: Vec::new(),
        })
    }

    // ── open_from_iceberg_state ─────────────────────────────────────────────

    /// Construct a `WalManager` from a pre-built `CollectionManager` and a
    /// known watermark, then replay any WAL entries above `replay_above_lsn`.
    ///
    /// Used by the `iceberg-recovery` path: Iceberg provides the bulk state;
    /// the WAL covers only the narrow in-flight gap above the watermark.
    #[cfg(feature = "iceberg-recovery")]
    pub fn open_from_iceberg_state(
        dir: &Path,
        inner: CollectionManager,
        iceberg_watermark: u64,
    ) -> Result<Self, PersistError> {
        Self::open_from_iceberg_state_with_config(
            dir,
            inner,
            iceberg_watermark,
            WalConfig::default(),
        )
    }

    /// Iceberg recovery variant of [`WalManager::open_with_config`].
    #[cfg(feature = "iceberg-recovery")]
    pub fn open_from_iceberg_state_with_config(
        dir: &Path,
        inner: CollectionManager,
        iceberg_watermark: u64,
        config: WalConfig,
    ) -> Result<Self, PersistError> {
        std::fs::create_dir_all(dir).map_err(PersistError::Io)?;
        let wal_path = dir.join(Self::WAL_FILE);

        let mut inner = inner;
        let mut next_lsn = iceberg_watermark + 1;
        let mut wal_entries = 0;
        let mut entries_since_checkpoint = 0;
        if wal_path.exists() {
            (next_lsn, wal_entries, entries_since_checkpoint) =
                Self::replay_wal(&wal_path, &mut inner, iceberg_watermark, dir)?;
        }

        let wal = WalWriter::open_append(&wal_path).map_err(PersistError::Io)?;
        Ok(Self {
            inner,
            wal,
            config,
            wal_entries,
            next_lsn,
            entries_written: 0,
            entries_since_checkpoint,
            snapshot_lsn: 0,
            dir: dir.to_path_buf(),
            iceberg_watermark,
            unflushed: Vec::new(),
        })
    }

    /// Replay WAL entries with LSN > `snapshot_lsn`. Returns the `next_lsn`,
    /// the number of valid entries occupying the WAL, and the number of
    /// replayable entries newer than the snapshot.
    fn replay_wal(
        path: &Path,
        mgr: &mut CollectionManager,
        snapshot_lsn: u64,
        data_dir: &Path,
    ) -> Result<(u64, u64, u64), PersistError> {
        let file = File::open(path).map_err(PersistError::Io)?;
        let reader = BufReader::new(file);
        let mut iter = FrameIter::new(reader);
        let mut last_lsn = snapshot_lsn;
        let mut wal_entries = 0u64;
        let mut entries_since_checkpoint = 0u64;

        for item in &mut iter {
            let (payload, stored_crc) = item.map_err(PersistError::Io)?;

            let computed = checksum(&payload);
            if computed != stored_crc {
                // If no bytes follow this frame it is a crash-truncated tail
                // (the last write never completed); discard it and stop replay.
                // If bytes remain after it, the corruption is mid-log and must
                // be surfaced as a hard error.
                let more = iter.has_remaining_bytes().map_err(PersistError::Io)?;
                if !more {
                    break;
                }
                return Err(PersistError::Crc {
                    expected: stored_crc,
                    got: computed,
                });
            }

            let entry: WalEntry = bincode_opts()
                .deserialize(&payload)
                .map_err(PersistError::Decode)?;
            wal_entries = wal_entries.saturating_add(1);

            if entry.lsn <= snapshot_lsn {
                continue;
            }

            apply_op(mgr, entry.op, entry.lsn, Some(data_dir))?;
            last_lsn = entry.lsn;
            entries_since_checkpoint = entries_since_checkpoint.saturating_add(1);
        }

        Ok((last_lsn + 1, wal_entries, entries_since_checkpoint))
    }

    fn record_append(&mut self, _entry: &WalEntry) {
        self.wal_entries = self.wal_entries.saturating_add(1);
        self.entries_written = self.entries_written.saturating_add(1);
        self.entries_since_checkpoint = self.entries_since_checkpoint.saturating_add(1);
        #[cfg(feature = "iceberg-recovery")]
        if let Ok(payload) = bincode_opts().serialize(_entry) {
            self.unflushed.push((_entry.lsn, payload));
        }
        self.next_lsn += 1;
    }

    /// Append a WAL entry then apply `f` to the inner manager.
    fn log_and_apply<T, F>(&mut self, op: WalOp, f: F) -> Result<T, PersistError>
    where
        F: FnOnce(&mut CollectionManager) -> likhadb_core::Result<T>,
    {
        let _span = tracing::debug_span!("wal_append", lsn = self.next_lsn).entered();
        let entry = WalEntry {
            lsn: self.next_lsn,
            op,
        };
        self.wal.append(&entry)?;
        self.record_append(&entry);
        let result = f(&mut self.inner).map_err(PersistError::Apply)?;
        self.maybe_checkpoint()?;
        Ok(result)
    }

    fn maybe_checkpoint(&mut self) -> Result<(), PersistError> {
        let entries_due = self.config.checkpoint_every_n_entries > 0
            && self.wal_entries >= self.config.checkpoint_every_n_entries;
        let bytes_due = self.config.checkpoint_every_n_bytes > 0
            && self.wal.bytes_written >= self.config.checkpoint_every_n_bytes;

        if entries_due || bytes_due {
            self.checkpoint()?;
        }
        Ok(())
    }

    // ── Iceberg recovery helpers ────────────────────────────────────────────

    #[cfg(feature = "iceberg-recovery")]
    pub fn iceberg_watermark(&self) -> u64 {
        self.iceberg_watermark
    }

    #[cfg(feature = "iceberg-recovery")]
    pub fn set_iceberg_watermark(&mut self, lsn: u64) {
        if lsn > self.iceberg_watermark {
            self.iceberg_watermark = lsn;
            self.unflushed.retain(|(entry_lsn, _)| *entry_lsn > lsn);
        }
    }

    /// Return WAL entries with `lsn > iceberg_watermark` that have not yet
    /// been flushed to Iceberg staging.  Returns `(lsn, entry)` pairs.
    #[cfg(feature = "iceberg-recovery")]
    pub fn collect_unflushed(&self) -> Vec<WalEntry> {
        let watermark = self.iceberg_watermark;
        self.unflushed
            .iter()
            .filter(|(lsn, _)| *lsn > watermark)
            .filter_map(|(_, payload)| bincode_opts().deserialize::<WalEntry>(payload).ok())
            .collect()
    }

    /// Rewrite `wal.log` keeping only frames with `lsn > watermark`.
    ///
    /// Uses write-to-tmp + atomic rename for crash safety, then reopens the
    /// WAL writer for new appends.
    #[cfg(feature = "iceberg-recovery")]
    pub fn truncate_wal_up_to(&mut self, watermark: u64) -> Result<(), PersistError> {
        let wal_path = self.dir.join(Self::WAL_FILE);
        let tmp_path = self.dir.join("wal.log.tmp");

        // Collect all frames above the watermark.
        let entries_to_keep: Vec<WalEntry> = if wal_path.exists() {
            let file = File::open(&wal_path).map_err(PersistError::Io)?;
            let reader = BufReader::new(file);
            let mut iter = frame::FrameIter::new(reader);
            let mut kept = Vec::new();
            for item in &mut iter {
                let (payload, stored_crc) = item.map_err(PersistError::Io)?;
                if frame::checksum(&payload) != stored_crc {
                    break; // Treat corrupt tail as end of log.
                }
                let entry: WalEntry = bincode_opts()
                    .deserialize(&payload)
                    .map_err(PersistError::Decode)?;
                if entry.lsn > watermark {
                    kept.push(entry);
                }
            }
            kept
        } else {
            Vec::new()
        };

        // Write kept entries to tmp file.
        {
            let file = File::create(&tmp_path).map_err(PersistError::Io)?;
            let mut writer = BufWriter::new(file);
            for entry in &entries_to_keep {
                let payload = bincode_opts()
                    .serialize(entry)
                    .map_err(PersistError::Encode)?;
                frame::write_frame(&mut writer, &payload).map_err(PersistError::Io)?;
            }
            writer.flush().map_err(PersistError::Io)?;
            writer.get_mut().sync_all().map_err(PersistError::Io)?;
        }

        // Atomic rename then reopen.
        std::fs::rename(&tmp_path, &wal_path).map_err(PersistError::Io)?;
        self.wal = WalWriter::open_append(&wal_path).map_err(PersistError::Io)?;
        self.wal_entries = entries_to_keep.len() as u64;
        self.entries_since_checkpoint = entries_to_keep
            .iter()
            .filter(|entry| entry.lsn > self.snapshot_lsn)
            .count() as u64;

        Ok(())
    }

    // ── Collection DDL ─────────────────────────────────────────────────────

    pub fn create_collection(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: Metric,
    ) -> Result<(), PersistError> {
        let name = name.into();
        self.log_and_apply(
            WalOp::CreateCollection {
                name: name.clone(),
                dim,
                metric,
                kind: IndexKind::Flat,
            },
            |mgr| mgr.create_collection(name, dim, metric),
        )
    }

    pub fn create_ivf_collection(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: Metric,
        nlist: usize,
        nprobe: usize,
    ) -> Result<(), PersistError> {
        let name = name.into();
        self.log_and_apply(
            WalOp::CreateCollection {
                name: name.clone(),
                dim,
                metric,
                kind: IndexKind::Ivf { nlist, nprobe },
            },
            |mgr| mgr.create_ivf_collection(name, dim, metric, nlist, nprobe),
        )
    }

    pub fn create_ivf_sq8_collection(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: Metric,
        nlist: usize,
        nprobe: usize,
    ) -> Result<(), PersistError> {
        let name = name.into();
        self.log_and_apply(
            WalOp::CreateCollection {
                name: name.clone(),
                dim,
                metric,
                kind: IndexKind::IvfSq8 { nlist, nprobe },
            },
            |mgr| mgr.create_ivf_sq8_collection(name, dim, metric, nlist, nprobe),
        )
    }

    pub fn create_hnsw_collection(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: Metric,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Result<(), PersistError> {
        let name = name.into();
        self.log_and_apply(
            WalOp::CreateCollection {
                name: name.clone(),
                dim,
                metric,
                kind: IndexKind::Hnsw {
                    m,
                    ef_construction,
                    ef_search,
                },
            },
            |mgr| mgr.create_hnsw_collection(name, dim, metric, m, ef_construction, ef_search),
        )
    }

    pub fn drop_collection(&mut self, name: &str) -> Result<(), PersistError> {
        let name = name.to_owned();
        self.log_and_apply(WalOp::DropCollection { name: name.clone() }, |mgr| {
            mgr.drop_collection(&name)
        })
    }

    // ── Vector DML ─────────────────────────────────────────────────────────

    pub fn insert(
        &mut self,
        collection: &str,
        id: VecId,
        vector: Vector,
        payload: Option<Value>,
    ) -> Result<(), PersistError> {
        let col = collection.to_owned();
        let lsn = self.next_lsn;
        self.log_and_apply(
            WalOp::Insert {
                collection: col.clone(),
                id,
                vector: vector.clone(),
                payload: payload.clone(),
            },
            |mgr| mgr.get_mut(&col)?.insert(id, vector, payload, lsn),
        )
    }

    pub fn delete(&mut self, collection: &str, id: VecId) -> Result<bool, PersistError> {
        let col = collection.to_owned();
        let lsn = self.next_lsn;
        self.log_and_apply(
            WalOp::Delete {
                collection: col.clone(),
                id,
            },
            |mgr| mgr.get_mut(&col)?.delete(id, lsn),
        )
    }

    // ── FTS ────────────────────────────────────────────────────────────────

    #[cfg(feature = "fts")]
    pub fn enable_fts(&mut self, name: &str) -> Result<(), PersistError> {
        let col = name.to_owned();
        let fts_dir = self.dir.join("fts").join(&col);
        self.log_and_apply(
            WalOp::EnableFts {
                collection: col.clone(),
            },
            |mgr| mgr.enable_fts(&col, Some(&fts_dir)),
        )
    }

    pub fn set_source_binding(
        &mut self,
        collection: &str,
        binding: likhadb_core::SourceBinding,
    ) -> Result<(), PersistError> {
        let col = collection.to_owned();
        self.log_and_apply(
            WalOp::SetSourceBinding {
                collection: col.clone(),
                binding: binding.clone(),
            },
            |mgr| mgr.set_source_binding(&col, binding),
        )
    }

    /// Apply a source-table snapshot delta without writing it to the internal WAL.
    ///
    /// The caller must supply the binding and watermark observed before scanning.
    /// Both are revalidated under the store write lock so rows from a stale scan
    /// are never applied after a rebind or a concurrent maintenance tick. `Ok(false)`
    /// means that validation failed and the caller should discard the scan.
    ///
    /// The source watermark advances only after every row applies successfully.
    /// A partial failure therefore leaves the old watermark in place and the
    /// idempotent range can be retried on the next tick.
    pub fn apply_source_delta(
        &mut self,
        collection: &str,
        expected_binding: &SourceBinding,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: i64,
        rows: impl IntoIterator<Item = DeltaRow>,
    ) -> likhadb_core::Result<bool> {
        let col = self.inner.get_mut(collection)?;
        if col.source_binding.as_ref() != Some(expected_binding)
            || col.source_snapshot_id != from_snapshot_id
        {
            return Ok(false);
        }

        for row in rows {
            col.apply_delta_row(row, u64::MAX)?;
        }
        col.source_snapshot_id = Some(to_snapshot_id);
        Ok(true)
    }

    // ── Read-through ────────────────────────────────────────────────────────

    pub fn get(&self, name: &str) -> likhadb_core::Result<&Collection> {
        self.inner.get(name)
    }

    pub fn list(&self) -> Vec<&str> {
        self.inner.list()
    }

    /// Return a point-in-time view of WAL activity and checkpoint progress.
    pub fn stats(&self) -> WalStats {
        WalStats {
            entries_written: self.entries_written,
            entries_since_checkpoint: self.entries_since_checkpoint,
            wal_bytes: self.wal.bytes_on_disk(),
            last_lsn: self.next_lsn.saturating_sub(1),
            snapshot_lsn: self.snapshot_lsn,
        }
    }

    // ── Checkpoint ─────────────────────────────────────────────────────────

    /// Write a snapshot capturing the current state (including `last_lsn`),
    /// then truncate `wal.log`.  Call on graceful shutdown or periodically to
    /// bound recovery time.
    pub fn checkpoint(&mut self) -> Result<(), PersistError> {
        let last_lsn = self.next_lsn.saturating_sub(1);
        let snapshot_path = self.dir.join(Self::SNAPSHOT_FILE);
        let tmp_path = self.dir.join("snapshot.bin.tmp");
        let wal_path = self.dir.join(Self::WAL_FILE);

        // Write snapshot to tmp then atomically rename.
        {
            use likhadb_store::ManagerSnapshot;
            let snap: ManagerSnapshot = self.inner.to_snapshot_with_lsn(last_lsn);
            let file = File::create(&tmp_path).map_err(PersistError::Io)?;
            let mut writer = BufWriter::new(file);
            bincode_opts()
                .serialize_into(&mut writer, &snap)
                .map_err(PersistError::Encode)?;
            writer.flush().map_err(PersistError::Io)?;
            writer.get_mut().sync_all().map_err(PersistError::Io)?;
        }
        std::fs::rename(&tmp_path, &snapshot_path).map_err(PersistError::Io)?;
        self.snapshot_lsn = last_lsn;
        self.entries_since_checkpoint = 0;

        // Truncate WAL and reopen for appending.
        WalWriter::truncate(&wal_path).map_err(PersistError::Io)?;
        self.wal = WalWriter::open_append(&wal_path).map_err(PersistError::Io)?;
        self.wal_entries = 0;

        Ok(())
    }
}
