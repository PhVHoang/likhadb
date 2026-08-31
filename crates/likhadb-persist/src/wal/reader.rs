use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use bincode::Options as _;

use crate::{bincode_opts, PersistError};

use super::entry::WalEntry;
use super::frame::{checksum, FrameIter};

/// A read-only iterator over entries in `wal.log`.
///
/// Opening a reader never creates or modifies files and does not replay any
/// operations. A crash-truncated or CRC-mismatched final frame is treated as
/// an uncommitted tail and ignored, matching [`super::WalManager`] recovery.
/// Corruption before the final frame is returned as an error.
pub struct WalReader {
    iter: FrameIter<BufReader<File>>,
    after_lsn: Option<u64>,
    done: bool,
}

impl WalReader {
    /// Open `<dir>/wal.log` for side-effect-free inspection.
    pub fn open(dir: &Path) -> Result<Self, PersistError> {
        Self::open_path(&dir.join(super::WalManager::WAL_FILE), None)
    }

    pub(super) fn open_after(dir: &Path, after_lsn: u64) -> Result<Self, PersistError> {
        Self::open_path(&dir.join(super::WalManager::WAL_FILE), Some(after_lsn))
    }

    pub(super) fn open_file(path: &Path) -> Result<Self, PersistError> {
        Self::open_path(path, None)
    }

    fn open_path(path: &Path, after_lsn: Option<u64>) -> Result<Self, PersistError> {
        let file = File::open(path).map_err(PersistError::Io)?;
        Ok(Self {
            iter: FrameIter::new(BufReader::new(file)),
            after_lsn,
            done: false,
        })
    }

    fn fail(&mut self, error: PersistError) -> Option<Result<WalEntry, PersistError>> {
        self.done = true;
        Some(Err(error))
    }
}

impl Iterator for WalReader {
    type Item = Result<WalEntry, PersistError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let (payload, stored_crc) = match self.iter.next()? {
                Ok(frame) => frame,
                Err(error) => return self.fail(PersistError::Io(error)),
            };

            let computed_crc = checksum(&payload);
            if computed_crc != stored_crc {
                return match self.iter.has_remaining_bytes() {
                    // The final frame was not durably committed.
                    Ok(false) => {
                        self.done = true;
                        None
                    }
                    Ok(true) => self.fail(PersistError::Crc {
                        expected: stored_crc,
                        got: computed_crc,
                    }),
                    Err(error) => self.fail(PersistError::Io(error)),
                };
            }

            let entry: WalEntry = match bincode_opts().deserialize(&payload) {
                Ok(entry) => entry,
                Err(error) => return self.fail(PersistError::Decode(error)),
            };

            if self
                .after_lsn
                .is_some_and(|after_lsn| entry.lsn <= after_lsn)
            {
                continue;
            }

            return Some(Ok(entry));
        }
    }
}
