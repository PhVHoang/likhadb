mod error;
pub mod wal;

pub use error::PersistError;
pub use wal::WalManager;

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use bincode::Options;
use likhadb_store::{CollectionManager, ManagerSnapshot};

/// 16 GiB hard cap on snapshot size — prevents a corrupt length field from
/// causing a multi-terabyte allocation attempt on malformed input.
const MAX_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024 * 1024;

fn bincode_opts() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_SNAPSHOT_BYTES)
}

/// Extension trait that adds `save` / `load` to [`CollectionManager`].
///
/// Import this trait to use:
/// ```rust,ignore
/// use likhadb_persist::PersistExt;
/// mgr.save(Path::new("snapshot.bin"))?;
/// let mgr = CollectionManager::load(Path::new("snapshot.bin"))?;
/// ```
pub trait PersistExt: Sized {
    fn save(&self, path: &Path) -> Result<(), PersistError>;
    fn load(path: &Path) -> Result<Self, PersistError>;
}

impl PersistExt for CollectionManager {
    fn save(&self, path: &Path) -> Result<(), PersistError> {
        let snap: ManagerSnapshot = self.to_snapshot();
        let file = File::create(path).map_err(PersistError::Io)?;
        let mut writer = BufWriter::new(file);
        bincode_opts()
            .serialize_into(&mut writer, &snap)
            .map_err(PersistError::Encode)?;
        writer.flush().map_err(PersistError::Io)?;
        writer.get_mut().sync_all().map_err(PersistError::Io)?;
        Ok(())
    }

    fn load(path: &Path) -> Result<Self, PersistError> {
        let file = File::open(path).map_err(PersistError::Io)?;
        let reader = BufReader::new(file);
        let snap: ManagerSnapshot = bincode_opts()
            .deserialize_from(reader)
            .map_err(PersistError::Decode)?;
        Ok(CollectionManager::from_snapshot(snap, None))
    }
}
