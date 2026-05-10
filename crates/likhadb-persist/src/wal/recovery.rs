use likhadb_core::LikhaDbError;
use likhadb_store::CollectionManager;

use crate::PersistError;

use super::entry::{IndexKind, WalOp};

/// Apply a single WAL operation to a live `CollectionManager`.
///
/// `CreateCollection` is idempotent: if the collection already exists (e.g.
/// replaying after a partial checkpoint), the op is silently skipped.
pub fn apply_op(mgr: &mut CollectionManager, op: WalOp) -> Result<(), PersistError> {
    match op {
        WalOp::CreateCollection {
            name,
            dim,
            metric,
            kind,
        } => {
            let result = match kind {
                IndexKind::Flat => mgr.create_collection(name, dim, metric),
                IndexKind::Ivf { nlist, nprobe } => {
                    mgr.create_ivf_collection(name, dim, metric, nlist, nprobe)
                }
                IndexKind::IvfSq8 { nlist, nprobe } => {
                    mgr.create_ivf_sq8_collection(name, dim, metric, nlist, nprobe)
                }
                IndexKind::Hnsw {
                    m,
                    ef_construction,
                    ef_search,
                } => mgr.create_hnsw_collection(name, dim, metric, m, ef_construction, ef_search),
            };
            // Idempotent: ignore "already exists" errors that can occur when
            // replaying WAL entries that were also captured in the snapshot.
            match result {
                Ok(()) | Err(LikhaDbError::CollectionAlreadyExists(_)) => Ok(()),
                Err(e) => Err(PersistError::Apply(e)),
            }
        }
        WalOp::DropCollection { name } => match mgr.drop_collection(&name) {
            Ok(()) | Err(LikhaDbError::CollectionNotFound(_)) => Ok(()),
            Err(e) => Err(PersistError::Apply(e)),
        },
        WalOp::Insert {
            collection,
            id,
            vector,
            payload,
        } => mgr
            .get_mut(&collection)
            .and_then(|col| col.insert(id, vector, payload))
            .map_err(PersistError::Apply),
        WalOp::Delete { collection, id } => mgr
            .get_mut(&collection)
            .and_then(|col| col.delete(id).map(|_| ()))
            .map_err(PersistError::Apply),
        WalOp::EnableFts { collection } => {
            #[cfg(feature = "fts")]
            match mgr.enable_fts(&collection) {
                Ok(()) | Err(LikhaDbError::CollectionNotFound(_)) => {}
                Err(e) => return Err(PersistError::Apply(e)),
            }
            #[cfg(not(feature = "fts"))]
            let _ = collection;
            Ok(())
        }
    }
}
