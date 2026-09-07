use std::path::Path;

use likhadb_core::LikhaDbError;
use likhadb_store::{CollectionManager, DeltaRow};

use crate::PersistError;

use super::entry::{IndexKind, WalOp};

/// Apply a single WAL operation to a live `CollectionManager`.
///
/// Operations are applied idempotently so replaying entries that were already
/// captured by a partial checkpoint converges on the WAL's requested state.
pub fn apply_op(
    mgr: &mut CollectionManager,
    op: WalOp,
    lsn: u64,
    data_dir: Option<&Path>,
) -> Result<(), PersistError> {
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
        } => {
            let col = mgr.get_mut(&collection).map_err(PersistError::Apply)?;

            // A snapshot may already contain this exact write. Avoid applying
            // it again, which is logically harmless but can leave an extra
            // overwrite tombstone in indexes such as HNSW.
            if col.get(id).map_err(PersistError::Apply)? == Some((vector.clone(), payload.clone()))
            {
                return Ok(());
            }

            col.apply_delta_row(
                DeltaRow::Upsert {
                    id,
                    vector,
                    payload,
                },
                lsn,
            )
            .map_err(PersistError::Apply)
        }
        WalOp::Delete { collection, id } => mgr
            .get_mut(&collection)
            .and_then(|col| col.apply_delta_row(DeltaRow::Delete { id }, lsn))
            .map_err(PersistError::Apply),
        WalOp::EnableFts { collection } => {
            #[cfg(feature = "fts")]
            {
                let fts_dir = data_dir.map(|d| d.join("fts").join(&collection));
                match mgr.enable_fts(&collection, fts_dir.as_deref()) {
                    Ok(()) | Err(LikhaDbError::CollectionNotFound(_)) => {}
                    Err(e) => return Err(PersistError::Apply(e)),
                }
            }
            #[cfg(not(feature = "fts"))]
            {
                let _ = (collection, data_dir);
            }
            Ok(())
        }
        WalOp::SetSourceBinding {
            collection,
            binding,
        } => match mgr.set_source_binding(&collection, binding) {
            Ok(()) | Err(LikhaDbError::CollectionNotFound(_)) => Ok(()),
            Err(e) => Err(PersistError::Apply(e)),
        },
    }
}

#[cfg(test)]
mod tests {
    use likhadb_core::Metric;
    use serde_json::json;

    use super::*;

    fn manager_with_collection() -> CollectionManager {
        let mut mgr = CollectionManager::new();
        mgr.create_collection("col", 3, Metric::L2).unwrap();
        mgr
    }

    #[test]
    fn replaying_identical_insert_is_idempotent() {
        let mut mgr = manager_with_collection();
        let op = WalOp::Insert {
            collection: "col".into(),
            id: 5,
            vector: vec![1.0, 2.0, 3.0],
            payload: Some(json!({"version": 1})),
        };

        apply_op(&mut mgr, op, 1, None).unwrap();
        apply_op(
            &mut mgr,
            WalOp::Insert {
                collection: "col".into(),
                id: 5,
                vector: vec![1.0, 2.0, 3.0],
                payload: Some(json!({"version": 1})),
            },
            1,
            None,
        )
        .unwrap();

        let col = mgr.get("col").unwrap();
        assert_eq!(col.len(), 1);
        assert_eq!(
            col.get(5).unwrap(),
            Some((vec![1.0, 2.0, 3.0], Some(json!({"version": 1}))))
        );
    }

    #[test]
    fn replaying_insert_uses_last_write_wins() {
        let mut mgr = manager_with_collection();
        apply_op(
            &mut mgr,
            WalOp::Insert {
                collection: "col".into(),
                id: 5,
                vector: vec![1.0, 2.0, 3.0],
                payload: Some(json!({"version": 1})),
            },
            1,
            None,
        )
        .unwrap();
        apply_op(
            &mut mgr,
            WalOp::Insert {
                collection: "col".into(),
                id: 5,
                vector: vec![3.0, 2.0, 1.0],
                payload: Some(json!({"version": 2})),
            },
            2,
            None,
        )
        .unwrap();

        let col = mgr.get("col").unwrap();
        assert_eq!(col.len(), 1);
        assert_eq!(
            col.get(5).unwrap(),
            Some((vec![3.0, 2.0, 1.0], Some(json!({"version": 2}))))
        );
    }

    #[test]
    fn replaying_delete_of_missing_vector_is_idempotent() {
        let mut mgr = manager_with_collection();

        apply_op(
            &mut mgr,
            WalOp::Delete {
                collection: "col".into(),
                id: 5,
            },
            1,
            None,
        )
        .unwrap();
        apply_op(
            &mut mgr,
            WalOp::Delete {
                collection: "col".into(),
                id: 5,
            },
            1,
            None,
        )
        .unwrap();

        assert!(mgr.get("col").unwrap().get(5).unwrap().is_none());
    }
}
