use std::path::Path;

use likhadb_core::{Metric, SourceBinding};
use likhadb_index::IndexSnapshot;

use crate::collection::Collection;
use crate::manager::CollectionManager;
use crate::meta::MetaStore;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct CollectionSnapshot {
    pub name: String,
    pub dim: usize,
    pub metric: Metric,
    pub index: IndexSnapshot,
    pub meta: MetaStore,
    #[serde(default)]
    pub fts_enabled: bool,
    #[serde(default)]
    pub source_binding: Option<SourceBinding>,
    #[serde(default)]
    pub source_snapshot_id: Option<i64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ManagerSnapshot {
    pub collections: Vec<CollectionSnapshot>,
    #[serde(default)]
    pub last_lsn: u64,
}

impl Collection {
    pub fn to_snapshot(&self) -> CollectionSnapshot {
        CollectionSnapshot {
            name: self.name.clone(),
            dim: self.dim,
            metric: self.metric,
            index: self.index.to_snapshot(),
            meta: self.meta.clone(),
            fts_enabled: self.is_fts_enabled(),
            source_binding: self.source_binding.clone(),
            source_snapshot_id: self.source_snapshot_id,
        }
    }

    fn is_fts_enabled(&self) -> bool {
        #[cfg(feature = "fts")]
        {
            self.fts_index.is_some()
        }
        #[cfg(not(feature = "fts"))]
        {
            false
        }
    }

    pub fn from_snapshot(snap: CollectionSnapshot, data_dir: Option<&Path>) -> Self {
        let mut col = Self::with_index(snap.name, snap.dim, snap.metric, snap.index.into_box())
            .with_meta(snap.meta);
        col.source_binding = snap.source_binding;
        col.source_snapshot_id = snap.source_snapshot_id;
        #[cfg(feature = "fts")]
        {
            if snap.fts_enabled {
                let fts_dir = data_dir.map(|d| d.join("fts").join(&col.name));
                let _ = col.enable_fts(fts_dir.as_deref());
            }
        }
        col
    }
}

impl CollectionManager {
    pub fn to_snapshot(&self) -> ManagerSnapshot {
        self.to_snapshot_with_lsn(0)
    }

    pub fn to_snapshot_with_lsn(&self, last_lsn: u64) -> ManagerSnapshot {
        ManagerSnapshot {
            collections: self.all_collections().map(|c| c.to_snapshot()).collect(),
            last_lsn,
        }
    }

    pub fn from_snapshot(snap: ManagerSnapshot, data_dir: Option<&Path>) -> Self {
        let mut mgr = CollectionManager::new();
        for col_snap in snap.collections {
            mgr.insert_collection(Collection::from_snapshot(col_snap, data_dir));
        }
        mgr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use likhadb_core::Metric;

    #[test]
    fn binding_and_source_snapshot_id_round_trip() {
        let mut col = Collection::new("c".into(), 4, Metric::L2);
        col.source_binding = Some(SourceBinding {
            source_namespace: vec!["lake".into(), "v1".into()],
            source_table: "embeddings".into(),
            id_column: "id".into(),
            vector_column: "embedding".into(),
            payload_columns: vec!["title".into()],
        });
        col.source_snapshot_id = Some(42);

        let restored = Collection::from_snapshot(col.to_snapshot(), None);
        let binding = restored
            .source_binding
            .expect("binding survives round-trip");
        assert_eq!(binding.source_namespace, vec!["lake", "v1"]);
        assert_eq!(binding.source_table, "embeddings");
        assert_eq!(restored.source_snapshot_id, Some(42));
    }

    #[test]
    fn unbound_collection_round_trips_as_none() {
        let col = Collection::new("c".into(), 4, Metric::L2);
        let restored = Collection::from_snapshot(col.to_snapshot(), None);
        assert!(restored.source_binding.is_none());
        assert!(restored.source_snapshot_id.is_none());
    }
}
