use likhadb_core::Metric;
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
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ManagerSnapshot {
    pub collections: Vec<CollectionSnapshot>,
}

impl Collection {
    pub fn to_snapshot(&self) -> CollectionSnapshot {
        CollectionSnapshot {
            name: self.name.clone(),
            dim: self.dim,
            metric: self.metric,
            index: self.index.to_snapshot(),
            meta: self.meta.clone(),
        }
    }

    pub fn from_snapshot(snap: CollectionSnapshot) -> Self {
        Self::with_index(snap.name, snap.dim, snap.metric, snap.index.into_box())
            .with_meta(snap.meta)
    }
}

impl CollectionManager {
    pub fn to_snapshot(&self) -> ManagerSnapshot {
        ManagerSnapshot {
            collections: self.all_collections().map(|c| c.to_snapshot()).collect(),
        }
    }

    pub fn from_snapshot(snap: ManagerSnapshot) -> Self {
        let mut mgr = CollectionManager::new();
        for col_snap in snap.collections {
            mgr.insert_collection(Collection::from_snapshot(col_snap));
        }
        mgr
    }
}
