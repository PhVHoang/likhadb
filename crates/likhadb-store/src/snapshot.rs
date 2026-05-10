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
    #[serde(default)]
    pub fts_enabled: bool,
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

    pub fn from_snapshot(snap: CollectionSnapshot) -> Self {
        let col = Self::with_index(snap.name, snap.dim, snap.metric, snap.index.into_box())
            .with_meta(snap.meta);
        #[cfg(feature = "fts")]
        let col = {
            let mut c = col;
            if snap.fts_enabled {
                let _ = c.enable_fts();
            }
            c
        };
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

    pub fn from_snapshot(snap: ManagerSnapshot) -> Self {
        let mut mgr = CollectionManager::new();
        for col_snap in snap.collections {
            mgr.insert_collection(Collection::from_snapshot(col_snap));
        }
        mgr
    }
}
