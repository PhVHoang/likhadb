use likhadb_core::{Metric, Result, ScoredResult, VecId, Vector};
use likhadb_index::{FlatIndex, VectorIndex};
use serde_json::Value;

use crate::meta::MetaStore;

pub struct Collection {
    pub name: String,
    pub dim: usize,
    pub metric: Metric,
    index: Box<dyn VectorIndex>,
    meta: MetaStore,
}

impl Collection {
    /// Creates a new Collection backed by a `FlatIndex` (exact brute-force search).
    pub fn new(name: String, dim: usize, metric: Metric) -> Self {
        Self::with_index(name, dim, metric, Box::new(FlatIndex::new(dim, metric)))
    }

    /// Creates a Collection backed by a caller-supplied index implementation.
    /// Use this to inject `IvfIndex`, or any future `VectorIndex` implementation.
    pub fn with_index(
        name: String,
        dim: usize,
        metric: Metric,
        index: Box<dyn VectorIndex>,
    ) -> Self {
        Self {
            name,
            dim,
            metric,
            index,
            meta: MetaStore::new(),
        }
    }

    pub fn insert(&mut self, id: VecId, vec: Vector, payload: Option<Value>) -> Result<()> {
        self.index.insert(id, vec)?;
        if let Some(p) = payload {
            self.meta.set(id, p);
        }
        Ok(())
    }

    pub fn delete(&mut self, id: VecId) -> Result<bool> {
        let existed = self.index.delete(id);
        self.meta.remove(id);
        Ok(existed)
    }

    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        predicate: Option<&Value>,
    ) -> Result<Vec<ScoredResult>> {
        let filter_box = self.meta.make_filter(predicate);
        let filter = filter_box.as_deref();
        self.index.search(query, k, filter)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}
