use likhadb_core::{Metric, Result, ScoredResult, VecId, Vector};
use likhadb_index::{FlatIndex, VectorIndex};
use serde_json::Value;

use crate::meta::MetaStore;

pub type VectorWithPayload = (Vector, Option<Value>);

pub struct Collection {
    pub name: String,
    pub dim: usize,
    pub metric: Metric,
    pub(crate) index: Box<dyn VectorIndex>,
    pub(crate) meta: MetaStore,
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

    #[cfg(feature = "persist")]
    pub(crate) fn with_meta(mut self, meta: crate::meta::MetaStore) -> Self {
        self.meta = meta;
        self
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
        include_payload: bool,
    ) -> Result<Vec<ScoredResult>> {
        let filter_box = self.meta.make_filter(predicate);
        let filter = filter_box.as_deref();
        let mut results = self.index.search(query, k, filter)?;
        if include_payload {
            for r in &mut results {
                r.payload = self.meta.get(r.id).cloned();
            }
        }
        Ok(results)
    }

    pub fn get(&self, id: VecId) -> Result<Option<VectorWithPayload>> {
        let Some(vec) = self.index.get(id) else {
            return Ok(None);
        };
        Ok(Some((vec, self.meta.get(id).cloned())))
    }

    pub fn get_batch(&self, ids: &[VecId]) -> Result<Vec<Option<VectorWithPayload>>> {
        ids.iter().map(|&id| self.get(id)).collect()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn index_type(&self) -> &'static str {
        self.index.index_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_collection() -> Collection {
        Collection::new("test".into(), 3, Metric::L2)
    }

    #[test]
    fn get_returns_vector_and_payload() {
        let mut c = make_collection();
        c.insert(1, vec![1.0, 2.0, 3.0], Some(json!({"tag": "a"}))).unwrap();
        let (vec, payload) = c.get(1).unwrap().unwrap();
        assert_eq!(vec, vec![1.0, 2.0, 3.0]);
        assert_eq!(payload.unwrap()["tag"], "a");
    }

    #[test]
    fn get_returns_none_for_missing_id() {
        let c = make_collection();
        assert!(c.get(99).unwrap().is_none());
    }

    #[test]
    fn get_returns_none_after_delete() {
        let mut c = make_collection();
        c.insert(2, vec![0.0, 1.0, 0.0], None).unwrap();
        c.delete(2).unwrap();
        assert!(c.get(2).unwrap().is_none());
    }

    #[test]
    fn get_payload_is_none_when_not_set() {
        let mut c = make_collection();
        c.insert(3, vec![1.0, 0.0, 0.0], None).unwrap();
        let (_, payload) = c.get(3).unwrap().unwrap();
        assert!(payload.is_none());
    }

    #[test]
    fn get_batch_returns_mixed_some_and_none() {
        let mut c = make_collection();
        c.insert(1, vec![1.0, 0.0, 0.0], Some(json!({"x": 1}))).unwrap();
        c.insert(3, vec![3.0, 0.0, 0.0], None).unwrap();
        let results = c.get_batch(&[1, 2, 3]).unwrap();
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
    }
}
