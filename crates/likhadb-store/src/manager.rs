use std::collections::HashMap;

use likhadb_core::{LikhaDbError, Metric, Result};

use crate::collection::Collection;

pub struct CollectionManager {
    collections: HashMap<String, Collection>,
}

impl CollectionManager {
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    pub fn create_collection(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: Metric,
    ) -> Result<()> {
        let name = name.into();
        if self.collections.contains_key(&name) {
            return Err(LikhaDbError::CollectionAlreadyExists(name));
        }
        self.collections
            .insert(name.clone(), Collection::new(name, dim, metric));
        Ok(())
    }

    pub fn drop_collection(&mut self, name: &str) -> Result<()> {
        self.collections
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| LikhaDbError::CollectionNotFound(name.to_owned()))
    }

    pub fn get(&self, name: &str) -> Result<&Collection> {
        self.collections
            .get(name)
            .ok_or_else(|| LikhaDbError::CollectionNotFound(name.to_owned()))
    }

    pub fn get_mut(&mut self, name: &str) -> Result<&mut Collection> {
        self.collections
            .get_mut(name)
            .ok_or_else(|| LikhaDbError::CollectionNotFound(name.to_owned()))
    }

    pub fn list(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.collections.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }
}

impl Default for CollectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use likhadb_core::Metric;
    use serde_json::json;

    fn setup_manager_with_vectors(n: usize) -> CollectionManager {
        let mut mgr = CollectionManager::new();
        mgr.create_collection("test", 4, Metric::L2).unwrap();
        let col = mgr.get_mut("test").unwrap();
        for i in 0..n as u64 {
            let vec = vec![i as f32, 0.0, 0.0, 0.0];
            col.insert(i, vec, Some(json!({"id_tag": i}))).unwrap();
        }
        mgr
    }

    #[test]
    fn create_duplicate_collection_errors() {
        let mut mgr = CollectionManager::new();
        mgr.create_collection("c1", 4, Metric::L2).unwrap();
        assert!(matches!(
            mgr.create_collection("c1", 4, Metric::L2),
            Err(LikhaDbError::CollectionAlreadyExists(_))
        ));
    }

    #[test]
    fn drop_nonexistent_collection_errors() {
        let mut mgr = CollectionManager::new();
        assert!(matches!(
            mgr.drop_collection("nope"),
            Err(LikhaDbError::CollectionNotFound(_))
        ));
    }

    #[test]
    fn get_nonexistent_errors() {
        let mgr = CollectionManager::new();
        assert!(matches!(
            mgr.get("nope"),
            Err(LikhaDbError::CollectionNotFound(_))
        ));
    }

    #[test]
    fn list_returns_sorted_names() {
        let mut mgr = CollectionManager::new();
        mgr.create_collection("zoo", 4, Metric::L2).unwrap();
        mgr.create_collection("alpha", 4, Metric::L2).unwrap();
        assert_eq!(mgr.list(), vec!["alpha", "zoo"]);
    }

    #[test]
    fn integration_insert_search_delete() {
        let mut mgr = setup_manager_with_vectors(50);

        // search top-5 near origin
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = mgr.get("test").unwrap().search(&query, 5, None).unwrap();
        assert_eq!(results.len(), 5);

        // results must be ordered ascending by score
        for w in results.windows(2) {
            assert!(
                w[0].score <= w[1].score,
                "results not sorted: {} > {}",
                w[0].score,
                w[1].score
            );
        }

        // the 5 closest should be ids 0..5 (nearest to origin along x-axis)
        let ids: Vec<u64> = results.iter().map(|r| r.id).collect();
        assert!(ids.contains(&0), "id 0 should be in top-5, got {ids:?}");

        // delete ids 0 and 1
        let col = mgr.get_mut("test").unwrap();
        assert!(col.delete(0).unwrap());
        assert!(col.delete(1).unwrap());

        // re-search
        let results2 = mgr.get("test").unwrap().search(&query, 5, None).unwrap();
        assert_eq!(results2.len(), 5);
        let ids2: Vec<u64> = results2.iter().map(|r| r.id).collect();
        assert!(!ids2.contains(&0), "deleted id 0 should not appear");
        assert!(!ids2.contains(&1), "deleted id 1 should not appear");
    }

    #[test]
    fn metadata_filter_integration() {
        let mut mgr = CollectionManager::new();
        mgr.create_collection("tagged", 4, Metric::L2).unwrap();
        let col = mgr.get_mut("tagged").unwrap();

        for i in 0..20u64 {
            let tag = if i % 2 == 0 { "even" } else { "odd" };
            col.insert(
                i,
                vec![i as f32, 0.0, 0.0, 0.0],
                Some(json!({"parity": tag})),
            )
            .unwrap();
        }

        let pred = serde_json::json!({"field": "parity", "op": "eq", "value": "even"});
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let col = mgr.get("tagged").unwrap();
        let results = col.search(&query, 5, Some(&pred)).unwrap();
        assert!(results.iter().all(|r| r.id % 2 == 0));
    }
}
