use std::collections::HashMap;
use std::path::Path;

use likhadb_core::{LikhaDbError, Metric, Result};
use likhadb_index::{HnswIndex, IvfIndex};

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

    /// Creates a collection backed by an IVF (Inverted File Index) for approximate
    /// nearest-neighbour search.
    ///
    /// - `nlist`: number of k-means clusters. Training fires automatically once this
    ///   many vectors have been inserted. Before that threshold, searches fall back to
    ///   brute-force over the staging area.
    /// - `nprobe`: clusters searched per query. Must satisfy `1 <= nprobe <= nlist`.
    ///   Higher `nprobe` → better recall, higher latency. Set `nprobe == nlist` for
    ///   exact recall equivalent to `FlatIndex`.
    pub fn create_ivf_collection(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: Metric,
        nlist: usize,
        nprobe: usize,
    ) -> Result<()> {
        let name = name.into();
        if self.collections.contains_key(&name) {
            return Err(LikhaDbError::CollectionAlreadyExists(name));
        }
        let index = IvfIndex::new(dim, metric, nlist, nprobe)?;
        let collection = Collection::with_index(name.clone(), dim, metric, Box::new(index));
        self.collections.insert(name, collection);
        Ok(())
    }

    /// Creates a collection backed by an IVF index with SQ8 scalar quantization.
    ///
    /// Identical to [`create_ivf_collection`](Self::create_ivf_collection) except
    /// that after training, each stored vector is compressed from `dim × 4` bytes
    /// (f32) to `dim × 1` byte (u8), giving a 4× memory reduction. Distances at
    /// query time use asymmetric computation: the query stays in f32 while stored
    /// codes are decoded on-the-fly.
    pub fn create_ivf_sq8_collection(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: Metric,
        nlist: usize,
        nprobe: usize,
    ) -> Result<()> {
        let name = name.into();
        if self.collections.contains_key(&name) {
            return Err(LikhaDbError::CollectionAlreadyExists(name));
        }
        let index = IvfIndex::new_sq8(dim, metric, nlist, nprobe)?;
        let collection = Collection::with_index(name.clone(), dim, metric, Box::new(index));
        self.collections.insert(name, collection);
        Ok(())
    }

    /// Creates a collection backed by an HNSW (Hierarchical Navigable Small World) graph
    /// for approximate nearest-neighbour search.
    ///
    /// - `m`: max edges per node per layer (layer 0 uses `2 * m`). Typical: 16.
    /// - `ef_construction`: beam width during graph construction. Typical: 200. Must be ≥ `m`.
    /// - `ef_search`: beam width during queries. Must be ≥ 1. Higher → better recall, higher latency.
    pub fn create_hnsw_collection(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: Metric,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Result<()> {
        let name = name.into();
        if self.collections.contains_key(&name) {
            return Err(LikhaDbError::CollectionAlreadyExists(name));
        }
        let index = HnswIndex::new(dim, metric, m, ef_construction, ef_search)?;
        let collection = Collection::with_index(name.clone(), dim, metric, Box::new(index));
        self.collections.insert(name, collection);
        Ok(())
    }

    #[cfg(feature = "fts")]
    pub fn enable_fts(&mut self, name: &str, fts_dir: Option<&Path>) -> likhadb_core::Result<()> {
        self.get_mut(name)?.enable_fts(fts_dir)
    }

    #[cfg(feature = "persist")]
    pub(crate) fn insert_collection(&mut self, col: Collection) {
        self.collections.insert(col.name.clone(), col);
    }

    #[cfg(feature = "persist")]
    pub(crate) fn all_collections(&self) -> impl Iterator<Item = &Collection> {
        self.collections.values()
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
            col.insert(i, vec, Some(json!({"id_tag": i})), u64::MAX)
                .unwrap();
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
        let results = mgr
            .get("test")
            .unwrap()
            .search(&query, 5, None, false)
            .unwrap();
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
        assert!(col.delete(0, u64::MAX).unwrap());
        assert!(col.delete(1, u64::MAX).unwrap());

        // re-search
        let results2 = mgr
            .get("test")
            .unwrap()
            .search(&query, 5, None, false)
            .unwrap();
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
                u64::MAX,
            )
            .unwrap();
        }

        let pred = serde_json::json!({"field": "parity", "op": "eq", "value": "even"});
        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let col = mgr.get("tagged").unwrap();
        let results = col.search(&query, 5, Some(&pred), false).unwrap();
        assert!(results.iter().all(|r| r.id % 2 == 0));
    }

    // --- IVF integration tests ---

    #[test]
    fn create_ivf_collection_duplicate_errors() {
        let mut mgr = CollectionManager::new();
        mgr.create_ivf_collection("ivf", 4, Metric::L2, 4, 2)
            .unwrap();
        assert!(matches!(
            mgr.create_ivf_collection("ivf", 4, Metric::L2, 4, 2),
            Err(LikhaDbError::CollectionAlreadyExists(_))
        ));
    }

    #[test]
    fn create_ivf_collection_bad_params() {
        let mut mgr = CollectionManager::new();
        assert!(matches!(
            mgr.create_ivf_collection("ivf", 4, Metric::L2, 4, 5), // nprobe > nlist
            Err(LikhaDbError::InvalidArgument(_))
        ));
    }

    #[test]
    fn ivf_collection_end_to_end() {
        let nlist = 8usize;
        let mut mgr = CollectionManager::new();
        mgr.create_ivf_collection("ivf", 4, Metric::L2, nlist, nlist)
            .unwrap();
        let col = mgr.get_mut("ivf").unwrap();

        for i in 0..(nlist + 50) as u64 {
            col.insert(i, vec![i as f32, 0.0, 0.0, 0.0], None, u64::MAX)
                .unwrap();
        }

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = mgr
            .get("ivf")
            .unwrap()
            .search(&query, 5, None, false)
            .unwrap();
        assert_eq!(results.len(), 5);
        for w in results.windows(2) {
            assert!(w[0].score <= w[1].score, "results not sorted");
        }

        // Delete the nearest vector and verify it disappears.
        let nearest_id = results[0].id;
        mgr.get_mut("ivf")
            .unwrap()
            .delete(nearest_id, u64::MAX)
            .unwrap();
        let results2 = mgr
            .get("ivf")
            .unwrap()
            .search(&query, 5, None, false)
            .unwrap();
        assert!(results2.iter().all(|r| r.id != nearest_id));
    }

    #[test]
    fn create_ivf_sq8_collection_end_to_end() {
        let nlist = 8usize;
        let mut mgr = CollectionManager::new();
        mgr.create_ivf_sq8_collection("ivf_sq8", 4, Metric::L2, nlist, nlist)
            .unwrap();
        let col = mgr.get_mut("ivf_sq8").unwrap();

        for i in 0..(nlist + 50) as u64 {
            col.insert(i, vec![i as f32, 0.0, 0.0, 0.0], None, u64::MAX)
                .unwrap();
        }

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = mgr
            .get("ivf_sq8")
            .unwrap()
            .search(&query, 5, None, false)
            .unwrap();
        assert_eq!(results.len(), 5);
        for w in results.windows(2) {
            assert!(w[0].score <= w[1].score, "SQ8 results not sorted");
        }

        // Delete the nearest vector and verify it disappears.
        let nearest_id = results[0].id;
        mgr.get_mut("ivf_sq8")
            .unwrap()
            .delete(nearest_id, u64::MAX)
            .unwrap();
        let results2 = mgr
            .get("ivf_sq8")
            .unwrap()
            .search(&query, 5, None, false)
            .unwrap();
        assert!(results2.iter().all(|r| r.id != nearest_id));
    }

    #[test]
    fn create_ivf_sq8_collection_duplicate_errors() {
        let mut mgr = CollectionManager::new();
        mgr.create_ivf_sq8_collection("sq8", 4, Metric::L2, 4, 2)
            .unwrap();
        assert!(matches!(
            mgr.create_ivf_sq8_collection("sq8", 4, Metric::L2, 4, 2),
            Err(LikhaDbError::CollectionAlreadyExists(_))
        ));
    }

    // --- HNSW integration tests ---

    #[test]
    fn create_hnsw_collection_duplicate_errors() {
        let mut mgr = CollectionManager::new();
        mgr.create_hnsw_collection("hnsw", 4, Metric::L2, 4, 8, 10)
            .unwrap();
        assert!(matches!(
            mgr.create_hnsw_collection("hnsw", 4, Metric::L2, 4, 8, 10),
            Err(LikhaDbError::CollectionAlreadyExists(_))
        ));
    }

    #[test]
    fn create_hnsw_collection_bad_params() {
        let mut mgr = CollectionManager::new();
        // m < 2
        assert!(matches!(
            mgr.create_hnsw_collection("h", 4, Metric::L2, 1, 8, 10),
            Err(LikhaDbError::InvalidArgument(_))
        ));
        // ef_construction < m
        assert!(matches!(
            mgr.create_hnsw_collection("h2", 4, Metric::L2, 8, 4, 10),
            Err(LikhaDbError::InvalidArgument(_))
        ));
    }

    #[test]
    fn hnsw_collection_end_to_end() {
        let mut mgr = CollectionManager::new();
        mgr.create_hnsw_collection("hnsw", 4, Metric::L2, 4, 8, 10)
            .unwrap();
        let col = mgr.get_mut("hnsw").unwrap();

        for i in 0..50u64 {
            col.insert(i, vec![i as f32, 0.0, 0.0, 0.0], None, u64::MAX)
                .unwrap();
        }

        let query = [0.0_f32, 0.0, 0.0, 0.0];
        let results = mgr
            .get("hnsw")
            .unwrap()
            .search(&query, 5, None, false)
            .unwrap();
        assert_eq!(results.len(), 5);
        for w in results.windows(2) {
            assert!(w[0].score <= w[1].score, "results not sorted");
        }

        // Delete the nearest vector and verify it disappears.
        let nearest_id = results[0].id;
        mgr.get_mut("hnsw")
            .unwrap()
            .delete(nearest_id, u64::MAX)
            .unwrap();
        let results2 = mgr
            .get("hnsw")
            .unwrap()
            .search(&query, 5, None, false)
            .unwrap();
        assert!(results2.iter().all(|r| r.id != nearest_id));
    }

    #[test]
    fn include_payload_false_returns_none() {
        let mut mgr = CollectionManager::new();
        mgr.create_collection("p", 4, Metric::L2).unwrap();
        let col = mgr.get_mut("p").unwrap();
        col.insert(0, vec![0.0; 4], Some(json!({"tag": "a"})), u64::MAX)
            .unwrap();

        let results = mgr
            .get("p")
            .unwrap()
            .search(&[0.0; 4], 1, None, false)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].payload.is_none());
    }

    #[test]
    fn include_payload_true_returns_payload() {
        let mut mgr = CollectionManager::new();
        mgr.create_collection("p", 4, Metric::L2).unwrap();
        let col = mgr.get_mut("p").unwrap();
        col.insert(0, vec![0.0; 4], Some(json!({"tag": "a"})), u64::MAX)
            .unwrap();
        col.insert(
            1,
            vec![1.0, 0.0, 0.0, 0.0],
            Some(json!({"tag": "b"})),
            u64::MAX,
        )
        .unwrap();
        col.insert(2, vec![2.0, 0.0, 0.0, 0.0], None, u64::MAX)
            .unwrap();

        let results = mgr
            .get("p")
            .unwrap()
            .search(&[0.0; 4], 3, None, true)
            .unwrap();
        assert_eq!(results.len(), 3);

        let r0 = results.iter().find(|r| r.id == 0).unwrap();
        assert_eq!(r0.payload, Some(json!({"tag": "a"})));

        let r1 = results.iter().find(|r| r.id == 1).unwrap();
        assert_eq!(r1.payload, Some(json!({"tag": "b"})));

        let r2 = results.iter().find(|r| r.id == 2).unwrap();
        assert!(r2.payload.is_none());
    }

    #[test]
    fn include_payload_with_filter() {
        let mut mgr = CollectionManager::new();
        mgr.create_collection("pf", 4, Metric::L2).unwrap();
        let col = mgr.get_mut("pf").unwrap();
        for i in 0..10u64 {
            let tag = if i % 2 == 0 { "even" } else { "odd" };
            col.insert(
                i,
                vec![i as f32, 0.0, 0.0, 0.0],
                Some(json!({"parity": tag})),
                u64::MAX,
            )
            .unwrap();
        }

        let pred = json!({"field": "parity", "op": "eq", "value": "even"});
        let results = mgr
            .get("pf")
            .unwrap()
            .search(&[0.0; 4], 5, Some(&pred), true)
            .unwrap();
        assert!(!results.is_empty());
        for r in &results {
            let p = r.payload.as_ref().unwrap();
            assert_eq!(p["parity"], json!("even"));
        }
    }
}
