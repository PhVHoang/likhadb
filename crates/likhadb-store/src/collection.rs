use likhadb_core::{Metric, Result, ScoredResult, VecId, Vector};
use likhadb_index::{FlatIndex, VectorIndex};
use serde_json::Value;

use crate::meta::MetaStore;

pub type VectorWithPayload = (Vector, Option<Value>);

#[cfg(feature = "fts")]
fn extract_text_fields(value: &Value) -> String {
    let mut out = String::new();
    collect_strings(value, &mut out);
    out
}

#[cfg(feature = "fts")]
fn collect_strings(value: &Value, out: &mut String) {
    match value {
        Value::String(s) => {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(s);
        }
        Value::Object(map) => {
            for v in map.values() {
                collect_strings(v, out);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_strings(v, out);
            }
        }
        _ => {}
    }
}

pub struct Collection {
    pub name: String,
    pub dim: usize,
    pub metric: Metric,
    pub(crate) index: Box<dyn VectorIndex>,
    pub(crate) meta: MetaStore,
    #[cfg(feature = "fts")]
    pub(crate) fts_index: Option<Box<dyn likhadb_fts::FtsIndex>>,
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
            #[cfg(feature = "fts")]
            fts_index: None,
        }
    }

    #[cfg(feature = "persist")]
    pub(crate) fn with_meta(mut self, meta: crate::meta::MetaStore) -> Self {
        self.meta = meta;
        self
    }

    #[cfg(feature = "fts")]
    pub fn enable_fts(&mut self) -> Result<()> {
        self.fts_index = Some(Box::new(likhadb_fts::TantivyFtsIndex::new()?));
        Ok(())
    }

    #[cfg(feature = "fts")]
    pub fn fts_search(&self, query: &str, k: usize) -> Result<Vec<likhadb_fts::FtsResult>> {
        match &self.fts_index {
            Some(idx) => idx.search(query, k),
            None => Ok(vec![]),
        }
    }

    pub fn insert(&mut self, id: VecId, vec: Vector, payload: Option<Value>) -> Result<()> {
        self.index.insert(id, vec)?;
        if let Some(p) = payload {
            #[cfg(feature = "fts")]
            if let Some(fts) = &mut self.fts_index {
                let text = extract_text_fields(&p);
                if !text.is_empty() {
                    fts.index_document(id, &text)?;
                }
            }
            self.meta.set(id, p);
        }
        Ok(())
    }

    pub fn delete(&mut self, id: VecId) -> Result<bool> {
        let existed = self.index.delete(id);
        self.meta.remove(id);
        #[cfg(feature = "fts")]
        if let Some(fts) = &mut self.fts_index {
            fts.delete_document(id)?;
        }
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
        c.insert(1, vec![1.0, 2.0, 3.0], Some(json!({"tag": "a"})))
            .unwrap();
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
        c.insert(1, vec![1.0, 0.0, 0.0], Some(json!({"x": 1})))
            .unwrap();
        c.insert(3, vec![3.0, 0.0, 0.0], None).unwrap();
        let results = c.get_batch(&[1, 2, 3]).unwrap();
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
    }

    #[cfg(feature = "fts")]
    mod fts_tests {
        use super::*;

        fn make_fts_collection() -> Collection {
            let mut c = Collection::new("fts_test".into(), 3, Metric::L2);
            c.enable_fts().unwrap();
            c
        }

        #[test]
        fn fts_search_finds_exact_term() {
            let mut c = make_fts_collection();
            c.insert(
                1,
                vec![1.0, 0.0, 0.0],
                Some(json!({"body": "the quick brown fox"})),
            )
            .unwrap();
            c.insert(
                2,
                vec![0.0, 1.0, 0.0],
                Some(json!({"body": "lazy dog sleeps"})),
            )
            .unwrap();
            let results = c.fts_search("fox", 5).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].id, 1);
        }

        #[test]
        fn fts_search_empty_query_returns_empty() {
            let mut c = make_fts_collection();
            c.insert(1, vec![1.0, 0.0, 0.0], Some(json!({"body": "hello world"})))
                .unwrap();
            let results = c.fts_search("", 5).unwrap();
            assert!(results.is_empty());
        }

        #[test]
        fn fts_search_no_fts_enabled_returns_empty() {
            let mut c = Collection::new("no_fts".into(), 3, Metric::L2);
            c.insert(1, vec![1.0, 0.0, 0.0], Some(json!({"body": "hello world"})))
                .unwrap();
            let results = c.fts_search("hello", 5).unwrap();
            assert!(results.is_empty());
        }

        #[test]
        fn fts_delete_removes_document_from_index() {
            let mut c = make_fts_collection();
            c.insert(
                1,
                vec![1.0, 0.0, 0.0],
                Some(json!({"body": "tantivy full text search"})),
            )
            .unwrap();
            c.insert(
                2,
                vec![0.0, 1.0, 0.0],
                Some(json!({"body": "vector similarity search"})),
            )
            .unwrap();

            // both are findable before delete
            let before = c.fts_search("search", 5).unwrap();
            assert_eq!(before.len(), 2);

            c.delete(1).unwrap();
            let after = c.fts_search("tantivy", 5).unwrap();
            assert!(after.is_empty(), "deleted doc should not appear");
        }

        #[test]
        fn fts_search_returns_top_k_only() {
            let mut c = make_fts_collection();
            for i in 0..10u64 {
                c.insert(
                    i,
                    vec![i as f32, 0.0, 0.0],
                    Some(json!({"body": "rust programming language"})),
                )
                .unwrap();
            }
            let results = c.fts_search("rust", 3).unwrap();
            assert_eq!(results.len(), 3);
        }

        #[test]
        fn fts_search_scores_are_positive() {
            let mut c = make_fts_collection();
            c.insert(
                1,
                vec![1.0, 0.0, 0.0],
                Some(json!({"title": "hello world", "desc": "a test document"})),
            )
            .unwrap();
            let results = c.fts_search("hello", 5).unwrap();
            assert!(!results.is_empty());
            for r in &results {
                assert!(r.score > 0.0, "BM25 score should be positive");
            }
        }

        #[test]
        fn fts_indexes_nested_string_fields() {
            let mut c = make_fts_collection();
            c.insert(
                1,
                vec![1.0, 0.0, 0.0],
                Some(
                    json!({"metadata": {"title": "deep nested text", "tags": ["rust", "search"]}}),
                ),
            )
            .unwrap();
            let results = c.fts_search("nested", 5).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].id, 1);
        }

        #[test]
        fn fts_insert_without_payload_does_not_index() {
            let mut c = make_fts_collection();
            c.insert(1, vec![1.0, 0.0, 0.0], None).unwrap();
            let results = c.fts_search("anything", 5).unwrap();
            assert!(results.is_empty());
        }

        #[test]
        fn fts_1k_docs_top_result_is_best_match() {
            let mut c = make_fts_collection();
            for i in 0..1000u64 {
                let body = if i == 42 {
                    "exclusive unique term: xylophone music".to_string()
                } else {
                    format!("document number {i} with generic content")
                };
                c.insert(i, vec![i as f32, 0.0, 0.0], Some(json!({"body": body})))
                    .unwrap();
            }
            let results = c.fts_search("xylophone", 5).unwrap();
            assert!(!results.is_empty(), "should find the unique doc");
            assert_eq!(results[0].id, 42, "best match should be id 42");
        }
    }
}
