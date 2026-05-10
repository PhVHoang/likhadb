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

    /// Hybrid vector + full-text search using Reciprocal Rank Fusion.
    ///
    /// `rrf_score(id) = 1/(rrf_k + rank_vec(id)) + 1/(rrf_k + rank_fts(id))`
    /// where ranks are 1-based. IDs appearing in only one result list
    /// contribute that list's term alone.
    #[cfg(feature = "fts")]
    pub fn hybrid_search(
        &self,
        vector: &[f32],
        text: &str,
        k: usize,
        rrf_k: u32,
        filter: Option<&Value>,
        include_payload: bool,
    ) -> Result<Vec<ScoredResult>> {
        use std::collections::HashMap;

        let candidate_k = k.saturating_mul(2).max(k);

        let vec_results = self.search(vector, candidate_k, filter, false)?;
        let fts_results = match &self.fts_index {
            Some(idx) => idx.search(text, candidate_k)?,
            None => vec![],
        };

        let mut scores: HashMap<VecId, f32> = HashMap::new();

        for (rank0, r) in vec_results.iter().enumerate() {
            let rrf = 1.0 / (rrf_k as f32 + rank0 as f32 + 1.0);
            *scores.entry(r.id).or_insert(0.0) += rrf;
        }

        for (rank0, r) in fts_results.iter().enumerate() {
            let rrf = 1.0 / (rrf_k as f32 + rank0 as f32 + 1.0);
            *scores.entry(r.id).or_insert(0.0) += rrf;
        }

        let mut ranked: Vec<(VecId, f32)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(k);

        let mut results: Vec<ScoredResult> = ranked
            .into_iter()
            .map(|(id, score)| ScoredResult {
                id,
                score,
                payload: None,
            })
            .collect();

        if include_payload {
            for r in &mut results {
                r.payload = self.meta.get(r.id).cloned();
            }
        }

        Ok(results)
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

        // ── hybrid_search tests ───────────────────────────────────────────────

        #[test]
        fn hybrid_rrf_scores_are_positive_and_top_k_respected() {
            let mut c = make_fts_collection();
            for i in 0..10u64 {
                c.insert(
                    i,
                    vec![i as f32, 0.0, 0.0],
                    Some(json!({"body": format!("doc {i} content")})),
                )
                .unwrap();
            }
            let results = c
                .hybrid_search(&[0.0, 0.0, 0.0], "doc", 3, 60, None, false)
                .unwrap();
            assert_eq!(results.len(), 3);
            for r in &results {
                assert!(r.score > 0.0, "RRF score must be positive");
            }
            // sorted descending
            for w in results.windows(2) {
                assert!(
                    w[0].score >= w[1].score,
                    "results must be sorted by score descending"
                );
            }
        }

        #[test]
        fn hybrid_include_payload_returns_payload() {
            let mut c = make_fts_collection();
            c.insert(
                1,
                vec![1.0, 0.0, 0.0],
                Some(json!({"body": "hello world", "tag": "a"})),
            )
            .unwrap();
            c.insert(
                2,
                vec![2.0, 0.0, 0.0],
                Some(json!({"body": "foo bar", "tag": "b"})),
            )
            .unwrap();

            let with_payload = c
                .hybrid_search(&[1.0, 0.0, 0.0], "hello", 2, 60, None, true)
                .unwrap();
            assert!(with_payload.iter().all(|r| r.payload.is_some()));

            let without_payload = c
                .hybrid_search(&[1.0, 0.0, 0.0], "hello", 2, 60, None, false)
                .unwrap();
            assert!(without_payload.iter().all(|r| r.payload.is_none()));
        }

        #[test]
        fn hybrid_beats_each_modality_alone_on_mixed_dataset() {
            // Success criterion from BIZ.md:
            // TARGET (id=99) wins hybrid top-1 even though:
            //   - vector alone puts id=0 at top-1  (id=99 is vec rank 2)
            //   - fts alone puts id=1 at top-1     (id=99 is fts rank 3)
            //
            // Dataset layout (6 docs):
            //   id=0  (VEC CHAMP):   vec=[0.001], text="animals birds"   → vec rank 1, no FTS match
            //   id=99 (TARGET):      vec=[0.1],   text="special word"    → vec rank 2, fts rank 3
            //   id=3  (FILLER VEC):  vec=[1.0],   text="generic noise"   → vec rank 3, no FTS match
            //   id=4  (FILLER VEC):  vec=[5.0],   text="generic noise"   → vec rank 4, no FTS match
            //   id=1  (TEXT CHAMP):  vec=[100.0], text="special special special" → vec rank 5, fts rank 1
            //   id=2  (NOISE TEXT):  vec=[101.0], text="special special mention" → vec rank 6, fts rank 2
            //
            // RRF scores (rrf_k=60, 0-based ranks):
            //   id=0:  1/61            ≈ 0.01639
            //   id=99: 1/62 + 1/63    ≈ 0.03200  ← WINS
            //   id=1:  1/65 + 1/61    ≈ 0.03178
            //   id=2:  1/66 + 1/62    ≈ 0.03128
            let mut c = make_fts_collection();

            c.insert(
                0,
                vec![0.001, 0.0, 0.0],
                Some(json!({"body": "animals birds completely unrelated"})),
            )
            .unwrap();
            c.insert(
                99,
                vec![0.1, 0.0, 0.0],
                Some(json!({"body": "special word"})),
            )
            .unwrap();
            c.insert(
                3,
                vec![1.0, 0.0, 0.0],
                Some(json!({"body": "generic noise text"})),
            )
            .unwrap();
            c.insert(
                4,
                vec![5.0, 0.0, 0.0],
                Some(json!({"body": "generic noise content"})),
            )
            .unwrap();
            c.insert(
                1,
                vec![100.0, 0.0, 0.0],
                Some(json!({"body": "special special special highlight"})),
            )
            .unwrap();
            c.insert(
                2,
                vec![101.0, 0.0, 0.0],
                Some(json!({"body": "special special mention"})),
            )
            .unwrap();

            let query_vec = [0.0_f32, 0.0, 0.0];
            let query_text = "special";

            // vector alone: id=0 is closest → top-1; id=99 is rank 2, not top-1
            let vec_only = c.search(&query_vec, 6, None, false).unwrap();
            assert_eq!(vec_only[0].id, 0, "vector alone: id=0 should be top-1");

            // fts alone: id=1 has highest TF → top-1; id=99 is rank 3, not top-1
            let fts_only = c.fts_search(query_text, 6).unwrap();
            assert_eq!(fts_only[0].id, 1, "fts alone: id=1 should be top-1");
            assert_ne!(fts_only[0].id, 99, "fts alone: id=99 must not be top-1");

            // hybrid: id=99 accumulates from vec rank 2 + fts rank 3 → beats both champions
            let hybrid = c
                .hybrid_search(&query_vec, query_text, 6, 60, None, false)
                .unwrap();
            assert_eq!(
                hybrid[0].id,
                99,
                "hybrid top-1 should be id=99; got {:?}",
                hybrid.iter().map(|r| r.id).collect::<Vec<_>>()
            );
        }
    }
}
