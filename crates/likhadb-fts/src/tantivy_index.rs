use std::sync::Mutex;

use likhadb_core::{LikhaDbError, Result, VecId};
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::{Field, Schema, Value, INDEXED, STORED, TEXT},
    Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term,
};

use crate::{FtsIndex, FtsResult};

pub struct TantivyFtsIndex {
    index: Index,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
    id_field: Field,
    text_field: Field,
}

impl TantivyFtsIndex {
    pub fn new() -> Result<Self> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", INDEXED | STORED);
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let writer = index
            .writer(15_000_000)
            .map_err(|e| LikhaDbError::Fts(e.to_string()))?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| LikhaDbError::Fts(e.to_string()))?;

        Ok(Self {
            index,
            writer: Mutex::new(writer),
            reader,
            id_field,
            text_field,
        })
    }
}

impl FtsIndex for TantivyFtsIndex {
    fn index_document(&mut self, id: VecId, text: &str) -> Result<()> {
        let doc = tantivy::doc!(self.id_field => id, self.text_field => text);
        {
            let mut writer = self.writer.lock().unwrap();
            writer
                .add_document(doc)
                .map_err(|e| LikhaDbError::Fts(e.to_string()))?;
            writer
                .commit()
                .map_err(|e| LikhaDbError::Fts(e.to_string()))?;
        }
        self.reader
            .reload()
            .map_err(|e| LikhaDbError::Fts(e.to_string()))
    }

    fn delete_document(&mut self, id: VecId) -> Result<()> {
        let term = Term::from_field_u64(self.id_field, id);
        {
            let mut writer = self.writer.lock().unwrap();
            writer.delete_term(term);
            writer
                .commit()
                .map_err(|e| LikhaDbError::Fts(e.to_string()))?;
        }
        self.reader
            .reload()
            .map_err(|e| LikhaDbError::Fts(e.to_string()))
    }

    fn search(&self, query: &str, k: usize) -> Result<Vec<FtsResult>> {
        if k == 0 || query.trim().is_empty() {
            return Ok(vec![]);
        }
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.text_field]);
        let parsed = query_parser
            .parse_query(query)
            .map_err(|e| LikhaDbError::Fts(e.to_string()))?;
        let top_docs = searcher
            .search(&parsed, &TopDocs::with_limit(k))
            .map_err(|e| LikhaDbError::Fts(e.to_string()))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| LikhaDbError::Fts(e.to_string()))?;
            if let Some(id) = doc.get_first(self.id_field).and_then(|v| v.as_u64()) {
                results.push(FtsResult { id, score });
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index() -> TantivyFtsIndex {
        TantivyFtsIndex::new().expect("index creation failed")
    }

    #[test]
    fn new_index_search_returns_empty() {
        let idx = make_index();
        let results = idx.search("anything", 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn empty_query_returns_empty() {
        let mut idx = make_index();
        idx.index_document(1, "hello world").unwrap();
        assert!(idx.search("", 10).unwrap().is_empty());
        assert!(idx.search("   ", 10).unwrap().is_empty());
    }

    #[test]
    fn k_zero_returns_empty() {
        let mut idx = make_index();
        idx.index_document(1, "hello world").unwrap();
        assert!(idx.search("hello", 0).unwrap().is_empty());
    }

    #[test]
    fn index_and_search_finds_document() {
        let mut idx = make_index();
        idx.index_document(42, "tantivy full text search engine")
            .unwrap();
        let results = idx.search("tantivy", 5).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 42);
    }

    #[test]
    fn search_returns_positive_scores() {
        let mut idx = make_index();
        idx.index_document(1, "rust programming language").unwrap();
        let results = idx.search("rust", 5).unwrap();
        assert!(!results.is_empty());
        for r in &results {
            assert!(r.score > 0.0, "BM25 score must be positive");
        }
    }

    #[test]
    fn unrelated_query_returns_empty() {
        let mut idx = make_index();
        idx.index_document(1, "the quick brown fox").unwrap();
        let results = idx.search("xylophone", 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn delete_removes_document_from_results() {
        let mut idx = make_index();
        idx.index_document(1, "delete me from results").unwrap();
        idx.index_document(2, "keep this document in results")
            .unwrap();

        let before = idx.search("results", 10).unwrap();
        assert_eq!(before.len(), 2);

        idx.delete_document(1).unwrap();

        let after = idx.search("delete", 10).unwrap();
        assert!(after.is_empty(), "deleted doc should not appear");

        let kept = idx.search("keep", 10).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, 2);
    }

    #[test]
    fn delete_nonexistent_is_a_noop() {
        let mut idx = make_index();
        idx.index_document(1, "only document").unwrap();
        idx.delete_document(999).unwrap(); // should not error
        let results = idx.search("only", 5).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn search_respects_top_k_limit() {
        let mut idx = make_index();
        for i in 0..20u64 {
            idx.index_document(i, "common keyword in every document")
                .unwrap();
        }
        let results = idx.search("common", 5).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn results_are_ordered_by_score_descending() {
        let mut idx = make_index();
        // doc 1 has "rust" once, doc 2 has it three times — doc 2 should score higher
        idx.index_document(1, "rust").unwrap();
        idx.index_document(2, "rust rust rust language").unwrap();
        let results = idx.search("rust", 10).unwrap();
        assert_eq!(results.len(), 2);
        assert!(
            results[0].score >= results[1].score,
            "results must be ordered score descending"
        );
    }

    #[test]
    fn multiple_fields_in_same_document_all_searchable() {
        let mut idx = make_index();
        idx.index_document(1, "alpha beta gamma").unwrap();
        assert_eq!(idx.search("alpha", 5).unwrap().len(), 1);
        assert_eq!(idx.search("beta", 5).unwrap().len(), 1);
        assert_eq!(idx.search("gamma", 5).unwrap().len(), 1);
    }

    #[test]
    fn one_thousand_docs_top_result_is_best_match() {
        let mut idx = make_index();
        for i in 0..1000u64 {
            let text = if i == 777 {
                "exclusive unique term zephyr mountainpeak".to_string()
            } else {
                format!("generic document number {i} with common words")
            };
            idx.index_document(i, &text).unwrap();
        }
        let results = idx.search("zephyr", 5).unwrap();
        assert!(!results.is_empty(), "should find the unique document");
        assert_eq!(results[0].id, 777, "top result must be the unique doc");
    }
}
