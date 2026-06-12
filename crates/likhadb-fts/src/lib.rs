pub mod tantivy_index;

use likhadb_core::{Result, VecId};

pub use tantivy_index::TantivyFtsIndex;

#[derive(Debug, Clone)]
pub struct FtsResult {
    pub id: VecId,
    pub score: f32,
}

pub trait FtsIndex: Send + Sync {
    fn index_document(&mut self, id: VecId, text: &str) -> Result<()>;
    fn delete_document(&mut self, id: VecId) -> Result<()>;
    fn search(&self, query: &str, k: usize) -> Result<Vec<FtsResult>>;
    fn last_lsn(&self) -> u64;
    fn set_lsn(&mut self, lsn: u64) -> Result<()>;
}
