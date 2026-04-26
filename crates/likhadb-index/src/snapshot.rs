use crate::{FlatIndex, HnswIndex, IvfIndex, VectorIndex};

/// Tagged union over all concrete index types, used for snapshot serialization.
/// Uses serde's default (externally tagged) representation so binary formats
/// like bincode that lack `deserialize_any` work correctly.
#[derive(serde::Serialize, serde::Deserialize)]
pub enum IndexSnapshot {
    Flat(FlatIndex),
    Ivf(IvfIndex),
    Hnsw(HnswIndex),
}

impl IndexSnapshot {
    pub fn into_box(self) -> Box<dyn VectorIndex> {
        match self {
            IndexSnapshot::Flat(f) => Box::new(f),
            IndexSnapshot::Ivf(i) => Box::new(i),
            IndexSnapshot::Hnsw(h) => Box::new(h),
        }
    }
}
