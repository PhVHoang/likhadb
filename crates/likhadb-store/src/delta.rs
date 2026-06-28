use likhadb_core::{VecId, Vector};
use serde_json::Value;

/// One ordered change to apply to a collection, regardless of where it came
/// from. Both the WAL → staging recovery path and (later) the source-table
/// incremental-maintenance path funnel through this single representation so
/// they share one apply choke point ([`crate::Collection::apply_delta_row`]).
///
/// `Upsert` is idempotent (it overwrites), and `Delete` is idempotent (deleting
/// a missing id is a no-op), so re-applying a partially-applied range is safe.
pub enum DeltaRow {
    Upsert {
        id: VecId,
        vector: Vector,
        payload: Option<Value>,
    },
    Delete {
        id: VecId,
    },
}
