use std::collections::HashMap;

use serde_json::Value;

use likhadb_core::VecId;

pub struct MetaStore {
    payloads: HashMap<VecId, Value>,
}

impl MetaStore {
    pub fn new() -> Self {
        Self {
            payloads: HashMap::new(),
        }
    }

    pub fn set(&mut self, id: VecId, payload: Value) {
        self.payloads.insert(id, payload);
    }

    pub fn get(&self, id: VecId) -> Option<&Value> {
        self.payloads.get(&id)
    }

    pub fn remove(&mut self, id: VecId) -> bool {
        self.payloads.remove(&id).is_some()
    }

    /// Build a FilterFn closure for use in VectorIndex::search.
    ///
    /// Accepted predicate shape:
    /// ```json
    /// { "field": "<key>", "op": "eq" | "ne" | "exists", "value": <json> }
    /// ```
    ///
    /// Tier 1 supports: "eq", "ne", "exists".
    /// TODO(tier-2): add "gt", "lt", "in", compound "and"/"or" predicates.
    pub fn make_filter(
        &self,
        predicate: Option<&Value>,
    ) -> Option<Box<dyn Fn(VecId) -> bool + Send + Sync + '_>> {
        let pred = predicate?;

        let field = pred.get("field")?.as_str()?.to_owned();
        let op = pred.get("op")?.as_str()?.to_owned();
        let value = pred.get("value").cloned();

        Some(Box::new(move |id: VecId| {
            let payload = match self.payloads.get(&id) {
                Some(p) => p,
                None => return false,
            };

            let field_val = payload.get(&field);

            match op.as_str() {
                "eq" => {
                    let Some(v) = value.as_ref() else { return false };
                    field_val == Some(v)
                }
                "ne" => {
                    let Some(v) = value.as_ref() else { return false };
                    field_val != Some(v)
                }
                "exists" => field_val.is_some(),
                _ => false,
            }
        }))
    }
}

impl Default for MetaStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_get_remove() {
        let mut store = MetaStore::new();
        store.set(1, json!({"tag": "cat"}));
        assert_eq!(store.get(1), Some(&json!({"tag": "cat"})));
        assert!(store.remove(1));
        assert!(store.get(1).is_none());
    }

    #[test]
    fn filter_eq() {
        let mut store = MetaStore::new();
        store.set(1, json!({"tag": "cat"}));
        store.set(2, json!({"tag": "dog"}));

        let pred = json!({"field": "tag", "op": "eq", "value": "cat"});
        let f = store.make_filter(Some(&pred)).unwrap();
        assert!(f(1));
        assert!(!f(2));
    }

    #[test]
    fn filter_ne() {
        let mut store = MetaStore::new();
        store.set(1, json!({"tag": "cat"}));
        store.set(2, json!({"tag": "dog"}));

        let pred = json!({"field": "tag", "op": "ne", "value": "cat"});
        let f = store.make_filter(Some(&pred)).unwrap();
        assert!(!f(1));
        assert!(f(2));
    }

    #[test]
    fn filter_exists() {
        let mut store = MetaStore::new();
        store.set(1, json!({"tag": "cat"}));
        store.set(2, json!({}));

        let pred = json!({"field": "tag", "op": "exists"});
        let f = store.make_filter(Some(&pred)).unwrap();
        assert!(f(1));
        assert!(!f(2));
    }

    #[test]
    fn filter_none_predicate_returns_none() {
        let store = MetaStore::new();
        assert!(store.make_filter(None).is_none());
    }
}
