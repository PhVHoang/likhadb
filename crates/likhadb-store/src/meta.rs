use std::collections::HashMap;

use serde_json::Value;

use likhadb_core::VecId;

/// Serializes `serde_json::Value` as a JSON string so that binary formats
/// (e.g. bincode) can handle it — bincode does not support `deserialize_any`.
#[cfg(feature = "persist")]
mod json_value_as_string {
    use likhadb_core::VecId;
    use serde::{Deserialize, Deserializer, Serializer};
    use serde_json::Value;
    use std::collections::HashMap;

    pub fn serialize<S>(map: &HashMap<VecId, Value>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::Serialize;
        let encoded: HashMap<VecId, String> =
            map.iter().map(|(k, v)| (*k, v.to_string())).collect();
        encoded.serialize(s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<HashMap<VecId, Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded: HashMap<VecId, String> = HashMap::deserialize(d)?;
        encoded
            .into_iter()
            .map(|(k, s)| {
                let v: Value = serde_json::from_str(&s).map_err(serde::de::Error::custom)?;
                Ok((k, v))
            })
            .collect()
    }
}

#[cfg_attr(feature = "persist", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone)]
pub struct MetaStore {
    #[cfg_attr(feature = "persist", serde(with = "json_value_as_string"))]
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
    /// ## Leaf predicates
    /// ```json
    /// { "op": "eq",     "field": "tag",   "value": "sports" }
    /// { "op": "ne",     "field": "tag",   "value": "sports" }
    /// { "op": "exists", "field": "score" }
    /// { "op": "gt",     "field": "price", "value": 10.0 }
    /// { "op": "lt",     "field": "year",  "value": 2024 }
    /// { "op": "gte",    "field": "score", "value": 0.5 }
    /// { "op": "lte",    "field": "rank",  "value": 100 }
    /// { "op": "in",     "field": "tag",   "value": ["sports", "news"] }
    /// ```
    ///
    /// ## Compound predicates
    /// ```json
    /// { "op": "and", "filters": [ <pred>, ... ] }
    /// { "op": "or",  "filters": [ <pred>, ... ] }
    /// ```
    /// Compounds can be nested to arbitrary depth.
    pub fn make_filter(
        &self,
        predicate: Option<&Value>,
    ) -> Option<Box<dyn Fn(VecId) -> bool + Send + Sync + '_>> {
        let pred = predicate?.clone();
        Some(Box::new(move |id: VecId| match self.payloads.get(&id) {
            Some(payload) => eval_predicate(payload, &pred),
            None => false,
        }))
    }
}

/// Recursively evaluate a predicate JSON object against a payload.
fn eval_predicate(payload: &Value, pred: &Value) -> bool {
    let op = match pred.get("op").and_then(|v| v.as_str()) {
        Some(op) => op,
        None => return false,
    };

    match op {
        "and" => pred
            .get("filters")
            .and_then(|v| v.as_array())
            .is_some_and(|filters| filters.iter().all(|f| eval_predicate(payload, f))),
        "or" => pred
            .get("filters")
            .and_then(|v| v.as_array())
            .is_some_and(|filters| filters.iter().any(|f| eval_predicate(payload, f))),
        leaf_op => {
            let field = match pred.get("field").and_then(|v| v.as_str()) {
                Some(f) => f,
                None => return false,
            };
            let field_val = payload.get(field);
            let value = pred.get("value");

            match leaf_op {
                "eq" => match value {
                    Some(v) => field_val == Some(v),
                    None => false,
                },
                "ne" => match value {
                    Some(v) => field_val != Some(v),
                    None => false,
                },
                "exists" => field_val.is_some(),
                "gt" | "lt" | "gte" | "lte" => {
                    let a = field_val.and_then(|v| v.as_f64());
                    let b = value.and_then(|v| v.as_f64());
                    match (a, b) {
                        (Some(a), Some(b)) => match leaf_op {
                            "gt" => a > b,
                            "lt" => a < b,
                            "gte" => a >= b,
                            "lte" => a <= b,
                            _ => unreachable!(),
                        },
                        _ => false,
                    }
                }
                "in" => {
                    let arr = match value.and_then(|v| v.as_array()) {
                        Some(a) => a,
                        None => return false,
                    };
                    field_val.is_some_and(|fv| arr.contains(fv))
                }
                _ => false,
            }
        }
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

    fn make_store() -> MetaStore {
        let mut s = MetaStore::new();
        s.set(1, json!({"price": 5.0,  "tag": "sports", "rank": 1}));
        s.set(2, json!({"price": 15.0, "tag": "news",   "rank": 2}));
        s.set(3, json!({"price": 25.0, "tag": "sports", "rank": 3}));
        s.set(4, json!({"other": true}));
        s
    }

    #[test]
    fn filter_gt() {
        let s = make_store();
        let f = s
            .make_filter(Some(&json!({"op":"gt","field":"price","value":10.0})))
            .unwrap();
        assert!(!f(1));
        assert!(f(2));
        assert!(f(3));
        assert!(!f(4)); // field missing → false
    }

    #[test]
    fn filter_lt() {
        let s = make_store();
        let f = s
            .make_filter(Some(&json!({"op":"lt","field":"price","value":10.0})))
            .unwrap();
        assert!(f(1));
        assert!(!f(2));
        assert!(!f(3));
    }

    #[test]
    fn filter_gte() {
        let s = make_store();
        let f = s
            .make_filter(Some(&json!({"op":"gte","field":"price","value":15.0})))
            .unwrap();
        assert!(!f(1));
        assert!(f(2)); // 15.0 >= 15.0
        assert!(f(3));
    }

    #[test]
    fn filter_lte() {
        let s = make_store();
        let f = s
            .make_filter(Some(&json!({"op":"lte","field":"price","value":15.0})))
            .unwrap();
        assert!(f(1));
        assert!(f(2)); // 15.0 <= 15.0
        assert!(!f(3));
    }

    #[test]
    fn filter_in() {
        let s = make_store();
        let f = s
            .make_filter(Some(
                &json!({"op":"in","field":"tag","value":["sports","tech"]}),
            ))
            .unwrap();
        assert!(f(1)); // "sports" in list
        assert!(!f(2)); // "news" not in list
        assert!(f(3));
        assert!(!f(4)); // field missing
    }

    #[test]
    fn filter_in_missing_value_key_returns_false() {
        let s = make_store();
        let f = s
            .make_filter(Some(&json!({"op":"in","field":"tag"})))
            .unwrap();
        assert!(!f(1));
    }

    #[test]
    fn filter_and() {
        let s = make_store();
        let pred = json!({
            "op": "and",
            "filters": [
                {"op": "eq",  "field": "tag",   "value": "sports"},
                {"op": "gt",  "field": "price", "value": 10.0}
            ]
        });
        let f = s.make_filter(Some(&pred)).unwrap();
        assert!(!f(1)); // sports but price=5 not > 10
        assert!(!f(2)); // price > 10 but tag=news
        assert!(f(3)); // sports AND price=25 > 10
    }

    #[test]
    fn filter_or() {
        let s = make_store();
        let pred = json!({
            "op": "or",
            "filters": [
                {"op": "eq", "field": "tag",   "value": "news"},
                {"op": "gt", "field": "price", "value": 20.0}
            ]
        });
        let f = s.make_filter(Some(&pred)).unwrap();
        assert!(!f(1)); // neither
        assert!(f(2)); // tag=news
        assert!(f(3)); // price=25 > 20
    }

    #[test]
    fn filter_nested_compound() {
        let s = make_store();
        // (tag=sports OR tag=news) AND price <= 15
        let pred = json!({
            "op": "and",
            "filters": [
                {"op": "or", "filters": [
                    {"op": "eq", "field": "tag", "value": "sports"},
                    {"op": "eq", "field": "tag", "value": "news"}
                ]},
                {"op": "lte", "field": "price", "value": 15.0}
            ]
        });
        let f = s.make_filter(Some(&pred)).unwrap();
        assert!(f(1)); // sports, price=5  ✓
        assert!(f(2)); // news,   price=15 ✓
        assert!(!f(3)); // sports, price=25 ✗
        assert!(!f(4)); // no tag field
    }

    #[test]
    fn filter_numeric_missing_field_returns_false() {
        let s = make_store();
        let f = s
            .make_filter(Some(&json!({"op":"gt","field":"price","value":1.0})))
            .unwrap();
        assert!(!f(4)); // id=4 has no "price" field
    }

    #[test]
    fn filter_unknown_op_returns_false() {
        let s = make_store();
        let f = s
            .make_filter(Some(&json!({"op":"regex","field":"tag","value":".*"})))
            .unwrap();
        assert!(!f(1));
    }
}
