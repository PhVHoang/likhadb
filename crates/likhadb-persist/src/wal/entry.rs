use likhadb_core::{Metric, VecId};
use serde_json::Value;

/// Index configuration captured at collection-creation time so WAL replay can
/// reconstruct the right index type without touching the store layer.
#[derive(serde::Serialize, serde::Deserialize)]
pub enum IndexKind {
    Flat,
    Ivf {
        nlist: usize,
        nprobe: usize,
    },
    IvfSq8 {
        nlist: usize,
        nprobe: usize,
    },
    Hnsw {
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum WalOp {
    CreateCollection {
        name: String,
        dim: usize,
        metric: Metric,
        kind: IndexKind,
    },
    DropCollection {
        name: String,
    },
    Insert {
        collection: String,
        id: VecId,
        vector: Vec<f32>,
        #[serde(with = "opt_json_value_as_string")]
        payload: Option<Value>,
    },
    Delete {
        collection: String,
        id: VecId,
    },
    EnableFts {
        collection: String,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct WalEntry {
    pub lsn: u64,
    pub op: WalOp,
}

/// Serialize `Option<serde_json::Value>` as `Option<String>` so bincode (which
/// does not support `deserialize_any`) can handle the JSON value payload.
mod opt_json_value_as_string {
    use serde::{Deserialize, Deserializer, Serializer};
    use serde_json::Value;

    pub fn serialize<S>(v: &Option<Value>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::Serialize;
        match v {
            Some(val) => Some(val.to_string()).serialize(s),
            None => None::<String>.serialize(s),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(d)?;
        match s {
            None => Ok(None),
            Some(raw) => {
                let v: Value = serde_json::from_str(&raw).map_err(serde::de::Error::custom)?;
                Ok(Some(v))
            }
        }
    }
}
