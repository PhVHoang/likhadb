use std::collections::HashSet;

use likhadb_core::Metric;
use likhadb_persist::PersistExt;
use likhadb_store::CollectionManager;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::json;

fn tmp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("likhadb_test_{name}.bin"))
}

fn random_vecs(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n).map(|_| (0..dim).map(|_| rng.gen::<f32>()).collect()).collect()
}

#[test]
fn flat_round_trip() {
    let dim = 4usize;
    let vecs = random_vecs(100, dim, 1);

    let mut mgr = CollectionManager::new();
    mgr.create_collection("flat", dim, Metric::L2).unwrap();
    let col = mgr.get_mut("flat").unwrap();
    for (i, v) in vecs.iter().enumerate() {
        col.insert(i as u64, v.clone(), Some(json!({"i": i}))).unwrap();
    }

    let query: Vec<f32> = vecs[0].clone();
    let before = mgr.get("flat").unwrap().search(&query, 5, None, true).unwrap();

    let path = tmp_path("flat");
    mgr.save(&path).unwrap();
    let mgr2 = CollectionManager::load(&path).unwrap();

    let after = mgr2.get("flat").unwrap().search(&query, 5, None, true).unwrap();
    let before_ids: Vec<u64> = before.iter().map(|r| r.id).collect();
    let after_ids: Vec<u64> = after.iter().map(|r| r.id).collect();
    assert_eq!(before_ids, after_ids, "flat: result IDs must match after round-trip");
    assert_eq!(after[0].payload, Some(json!({"i": after[0].id as usize})));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn ivf_round_trip() {
    let dim = 4usize;
    let nlist = 8usize;
    let vecs = random_vecs(nlist + 50, dim, 2);

    let mut mgr = CollectionManager::new();
    mgr.create_ivf_collection("ivf", dim, Metric::L2, nlist, nlist).unwrap();
    let col = mgr.get_mut("ivf").unwrap();
    for (i, v) in vecs.iter().enumerate() {
        col.insert(i as u64, v.clone(), None).unwrap();
    }

    let query: Vec<f32> = vecs[0].clone();
    let before = mgr.get("ivf").unwrap().search(&query, 5, None, false).unwrap();

    let path = tmp_path("ivf");
    mgr.save(&path).unwrap();
    let mgr2 = CollectionManager::load(&path).unwrap();

    let after = mgr2.get("ivf").unwrap().search(&query, 5, None, false).unwrap();
    let before_ids: HashSet<u64> = before.iter().map(|r| r.id).collect();
    let after_ids: HashSet<u64> = after.iter().map(|r| r.id).collect();
    assert_eq!(before_ids, after_ids, "ivf: result IDs must match after round-trip");
    assert_eq!(mgr2.get("ivf").unwrap().index_type(), "IvfIndex");

    let _ = std::fs::remove_file(&path);
}

#[test]
fn ivf_sq8_round_trip() {
    let dim = 4usize;
    let nlist = 8usize;
    let vecs = random_vecs(nlist + 50, dim, 3);

    let mut mgr = CollectionManager::new();
    mgr.create_ivf_sq8_collection("sq8", dim, Metric::L2, nlist, nlist).unwrap();
    let col = mgr.get_mut("sq8").unwrap();
    for (i, v) in vecs.iter().enumerate() {
        col.insert(i as u64, v.clone(), None).unwrap();
    }

    let query: Vec<f32> = vecs[0].clone();
    let before = mgr.get("sq8").unwrap().search(&query, 5, None, false).unwrap();

    let path = tmp_path("ivf_sq8");
    mgr.save(&path).unwrap();
    let mgr2 = CollectionManager::load(&path).unwrap();

    let after = mgr2.get("sq8").unwrap().search(&query, 5, None, false).unwrap();
    assert!(!after.is_empty(), "sq8: results must not be empty after round-trip");
    assert_eq!(mgr2.get("sq8").unwrap().index_type(), "IvfIndex");

    let before_ids: HashSet<u64> = before.iter().map(|r| r.id).collect();
    let after_ids: HashSet<u64> = after.iter().map(|r| r.id).collect();
    let overlap = before_ids.intersection(&after_ids).count();
    assert!(overlap >= 4, "sq8: expected ≥4/5 overlap, got {overlap}");

    let _ = std::fs::remove_file(&path);
}

#[test]
fn hnsw_round_trip() {
    let dim = 4usize;
    let vecs = random_vecs(200, dim, 4);

    let mut mgr = CollectionManager::new();
    mgr.create_hnsw_collection("hnsw", dim, Metric::L2, 4, 16, 20).unwrap();
    let col = mgr.get_mut("hnsw").unwrap();
    for (i, v) in vecs.iter().enumerate() {
        col.insert(i as u64, v.clone(), None).unwrap();
    }

    let query: Vec<f32> = vecs[0].clone();
    let before = mgr.get("hnsw").unwrap().search(&query, 10, None, false).unwrap();

    let path = tmp_path("hnsw");
    mgr.save(&path).unwrap();
    let mgr2 = CollectionManager::load(&path).unwrap();

    let after = mgr2.get("hnsw").unwrap().search(&query, 10, None, false).unwrap();
    assert_eq!(mgr2.get("hnsw").unwrap().index_type(), "HnswIndex");

    let before_ids: HashSet<u64> = before.iter().map(|r| r.id).collect();
    let after_ids: HashSet<u64> = after.iter().map(|r| r.id).collect();
    let overlap = before_ids.intersection(&after_ids).count();
    assert!(overlap >= 9, "hnsw: expected ≥90% recall, got {overlap}/10");

    let _ = std::fs::remove_file(&path);
}

#[test]
fn multi_collection_round_trip() {
    let dim = 4usize;
    let vecs = random_vecs(20, dim, 5);

    let mut mgr = CollectionManager::new();
    mgr.create_collection("flat", dim, Metric::L2).unwrap();
    mgr.create_ivf_collection("ivf", dim, Metric::L2, 4, 4).unwrap();
    mgr.create_hnsw_collection("hnsw", dim, Metric::L2, 4, 8, 10).unwrap();

    for name in ["flat", "ivf", "hnsw"] {
        let col = mgr.get_mut(name).unwrap();
        for (i, v) in vecs.iter().enumerate() {
            col.insert(i as u64, v.clone(), None).unwrap();
        }
    }

    let path = tmp_path("multi");
    mgr.save(&path).unwrap();
    let mgr2 = CollectionManager::load(&path).unwrap();

    let mut names = mgr2.list();
    names.sort_unstable();
    assert_eq!(names, vec!["flat", "hnsw", "ivf"]);
    assert_eq!(mgr2.get("flat").unwrap().index_type(), "FlatIndex");
    assert_eq!(mgr2.get("ivf").unwrap().index_type(), "IvfIndex");
    assert_eq!(mgr2.get("hnsw").unwrap().index_type(), "HnswIndex");

    let _ = std::fs::remove_file(&path);
}

#[test]
fn empty_manager_round_trip() {
    let mgr = CollectionManager::new();
    let path = tmp_path("empty");
    mgr.save(&path).unwrap();
    let mgr2 = CollectionManager::load(&path).unwrap();
    assert!(mgr2.list().is_empty());
    let _ = std::fs::remove_file(&path);
}

#[test]
fn missing_file_returns_io_error() {
    let result = CollectionManager::load(std::path::Path::new("/nonexistent/path.bin"));
    assert!(
        matches!(result, Err(likhadb_persist::PersistError::Io(_))),
        "expected Io error"
    );
}

#[test]
fn corrupt_file_returns_decode_error() {
    let path = tmp_path("corrupt");
    std::fs::write(&path, b"not valid bincode data at all").unwrap();
    let result = CollectionManager::load(&path);
    assert!(
        matches!(result, Err(likhadb_persist::PersistError::Decode(_))),
        "expected Decode error"
    );
    let _ = std::fs::remove_file(&path);
}
