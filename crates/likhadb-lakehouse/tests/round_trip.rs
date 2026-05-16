use likhadb_core::Metric;
use likhadb_lakehouse::LakehouseExt;
use likhadb_store::manager::CollectionManager;
use tempfile::tempdir;

#[test]
fn parquet_round_trip_10k_vectors() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("test.parquet");

    // 1. Build source collection with 10k vectors (dim=16, L2)
    let mut src = CollectionManager::new();
    src.create_collection("src", 16, Metric::L2).unwrap();
    {
        let col = src.get_mut("src").unwrap();
        for i in 0u64..10_000 {
            let vec: Vec<f32> = (0..16).map(|d| (i * 16 + d) as f32 / 1000.0).collect();
            let payload = Some(serde_json::json!({"idx": i, "label": format!("item_{i}")}));
            col.insert(i, vec, payload).unwrap();
        }
    }

    // 2. Export
    src.export_parquet("src", &parquet_path).unwrap();
    assert!(parquet_path.exists());

    // 3. Import into a new collection
    let mut dst = CollectionManager::new();
    dst.create_collection("dst", 16, Metric::L2).unwrap();
    let count = dst
        .import_parquet("dst", &parquet_path, "id", "vector", &["payload"])
        .unwrap();
    assert_eq!(count, 10_000);
    assert_eq!(dst.get("dst").unwrap().len(), 10_000);

    // 4. Verify search results are identical
    let query: Vec<f32> = (0..16).map(|d| d as f32 / 1000.0).collect();
    let src_results = src.get("src").unwrap().search(&query, 10, None, false).unwrap();
    let dst_results = dst.get("dst").unwrap().search(&query, 10, None, false).unwrap();

    assert_eq!(src_results.len(), dst_results.len());

    let mut src_ids: Vec<u64> = src_results.iter().map(|r| r.id).collect();
    let mut dst_ids: Vec<u64> = dst_results.iter().map(|r| r.id).collect();
    src_ids.sort_unstable();
    dst_ids.sort_unstable();
    assert_eq!(src_ids, dst_ids, "top-10 result IDs must match after round-trip");
}
