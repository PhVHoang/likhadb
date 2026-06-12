#![cfg(feature = "minio")]

use std::sync::Arc;

use likhadb_core::Metric;
use likhadb_lakehouse::minio::ObjectStoreLakehouseExt;
use likhadb_store::manager::CollectionManager;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;

fn make_manager(name: &str, dim: usize) -> CollectionManager {
    let mut m = CollectionManager::new();
    m.create_collection(name, dim, Metric::L2).unwrap();
    m
}

#[tokio::test]
async fn round_trip_in_memory_store() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = StorePath::from("test/vectors.parquet");

    let mut src = make_manager("src", 4);
    {
        let col = src.get_mut("src").unwrap();
        for i in 0u64..100 {
            let vec = vec![i as f32, (i + 1) as f32, (i + 2) as f32, (i + 3) as f32];
            col.insert(i, vec, Some(serde_json::json!({"idx": i})), u64::MAX)
                .unwrap();
        }
    }

    src.export_parquet_to_store("src", &store, &path)
        .await
        .unwrap();

    let mut dst = make_manager("dst", 4);
    let count = dst
        .import_parquet_from_store("dst", &store, &path, "id", "vector", &["payload"])
        .await
        .unwrap();

    assert_eq!(count, 100);
    assert_eq!(dst.get("dst").unwrap().len(), 100);

    let (vec, payload) = dst.get("dst").unwrap().get(42).unwrap().unwrap();
    assert_eq!(vec, vec![42.0, 43.0, 44.0, 45.0]);
    assert_eq!(payload.unwrap()["idx"], 42);
}

#[tokio::test]
async fn empty_collection_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = StorePath::from("empty/vectors.parquet");

    let src = make_manager("src", 8);
    src.export_parquet_to_store("src", &store, &path)
        .await
        .unwrap();

    let mut dst = make_manager("dst", 8);
    let count = dst
        .import_parquet_from_store("dst", &store, &path, "id", "vector", &[])
        .await
        .unwrap();

    assert_eq!(count, 0);
    assert_eq!(dst.get("dst").unwrap().len(), 0);
}

#[tokio::test]
async fn dim_mismatch_returns_error() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = StorePath::from("mismatch/vectors.parquet");

    // Export dim=4 collection
    let mut src = make_manager("src", 4);
    src.get_mut("src")
        .unwrap()
        .insert(1, vec![1.0, 2.0, 3.0, 4.0], None, u64::MAX)
        .unwrap();
    src.export_parquet_to_store("src", &store, &path)
        .await
        .unwrap();

    // Try to import into dim=8 collection — must fail
    let mut dst = make_manager("dst", 8);
    let err = dst
        .import_parquet_from_store("dst", &store, &path, "id", "vector", &[])
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            likhadb_lakehouse::LakehouseError::DimMismatch {
                expected: 8,
                got: 4
            }
        ),
        "unexpected error: {err}"
    );
}

/// Integration test against a real MinIO instance.
///
/// Start MinIO locally with:
///   docker run -p 9000:9000 -p 9001:9001 \
///     -e MINIO_ROOT_USER=minioadmin \
///     -e MINIO_ROOT_PASSWORD=minioadmin \
///     quay.io/minio/minio server /data --console-address ":9001"
///
/// Then create the bucket (e.g. via `mc mb myminio/likhadb-test`) and run:
///   MINIO_ENDPOINT=http://localhost:9000 \
///   MINIO_BUCKET=likhadb-test \
///   MINIO_ACCESS_KEY=minioadmin \
///   MINIO_SECRET_KEY=minioadmin \
///   cargo test --features minio -p likhadb-lakehouse -- minio_real --ignored --nocapture
#[tokio::test]
#[ignore]
async fn minio_real_round_trip() {
    let endpoint = std::env::var("MINIO_ENDPOINT").expect("MINIO_ENDPOINT required");
    let bucket = std::env::var("MINIO_BUCKET").expect("MINIO_BUCKET required");
    let access_key = std::env::var("MINIO_ACCESS_KEY").expect("MINIO_ACCESS_KEY required");
    let secret_key = std::env::var("MINIO_SECRET_KEY").expect("MINIO_SECRET_KEY required");

    let config = likhadb_lakehouse::MinioConfig {
        endpoint,
        bucket,
        access_key,
        secret_key,
        region: "us-east-1".to_string(),
    };
    let store = likhadb_lakehouse::build_minio_store(&config).unwrap();
    let path = StorePath::from("integration-test/vectors.parquet");

    let dim = 8;
    let n = 1_000u64;

    let mut src = make_manager("src", dim);
    {
        let col = src.get_mut("src").unwrap();
        for i in 0..n {
            let vec: Vec<f32> = (0..dim)
                .map(|d| (i * dim as u64 + d as u64) as f32 / 1000.0)
                .collect();
            col.insert(i, vec, Some(serde_json::json!({"i": i})), u64::MAX)
                .unwrap();
        }
    }

    src.export_parquet_to_store("src", &store, &path)
        .await
        .unwrap();
    println!("exported {n} vectors to MinIO");

    let mut dst = make_manager("dst", dim);
    let count = dst
        .import_parquet_from_store("dst", &store, &path, "id", "vector", &["payload"])
        .await
        .unwrap();

    assert_eq!(count, n as usize);
    assert_eq!(dst.get("dst").unwrap().len(), n as usize);

    let (vec, payload) = dst.get("dst").unwrap().get(0).unwrap().unwrap();
    assert_eq!(vec.len(), dim);
    assert_eq!(payload.unwrap()["i"], 0);
    println!("imported {count} vectors from MinIO — round-trip OK");
}
