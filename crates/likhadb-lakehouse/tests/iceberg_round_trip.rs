//! Integration tests for Iceberg import (L3).
//!
//! All tests in this file are `#[ignore]`d and require external services.
//!
//! # Setup
//!
//! 1. MinIO:
//!    ```sh
//!    docker run -p 9000:9000 -p 9001:9001 \
//!      -e MINIO_ROOT_USER=minioadmin \
//!      -e MINIO_ROOT_PASSWORD=minioadmin \
//!      quay.io/minio/minio server /data --console-address ":9001"
//!    mc mb myminio/likhadb-test
//!    ```
//!
//! 2. Iceberg REST catalog:
//!    ```sh
//!    docker run -p 8181:8181 \
//!      -e CATALOG_WAREHOUSE=s3://likhadb-test/warehouse \
//!      -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
//!      -e CATALOG_S3_ENDPOINT=http://host.docker.internal:9000 \
//!      -e CATALOG_S3_PATH__STYLE__ACCESS=true \
//!      -e AWS_ACCESS_KEY_ID=minioadmin \
//!      -e AWS_SECRET_ACCESS_KEY=minioadmin \
//!      -e AWS_REGION=us-east-1 \
//!      tabulario/iceberg-rest:latest
//!    ```
//!
//! 3. Pre-populate an Iceberg table called `likhadb_test.vectors` using
//!    PyIceberg or the iceberg-rs CLI.  Required columns:
//!    - `id` (long / int64)
//!    - `vector` (fixed[32], 8 floats serialised as bytes) — **or** use
//!      Arrow `FixedSizeList<Float32>` if your tooling supports it.
//!    - `payload` (string, nullable, JSON)
//!
//! 4. Run:
//!    ```sh
//!    ICEBERG_CATALOG_URI=http://localhost:8181 \
//!    MINIO_ENDPOINT=http://localhost:9000 \
//!    MINIO_BUCKET=likhadb-test \
//!    MINIO_ACCESS_KEY=minioadmin \
//!    MINIO_SECRET_KEY=minioadmin \
//!    EXPECTED_COUNT=100 \
//!    cargo test --features iceberg -p likhadb-lakehouse \
//!      -- iceberg_real --ignored --nocapture
//!    ```

#![cfg(feature = "iceberg")]

use std::collections::HashMap;

use iceberg::{Catalog, NamespaceIdent, TableIdent};
use likhadb_core::Metric;
use likhadb_lakehouse::iceberg_io::IcebergLakehouseExt;
use likhadb_store::manager::CollectionManager;

fn make_manager(name: &str, dim: usize) -> CollectionManager {
    let mut m = CollectionManager::new();
    m.create_collection(name, dim, Metric::L2).unwrap();
    m
}

/// Import from a pre-existing Iceberg table and verify the row count.
///
/// The table must already contain rows with an `id` (int64) column, a
/// `vector` (`FixedSizeList<Float32>` of length `dim`) column, and an
/// optional `payload` (string) column.
///
/// Set `EXPECTED_COUNT` to the number of rows in the table.
#[tokio::test]
#[ignore]
async fn iceberg_real_round_trip() {
    let catalog_uri = std::env::var("ICEBERG_CATALOG_URI").expect("ICEBERG_CATALOG_URI required");
    let s3_endpoint = std::env::var("MINIO_ENDPOINT").expect("MINIO_ENDPOINT required");
    let bucket = std::env::var("MINIO_BUCKET").expect("MINIO_BUCKET required");
    let access_key = std::env::var("MINIO_ACCESS_KEY").expect("MINIO_ACCESS_KEY required");
    let secret_key = std::env::var("MINIO_SECRET_KEY").expect("MINIO_SECRET_KEY required");
    let expected_count: usize = std::env::var("EXPECTED_COUNT")
        .unwrap_or_else(|_| "100".to_string())
        .parse()
        .expect("EXPECTED_COUNT must be a number");

    let dim: usize = std::env::var("DIM")
        .unwrap_or_else(|_| "8".to_string())
        .parse()
        .expect("DIM must be a number");

    let config = likhadb_lakehouse::IcebergConfig {
        catalog_uri,
        s3_endpoint,
        access_key,
        secret_key,
        region: "us-east-1".to_string(),
        warehouse: format!("s3://{bucket}/warehouse"),
        extra_properties: HashMap::new(),
    };
    let catalog = likhadb_lakehouse::build_rest_catalog(&config).unwrap();

    let ns = NamespaceIdent::new("likhadb_test".to_string());
    let ident = TableIdent::new(ns, "vectors".to_string());

    let mut manager = make_manager("dst", dim);
    let count = manager
        .import_iceberg("dst", &catalog, &ident, "id", "vector", &["payload"])
        .await
        .unwrap();

    assert_eq!(count, expected_count);
    println!("iceberg round-trip OK: {count} vectors imported from catalog");
}

/// Verify that `import_iceberg` returns `CollectionNotFound` for a missing collection.
///
/// This is the only test that can run without a real catalog because it fails
/// at the collection-lookup step before touching the network.
///
/// We use a RestCatalog pointing at a non-existent server — the collection
/// check happens first (synchronously), so the catalog is never actually
/// contacted.
#[tokio::test]
#[ignore]
async fn missing_collection_errors_before_catalog_hit() {
    let config = likhadb_lakehouse::IcebergConfig {
        catalog_uri: "http://localhost:8181".to_string(),
        s3_endpoint: "http://localhost:9000".to_string(),
        access_key: "unused".to_string(),
        secret_key: "unused".to_string(),
        region: "us-east-1".to_string(),
        warehouse: "s3://unused/warehouse".to_string(),
        extra_properties: HashMap::new(),
    };
    let catalog = likhadb_lakehouse::build_rest_catalog(&config).unwrap();

    let mut manager = CollectionManager::new();
    // "no_such" collection does not exist.
    let ns = NamespaceIdent::new("ns".to_string());
    let ident = TableIdent::new(ns, "t".to_string());

    let err = manager
        .import_iceberg("no_such", &catalog, &ident, "id", "vector", &[])
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            likhadb_lakehouse::LakehouseError::CollectionNotFound(_)
        ),
        "unexpected error: {err}"
    );
}
