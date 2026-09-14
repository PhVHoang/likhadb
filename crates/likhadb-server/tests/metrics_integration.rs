use axum::body::Body;
use axum::http::{Request, StatusCode};
use likhadb_persist::WalManager;
use likhadb_server::{install_prometheus, router, seed_collection_gauges, ApiToken, AppState};
use tempfile::TempDir;
use tower::ServiceExt;

#[cfg(feature = "iceberg-recovery")]
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "iceberg-recovery")]
use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Int64Array};
#[cfg(feature = "iceberg-recovery")]
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
#[cfg(feature = "iceberg-recovery")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "iceberg-recovery")]
use bytes::Bytes;
#[cfg(feature = "iceberg-recovery")]
use iceberg::arrow::arrow_schema_to_schema;
#[cfg(feature = "iceberg-recovery")]
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
#[cfg(feature = "iceberg-recovery")]
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
#[cfg(feature = "iceberg-recovery")]
use iceberg::transaction::{ApplyTransactionAction, Transaction};
#[cfg(feature = "iceberg-recovery")]
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
#[cfg(feature = "iceberg-recovery")]
use likhadb_core::{Metric, SourceBinding};
#[cfg(feature = "iceberg-recovery")]
use likhadb_server::IndexMaintenanceTask;
#[cfg(feature = "iceberg-recovery")]
use parquet::arrow::ArrowWriter;

fn build_app() -> (axum::Router, TempDir) {
    let dir = TempDir::new().unwrap();
    let wal = WalManager::open(dir.path()).unwrap();
    let prometheus = install_prometheus();
    seed_collection_gauges(&wal);
    let state = AppState::new(wal);
    // Auth disabled so existing assertions exercise the handlers directly.
    (router(state, prometheus, ApiToken::new(None)), dir)
}

fn json_request(method: &str, uri: &str, body: &'static str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn body_text(res: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[cfg(feature = "iceberg-recovery")]
async fn scrape_metrics(app: &axum::Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    body_text(response).await
}

#[tokio::test]
async fn metrics_endpoint_is_reachable() {
    let (app, _dir) = build_app();

    let res = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_endpoint_contains_expected_metric_names() {
    let (app, _dir) = build_app();

    // Create a collection then insert a vector so the insert histogram and
    // the vector-count gauge are both emitted before we scrape /metrics.
    let res = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/collections",
            r#"{"name":"smoke","dim":3,"metric":"l2"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);

    let res = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/collections/smoke/vectors",
            r#"{"id":1,"vector":[1.0,2.0,3.0]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let res = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/collections/smoke/query",
            r#"{"vector":[1.0,2.0,3.0],"k":1}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Scrape /metrics and verify all four instrumented names appear.
    let res = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let text = body_text(res).await;

    for name in [
        "likhadb_collection_vectors_total",
        "likhadb_insert_duration_seconds",
        "likhadb_search_duration_seconds",
        "likhadb_wal_bytes_written_total",
    ] {
        assert!(
            text.contains(name),
            "metric '{name}' missing from /metrics output\n---\n{text}"
        );
    }
}

#[tokio::test]
async fn metrics_histogram_uses_custom_buckets() {
    let (app, _dir) = build_app();

    // Trigger the insert histogram.
    app.clone()
        .oneshot(json_request(
            "POST",
            "/collections",
            r#"{"name":"buckets","dim":2,"metric":"l2"}"#,
        ))
        .await
        .unwrap();
    app.clone()
        .oneshot(json_request(
            "POST",
            "/collections/buckets/vectors",
            r#"{"id":1,"vector":[0.0,1.0]}"#,
        ))
        .await
        .unwrap();

    let res = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let text = body_text(res).await;

    // The custom lower bound (100µs) must appear; the default lower bound
    // (5ms = 0.005) must NOT be the first bucket.
    assert!(
        text.contains("le=\"0.0001\""),
        "custom 100µs bucket missing — default buckets may be in use\n---\n{text}"
    );
}

#[cfg(feature = "iceberg-recovery")]
fn source_field(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        "PARQUET:field_id".to_string(),
        id.to_string(),
    )]))
}

#[cfg(feature = "iceberg-recovery")]
fn source_schema() -> Arc<ArrowSchema> {
    let vector_element = Arc::new(source_field("element", DataType::Float32, false, 3));
    Arc::new(ArrowSchema::new(vec![
        source_field("id", DataType::Int64, false, 1),
        source_field(
            "embedding",
            DataType::FixedSizeList(vector_element, 2),
            false,
            2,
        ),
    ]))
}

#[cfg(feature = "iceberg-recovery")]
fn source_parquet(id: i64, vector: [f32; 2]) -> Vec<u8> {
    let schema = source_schema();
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    let vectors: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(source_field("element", DataType::Float32, false, 3)),
            2,
            Arc::new(Float32Array::from(vector.to_vec())),
            None,
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, vectors]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

#[cfg(feature = "iceberg-recovery")]
async fn append_source_row(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    file_name: &str,
    id: i64,
    vector: [f32; 2],
) -> iceberg::table::Table {
    let bytes = source_parquet(id, vector);
    let file_path = format!("{}/data/{file_name}.parquet", table.metadata().location());
    table
        .file_io()
        .new_output(&file_path)
        .unwrap()
        .write(Bytes::from(bytes.clone()))
        .await
        .unwrap();
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(file_path)
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .record_count(1)
        .file_size_in_bytes(bytes.len() as u64)
        .build()
        .unwrap();
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files([data_file])
        .apply(tx)
        .unwrap();
    tx.commit(catalog).await.unwrap()
}

#[cfg(feature = "iceberg-recovery")]
fn metric_value(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    text.lines()
        .find(|line| {
            line.starts_with(name)
                && labels
                    .iter()
                    .all(|(key, value)| line.contains(&format!(r#"{key}="{value}""#)))
        })
        .unwrap_or_else(|| panic!("metric '{name}' with labels {labels:?} missing\n---\n{text}"))
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap()
}

#[cfg(feature = "iceberg-recovery")]
#[tokio::test]
async fn maintenance_tick_exposes_snapshot_metrics() {
    let warehouse = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "maintenance-metrics-test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.path().display()),
                )]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("source_metrics".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let table_ident = TableIdent::new(namespace.clone(), "vectors".to_string());
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table_ident.name().to_string())
                .schema(arrow_schema_to_schema(source_schema().as_ref()).unwrap())
                .build(),
        )
        .await
        .unwrap();
    let table = append_source_row(catalog.as_ref(), &table, "baseline", 1, [1.0, 0.0]).await;

    let data_dir = TempDir::new().unwrap();
    let mut wal = WalManager::open(data_dir.path()).unwrap();
    let collection = "maintenance_observability";
    wal.create_hnsw_collection(collection, 2, Metric::L2, 4, 8, 10)
        .unwrap();
    for id in 10..20 {
        wal.insert(collection, id, vec![id as f32, 0.0], None)
            .unwrap();
    }
    for id in 10..13 {
        wal.delete(collection, id).unwrap();
    }
    let binding = SourceBinding {
        source_namespace: namespace.as_ref().clone(),
        source_table: table_ident.name().to_string(),
        id_column: "id".to_string(),
        vector_column: "embedding".to_string(),
        payload_columns: vec![],
    };
    wal.set_source_binding(collection, binding.clone()).unwrap();
    wal.apply_source_delta(collection, &binding, None, i64::MAX, [])
        .unwrap();

    let prometheus = install_prometheus();
    let state = AppState::new(wal);
    let task = IndexMaintenanceTask::new(state.wal_arc(), catalog.clone());
    let app = router(state, prometheus, ApiToken::new(None));

    // The invalid watermark forces the non-ancestor fallback path. The first
    // successful tick must count that full rescan exactly once.
    task.run_once().await;
    let writer_table = catalog.load_table(&table_ident).await.unwrap();
    append_source_row(
        catalog.as_ref(),
        &writer_table,
        "incremental",
        2,
        [0.0, 1.0],
    )
    .await;
    task.run_once().await;

    let text = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let text = scrape_metrics(&app).await;
            if metric_value(
                &text,
                "likhadb_index_compactions_total",
                &[("collection", collection), ("result", "success")],
            ) == 1.0
            {
                break text;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("maintenance compaction did not complete");
    let collection_label = [("collection", collection)];

    assert_eq!(
        metric_value(
            &text,
            "likhadb_delta_rows_applied_total",
            &[("collection", collection), ("op", "upsert")],
        ),
        2.0
    );
    assert_eq!(
        metric_value(
            &text,
            "likhadb_delta_rows_applied_total",
            &[("collection", collection), ("op", "delete")],
        ),
        0.0
    );
    assert_eq!(
        metric_value(&text, "likhadb_source_full_rescan_total", &collection_label),
        1.0
    );
    assert_eq!(
        metric_value(
            &text,
            "likhadb_unresolved_delete_files_total",
            &collection_label,
        ),
        0.0
    );
    assert_eq!(
        metric_value(&text, "likhadb_source_snapshot_lag", &collection_label),
        0.0
    );
    assert_eq!(
        metric_value(&text, "likhadb_index_tombstone_ratio", &collection_label),
        0.0
    );
    assert_eq!(
        metric_value(
            &text,
            "likhadb_index_compactions_total",
            &[("collection", collection), ("result", "success")],
        ),
        1.0
    );
    for result in ["failure", "stale"] {
        assert_eq!(
            metric_value(
                &text,
                "likhadb_index_compactions_total",
                &[("collection", collection), ("result", result)],
            ),
            0.0
        );
    }

    drop(table);
}
