use axum::body::Body;
use axum::http::{Request, StatusCode};
use likhadb_persist::WalManager;
use likhadb_server::{install_prometheus, router, seed_collection_gauges, AppState};
use tempfile::TempDir;
use tower::ServiceExt;

fn build_app() -> (axum::Router, TempDir) {
    let dir = TempDir::new().unwrap();
    let wal = WalManager::open(dir.path()).unwrap();
    let prometheus = install_prometheus();
    seed_collection_gauges(&wal);
    let state = AppState::new(wal);
    (router(state, prometheus), dir)
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
