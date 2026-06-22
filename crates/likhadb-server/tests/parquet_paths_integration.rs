use axum::body::Body;
use axum::http::{Request, StatusCode};
use likhadb_persist::WalManager;
use likhadb_server::{install_prometheus, router, seed_collection_gauges, ApiToken, AppState};
use tempfile::TempDir;
use tower::ServiceExt;

fn build_app() -> (axum::Router, TempDir) {
    let dir = TempDir::new().unwrap();
    let wal = WalManager::open(dir.path()).unwrap();
    let prometheus = install_prometheus();
    seed_collection_gauges(&wal);
    let state = AppState::new(wal);
    (router(state, prometheus, ApiToken::new(None)), dir)
}

fn import_req(path: &str) -> Request<Body> {
    let body = format!(r#"{{"path":"{path}","id_col":"id","vector_col":"vec"}}"#);
    Request::builder()
        .method("POST")
        .uri("/collections/c/import-parquet")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

// Single test: LIKHADB_PARQUET_ROOT is process-global, so we keep all
// assertions that mutate it in one sequential test.
#[tokio::test]
async fn parquet_paths_are_confined() {
    let (app, _dir) = build_app();

    // No root configured → endpoint disabled.
    std::env::remove_var("LIKHADB_PARQUET_ROOT");
    let res = app.clone().oneshot(import_req("x.parquet")).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // With a root set, traversal and absolute escapes are rejected.
    let root = TempDir::new().unwrap();
    std::env::set_var("LIKHADB_PARQUET_ROOT", root.path());

    let res = app
        .clone()
        .oneshot(import_req("../../../../etc/passwd"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    let res = app
        .clone()
        .oneshot(import_req("/etc/passwd"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    std::env::remove_var("LIKHADB_PARQUET_ROOT");
}
