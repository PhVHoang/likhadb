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

fn json_req(method: &str, uri: &str, body: impl Into<String>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.into()))
        .unwrap()
}

async fn query_status(app: &axum::Router, k: usize) -> StatusCode {
    let body = format!(r#"{{"vector":[0.1,0.2,0.3],"k":{k}}}"#);
    app.clone()
        .oneshot(json_req("POST", "/collections/c/query", body))
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn k_is_clamped() {
    let (app, _dir) = build_app();

    let res = app
        .clone()
        .oneshot(json_req(
            "POST",
            "/collections",
            r#"{"name":"c","dim":3,"metric":"l2"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);

    // k = 0 is rejected.
    assert_eq!(query_status(&app, 0).await, StatusCode::BAD_REQUEST);
    // k beyond MAX_K (1024) is rejected.
    assert_eq!(query_status(&app, 5000).await, StatusCode::BAD_REQUEST);
    // A sane k succeeds (empty collection still returns 200 with no results).
    assert_eq!(query_status(&app, 5).await, StatusCode::OK);
}

#[tokio::test]
async fn oversized_body_is_rejected() {
    let (app, _dir) = build_app();

    // ~5 MiB payload — over the 4 MiB DefaultBodyLimit.
    let big = "x".repeat(5 * 1024 * 1024);
    let body = format!(r#"{{"name":"c","dim":3,"metric":"l2","junk":"{big}"}}"#);

    let res = app
        .oneshot(json_req("POST", "/collections", body))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
