use axum::body::Body;
use axum::http::{Request, StatusCode};
use likhadb_persist::WalManager;
use likhadb_server::{install_prometheus, router, seed_collection_gauges, ApiToken, AppState};
use tempfile::TempDir;
use tower::ServiceExt;

const TOKEN: &str = "test-secret-token";

fn build_app() -> (axum::Router, TempDir) {
    let dir = TempDir::new().unwrap();
    let wal = WalManager::open(dir.path()).unwrap();
    let prometheus = install_prometheus();
    seed_collection_gauges(&wal);
    let state = AppState::new(wal);
    (
        router(state, prometheus, ApiToken::new(Some(TOKEN.into()))),
        dir,
    )
}

fn get(uri: &str, auth: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method("GET").uri(uri);
    if let Some(t) = auth {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn health_is_public() {
    let (app, _dir) = build_app();
    let res = app.oneshot(get("/health", None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn protected_route_rejects_without_token() {
    let (app, _dir) = build_app();
    let res = app.oneshot(get("/collections", None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn protected_route_rejects_wrong_token() {
    let (app, _dir) = build_app();
    let res = app
        .oneshot(get("/collections", Some("wrong")))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn protected_route_accepts_correct_token() {
    let (app, _dir) = build_app();
    let res = app.oneshot(get("/collections", Some(TOKEN))).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_is_gated() {
    let (app, _dir) = build_app();
    let res = app.oneshot(get("/metrics", None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
