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

async fn body_json(res: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn hybrid_query_returns_results() {
    let (app, _dir) = build_app();

    // Create collection with FTS enabled
    let res = app
        .clone()
        .oneshot(json_req(
            "POST",
            "/collections",
            r#"{"name":"hybrid_test","dim":3,"metric":"l2","enable_fts":true}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);

    // Insert documents
    for (id, body) in [
        (1u64, "the quick brown fox"),
        (2u64, "lazy dog sleeps all day"),
        (3u64, "rust programming language"),
    ] {
        let payload = serde_json::json!({"body": body});
        let vec_vals: Vec<f32> = vec![id as f32, 0.0, 0.0];
        let req_body = serde_json::json!({
            "id": id,
            "vector": vec_vals,
            "payload": payload
        })
        .to_string();
        let res = app
            .clone()
            .oneshot(json_req(
                "POST",
                "/collections/hybrid_test/vectors",
                req_body,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    // Hybrid query
    let res = app
        .clone()
        .oneshot(json_req(
            "POST",
            "/collections/hybrid_test/hybrid-query",
            r#"{"vector":[1.0,0.0,0.0],"text":"fox","k":3,"include_payload":true}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = body_json(res).await;
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty(), "hybrid query should return results");
    // top result should have a payload
    assert!(results[0]["payload"].is_object());
}

#[tokio::test]
async fn hybrid_query_without_fts_still_returns_vector_results() {
    // Collection without FTS: hybrid_search returns only vector results (no fts contribution)
    let (app, _dir) = build_app();

    let res = app
        .clone()
        .oneshot(json_req(
            "POST",
            "/collections",
            r#"{"name":"no_fts","dim":3,"metric":"l2"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);

    for id in 1u64..=5 {
        let req_body = serde_json::json!({
            "id": id,
            "vector": [id as f32, 0.0, 0.0],
            "payload": {"n": id}
        })
        .to_string();
        app.clone()
            .oneshot(json_req("POST", "/collections/no_fts/vectors", req_body))
            .await
            .unwrap();
    }

    let res = app
        .clone()
        .oneshot(json_req(
            "POST",
            "/collections/no_fts/hybrid-query",
            r#"{"vector":[1.0,0.0,0.0],"text":"anything","k":3}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_json(res).await;
    let results = body["results"].as_array().unwrap();
    // Should still get vector results (FTS contributes nothing but vector ranks fill in)
    assert!(!results.is_empty());
}

#[tokio::test]
async fn create_collection_without_enable_fts_defaults_false() {
    let (app, _dir) = build_app();

    // Omitting enable_fts should default to false (no FTS)
    let res = app
        .clone()
        .oneshot(json_req(
            "POST",
            "/collections",
            r#"{"name":"plain","dim":2,"metric":"l2"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
}
