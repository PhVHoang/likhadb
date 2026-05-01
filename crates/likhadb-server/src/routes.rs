use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;

use crate::{
    error::ApiError,
    state::AppState,
    types::{
        metric_str, parse_metric, CollectionInfo, CreateCollectionRequest, IndexConfig,
        InsertRequest, QueryRequest, QueryResponse, VectorResponse,
    },
};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/collections",
            get(list_collections).post(create_collection),
        )
        .route(
            "/collections/:name",
            get(get_collection).delete(drop_collection),
        )
        .route("/collections/:name/vectors", post(insert_vector))
        .route(
            "/collections/:name/vectors/:id",
            get(get_vector).delete(delete_vector),
        )
        .route("/collections/:name/query", post(query_vectors))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

async fn list_collections(State(state): State<AppState>) -> impl IntoResponse {
    let names: Vec<String> = {
        let guard = state.read().await;
        guard.list().into_iter().map(str::to_string).collect()
    };
    Json(json!({"collections": names}))
}

async fn create_collection(
    State(state): State<AppState>,
    Json(req): Json<CreateCollectionRequest>,
) -> Result<StatusCode, ApiError> {
    let metric = parse_metric(&req.metric)?;
    let mut guard = state.write().await;
    match req.index {
        IndexConfig::Flat => guard.create_collection(req.name, req.dim, metric)?,
        IndexConfig::Ivf { nlist, nprobe } => {
            guard.create_ivf_collection(req.name, req.dim, metric, nlist, nprobe)?
        }
        IndexConfig::IvfSq8 { nlist, nprobe } => {
            guard.create_ivf_sq8_collection(req.name, req.dim, metric, nlist, nprobe)?
        }
        IndexConfig::Hnsw {
            m,
            ef_construction,
            ef_search,
        } => guard.create_hnsw_collection(
            req.name,
            req.dim,
            metric,
            m,
            ef_construction,
            ef_search,
        )?,
    }
    Ok(StatusCode::CREATED)
}

async fn get_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let info = {
        let guard = state.read().await;
        let col = guard.get(&name)?;
        CollectionInfo {
            name: col.name.clone(),
            dim: col.dim,
            metric: metric_str(col.metric).to_string(),
            count: col.len(),
            index_type: col.index_type().to_string(),
        }
    };
    Ok(Json(info))
}

async fn drop_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.write().await.drop_collection(&name)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn insert_vector(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<InsertRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .write()
        .await
        .insert(&name, req.id, req.vector, req.payload)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_vector(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, u64)>,
) -> Result<impl IntoResponse, ApiError> {
    let resp = {
        let guard = state.read().await;
        let col = guard.get(&name)?;
        match col.get(id)? {
            None => {
                return Err(ApiError::NotFound(format!(
                    "vector {id} not found in '{name}'"
                )))
            }
            Some((vector, payload)) => VectorResponse {
                id,
                vector,
                payload,
            },
        }
    };
    Ok(Json(resp))
}

async fn delete_vector(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, u64)>,
) -> Result<StatusCode, ApiError> {
    state.write().await.delete(&name, id)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn query_vectors(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let results = {
        let guard = state.read().await;
        let col = guard.get(&name)?;
        col.search(&req.vector, req.k, req.filter.as_ref(), req.include_payload)?
    };
    Ok(Json(QueryResponse { results }))
}
