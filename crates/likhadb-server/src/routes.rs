use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::json;

use likhadb_lakehouse::LakehouseExt;

#[cfg(feature = "tier-q")]
use crate::types::RankedQueryResponse;
use crate::{
    error::ApiError,
    state::AppState,
    types::{
        metric_str, parse_metric, CollectionInfo, CreateCollectionRequest, ExportParquetRequest,
        HybridQueryRequest, HybridQueryResponse, ImportParquetRequest, ImportParquetResponse,
        IndexConfig, InsertRequest, QueryRequest, QueryResponse, VectorResponse,
    },
};
#[cfg(feature = "tier-q")]
use likhadb_query::pipeline::{Candidate, PipelineRequest};

pub fn router(state: AppState, prometheus: PrometheusHandle) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_endpoint))
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
        .route(
            "/collections/:name/hybrid-query",
            post(hybrid_query_vectors),
        )
        .route("/collections/:name/import-parquet", post(import_parquet))
        .route("/collections/:name/export-parquet", post(export_parquet))
        .layer(Extension(prometheus))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

async fn metrics_endpoint(Extension(handle): Extension<PrometheusHandle>) -> impl IntoResponse {
    handle.render()
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
    let name = req.name;
    let enable_fts = req.enable_fts;
    let mut guard = state.write().await;
    match req.index {
        IndexConfig::Flat => guard.create_collection(name.clone(), req.dim, metric)?,
        IndexConfig::Ivf { nlist, nprobe } => {
            guard.create_ivf_collection(name.clone(), req.dim, metric, nlist, nprobe)?
        }
        IndexConfig::IvfSq8 { nlist, nprobe } => {
            guard.create_ivf_sq8_collection(name.clone(), req.dim, metric, nlist, nprobe)?
        }
        IndexConfig::Hnsw {
            m,
            ef_construction,
            ef_search,
        } => guard.create_hnsw_collection(
            name.clone(),
            req.dim,
            metric,
            m,
            ef_construction,
            ef_search,
        )?,
    }
    if enable_fts {
        guard.enable_fts(&name)?;
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

#[tracing::instrument(skip(state, req), fields(collection = %name))]
async fn insert_vector(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<InsertRequest>,
) -> Result<StatusCode, ApiError> {
    let start = Instant::now();
    let (count, index_type) = {
        let mut guard = state.write().await;
        guard.insert(&name, req.id, req.vector, req.payload)?;
        let col = guard.get(&name).map_err(ApiError::from)?;
        (col.len(), col.index_type().to_string())
    };
    metrics::histogram!("likhadb_insert_duration_seconds", "collection" => name.clone())
        .record(start.elapsed().as_secs_f64());
    metrics::gauge!(
        "likhadb_collection_vectors_total",
        "collection" => name,
        "index_type" => index_type
    )
    .set(count as f64);
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
    let (count, index_type) = {
        let mut guard = state.write().await;
        guard.delete(&name, id)?;
        let col = guard.get(&name).map_err(ApiError::from)?;
        (col.len(), col.index_type().to_string())
    };
    metrics::gauge!(
        "likhadb_collection_vectors_total",
        "collection" => name,
        "index_type" => index_type
    )
    .set(count as f64);
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, req), fields(collection = %name, k = req.k))]
async fn query_vectors(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let start = Instant::now();
    let (results, index_type) = {
        let guard = state.read().await;
        let col = guard.get(&name)?;
        let index_type = col.index_type().to_string();
        let results = col.search(&req.vector, req.k, req.filter.as_ref(), req.include_payload)?;
        (results, index_type)
    };
    metrics::histogram!(
        "likhadb_search_duration_seconds",
        "collection" => name,
        "index_type" => index_type
    )
    .record(start.elapsed().as_secs_f64());

    #[cfg(feature = "tier-q")]
    if let Some(pipeline) = &state.pipeline {
        let candidates: Vec<Candidate> = results
            .iter()
            .enumerate()
            .map(|(i, r)| Candidate {
                id: r.id,
                ann_distance: r.score,
                ann_rank: i as u64 + 1,
            })
            .collect();
        let ranked = pipeline
            .execute(PipelineRequest {
                candidates,
                query_text: req.query_text.unwrap_or_default(),
                allowed_teams: req.allowed_teams,
                top_k: req.k,
            })
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        return Ok(Json(RankedQueryResponse { results: ranked }).into_response());
    }

    Ok(Json(QueryResponse { results }).into_response())
}

#[tracing::instrument(skip(state, req), fields(collection = %name, k = req.k))]
async fn hybrid_query_vectors(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<HybridQueryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let start = Instant::now();
    let (results, index_type) = {
        let guard = state.read().await;
        let col = guard.get(&name)?;
        let index_type = col.index_type().to_string();
        let results = col.hybrid_search(
            &req.vector,
            &req.text,
            req.k,
            req.rrf_k,
            req.filter.as_ref(),
            req.include_payload,
        )?;
        (results, index_type)
    };
    metrics::histogram!(
        "likhadb_search_duration_seconds",
        "collection" => name,
        "index_type" => index_type
    )
    .record(start.elapsed().as_secs_f64());

    #[cfg(feature = "tier-q")]
    if let Some(pipeline) = &state.pipeline {
        let candidates: Vec<Candidate> = results
            .iter()
            .enumerate()
            .map(|(i, r)| Candidate {
                id: r.id,
                ann_distance: r.score,
                ann_rank: i as u64 + 1,
            })
            .collect();
        let ranked = pipeline
            .execute(PipelineRequest {
                candidates,
                query_text: req.text.clone(),
                allowed_teams: req.allowed_teams,
                top_k: req.k,
            })
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        return Ok(Json(RankedQueryResponse { results: ranked }).into_response());
    }

    Ok(Json(HybridQueryResponse { results }).into_response())
}

async fn import_parquet(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<ImportParquetRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let path = std::path::Path::new(&req.path);
    let payload_cols: Vec<&str> = req.payload_cols.iter().map(String::as_str).collect();
    let imported = {
        let mut guard = state.write().await;
        guard.import_parquet(&name, path, &req.id_col, &req.vector_col, &payload_cols)?
    };
    Ok((StatusCode::OK, Json(ImportParquetResponse { imported })))
}

async fn export_parquet(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<ExportParquetRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let path = std::path::Path::new(&req.path);
    {
        let guard = state.read().await;
        guard.export_parquet(&name, path)?;
    }
    Ok(StatusCode::NO_CONTENT)
}
