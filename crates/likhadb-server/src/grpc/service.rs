// tonic::Status is inherently large (176 bytes); boxing it in every helper or
// closure in a tonic service file would add more noise than this lint saves.
#![allow(clippy::result_large_err)]

#[allow(clippy::all)]
pub mod proto {
    tonic::include_proto!("likhadb");
}

use proto::{
    likha_db_server::LikhaDb, CollectionInfo, CreateCollectionResponse, DeleteVectorResponse,
    DropCollectionResponse, HealthRequest, HealthResponse, HybridQueryResponse,
    InsertVectorResponse, ListCollectionsRequest, ListCollectionsResponse, QueryResponse,
    ScoredResult, VectorRecord,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    grpc::error::{api_err, core_err, persist_err},
    state::AppState,
    types::{metric_str, parse_metric},
};

pub struct LikhaDbGrpc {
    state: AppState,
}

impl LikhaDbGrpc {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl LikhaDb for LikhaDbGrpc {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
        }))
    }

    async fn list_collections(
        &self,
        _request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let collections: Vec<String> = {
            let guard = self.state.read().await;
            guard.list().into_iter().map(str::to_string).collect()
        };
        Ok(Response::new(ListCollectionsResponse { collections }))
    }

    async fn create_collection(
        &self,
        request: Request<proto::CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let req = request.into_inner();
        let metric = parse_metric(&req.metric).map_err(api_err)?;
        let dim = req.dim as usize;

        use proto::create_collection_request::IndexConfig;
        let name = req.name;
        let enable_fts = req.enable_fts;
        let mut guard = self.state.write().await;
        match req.index_config {
            None | Some(IndexConfig::Flat(_)) => guard
                .create_collection(name.clone(), dim, metric)
                .map_err(persist_err)?,
            Some(IndexConfig::Ivf(c)) => guard
                .create_ivf_collection(
                    name.clone(),
                    dim,
                    metric,
                    c.nlist as usize,
                    c.nprobe as usize,
                )
                .map_err(persist_err)?,
            Some(IndexConfig::IvfSq8(c)) => guard
                .create_ivf_sq8_collection(
                    name.clone(),
                    dim,
                    metric,
                    c.nlist as usize,
                    c.nprobe as usize,
                )
                .map_err(persist_err)?,
            Some(IndexConfig::Hnsw(c)) => guard
                .create_hnsw_collection(
                    name.clone(),
                    dim,
                    metric,
                    c.m as usize,
                    c.ef_construction as usize,
                    c.ef_search as usize,
                )
                .map_err(persist_err)?,
        }
        if enable_fts {
            guard.enable_fts(&name).map_err(persist_err)?;
        }
        Ok(Response::new(CreateCollectionResponse {}))
    }

    async fn get_collection(
        &self,
        request: Request<proto::GetCollectionRequest>,
    ) -> Result<Response<CollectionInfo>, Status> {
        let name = request.into_inner().name;
        let info = {
            let guard = self.state.read().await;
            let col = guard.get(&name).map_err(core_err)?;
            CollectionInfo {
                name: col.name.clone(),
                dim: col.dim as u32,
                metric: metric_str(col.metric).to_string(),
                count: col.len() as u64,
                index_type: col.index_type().to_string(),
            }
        };
        Ok(Response::new(info))
    }

    async fn drop_collection(
        &self,
        request: Request<proto::DropCollectionRequest>,
    ) -> Result<Response<DropCollectionResponse>, Status> {
        let name = request.into_inner().name;
        self.state
            .write()
            .await
            .drop_collection(&name)
            .map_err(persist_err)?;
        Ok(Response::new(DropCollectionResponse {}))
    }

    async fn insert_vector(
        &self,
        request: Request<proto::InsertVectorRequest>,
    ) -> Result<Response<InsertVectorResponse>, Status> {
        let req = request.into_inner();
        let payload = if req.payload_json.is_empty() {
            None
        } else {
            Some(
                serde_json::from_slice::<serde_json::Value>(&req.payload_json)
                    .map_err(|e| Status::invalid_argument(format!("payload_json: {e}")))?,
            )
        };
        self.state
            .write()
            .await
            .insert(&req.collection, req.id, req.vector, payload)
            .map_err(persist_err)?;
        Ok(Response::new(InsertVectorResponse {}))
    }

    async fn get_vector(
        &self,
        request: Request<proto::GetVectorRequest>,
    ) -> Result<Response<VectorRecord>, Status> {
        let req = request.into_inner();
        let record = {
            let guard = self.state.read().await;
            let col = guard.get(&req.collection).map_err(core_err)?;
            match col.get(req.id).map_err(core_err)? {
                None => {
                    return Err(Status::not_found(format!(
                        "vector {} not found in '{}'",
                        req.id, req.collection
                    )))
                }
                Some((vector, payload)) => {
                    let payload_json = payload_to_bytes(payload)?;
                    VectorRecord {
                        id: req.id,
                        vector,
                        payload_json,
                    }
                }
            }
        };
        Ok(Response::new(record))
    }

    async fn delete_vector(
        &self,
        request: Request<proto::DeleteVectorRequest>,
    ) -> Result<Response<DeleteVectorResponse>, Status> {
        let req = request.into_inner();
        self.state
            .write()
            .await
            .delete(&req.collection, req.id)
            .map_err(persist_err)?;
        Ok(Response::new(DeleteVectorResponse {}))
    }

    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();
        let filter = decode_filter(&req.filter_json)?;
        let results = {
            let guard = self.state.read().await;
            let col = guard.get(&req.collection).map_err(core_err)?;
            col.search(
                &req.vector,
                req.k as usize,
                filter.as_ref(),
                req.include_payload,
            )
            .map_err(core_err)?
        };
        let results = encode_scored_results(results)?;
        Ok(Response::new(QueryResponse { results }))
    }

    async fn hybrid_query(
        &self,
        request: Request<proto::HybridQueryRequest>,
    ) -> Result<Response<HybridQueryResponse>, Status> {
        let req = request.into_inner();
        let filter = decode_filter(&req.filter_json)?;
        let rrf_k = if req.rrf_k == 0 { 60 } else { req.rrf_k };
        let results = {
            let guard = self.state.read().await;
            let col = guard.get(&req.collection).map_err(core_err)?;
            col.hybrid_search(
                &req.vector,
                &req.text,
                req.k as usize,
                rrf_k,
                filter.as_ref(),
                req.include_payload,
            )
            .map_err(core_err)?
        };
        let results = encode_scored_results(results)?;
        Ok(Response::new(HybridQueryResponse { results }))
    }

    type QueryStreamStream = ReceiverStream<Result<ScoredResult, Status>>;

    async fn query_stream(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        let req = request.into_inner();
        let filter = decode_filter(&req.filter_json)?;
        let results = {
            let guard = self.state.read().await;
            let col = guard.get(&req.collection).map_err(core_err)?;
            col.search(
                &req.vector,
                req.k as usize,
                filter.as_ref(),
                req.include_payload,
            )
            .map_err(core_err)?
        };

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            for r in results {
                let item = encode_scored_result(r)
                    .unwrap_or_else(|e| Err(Status::internal(e.to_string())));
                if tx.send(item).await.is_err() {
                    break; // client disconnected
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

// tonic::Status is 176 bytes; boxing it throughout a tonic service file adds
// more noise than the lint saves.
#[allow(clippy::result_large_err)]
fn decode_filter(raw: &[u8]) -> Result<Option<serde_json::Value>, Status> {
    if raw.is_empty() {
        Ok(None)
    } else {
        serde_json::from_slice::<serde_json::Value>(raw)
            .map(Some)
            .map_err(|e| Status::invalid_argument(format!("filter_json: {e}")))
    }
}

#[allow(clippy::result_large_err)]
fn payload_to_bytes(payload: Option<serde_json::Value>) -> Result<Vec<u8>, Status> {
    match payload {
        Some(p) => serde_json::to_vec(&p).map_err(|e| Status::internal(e.to_string())),
        None => Ok(vec![]),
    }
}

#[allow(clippy::result_large_err)]
fn encode_scored_result(
    r: likhadb_core::ScoredResult,
) -> Result<Result<ScoredResult, Status>, serde_json::Error> {
    let payload_json = match r.payload {
        Some(p) => serde_json::to_vec(&p)?,
        None => vec![],
    };
    Ok(Ok(ScoredResult {
        id: r.id,
        score: r.score,
        payload_json,
    }))
}

#[allow(clippy::result_large_err)]
fn encode_scored_results(
    results: Vec<likhadb_core::ScoredResult>,
) -> Result<Vec<ScoredResult>, Status> {
    results
        .into_iter()
        .map(|r| {
            encode_scored_result(r)
                .map_err(|e| Status::internal(e.to_string()))
                .and_then(|inner| inner)
        })
        .collect()
}
