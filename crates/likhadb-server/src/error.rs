use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use likhadb_core::LikhaDbError;
use likhadb_lakehouse::LakehouseError;
use likhadb_persist::PersistError;
use serde_json::json;

pub enum ApiError {
    NotFound(String),
    Conflict(String),
    BadRequest(String),
    Unauthorized,
    Forbidden(String),
    Internal(String),
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::BadRequest(msg.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".into()),
            ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, Json(json!({"error": msg}))).into_response()
    }
}

impl From<LikhaDbError> for ApiError {
    fn from(e: LikhaDbError) -> Self {
        match e {
            LikhaDbError::CollectionNotFound(_) | LikhaDbError::VectorNotFound(_) => {
                ApiError::NotFound(e.to_string())
            }
            LikhaDbError::CollectionAlreadyExists(_) => ApiError::Conflict(e.to_string()),
            LikhaDbError::DimMismatch { .. } | LikhaDbError::InvalidArgument(_) => {
                ApiError::BadRequest(e.to_string())
            }
            LikhaDbError::Fts(_) => ApiError::Internal(e.to_string()),
        }
    }
}

impl From<LakehouseError> for ApiError {
    fn from(e: LakehouseError) -> Self {
        match e {
            LakehouseError::CollectionNotFound(_) => ApiError::NotFound(e.to_string()),
            LakehouseError::DimMismatch { .. }
            | LakehouseError::TypeMismatch { .. }
            | LakehouseError::Schema(_)
            | LakehouseError::ColumnNotFound(_) => ApiError::BadRequest(e.to_string()),
            _ => ApiError::Internal(e.to_string()),
        }
    }
}

// PersistError::Apply wraps a LikhaDbError — delegate so callers get the right
// HTTP status instead of a blanket 500 for logical errors like CollectionNotFound.
impl From<PersistError> for ApiError {
    fn from(e: PersistError) -> Self {
        match e {
            PersistError::Apply(inner) => inner.into(),
            other => ApiError::Internal(other.to_string()),
        }
    }
}
