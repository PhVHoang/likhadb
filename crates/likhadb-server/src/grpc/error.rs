use likhadb_core::LikhaDbError;
use likhadb_persist::PersistError;

use crate::error::ApiError;

pub fn core_err(e: LikhaDbError) -> tonic::Status {
    match e {
        LikhaDbError::CollectionNotFound(_) | LikhaDbError::VectorNotFound(_) => {
            tonic::Status::not_found(e.to_string())
        }
        LikhaDbError::CollectionAlreadyExists(_) => tonic::Status::already_exists(e.to_string()),
        LikhaDbError::DimMismatch { .. } | LikhaDbError::InvalidArgument(_) => {
            tonic::Status::invalid_argument(e.to_string())
        }
        LikhaDbError::Fts(_) => tonic::Status::internal(e.to_string()),
    }
}

pub fn persist_err(e: PersistError) -> tonic::Status {
    match e {
        PersistError::Apply(inner) => core_err(inner),
        other => tonic::Status::internal(other.to_string()),
    }
}

pub fn api_err(e: ApiError) -> tonic::Status {
    match e {
        ApiError::BadRequest(m) => tonic::Status::invalid_argument(m),
        ApiError::NotFound(m) => tonic::Status::not_found(m),
        ApiError::Conflict(m) => tonic::Status::already_exists(m),
        ApiError::Internal(m) => tonic::Status::internal(m),
    }
}
