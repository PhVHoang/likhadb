mod error;
pub mod parquet_io;

#[cfg(feature = "persist")]
mod wal_io;

#[cfg(feature = "minio")]
pub mod minio;

#[cfg(feature = "iceberg")]
pub mod iceberg_io;

pub use error::LakehouseError;
pub use parquet_io::LakehouseExt;

#[cfg(feature = "minio")]
pub use minio::{build_minio_store, MinioConfig, ObjectStoreLakehouseExt};

#[cfg(feature = "iceberg")]
pub use iceberg_io::{build_rest_catalog, IcebergConfig, IcebergLakehouseExt};
