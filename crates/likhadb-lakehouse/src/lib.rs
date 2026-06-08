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

#[cfg(feature = "iceberg-recovery")]
pub mod iceberg_flusher;
#[cfg(feature = "iceberg-recovery")]
pub mod iceberg_recovery;
#[cfg(feature = "iceberg-recovery")]
pub mod index_snapshot_io;
#[cfg(feature = "iceberg-recovery")]
pub mod staging_io;

#[cfg(feature = "iceberg-recovery")]
pub use iceberg::NamespaceIdent;
#[cfg(feature = "iceberg-recovery")]
pub use iceberg_flusher::IcebergFlusher;
#[cfg(feature = "iceberg-recovery")]
pub use iceberg_recovery::{open_with_iceberg, RecoveryError};
#[cfg(feature = "iceberg-recovery")]
pub use index_snapshot_io::{load_collection_snapshots, write_collection_snapshot};
#[cfg(feature = "iceberg-recovery")]
pub use staging_io::{
    append_to_staging, get_or_create_staging_table, read_watermark, scan_pending, PendingVector,
    StagingBatch, StagingRow, STAGING_WATERMARK_PROP,
};
