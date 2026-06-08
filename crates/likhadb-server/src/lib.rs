mod error;
mod grpc;
mod metrics;
mod routes;
mod state;
mod types;

pub use grpc::{GrpcMetricsLayer, LikhaDbGrpc, LikhaDbServer};
#[cfg(feature = "iceberg-recovery")]
pub use likhadb_lakehouse::{
    iceberg_io::IcebergConfig, iceberg_recovery::open_with_iceberg,
    iceberg_recovery::RecoveryError, IcebergFlusher, NamespaceIdent,
};
pub use metrics::{install as install_prometheus, seed_collection_gauges};
pub use metrics_exporter_prometheus::PrometheusHandle;
pub use routes::router;
pub use state::{spawn_checkpoint_task, AppState};
