mod auth;
mod error;
mod grpc;
mod metrics;
#[cfg(feature = "enriched-search")]
mod pipeline_setup;
mod routes;
mod state;
mod types;

pub use auth::{grpc_interceptor, require_bearer, ApiToken};
pub use grpc::{GrpcMetricsLayer, LikhaDbGrpc, LikhaDbServer};
#[cfg(feature = "iceberg-recovery")]
pub use likhadb_lakehouse::{
    iceberg_io::IcebergConfig, iceberg_recovery::open_with_iceberg,
    iceberg_recovery::RecoveryError, IcebergFlusher, NamespaceIdent,
};
pub use metrics::{install as install_prometheus, seed_collection_gauges};
pub use metrics_exporter_prometheus::PrometheusHandle;
#[cfg(feature = "enriched-search")]
pub use pipeline_setup::try_build_pipeline_from_env;
pub use routes::router;
pub use state::{spawn_checkpoint_task, AppState};
