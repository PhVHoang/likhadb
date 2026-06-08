use std::path::PathBuf;
use std::time::Duration;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let prometheus = likhadb_server::install_prometheus();

    let data_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"));

    // ── WAL / Iceberg startup ──────────────────────────────────────────────
    #[cfg(feature = "iceberg-recovery")]
    let (wal, iceberg_flusher_args) = {
        if let Ok(catalog_uri) = std::env::var("ICEBERG_CATALOG_URI") {
            use likhadb_server::{IcebergConfig, NamespaceIdent};
            use std::collections::HashMap;

            let config = IcebergConfig {
                catalog_uri,
                s3_endpoint: std::env::var("S3_ENDPOINT").unwrap_or_default(),
                access_key: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
                secret_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                region: std::env::var("AWS_DEFAULT_REGION")
                    .unwrap_or_else(|_| "us-east-1".to_string()),
                warehouse: std::env::var("ICEBERG_WAREHOUSE").unwrap_or_default(),
                extra_properties: HashMap::new(),
            };
            let namespace = NamespaceIdent::new(
                std::env::var("ICEBERG_NAMESPACE").unwrap_or_else(|_| "likhadb".to_string()),
            );

            let wal = likhadb_server::open_with_iceberg(&data_dir, &config, namespace.clone())
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(error = %e, "iceberg recovery failed");
                    std::process::exit(1);
                });

            (wal, Some((config, namespace)))
        } else {
            let wal = likhadb_persist::WalManager::open(&data_dir).unwrap_or_else(|e| {
                tracing::error!(dir = %data_dir.display(), error = %e, "failed to open data directory");
                std::process::exit(1);
            });
            (wal, None)
        }
    };

    #[cfg(not(feature = "iceberg-recovery"))]
    let wal = likhadb_persist::WalManager::open(&data_dir).unwrap_or_else(|e| {
        tracing::error!(dir = %data_dir.display(), error = %e, "failed to open data directory");
        std::process::exit(1);
    });

    likhadb_server::seed_collection_gauges(&wal);

    let state = likhadb_server::AppState::new(wal);

    // Spawn Iceberg background flusher when catalog is configured.
    #[cfg(feature = "iceberg-recovery")]
    if let Some((config, namespace)) = iceberg_flusher_args {
        use likhadb_server::IcebergFlusher;
        let _flusher = IcebergFlusher::new(state.wal_arc(), config, namespace).spawn();
        tracing::info!("iceberg background flusher started");
    }

    let _checkpoint =
        likhadb_server::spawn_checkpoint_task(state.clone(), Duration::from_secs(300));

    // gRPC server on port 50051.
    let grpc_addr = std::env::var("GRPC_ADDR")
        .unwrap_or_else(|_| "[::]:50051".into())
        .parse()
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "invalid GRPC_ADDR");
            std::process::exit(1);
        });

    // REST server on port 8080.
    let rest_addr = std::env::var("HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&rest_addr)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(addr = %rest_addr, error = %e, "failed to bind");
            std::process::exit(1);
        });

    tracing::info!(
        addr = %listener.local_addr().unwrap(),
        "likhadb REST listening"
    );
    tracing::info!(addr = %grpc_addr, "likhadb gRPC listening");

    let grpc_state = state.clone();
    let grpc_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(likhadb_server::GrpcMetricsLayer)
            .add_service(likhadb_server::LikhaDbServer::new(
                likhadb_server::LikhaDbGrpc::new(grpc_state),
            ))
            .serve(grpc_addr)
            .await
    });

    let rest_handle = tokio::spawn(async move {
        axum::serve(listener, likhadb_server::router(state, prometheus)).await
    });

    tokio::select! {
        res = grpc_handle => tracing::error!(?res, "gRPC server exited"),
        res = rest_handle => tracing::error!(?res, "REST server exited"),
    }
    std::process::exit(1);
}
