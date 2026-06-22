use std::path::PathBuf;
use std::time::{Duration, Instant};

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

    let api_token = likhadb_server::ApiToken::from_env();
    if !api_token.is_enabled() {
        tracing::warn!("LIKHADB_API_TOKEN unset — REST and gRPC are UNAUTHENTICATED (dev mode)");
    }

    // Attach the Tier Q pipeline when both Iceberg and LIKHADB_ENRICH_NAMESPACE
    // are configured. Failures inside the helper are logged; state.pipeline
    // stays None and the server keeps serving non-enriched queries.
    #[cfg(feature = "enriched-search")]
    let state = if let Some((ref config, _)) = iceberg_flusher_args {
        match likhadb_server::try_build_pipeline_from_env(config).await {
            Some(pipeline) => state.with_pipeline(pipeline),
            None => state,
        }
    } else {
        state
    };

    // Spawn Iceberg background flusher when catalog is configured.
    #[cfg(feature = "iceberg-recovery")]
    if let Some((config, namespace)) = iceberg_flusher_args {
        use likhadb_server::IcebergFlusher;
        let _flusher = IcebergFlusher::new(state.wal_arc(), config, namespace).spawn();
        tracing::info!("iceberg background flusher started");
    }

    let checkpoint_task =
        likhadb_server::spawn_checkpoint_task(state.clone(), Duration::from_secs(300));

    // Shutdown channel: receivers become ready once true is sent.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

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

    // Keep a clone of state alive for the final checkpoint after servers drain.
    let checkpoint_state = state.clone();

    // Cap inbound gRPC frames so a single oversized message can't exhaust memory.
    const MAX_GRPC_MSG: usize = 4 * 1024 * 1024;

    let grpc_state = state.clone();
    let grpc_token = api_token.clone();
    let grpc_shutdown_rx = shutdown_rx.clone();
    let mut grpc_handle = tokio::spawn(async move {
        let service = likhadb_server::LikhaDbServer::new(likhadb_server::LikhaDbGrpc::new(grpc_state))
            .max_decoding_message_size(MAX_GRPC_MSG);
        tonic::transport::Server::builder()
            .layer(likhadb_server::GrpcMetricsLayer)
            .add_service(tonic::service::interceptor::InterceptedService::new(
                service,
                likhadb_server::grpc_interceptor(grpc_token),
            ))
            .serve_with_shutdown(grpc_addr, async move {
                let mut rx = grpc_shutdown_rx;
                rx.changed().await.ok();
            })
            .await
    });

    let rest_shutdown_rx = shutdown_rx;
    let mut rest_handle = tokio::spawn(async move {
        axum::serve(
            listener,
            likhadb_server::router(state, prometheus, api_token),
        )
            .with_graceful_shutdown(async move {
                let mut rx = rest_shutdown_rx;
                rx.changed().await.ok();
            })
            .await
    });

    // Block until a server crashes unexpectedly or a shutdown signal arrives.
    let graceful = tokio::select! {
        res = &mut grpc_handle => {
            tracing::error!(?res, "gRPC server exited unexpectedly");
            false
        }
        res = &mut rest_handle => {
            tracing::error!(?res, "REST server exited unexpectedly");
            false
        }
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received — draining in-flight requests");
            true
        }
    };

    if !graceful {
        std::process::exit(1);
    }

    // Cancel the periodic checkpoint to avoid racing with the final one.
    checkpoint_task.abort();

    // Tell both servers to stop accepting connections.
    let _ = shutdown_tx.send(true);

    // Wait for in-flight requests to complete (30 s hard limit per server).
    let drain = Duration::from_secs(30);
    if tokio::time::timeout(drain, grpc_handle).await.is_err() {
        tracing::warn!("gRPC drain timed out after {drain:?}");
    }
    if tokio::time::timeout(drain, rest_handle).await.is_err() {
        tracing::warn!("REST drain timed out after {drain:?}");
    }

    // Write snapshot and truncate WAL so the next startup has nothing to replay.
    let t = Instant::now();
    match checkpoint_state.write().await.checkpoint() {
        Ok(()) => tracing::info!(elapsed = ?t.elapsed(), "final checkpoint complete"),
        Err(e) => tracing::error!(error = %e, "final checkpoint failed"),
    }

    tracing::info!("shutdown complete");
}

/// Resolves on SIGTERM (Unix) or Ctrl+C (all platforms).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
