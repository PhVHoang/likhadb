use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let prometheus = likhadb_server::install_prometheus();

    let data_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"));

    let wal = likhadb_persist::WalManager::open(&data_dir).unwrap_or_else(|e| {
        tracing::error!(dir = %data_dir.display(), error = %e, "failed to open data directory");
        std::process::exit(1);
    });

    likhadb_server::seed_collection_gauges(&wal);

    let state = likhadb_server::AppState::new(wal);

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
