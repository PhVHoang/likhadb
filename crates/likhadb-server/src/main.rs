use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let data_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"));

    let wal = likhadb_persist::WalManager::open(&data_dir).unwrap_or_else(|e| {
        eprintln!("error: failed to open '{}': {e}", data_dir.display());
        std::process::exit(1);
    });

    let state = likhadb_server::AppState::new(wal);

    let _checkpoint =
        likhadb_server::spawn_checkpoint_task(state.clone(), Duration::from_secs(300));

    // gRPC server on port 50051.
    let grpc_addr = std::env::var("GRPC_ADDR")
        .unwrap_or_else(|_| "[::]:50051".into())
        .parse()
        .unwrap_or_else(|e| {
            eprintln!("error: invalid GRPC_ADDR: {e}");
            std::process::exit(1);
        });

    // REST server on port 8080.
    let rest_addr = std::env::var("HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&rest_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: failed to bind to {rest_addr}: {e}");
            std::process::exit(1);
        });

    eprintln!("likhadb REST listening on {}", listener.local_addr().unwrap());
    eprintln!("likhadb gRPC listening on {grpc_addr}");

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
        axum::serve(listener, likhadb_server::router(state)).await
    });

    tokio::select! {
        res = grpc_handle => eprintln!("error: gRPC server exited: {res:?}"),
        res = rest_handle => eprintln!("error: REST server exited: {res:?}"),
    }
    std::process::exit(1);
}
