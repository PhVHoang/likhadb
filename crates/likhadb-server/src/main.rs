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

    let addr = "0.0.0.0:8080";
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: failed to bind to {addr}: {e}");
            std::process::exit(1);
        });

    eprintln!("likhadb listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, likhadb_server::router(state))
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: server exited unexpectedly: {e}");
            std::process::exit(1);
        });
}
