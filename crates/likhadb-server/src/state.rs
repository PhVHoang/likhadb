use std::sync::Arc;
use std::time::Duration;

use likhadb_persist::WalManager;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;

/// Shared, concurrency-safe handle to the WAL-backed store.
///
/// Cloning is cheap (increments an `Arc` refcount). Axum requires `Clone`
/// to extract state into handlers.
///
/// Read operations (`search`, `get`, `list`) acquire a shared read lock so
/// multiple handlers can run concurrently. Write operations (`insert`,
/// `delete`, DDL) acquire an exclusive write lock.
///
/// # Lock discipline
///
/// Guards must never be held across an `.await` point — Tokio's `RwLock`
/// is not safe to hold across an await. Always drop the guard (or end its
/// scope) before the next yield point.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<RwLock<WalManager>>,
}

impl AppState {
    pub fn new(wal: WalManager) -> Self {
        Self {
            inner: Arc::new(RwLock::new(wal)),
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, WalManager> {
        self.inner.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, WalManager> {
        self.inner.write().await
    }
}

/// Spawn a background task that calls [`WalManager::checkpoint`] on `interval`.
///
/// The first tick is skipped so the checkpoint does not fire immediately on
/// startup. On graceful shutdown, abort the returned handle and call
/// `checkpoint()` once directly before exiting.
pub fn spawn_checkpoint_task(state: AppState, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let mut guard = state.write().await;
            if let Err(e) = guard.checkpoint() {
                tracing::error!(error = %e, "checkpoint failed");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use likhadb_persist::WalManager;
    use tempfile::TempDir;

    fn open_wal(dir: &TempDir) -> WalManager {
        WalManager::open(dir.path()).expect("open wal")
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let dir = TempDir::new().unwrap();
        let state = AppState::new(open_wal(&dir));
        let clone = state.clone();

        {
            let mut guard = state.write().await;
            guard
                .create_collection("col", 4, likhadb_core::Metric::L2)
                .expect("create");
        }

        let guard = clone.read().await;
        assert!(guard.list().contains(&"col"));
    }

    #[tokio::test]
    async fn concurrent_reads_do_not_block_each_other() {
        let dir = TempDir::new().unwrap();
        let state = AppState::new(open_wal(&dir));

        let r1 = state.read().await;
        let r2 = state.read().await;
        assert_eq!(r1.list(), r2.list());
    }

    #[tokio::test]
    async fn write_excludes_reads() {
        let dir = TempDir::new().unwrap();
        let state = AppState::new(open_wal(&dir));

        let write_guard = state.write().await;
        // try_read must fail while write guard is held
        assert!(state.inner.try_read().is_err());
        drop(write_guard);
        // now read succeeds
        assert!(state.inner.try_read().is_ok());
    }
}
