use std::sync::OnceLock;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

// Buckets covering 100µs → 1s: captures fast flat searches at the low end
// and slow HNSW/IVF searches or insert-time training at the high end.
const LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global Prometheus metrics recorder with per-metric bucket config.
///
/// Idempotent: safe to call from multiple tests or init paths — the recorder
/// is installed exactly once and the same handle is returned on every call.
pub fn install() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .set_buckets_for_metric(
                    Matcher::Full("likhadb_search_duration_seconds".to_string()),
                    LATENCY_BUCKETS,
                )
                .expect("invalid search buckets")
                .set_buckets_for_metric(
                    Matcher::Full("likhadb_insert_duration_seconds".to_string()),
                    LATENCY_BUCKETS,
                )
                .expect("invalid insert buckets")
                .install_recorder()
                .expect("failed to install Prometheus recorder")
        })
        .clone()
}

/// Seed `likhadb_collection_vectors_total` from the already-loaded WAL state.
///
/// Called once after startup so the gauge reflects snapshot-loaded collections
/// before any REST writes arrive.
pub fn seed_collection_gauges(wal: &likhadb_persist::WalManager) {
    for name in wal.list() {
        if let Ok(col) = wal.get(name) {
            metrics::gauge!(
                "likhadb_collection_vectors_total",
                "collection" => name.to_string(),
                "index_type" => col.index_type().to_string()
            )
            .set(col.len() as f64);
        }
    }
}
