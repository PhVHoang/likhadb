//! Configuration and per-tick backpressure for source-table maintenance.
//!
//! The catalog polling loop is introduced separately. Keeping the row budget
//! here makes the opt-in/disabled behavior and carry-over semantics independent
//! of the source used by that loop.

use std::collections::VecDeque;
use std::time::Duration;

use likhadb_store::{Collection, DeltaRow};

/// Runtime settings for incremental source-table maintenance.
///
/// The default is deliberately disabled so omitting `[maintenance]` preserves
/// pre-maintenance behavior. Fields within an explicit block receive their
/// documented defaults when omitted.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MaintenanceConfig {
    /// Whether source-table maintenance is enabled.
    pub enabled: bool,
    /// Source snapshot polling cadence, in seconds.
    pub interval_s: u64,
    /// HNSW tombstone ratio that triggers compaction.
    pub hnsw_compaction_tombstone_ratio: f32,
    /// Number of applied IVF rows between compactions.
    pub ivf_compaction_every_n_rows: u64,
    /// Maximum rows applied in one tick; zero means unbounded.
    pub max_rows_per_tick: usize,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_s: 60,
            hnsw_compaction_tombstone_ratio: 0.2,
            ivf_compaction_every_n_rows: 1_000_000,
            max_rows_per_tick: 0,
        }
    }
}

impl MaintenanceConfig {
    /// Whether the maintenance scheduler should be spawned.
    pub fn should_run(&self) -> bool {
        self.enabled
    }

    /// Polling interval used by the maintenance task.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_s)
    }

    fn row_budget(&self, pending: usize) -> usize {
        if !self.should_run() {
            0
        } else if self.max_rows_per_tick == 0 {
            pending
        } else {
            pending.min(self.max_rows_per_tick)
        }
    }
}

/// Rows from a scanned snapshot range that have not yet been applied.
///
/// The maintenance task keeps this queue between ticks. The source watermark
/// must advance only after [`is_empty`](Self::is_empty) becomes true.
pub struct PendingMaintenanceRows {
    rows: VecDeque<DeltaRow>,
}

impl PendingMaintenanceRows {
    pub fn new(rows: Vec<DeltaRow>) -> Self {
        Self { rows: rows.into() }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Apply at most the configured row budget, leaving the remainder queued.
    ///
    /// If applying a row fails, that row is restored to the front of the queue
    /// so a later tick can safely retry it.
    pub fn apply_tick(
        &mut self,
        collection: &mut Collection,
        config: &MaintenanceConfig,
        lsn: u64,
    ) -> likhadb_core::Result<usize> {
        let budget = config.row_budget(self.rows.len());
        let mut applied = 0;
        while applied < budget {
            let Some(row) = self.rows.pop_front() else {
                break;
            };
            if let Err(error) = collection.apply_delta_row(row.clone(), lsn) {
                self.rows.push_front(row);
                return Err(error);
            }
            applied += 1;
        }
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use likhadb_core::Metric;

    #[derive(serde::Deserialize)]
    struct TestConfigFile {
        #[serde(default)]
        maintenance: MaintenanceConfig,
    }

    fn upsert(id: u64) -> DeltaRow {
        DeltaRow::Upsert {
            id,
            vector: vec![id as f32, 0.0],
            payload: None,
        }
    }

    #[test]
    fn absent_block_uses_disabled_defaults() {
        let parsed: TestConfigFile = toml::from_str("").unwrap();
        assert_eq!(parsed.maintenance, MaintenanceConfig::default());
        assert!(!parsed.maintenance.should_run());
        assert_eq!(parsed.maintenance.interval(), Duration::from_secs(60));
    }

    #[test]
    fn explicit_block_fills_missing_fields_with_defaults() {
        let parsed: TestConfigFile = toml::from_str(
            r#"
                [maintenance]
                enabled = true
                max_rows_per_tick = 2
            "#,
        )
        .unwrap();
        assert!(parsed.maintenance.enabled);
        assert_eq!(parsed.maintenance.interval_s, 60);
        assert_eq!(parsed.maintenance.max_rows_per_tick, 2);
        assert_eq!(parsed.maintenance.ivf_compaction_every_n_rows, 1_000_000);
    }

    #[test]
    fn documented_block_parses_all_values() {
        let parsed: TestConfigFile = toml::from_str(
            r#"
                [maintenance]
                enabled = true
                interval_s = 30
                hnsw_compaction_tombstone_ratio = 0.25
                ivf_compaction_every_n_rows = 1_000_000
                max_rows_per_tick = 500
            "#,
        )
        .unwrap();
        assert_eq!(
            parsed.maintenance,
            MaintenanceConfig {
                enabled: true,
                interval_s: 30,
                hnsw_compaction_tombstone_ratio: 0.25,
                ivf_compaction_every_n_rows: 1_000_000,
                max_rows_per_tick: 500,
            }
        );
    }

    #[test]
    fn disabled_config_never_applies_pending_rows() {
        let mut collection = Collection::new("test".to_string(), 2, Metric::L2);
        let mut pending = PendingMaintenanceRows::new(vec![upsert(1)]);

        let applied = pending
            .apply_tick(&mut collection, &MaintenanceConfig::default(), u64::MAX)
            .unwrap();

        assert_eq!(applied, 0);
        assert_eq!(pending.len(), 1);
        assert_eq!(collection.len(), 0);
    }

    #[test]
    fn capped_tick_carries_remainder_to_next_tick() {
        let config = MaintenanceConfig {
            enabled: true,
            max_rows_per_tick: 2,
            ..MaintenanceConfig::default()
        };
        let mut collection = Collection::new("test".to_string(), 2, Metric::L2);
        let mut pending = PendingMaintenanceRows::new(vec![upsert(1), upsert(2), upsert(3)]);

        assert_eq!(
            pending
                .apply_tick(&mut collection, &config, u64::MAX)
                .unwrap(),
            2
        );
        assert_eq!(pending.len(), 1);
        assert_eq!(collection.len(), 2);

        assert_eq!(
            pending
                .apply_tick(&mut collection, &config, u64::MAX)
                .unwrap(),
            1
        );
        assert!(pending.is_empty());
        assert_eq!(collection.len(), 3);
    }

    #[test]
    fn zero_row_cap_is_unbounded() {
        let config = MaintenanceConfig {
            enabled: true,
            ..MaintenanceConfig::default()
        };
        let mut collection = Collection::new("test".to_string(), 2, Metric::L2);
        let mut pending = PendingMaintenanceRows::new(vec![upsert(1), upsert(2), upsert(3)]);

        assert_eq!(
            pending
                .apply_tick(&mut collection, &config, u64::MAX)
                .unwrap(),
            3
        );
        assert!(pending.is_empty());
    }
}
