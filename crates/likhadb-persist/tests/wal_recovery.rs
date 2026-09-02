use likhadb_core::{Metric, SourceBinding};
use likhadb_persist::{PersistError, WalConfig, WalManager};
use likhadb_store::DeltaRow;
use serde_json::json;

fn tmp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("likhadb_wal_{label}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ── Basic open / create ────────────────────────────────────────────────────

#[test]
fn open_empty_dir_succeeds() {
    let dir = tmp_dir("open_empty");
    let mgr = WalManager::open(&dir).unwrap();
    assert!(mgr.list().is_empty());
    assert_eq!(
        mgr.stats(),
        likhadb_persist::WalStats {
            entries_written: 0,
            entries_since_checkpoint: 0,
            wal_bytes: 0,
            last_lsn: 0,
            snapshot_lsn: 0,
        }
    );
}

#[test]
fn wal_stats_track_writes_checkpoints_and_recovery() {
    let dir = tmp_dir("stats");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        mgr.delete("col", 1).unwrap();

        let stats = mgr.stats();
        assert_eq!(stats.entries_written, 3);
        assert_eq!(stats.entries_since_checkpoint, 3);
        assert_eq!(stats.last_lsn, 3);
        assert_eq!(stats.snapshot_lsn, 0);
        assert_eq!(
            stats.wal_bytes,
            std::fs::metadata(dir.join("wal.log")).unwrap().len()
        );
        assert!(stats.wal_bytes > 0);

        mgr.checkpoint().unwrap();
        assert_eq!(
            mgr.stats(),
            likhadb_persist::WalStats {
                entries_written: 3,
                entries_since_checkpoint: 0,
                wal_bytes: 0,
                last_lsn: 3,
                snapshot_lsn: 3,
            }
        );

        mgr.insert("col", 2, vec![2.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        let stats = mgr.stats();
        assert_eq!(stats.entries_written, 4);
        assert_eq!(stats.entries_since_checkpoint, 1);
        assert_eq!(stats.last_lsn, 4);
        assert_eq!(stats.snapshot_lsn, 3);
        assert!(stats.wal_bytes > 0);
    }

    let mgr = WalManager::open(&dir).unwrap();
    let stats = mgr.stats();
    assert_eq!(stats.entries_written, 0);
    assert_eq!(stats.entries_since_checkpoint, 1);
    assert_eq!(stats.last_lsn, 4);
    assert_eq!(stats.snapshot_lsn, 3);
    assert_eq!(
        stats.wal_bytes,
        std::fs::metadata(dir.join("wal.log")).unwrap().len()
    );
}

#[test]
fn wal_config_has_bounded_defaults() {
    let config = WalConfig::default();
    assert_eq!(config.checkpoint_every_n_entries, 100_000);
    assert_eq!(config.checkpoint_every_n_bytes, 256 * 1024 * 1024);
}

// ── Insert survives restart ────────────────────────────────────────────────

#[test]
fn insert_survives_restart() {
    let dir = tmp_dir("insert_restart");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        for i in 0..10u64 {
            mgr.insert("col", i, vec![i as f32, 0.0, 0.0, 0.0], None)
                .unwrap();
        }
    }

    let mgr = WalManager::open(&dir).unwrap();
    let results = mgr
        .get("col")
        .unwrap()
        .search(&[0.0; 4], 10, None, false)
        .unwrap();
    assert_eq!(results.len(), 10);
}

// ── Delete survives restart ────────────────────────────────────────────────

#[test]
fn delete_survives_restart() {
    let dir = tmp_dir("delete_restart");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        for i in 0..5u64 {
            mgr.insert("col", i, vec![i as f32, 0.0, 0.0, 0.0], None)
                .unwrap();
        }
        mgr.delete("col", 0).unwrap();
        mgr.delete("col", 1).unwrap();
    }

    let mgr = WalManager::open(&dir).unwrap();
    let results = mgr
        .get("col")
        .unwrap()
        .search(&[0.0; 4], 10, None, false)
        .unwrap();
    assert_eq!(results.len(), 3);
    let ids: Vec<u64> = results.iter().map(|r| r.id).collect();
    assert!(!ids.contains(&0));
    assert!(!ids.contains(&1));
}

// ── Payload survives restart ───────────────────────────────────────────────

#[test]
fn payload_survives_restart() {
    let dir = tmp_dir("payload_restart");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        mgr.insert(
            "col",
            1,
            vec![1.0, 0.0, 0.0, 0.0],
            Some(json!({"tag": "cat"})),
        )
        .unwrap();
        mgr.insert("col", 2, vec![2.0, 0.0, 0.0, 0.0], None)
            .unwrap();
    }

    let mgr = WalManager::open(&dir).unwrap();
    let results = mgr
        .get("col")
        .unwrap()
        .search(&[0.0; 4], 2, None, true)
        .unwrap();
    assert_eq!(results.len(), 2);
    let r1 = results.iter().find(|r| r.id == 1).unwrap();
    assert_eq!(r1.payload.as_ref().unwrap()["tag"], json!("cat"));
    let r2 = results.iter().find(|r| r.id == 2).unwrap();
    assert!(r2.payload.is_none());
}

// ── DDL survives restart ───────────────────────────────────────────────────

#[test]
fn create_drop_collection_survives_restart() {
    let dir = tmp_dir("ddl_restart");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("a", 4, Metric::L2).unwrap();
        mgr.create_collection("b", 4, Metric::L2).unwrap();
        mgr.drop_collection("a").unwrap();
    }

    let mgr = WalManager::open(&dir).unwrap();
    assert_eq!(mgr.list(), vec!["b"]);
}

// ── Source binding survives restart ────────────────────────────────────────

#[test]
fn source_binding_survives_restart() {
    let dir = tmp_dir("binding_restart");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        mgr.set_source_binding(
            "col",
            likhadb_core::SourceBinding {
                source_namespace: vec!["lake".into()],
                source_table: "embeddings".into(),
                id_column: "id".into(),
                vector_column: "embedding".into(),
                payload_columns: vec!["title".into()],
            },
        )
        .unwrap();
    }

    // Reopen: replay must restore the binding from the WAL op.
    let mgr = WalManager::open(&dir).unwrap();
    let binding = mgr
        .get("col")
        .unwrap()
        .source_binding
        .as_ref()
        .expect("binding restored after restart");
    assert_eq!(binding.source_table, "embeddings");
    assert_eq!(binding.vector_column, "embedding");
}

fn source_binding() -> SourceBinding {
    SourceBinding {
        source_namespace: vec!["lake".into()],
        source_table: "embeddings".into(),
        id_column: "id".into(),
        vector_column: "embedding".into(),
        payload_columns: vec!["title".into()],
    }
}

#[test]
fn source_delta_updates_index_and_advances_watermark() {
    let dir = tmp_dir("source_delta_apply");
    let mut mgr = WalManager::open(&dir).unwrap();
    mgr.create_hnsw_collection("col", 2, Metric::L2, 4, 8, 10)
        .unwrap();
    let binding = source_binding();
    mgr.set_source_binding("col", binding.clone()).unwrap();
    assert!(mgr
        .apply_source_delta("col", &binding, None, 10, [])
        .unwrap());

    assert!(mgr
        .apply_source_delta(
            "col",
            &binding,
            Some(10),
            11,
            [
                DeltaRow::Upsert {
                    id: 1,
                    vector: vec![1.0, 0.0],
                    payload: Some(json!({"source": "external"})),
                },
                DeltaRow::Upsert {
                    id: 2,
                    vector: vec![2.0, 0.0],
                    payload: None,
                },
                DeltaRow::Delete { id: 1 },
            ],
        )
        .unwrap());

    let collection = mgr.get("col").unwrap();
    assert_eq!(collection.source_snapshot_id, Some(11));
    assert!(collection.get(1).unwrap().is_none());
    assert_eq!(
        collection.search(&[2.0, 0.0], 1, None, false).unwrap()[0].id,
        2
    );
}

#[test]
fn failed_source_delta_keeps_watermark_for_idempotent_retry() {
    let dir = tmp_dir("source_delta_retry");
    let mut mgr = WalManager::open(&dir).unwrap();
    mgr.create_collection("col", 2, Metric::L2).unwrap();
    let binding = source_binding();
    mgr.set_source_binding("col", binding.clone()).unwrap();
    assert!(mgr
        .apply_source_delta("col", &binding, None, 20, [])
        .unwrap());

    let error = mgr
        .apply_source_delta(
            "col",
            &binding,
            Some(20),
            21,
            [
                DeltaRow::Upsert {
                    id: 1,
                    vector: vec![1.0, 0.0],
                    payload: None,
                },
                DeltaRow::Upsert {
                    id: 2,
                    vector: vec![2.0, 0.0, 0.0],
                    payload: None,
                },
            ],
        )
        .unwrap_err();
    assert!(error.to_string().contains("dimension mismatch"));
    assert_eq!(mgr.get("col").unwrap().source_snapshot_id, Some(20));
    assert!(mgr.get("col").unwrap().get(1).unwrap().is_some());

    assert!(mgr
        .apply_source_delta(
            "col",
            &binding,
            Some(20),
            21,
            [
                DeltaRow::Upsert {
                    id: 1,
                    vector: vec![1.0, 0.0],
                    payload: None,
                },
                DeltaRow::Upsert {
                    id: 2,
                    vector: vec![2.0, 0.0],
                    payload: None,
                },
            ],
        )
        .unwrap());
    assert_eq!(mgr.get("col").unwrap().source_snapshot_id, Some(21));
    assert_eq!(mgr.get("col").unwrap().len(), 2);
}

#[test]
fn stale_source_delta_is_discarded() {
    let dir = tmp_dir("source_delta_stale");
    let mut mgr = WalManager::open(&dir).unwrap();
    mgr.create_collection("col", 2, Metric::L2).unwrap();
    let binding = source_binding();
    mgr.set_source_binding("col", binding.clone()).unwrap();
    assert!(mgr
        .apply_source_delta("col", &binding, None, 30, [])
        .unwrap());

    assert!(!mgr
        .apply_source_delta(
            "col",
            &binding,
            Some(29),
            31,
            [DeltaRow::Upsert {
                id: 1,
                vector: vec![1.0, 0.0],
                payload: None,
            }],
        )
        .unwrap());
    assert_eq!(mgr.get("col").unwrap().source_snapshot_id, Some(30));
    assert!(mgr.get("col").unwrap().is_empty());
}

// ── Checkpoint clears WAL ──────────────────────────────────────────────────

#[test]
fn checkpoint_clears_wal() {
    let dir = tmp_dir("checkpoint");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        mgr.checkpoint().unwrap();
    }

    let wal_path = dir.join("wal.log");
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        0,
        "WAL should be empty after checkpoint"
    );

    let mgr = WalManager::open(&dir).unwrap();
    let results = mgr
        .get("col")
        .unwrap()
        .search(&[0.0; 4], 1, None, false)
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn auto_checkpoint_after_entry_threshold() {
    let dir = tmp_dir("auto_checkpoint_entries");
    let config = WalConfig {
        checkpoint_every_n_entries: 2,
        checkpoint_every_n_bytes: 0,
    };

    {
        let mut mgr = WalManager::open_with_config(&dir, config).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        assert!(!dir.join("snapshot.bin").exists());
        assert_eq!(mgr.stats().entries_since_checkpoint, 1);

        mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        assert!(dir.join("snapshot.bin").exists());
        assert_eq!(std::fs::metadata(dir.join("wal.log")).unwrap().len(), 0);
        assert_eq!(mgr.stats().entries_written, 2);
        assert_eq!(mgr.stats().entries_since_checkpoint, 0);
        assert_eq!(mgr.stats().last_lsn, 2);
        assert_eq!(mgr.stats().snapshot_lsn, 2);
    }

    let mgr = WalManager::open(&dir).unwrap();
    assert_eq!(
        mgr.get("col")
            .unwrap()
            .search(&[0.0; 4], 10, None, false)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn auto_checkpoint_after_byte_threshold() {
    let dir = tmp_dir("auto_checkpoint_bytes");
    let config = WalConfig {
        checkpoint_every_n_entries: 0,
        checkpoint_every_n_bytes: 1,
    };

    let mut mgr = WalManager::open_with_config(&dir, config).unwrap();
    mgr.create_collection("col", 4, Metric::L2).unwrap();

    assert!(dir.join("snapshot.bin").exists());
    assert_eq!(std::fs::metadata(dir.join("wal.log")).unwrap().len(), 0);
}

#[test]
fn zero_auto_checkpoint_thresholds_disable_triggers() {
    let dir = tmp_dir("auto_checkpoint_disabled");
    let config = WalConfig {
        checkpoint_every_n_entries: 0,
        checkpoint_every_n_bytes: 0,
    };

    let mut mgr = WalManager::open_with_config(&dir, config).unwrap();
    mgr.create_collection("col", 4, Metric::L2).unwrap();
    mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
        .unwrap();

    assert!(!dir.join("snapshot.bin").exists());
    assert!(std::fs::metadata(dir.join("wal.log")).unwrap().len() > 0);
}

#[test]
fn recovered_entries_count_toward_auto_checkpoint_threshold() {
    let dir = tmp_dir("auto_checkpoint_recovered_entries");
    let disabled = WalConfig {
        checkpoint_every_n_entries: 0,
        checkpoint_every_n_bytes: 0,
    };

    {
        let mut mgr = WalManager::open_with_config(&dir, disabled).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
    }

    let config = WalConfig {
        checkpoint_every_n_entries: 2,
        checkpoint_every_n_bytes: 0,
    };
    let mut mgr = WalManager::open_with_config(&dir, config).unwrap();
    mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
        .unwrap();

    assert!(dir.join("snapshot.bin").exists());
    assert_eq!(std::fs::metadata(dir.join("wal.log")).unwrap().len(), 0);
}

// ── Recovery across checkpoint boundary ───────────────────────────────────

#[test]
fn recovery_across_checkpoint_boundary() {
    let dir = tmp_dir("checkpoint_boundary");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        mgr.checkpoint().unwrap();
        // Writes after checkpoint go to fresh WAL.
        mgr.insert("col", 2, vec![2.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        mgr.insert("col", 3, vec![3.0, 0.0, 0.0, 0.0], None)
            .unwrap();
    }

    let mgr = WalManager::open(&dir).unwrap();
    let results = mgr
        .get("col")
        .unwrap()
        .search(&[0.0; 4], 10, None, false)
        .unwrap();
    assert_eq!(results.len(), 3, "all 3 vectors should be present");
    let ids: Vec<u64> = results.iter().map(|r| r.id).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
    assert!(ids.contains(&3));
}

// ── Truncated tail is silently ignored ────────────────────────────────────

#[test]
fn truncated_wal_tail_is_ignored() {
    let dir = tmp_dir("truncated_tail");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        // id=2 will be partially written (simulated by truncation below).
        mgr.insert("col", 2, vec![2.0, 0.0, 0.0, 0.0], None)
            .unwrap();
    }

    // Truncate the last 3 bytes of wal.log to simulate a crash mid-write.
    let wal_path = dir.join("wal.log");
    let original_len = std::fs::metadata(&wal_path).unwrap().len();
    let truncated_len = original_len.saturating_sub(3);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .unwrap();
    file.set_len(truncated_len).unwrap();

    // Recovery should succeed and the last incomplete entry should be dropped.
    // (At minimum id=1's CreateCollection must survive; id=2 may be gone.)
    let mgr = WalManager::open(&dir).unwrap();
    assert!(
        mgr.get("col").is_ok(),
        "collection should survive tail truncation"
    );
}

// ── Mid-log CRC corruption returns an error ────────────────────────────────

#[test]
fn mid_log_corruption_is_error() {
    let dir = tmp_dir("mid_log_corrupt");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        mgr.insert("col", 2, vec![2.0, 0.0, 0.0, 0.0], None)
            .unwrap();
    }

    // Flip a byte in the middle of the WAL (past the first frame).
    let wal_path = dir.join("wal.log");
    let mut data = std::fs::read(&wal_path).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&wal_path, &data).unwrap();

    let result = WalManager::open(&dir);
    assert!(
        matches!(result, Err(PersistError::Crc { .. })),
        "mid-log CRC corruption should surface as PersistError::Crc"
    );
}

// ── Mid-log corruption on the second frame returns an error ───────────────
//
// Regression: the original guard used `frames_read() <= 1`, which silently
// swallowed a corrupt second frame even when valid frames followed it.
// The correct signal is whether bytes remain after the corrupt frame — if they
// do, it is genuine mid-log corruption and must be a hard error.

#[test]
fn second_frame_mid_log_corruption_is_error() {
    let dir = tmp_dir("second_frame_mid_log_corrupt");

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        mgr.insert("col", 1, vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        // Third frame — ensures the corrupt second frame has data after it.
        mgr.insert("col", 2, vec![2.0, 0.0, 0.0, 0.0], None)
            .unwrap();
    }

    // Corrupt the second frame (the insert id=1 entry). Frame 3 follows it,
    // so this is mid-log corruption, not a truncated tail.
    let wal_path = dir.join("wal.log");
    let mut data = std::fs::read(&wal_path).unwrap();
    let first_frame_payload_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let second_frame_start = 4 + 4 + first_frame_payload_len;
    data[second_frame_start + 8 + 1] ^= 0xFF;
    std::fs::write(&wal_path, &data).unwrap();

    let result = WalManager::open(&dir);
    assert!(
        matches!(result, Err(PersistError::Crc { .. })),
        "second-frame mid-log corruption must surface as PersistError::Crc"
    );
}

// ── All index types survive restart ───────────────────────────────────────

#[test]
fn all_index_types_survive_restart() {
    let dir = tmp_dir("all_index_types");
    let n = 20u64;

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("flat", 4, Metric::L2).unwrap();
        mgr.create_ivf_collection("ivf", 4, Metric::L2, 4, 4)
            .unwrap();
        mgr.create_hnsw_collection("hnsw", 4, Metric::L2, 4, 8, 4)
            .unwrap();

        for i in 0..n {
            let v = vec![i as f32, 0.0, 0.0, 0.0];
            mgr.insert("flat", i, v.clone(), None).unwrap();
            mgr.insert("ivf", i, v.clone(), None).unwrap();
            mgr.insert("hnsw", i, v.clone(), None).unwrap();
        }
    }

    let mgr = WalManager::open(&dir).unwrap();
    for col_name in ["flat", "ivf", "hnsw"] {
        let col = mgr.get(col_name).unwrap();
        let results = col.search(&[0.0; 4], 5, None, false).unwrap();
        assert_eq!(
            results.len(),
            5,
            "{col_name} should return 5 results after restart"
        );
        // Results must be sorted ascending.
        for w in results.windows(2) {
            assert!(w[0].score <= w[1].score, "{col_name}: results not sorted");
        }
    }
}

// ── 1000-vector full replay after drop (sync_data guarantee) ──────────────
//
// Exercises the fsync path introduced to close the WAL sync_data gap: every
// append must be durable before the writer is dropped, so a simulated restart
// (drop + reopen) must recover all 1000 entries without loss.

#[test]
fn thousand_vectors_survive_restart() {
    let dir = tmp_dir("thousand_vectors");
    let n = 1000u64;

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("col", 4, Metric::L2).unwrap();
        for i in 0..n {
            mgr.insert("col", i, vec![i as f32, 0.0, 0.0, 0.0], None)
                .unwrap();
        }
    } // drop simulates process exit; all appends must already be on disk

    let mgr = WalManager::open(&dir).unwrap();
    let results = mgr
        .get("col")
        .unwrap()
        .search(&[0.0; 4], n as usize, None, false)
        .unwrap();
    assert_eq!(
        results.len(),
        n as usize,
        "all {n} vectors must be recovered after restart"
    );
}

// ── IVF-SQ8 survives restart ───────────────────────────────────────────────

#[test]
fn ivf_sq8_survives_restart() {
    let dir = tmp_dir("ivf_sq8_restart");
    let nlist = 4usize;

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_ivf_sq8_collection("sq8", 4, Metric::L2, nlist, nlist)
            .unwrap();
        for i in 0..(nlist + 20) as u64 {
            mgr.insert("sq8", i, vec![i as f32, 0.0, 0.0, 0.0], None)
                .unwrap();
        }
    }

    let mgr = WalManager::open(&dir).unwrap();
    let results = mgr
        .get("sq8")
        .unwrap()
        .search(&[0.0; 4], 5, None, false)
        .unwrap();
    assert_eq!(results.len(), 5);
}

// ── FTS survives restart without re-indexing ──────────────────────────────

#[cfg(feature = "fts")]
#[test]
fn fts_index_survives_restart() {
    let dir = tmp_dir("fts_restart");

    // Phase 1: insert docs with FTS enabled, then checkpoint.
    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("docs", 4, Metric::L2).unwrap();
        mgr.enable_fts("docs").unwrap();
        for i in 0..50u64 {
            let text = if i == 7 {
                "exclusive canary term zephyr".to_string()
            } else {
                format!("generic document number {i}")
            };
            mgr.insert(
                "docs",
                i,
                vec![i as f32, 0.0, 0.0, 0.0],
                Some(json!({"body": text})),
            )
            .unwrap();
        }
        mgr.checkpoint().unwrap();
    }

    // Phase 2: reopen — FTS index must be loaded from disk, not rebuilt.
    let mgr = WalManager::open(&dir).unwrap();
    let results = mgr.get("docs").unwrap().fts_search("zephyr", 5).unwrap();
    assert_eq!(
        results.len(),
        1,
        "FTS must find the canary doc after restart"
    );
    assert_eq!(results[0].id, 7, "canary doc id must be 7");

    // Confirm FTS still works for normal queries after restart.
    let generic = mgr.get("docs").unwrap().fts_search("generic", 10).unwrap();
    assert!(
        !generic.is_empty(),
        "generic docs must be searchable after restart"
    );
}
