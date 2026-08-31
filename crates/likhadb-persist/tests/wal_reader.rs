use std::io::{Seek, SeekFrom, Write};

use likhadb_core::Metric;
use likhadb_persist::wal::WalOp;
use likhadb_persist::{PersistError, WalManager, WalReader};

fn tmp_dir(label: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("likhadb_wal_reader_{label}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn reader_inspects_entries_without_replay() {
    let dir = tmp_dir("inspect");
    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("docs", 2, Metric::Cosine).unwrap();
        mgr.insert("docs", 7, vec![1.0, 0.0], None).unwrap();
    }

    let entries = WalReader::open(&dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].lsn, 1);
    assert!(matches!(
        &entries[0].op,
        WalOp::CreateCollection { name, .. } if name == "docs"
    ));
    assert_eq!(entries[1].lsn, 2);
    assert!(matches!(
        &entries[1].op,
        WalOp::Insert {
            collection,
            id: 7,
            ..
        } if collection == "docs"
    ));
}

#[test]
fn reader_does_not_create_a_missing_wal() {
    let dir =
        std::env::temp_dir().join(format!("likhadb_wal_reader_missing_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    assert!(matches!(WalReader::open(&dir), Err(PersistError::Io(_))));
    assert!(!dir.exists());
}

#[test]
fn pending_entries_exclude_records_already_in_snapshot() {
    let dir = tmp_dir("pending");
    let stale_wal;

    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("docs", 2, Metric::L2).unwrap();
        mgr.insert("docs", 1, vec![1.0, 0.0], None).unwrap();
        stale_wal = std::fs::read(dir.join("wal.log")).unwrap();
        mgr.checkpoint().unwrap();
    }

    // Simulate the recoverable state where snapshot rename succeeded but WAL
    // truncation did not: the log still contains entries captured by snapshot.
    std::fs::write(dir.join("wal.log"), stale_wal).unwrap();

    let mut mgr = WalManager::open(&dir).unwrap();
    mgr.insert("docs", 2, vec![2.0, 0.0], None).unwrap();

    let all_lsns = WalReader::open(&dir)
        .unwrap()
        .map(|entry| entry.map(|entry| entry.lsn))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(all_lsns, vec![1, 2, 3]);

    let pending_lsns = mgr
        .pending_entries()
        .unwrap()
        .map(|entry| entry.map(|entry| entry.lsn))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pending_lsns, vec![3]);
}

#[test]
fn reader_reports_mid_log_crc_corruption_and_stops() {
    let dir = tmp_dir("mid_log_crc");
    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("docs", 2, Metric::L2).unwrap();
        mgr.insert("docs", 1, vec![1.0, 0.0], None).unwrap();
        mgr.insert("docs", 2, vec![2.0, 0.0], None).unwrap();
    }

    let wal_path = dir.join("wal.log");
    let mut data = std::fs::read(&wal_path).unwrap();
    let first_payload_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let second_frame_start = 8 + first_payload_len;
    data[second_frame_start + 8] ^= 0xff;
    std::fs::write(&wal_path, data).unwrap();

    let mut reader = WalReader::open(&dir).unwrap();
    assert!(reader.next().unwrap().is_ok());
    assert!(matches!(reader.next(), Some(Err(PersistError::Crc { .. }))));
    assert!(reader.next().is_none());
}

#[test]
fn reader_ignores_a_crc_mismatched_tail() {
    let dir = tmp_dir("tail_crc");
    {
        let mut mgr = WalManager::open(&dir).unwrap();
        mgr.create_collection("docs", 2, Metric::L2).unwrap();
        mgr.insert("docs", 1, vec![1.0, 0.0], None).unwrap();
    }

    let wal_path = dir.join("wal.log");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&wal_path)
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    let last = std::fs::read(&wal_path).unwrap().last().copied().unwrap();
    file.write_all(&[last ^ 0xff]).unwrap();

    let entries = WalReader::open(&dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert!(matches!(entries[0].op, WalOp::CreateCollection { .. }));
}
