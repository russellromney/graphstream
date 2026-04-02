//! Streaming tests: compressed reading and compaction via hadb-changeset.

use graphstream::journal::{
    self, JournalCommand, JournalReader, JournalState, PendingEntry,
};
use graphstream::types::ParamValue;
use std::sync::Arc;

/// Write 1000 entries, seal (compresses), read back via JournalReader.
#[test]
fn test_compressed_read_1000_entries() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        10 * 1024 * 1024,
        100,
        state.clone(),
    );

    for i in 1..=1000 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}, data: '{}'}})", i, "x".repeat(100)),
            params: vec![
                ("id".into(), ParamValue::Int(i)),
                ("data".into(), ParamValue::String("x".repeat(100))),
            ],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let sealed_path = ack_rx.recv().unwrap();
    assert!(sealed_path.is_some(), "Seal should produce a path");

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let sealed = sealed_path.unwrap();
    let hdr = graphstream::format::read_header_from_path(&sealed).unwrap();
    assert!(hdr.is_sealed(), "Segment should be sealed");
    assert!(hdr.is_compressed(), "Segment should be compressed");

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1000, "Should read all 1000 entries");

    assert_eq!(entries[0].sequence, 1);
    assert_eq!(entries[999].sequence, 1000);

    let expected_hash = *state.chain_hash.lock().unwrap();
    assert_eq!(entries[999].chain_hash, expected_hash);

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 1000);
    assert_eq!(hash, expected_hash);
}

/// Compressed reading with from_sequence: skip first 500, read remaining 500.
#[test]
fn test_compressed_from_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        10 * 1024 * 1024,
        100,
        state.clone(),
    );

    for i in 1..=1000 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let reader = JournalReader::from_sequence(&journal_dir, 501, [0u8; 32]).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 500, "Should read 500 entries (501..=1000)");
    assert_eq!(entries[0].sequence, 501);
    assert_eq!(entries[499].sequence, 1000);
}

/// Compaction via background compaction: multiple segments into 1, verify readable.
#[test]
fn test_multi_segment_read() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        4096,
        100,
        state.clone(),
    );

    for i in 1..=100 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}}})", i),
            params: vec![("id".into(), ParamValue::Int(i))],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let sealed: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "hadbj"))
        .collect();
    assert!(sealed.len() >= 2, "Should have multiple segments, got {}", sealed.len());

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 100);
    assert_eq!(entries[0].sequence, 1);
    assert_eq!(entries[99].sequence, 100);
}

/// Many segments read correctly.
#[test]
fn test_many_segments() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        2048,
        100,
        state.clone(),
    );

    for i in 1..=200 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{id: {}, data: '{}'}})", i, "y".repeat(50)),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let sealed: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "hadbj"))
        .collect();
    assert!(sealed.len() >= 5, "Should have many segments, got {}", sealed.len());

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 200);
}
