//! Journal tests: writer lifecycle, seal, flush, read, recovery, large entries, param types.

use graphstream::journal::{
    self, JournalCommand, JournalReader, JournalState, PendingEntry,
};
use graphstream::types::ParamValue;
use hadb_changeset::journal as hj;
use std::sync::Arc;

/// Write entries via journal writer, read them back via JournalReader.
#[test]
fn test_write_and_read_entries() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Person {{name: 'Person{}'}})", i),
            params: vec![("name".into(), ParamValue::String(format!("Person{}", i)))],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    assert_eq!(state.sequence.load(std::sync::atomic::Ordering::SeqCst), 5);
    assert!(state.is_alive());

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 5);

    for (i, entry) in entries.iter().enumerate() {
        let expected_seq = (i + 1) as u64;
        assert_eq!(entry.sequence, expected_seq);
        assert!(entry.entry.query.contains(&format!("Person{}", i + 1)));
    }
}

/// Test that sealed segments are compressed and still readable.
#[test]
fn test_sealed_segment_is_compressed() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("MERGE (n:Node {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let sealed_path = ack_rx.recv().unwrap();
    assert!(sealed_path.is_some(), "Expected sealed segment path");

    let sealed_path = sealed_path.unwrap();
    assert!(sealed_path.exists(), "Sealed file should exist");

    // Verify it's sealed + compressed by reading header.
    let header = graphstream::format::read_header_from_path(&sealed_path).unwrap();
    assert!(header.is_sealed());
    assert!(header.is_compressed());
    assert_eq!(header.entry_count, 3);
    assert_eq!(header.first_seq, 1);
    assert_eq!(header.last_seq, 3);

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 3);
}

/// Test journal state recovery from sealed segments.
#[test]
fn test_recover_journal_state() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    for i in 1..=10 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    let expected_hash = *state.chain_hash.lock().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let (recovered_seq, recovered_hash) =
        journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(recovered_seq, 10);
    assert_eq!(recovered_hash, expected_hash);
}

/// Test recovery from empty journal directory.
#[test]
fn test_recover_empty_journal() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 0);
    assert_eq!(hash, [0u8; 32]);
}

/// Test recovery from nonexistent directory.
#[test]
fn test_recover_nonexistent_dir() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("does-not-exist");

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 0);
    assert_eq!(hash, [0u8; 32]);
}

/// Test segment rotation: writing more than segment_max_bytes triggers new segment.
#[test]
fn test_segment_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        512,
        100,
        state.clone(),
    );

    for i in 1..=10 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Person {{name: 'Person{}'}})", i),
            params: vec![("idx".into(), ParamValue::Int(i))],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));

    let segments: Vec<_> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "hadbj"))
        .collect();

    assert!(segments.len() > 1, "Expected multiple segments from rotation, got {}", segments.len());

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 10);
}

/// Test SealForUpload command.
#[test]
fn test_seal_for_upload() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let sealed = ack_rx.recv().unwrap();
    assert!(sealed.is_some());
    let sealed_path = sealed.unwrap();
    assert!(sealed_path.exists());

    // Second seal with no new entries should return None.
    let (ack_tx2, ack_rx2) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx2)).unwrap();
    let sealed2 = ack_rx2.recv().unwrap();
    assert!(sealed2.is_none());

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
}

/// Test journal with large entries.
#[test]
fn test_large_entries() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        10 * 1024 * 1024,
        100,
        state.clone(),
    );

    let big_query = format!("CREATE (:Node {{data: '{}'}})", "x".repeat(100_000));
    tx.send(JournalCommand::Write(PendingEntry {
        query: big_query.clone(),
        params: vec![],
    }))
    .unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].entry.query, big_query);
}

/// Test that all param types survive write/read roundtrip.
#[test]
fn test_all_param_types() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    tx.send(JournalCommand::Write(PendingEntry {
        query: "MATCH (n) RETURN n".into(),
        params: vec![
            ("i".into(), ParamValue::Int(42)),
            ("f".into(), ParamValue::Float(3.14)),
            ("s".into(), ParamValue::String("hello".into())),
            ("b".into(), ParamValue::Bool(true)),
            ("n".into(), ParamValue::Null),
            ("l".into(), ParamValue::List(vec![ParamValue::Int(1), ParamValue::Int(2)])),
        ],
    }))
    .unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1);

    let params = graphstream::map_entries_to_param_values(&entries[0].entry.params);
    assert_eq!(params.len(), 6);
}

/// Test from_sequence skips earlier entries.
#[test]
fn test_from_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    for i in 1..=10 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let reader = JournalReader::from_sequence(&journal_dir, 6, [0u8; 32]).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 5); // entries 6..=10
    assert_eq!(entries[0].sequence, 6);
    assert_eq!(entries[4].sequence, 10);
}
