//! Chain hash tests: chain hash trailer writing and recovery.

use graphstream::journal::{
    self, JournalCommand, JournalState, PendingEntry,
};
use graphstream::format;
use std::sync::Arc;

/// After a seal, the sealed segment should have FLAG_HAS_CHAIN_HASH and a valid trailer.
#[test]
fn test_chain_hash_trailer_written_after_seal() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 50, state.clone());

    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let sealed = ack_rx.recv().unwrap();
    assert!(sealed.is_some());
    let sealed_path = sealed.unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let header = format::read_header_from_path(&sealed_path).unwrap();
    assert!(header.is_sealed());
    assert!(header.has_chain_hash(), "FLAG_HAS_CHAIN_HASH should be set");
    assert_eq!(header.last_seq, 5);

    let trailer_hash = format::read_chain_hash_from_path(&sealed_path, &header).unwrap();
    let expected_hash = *state.chain_hash.lock().unwrap();
    assert_eq!(trailer_hash, expected_hash, "Trailer hash should match writer state");
    assert_ne!(trailer_hash, [0u8; 32], "Hash should not be all zeros");
}

/// Recovery should read chain hash from trailer (O(1), no full scan).
#[test]
fn test_chain_hash_trailer_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 50, state.clone());

    for i in 1..=10 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let expected_seq = state.sequence.load(std::sync::atomic::Ordering::SeqCst);
    let expected_hash = *state.chain_hash.lock().unwrap();

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, expected_seq);
    assert_eq!(hash, expected_hash);
}

/// Sealed segments without chain hash trailer fall back to full scan.
#[test]
fn test_recovery_v1_segment_falls_back_to_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 50, state.clone());

    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let sealed = ack_rx.recv().unwrap().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let expected_seq = state.sequence.load(std::sync::atomic::Ordering::SeqCst);
    let expected_hash = *state.chain_hash.lock().unwrap();

    // Strip the chain hash flag and truncate the trailer to simulate a v1 segment.
    {
        use hadb_changeset::journal::{FLAG_HAS_CHAIN_HASH, HEADER_SIZE};
        let mut data = std::fs::read(&sealed).unwrap();
        // Clear FLAG_HAS_CHAIN_HASH in flags byte (offset 6 in hadbj format).
        data[6] &= !FLAG_HAS_CHAIN_HASH;
        // Truncate off the 32-byte trailer.
        let header = hadb_changeset::journal::decode_header(&data).unwrap();
        let new_len = HEADER_SIZE + header.body_len as usize;
        data.truncate(new_len);
        std::fs::write(&sealed, &data).unwrap();
    }

    // Recovery should still work via full scan.
    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, expected_seq);
    assert_eq!(hash, expected_hash);
}

/// Chain hash trailer works correctly across segment rotation.
#[test]
fn test_chain_hash_trailer_on_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 256, 50, state.clone());

    for i in 1..=20 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}, data: 'padding_to_fill_segment_{}'}})", i, i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let segments: Vec<_> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "hadbj"))
        .collect();
    assert!(segments.len() > 1, "Should have multiple segments from rotation");

    for seg in &segments {
        let hdr = format::read_header_from_path(seg).unwrap();
        if hdr.is_sealed() {
            assert!(hdr.has_chain_hash(), "Sealed segment {} should have chain hash trailer", seg.display());
        }
    }

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    let expected_seq = state.sequence.load(std::sync::atomic::Ordering::SeqCst);
    let expected_hash = *state.chain_hash.lock().unwrap();
    assert_eq!(seq, expected_seq);
    assert_eq!(hash, expected_hash);
}

/// Empty journal dir with no segments and no recovery.json returns (0, [0;32]).
#[test]
fn test_recovery_empty_journal_dir_no_cache() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 0);
    assert_eq!(hash, [0u8; 32]);
}

/// Shutdown seal also writes chain hash trailer.
#[test]
fn test_chain_hash_trailer_on_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 50, state.clone());

    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{id: {}}})", i),
            params: vec![],
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
    assert_eq!(segments.len(), 1);

    let hdr = format::read_header_from_path(&segments[0]).unwrap();
    assert!(hdr.is_sealed());
    assert!(hdr.has_chain_hash(), "Shutdown seal should have chain hash trailer");

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 3);
    assert_ne!(hash, [0u8; 32]);
}

/// Regression test: recovery.json without any journal segments should be trusted.
#[test]
fn test_recovery_json_no_segments_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    let fake_hash = [42u8; 32];
    graphstream::write_recovery_state(&journal_dir, 10000, fake_hash).unwrap();
    assert!(journal_dir.join("recovery.json").exists());

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 10000, "Should use cached seq from recovery.json");
    assert_eq!(hash, fake_hash, "Should use cached hash from recovery.json");
}

/// Corrupt recovery.json is ignored when segments with chain hash trailers exist.
#[test]
fn test_recovery_ignores_corrupt_recovery_json_with_trailer() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 50, state.clone());

    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let expected_seq = state.sequence.load(std::sync::atomic::Ordering::SeqCst);
    let expected_hash = *state.chain_hash.lock().unwrap();

    std::fs::write(journal_dir.join("recovery.json"), b"not valid json").unwrap();

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, expected_seq);
    assert_eq!(hash, expected_hash);
}
