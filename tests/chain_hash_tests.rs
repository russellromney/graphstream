//! Chain hash tests: chain hash trailer writing and recovery.

use graphstream::journal::{
    self, JournalCommand, JournalState, PendingEntry,
};
use graphstream::graphj;
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

    // Read header and verify chain hash flag + trailer.
    let mut f = std::fs::File::open(&sealed_path).unwrap();
    let header = graphj::read_header(&mut f).unwrap().unwrap();
    assert!(header.is_sealed());
    assert!(header.has_chain_hash(), "FLAG_HAS_CHAIN_HASH should be set");
    assert_eq!(header.last_seq, 5);

    let trailer_hash = graphj::read_chain_hash_trailer(&mut f, &header).unwrap();
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

/// Sealed segments without chain hash trailer (v1 format) fall back to full scan.
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
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&sealed).unwrap();
        let header = graphj::read_header(&mut f).unwrap().unwrap();
        assert!(header.has_chain_hash());

        // Clear the FLAG_HAS_CHAIN_HASH bit in the flags byte (offset 8).
        let new_flags = header.flags & !graphj::FLAG_HAS_CHAIN_HASH;
        use std::io::{Seek, SeekFrom, Write};
        f.seek(SeekFrom::Start(8)).unwrap();
        f.write_all(&[new_flags]).unwrap();

        // Truncate off the 32-byte trailer.
        let new_len = graphj::HEADER_SIZE as u64 + header.body_len;
        f.set_len(new_len).unwrap();
        f.sync_all().unwrap();
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
    // Small segment (256 bytes) to force rotation.
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

    // All sealed segments should have chain hash trailers.
    let segments: Vec<_> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "graphj"))
        .collect();
    assert!(segments.len() > 1, "Should have multiple segments from rotation");

    for seg in &segments {
        let mut f = std::fs::File::open(seg).unwrap();
        let hdr = graphj::read_header(&mut f).unwrap().unwrap();
        if hdr.is_sealed() {
            assert!(hdr.has_chain_hash(), "Sealed segment {} should have chain hash trailer", seg.display());
        }
    }

    // Recovery should use the last sealed segment's trailer.
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

    // Find the sealed segment written on shutdown.
    let segments: Vec<_> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "graphj"))
        .collect();
    assert_eq!(segments.len(), 1);

    let mut f = std::fs::File::open(&segments[0]).unwrap();
    let hdr = graphj::read_header(&mut f).unwrap().unwrap();
    assert!(hdr.is_sealed());
    assert!(hdr.has_chain_hash(), "Shutdown seal should have chain hash trailer");

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 3);
    assert_ne!(hash, [0u8; 32]);
}

/// Regression test: recovery.json without any journal segments should be trusted.
/// This happens during snapshot bootstrap: the database is restored from a snapshot,
/// recovery.json is written with the snapshot's seq/hash, but no .graphj segments exist.
#[test]
fn test_recovery_json_no_segments_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    // Simulate snapshot bootstrap: write recovery.json with a known state,
    // but don't create any journal segment files.
    let fake_hash = [42u8; 32];
    graphstream::write_recovery_state(&journal_dir, 10000, fake_hash).unwrap();

    // recovery.json should exist.
    assert!(journal_dir.join("recovery.json").exists());

    // Recover should trust the cached state since no segments contradict it.
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

    // Write garbage recovery.json — should be irrelevant since trailer is authoritative.
    std::fs::write(journal_dir.join("recovery.json"), b"not valid json").unwrap();

    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, expected_seq);
    assert_eq!(hash, expected_hash);
}
