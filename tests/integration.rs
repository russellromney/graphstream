//! Integration tests for graphstream: write entries, read back, seal, recover state.

use graphstream::journal::{
    self, JournalCommand, JournalReader, JournalState, PendingEntry,
};
use graphstream::types::ParamValue;
use graphstream::graphj;
use std::sync::Arc;

/// Write entries via journal writer, read them back via JournalReader.
#[test]
fn test_write_and_read_entries() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024, // 1MB segments
        100,
        state.clone(),
    );

    // Write 5 entries.
    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Person {{name: 'Person{}'}})", i),
            params: vec![("name".into(), ParamValue::String(format!("Person{}", i)))],
        }))
        .unwrap();
    }

    // Flush to ensure writes are durable.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Verify state updated.
    assert_eq!(state.sequence.load(std::sync::atomic::Ordering::SeqCst), 5);
    assert!(state.is_alive());

    // Shutdown the writer (seals the segment).
    tx.send(JournalCommand::Shutdown).unwrap();

    // Small wait for writer thread to finish.
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Read entries back via JournalReader.
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

    // Write entries.
    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("MERGE (n:Node {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    // Seal for upload.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let sealed_path = ack_rx.recv().unwrap();
    assert!(sealed_path.is_some(), "Expected sealed segment path");

    let sealed_path = sealed_path.unwrap();
    assert!(sealed_path.exists(), "Sealed file should exist");

    // Verify it's sealed + compressed by reading header.
    let mut f = std::fs::File::open(&sealed_path).unwrap();
    let header = graphj::read_header(&mut f).unwrap().unwrap();
    assert!(header.is_sealed());
    assert!(header.is_compressed());
    assert_eq!(header.entry_count, 3);
    assert_eq!(header.first_seq, 1);
    assert_eq!(header.last_seq, 3);

    // Shutdown writer.
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Read entries back from sealed segment.
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

    // Write 10 entries.
    for i in 1..=10 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    // Get the chain hash before shutdown.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    let expected_hash = *state.chain_hash.lock().unwrap();

    // Shutdown (seals segment).
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Recover state from disk.
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
        512, // Very small segment: 512 bytes (header=128, so only ~384 for entries)
        100,
        state.clone(),
    );

    // Write enough entries to trigger rotation (each entry is ~100+ bytes).
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
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Count .graphj files — should be multiple due to small segment size.
    let segment_count = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .map_or(false, |ext| ext == "graphj")
        })
        .count();
    assert!(
        segment_count > 1,
        "Expected multiple segments due to rotation, got {}",
        segment_count
    );

    // Read all entries back — should still get all 10 across segments.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 10);

    // Chain is continuous.
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence, (i + 1) as u64);
    }
}

/// Test JournalReader::from_sequence skips earlier entries.
#[test]
fn test_reader_from_sequence() {
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
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Read from sequence 3 — should get entries 3, 4, 5.
    let reader = JournalReader::from_sequence(&journal_dir, 3, [0u8; 32]).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].sequence, 3);
    assert_eq!(entries[1].sequence, 4);
    assert_eq!(entries[2].sequence, 5);
}

/// Test graphj compact: merge multiple segments into one.
#[test]
fn test_graphj_compact() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        512, // Small segments to force rotation.
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

    // Collect all segment paths.
    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "graphj") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    segments.sort();

    assert!(segments.len() > 1, "Need multiple segments to test compact");

    // Compact into a single file.
    let output = dir.path().join("compacted.graphj");
    let header = graphj::compact(&segments, &output, true, 3, None).unwrap();

    assert!(header.is_sealed());
    assert!(header.is_compressed());
    assert_eq!(header.entry_count, 10);
    assert_eq!(header.first_seq, 1);
    assert_eq!(header.last_seq, 10);

    // Read compacted file entries.
    let mut f = std::fs::File::open(&output).unwrap();
    let hdr = graphj::read_header(&mut f).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    f.read_exact(&mut body).unwrap();
    let raw = graphj::decode_body(&hdr, &body, None).unwrap();

    // Verify we can parse entries from the decoded body.
    use std::io::Read;
    let mut cursor = std::io::Cursor::new(raw);
    let mut count = 0u64;
    loop {
        let mut hdr_bytes = [0u8; 48];
        match cursor.read_exact(&mut hdr_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("Read error: {e}"),
        }
        let payload_len =
            u32::from_le_bytes([hdr_bytes[4], hdr_bytes[5], hdr_bytes[6], hdr_bytes[7]]) as usize;
        let mut payload = vec![0u8; payload_len];
        cursor.read_exact(&mut payload).unwrap();
        count += 1;
    }
    assert_eq!(count, 10);
}

/// Test graphj seal_file on an unsealed segment.
#[test]
fn test_graphj_seal_file() {
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

    // Flush but don't shutdown (so segment remains unsealed).
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Find the unsealed segment.
    let segment: std::path::PathBuf = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "graphj") {
                Some(p)
            } else {
                None
            }
        })
        .next()
        .expect("Should have one segment");

    // Verify it's unsealed.
    {
        let mut f = std::fs::File::open(&segment).unwrap();
        let hdr = graphj::read_header(&mut f).unwrap().unwrap();
        assert!(!hdr.is_sealed());
    }

    // Seal it externally.
    let sealed_hdr = graphj::seal_file(&segment).unwrap();
    assert!(sealed_hdr.is_sealed());
    assert!(!sealed_hdr.is_compressed()); // seal_file doesn't compress
    assert_eq!(sealed_hdr.entry_count, 3);

    // Shutdown writer.
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
}

/// Writer is_alive() goes false after shutdown.
#[test]
fn test_writer_alive_after_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    assert!(state.is_alive());

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));

    assert!(!state.is_alive(), "Writer should be dead after shutdown");
}

/// Writer is_alive() goes false when sender is dropped (disconnect).
#[test]
fn test_writer_alive_after_disconnect() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    // Write an entry so the segment exists.
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:N {v: 1})".into(),
        params: vec![],
    }))
    .unwrap();
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Drop the sender — writer thread should detect disconnect and exit.
    drop(tx);
    std::thread::sleep(std::time::Duration::from_millis(300));

    assert!(!state.is_alive(), "Writer should be dead after sender drop");

    // Entry should still be readable (segment was sealed on disconnect).
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1);
}

/// SealForUpload with no writes returns None.
#[test]
fn test_seal_for_upload_no_writes() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    // SealForUpload with no writes should return None.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let sealed_path = ack_rx.recv().unwrap();
    assert!(sealed_path.is_none(), "No segment to seal when no writes");

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
}

/// Multiple SealForUpload calls: first seals, second returns None (no new writes).
#[test]
fn test_multiple_seal_for_upload() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    // Write entries.
    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    // First seal — returns the sealed path.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let first_seal = ack_rx.recv().unwrap();
    assert!(first_seal.is_some(), "First seal should return a path");

    // Second seal with no new writes — returns None.
    let (ack_tx2, ack_rx2) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx2)).unwrap();
    let second_seal = ack_rx2.recv().unwrap();
    assert!(second_seal.is_none(), "Second seal with no new writes returns None");

    // Write more, then seal again — should return a path.
    for i in 4..=6 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }
    let (ack_tx3, ack_rx3) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx3)).unwrap();
    let third_seal = ack_rx3.recv().unwrap();
    assert!(third_seal.is_some(), "Third seal after new writes should return a path");

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // All 6 entries should be readable across sealed segments.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 6);
}

/// Flush with no pending writes is a no-op.
#[test]
fn test_flush_no_pending_writes() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    // Flush with no writes — should not panic.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap(); // succeeds

    assert_eq!(state.sequence.load(std::sync::atomic::Ordering::SeqCst), 0);

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
}

/// Encrypted segment round-trip: write → compact with key → read with key.
#[test]
fn test_encrypted_segment_roundtrip() {
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
            query: format!("CREATE (:Secret {{id: {}}})", i),
            params: vec![("id".into(), ParamValue::Int(i))],
        }))
        .unwrap();
    }

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Collect sealed segments.
    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "graphj") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    segments.sort();

    // Compact with encryption.
    let key: [u8; 32] = [42u8; 32]; // test key
    let output = dir.path().join("encrypted.graphj");
    let header = graphj::compact(&segments, &output, true, 3, Some(&key)).unwrap();

    assert!(header.is_sealed());
    assert!(header.is_compressed());
    assert!(header.is_encrypted());
    assert_eq!(header.entry_count, 5);
    assert_eq!(header.encryption, graphj::ENCRYPTION_XCHACHA20POLY1305);

    // Read back with the correct key.
    let encrypted_dir = dir.path().join("encrypted_journal");
    std::fs::create_dir_all(&encrypted_dir).unwrap();
    std::fs::copy(&output, encrypted_dir.join("journal-0000000000000001.graphj")).unwrap();

    let reader = JournalReader::open_with_key(&encrypted_dir, Some(key)).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 5);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence, (i + 1) as u64);
        assert!(entry.entry.query.contains("Secret"));
    }
}

/// Encrypted segment with wrong key fails decryption (test decode_body directly).
#[test]
fn test_encrypted_segment_wrong_key() {
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
        query: "CREATE (:N {v: 1})".into(),
        params: vec![],
    }))
    .unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "graphj") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    segments.sort();

    // Compact with one key.
    let key: [u8; 32] = [42u8; 32];
    let output = dir.path().join("encrypted.graphj");
    graphj::compact(&segments, &output, true, 3, Some(&key)).unwrap();

    // Read header + body from the encrypted file.
    use std::io::Read;
    let mut f = std::fs::File::open(&output).unwrap();
    let header = graphj::read_header(&mut f).unwrap().unwrap();
    assert!(header.is_encrypted());
    let mut body = vec![0u8; header.body_len as usize];
    f.read_exact(&mut body).unwrap();

    // Correct key succeeds.
    assert!(graphj::decode_body(&header, &body, Some(&key)).is_ok());

    // Wrong key fails with decryption error.
    let wrong_key: [u8; 32] = [99u8; 32];
    let result = graphj::decode_body(&header, &body, Some(&wrong_key));
    assert!(result.is_err(), "Wrong key should cause decryption error");
    assert!(
        result.unwrap_err().contains("Decryption failed"),
        "Error should mention decryption"
    );
}

/// Compact with a single input file.
#[test]
fn test_compact_single_input() {
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

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "graphj") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    segments.sort();
    assert_eq!(segments.len(), 1);

    let output = dir.path().join("compacted.graphj");
    let header = graphj::compact(&segments, &output, true, 3, None).unwrap();
    assert!(header.is_sealed());
    assert!(header.is_compressed());
    assert_eq!(header.entry_count, 3);
    assert_eq!(header.first_seq, 1);
    assert_eq!(header.last_seq, 3);
}

/// Compact without compression.
#[test]
fn test_compact_uncompressed() {
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

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "graphj") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    segments.sort();

    let output = dir.path().join("compacted.graphj");
    let header = graphj::compact(&segments, &output, false, 0, None).unwrap();
    assert!(header.is_sealed());
    assert!(!header.is_compressed());
    assert!(!header.is_encrypted());
    assert_eq!(header.entry_count, 3);
}

/// Reader on empty directory returns no entries.
#[test]
fn test_reader_empty_dir() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 0);
}

/// Reader from_sequence on empty dir returns no entries.
#[test]
fn test_reader_from_sequence_empty_dir() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    let reader = JournalReader::from_sequence(&journal_dir, 5, [0u8; 32]).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 0);
}

/// ParamValue edge cases: empty string, negative ints, extreme floats, nested lists, empty lists.
#[test]
fn test_param_value_edge_cases() {
    use graphstream::types::{param_values_to_map_entries, map_entries_to_param_values};

    let params = vec![
        ("empty".into(), ParamValue::String("".into())),
        ("neg".into(), ParamValue::Int(-9999)),
        ("big_int".into(), ParamValue::Int(i64::MAX)),
        ("small_int".into(), ParamValue::Int(i64::MIN)),
        ("inf".into(), ParamValue::Float(f64::INFINITY)),
        ("neg_inf".into(), ParamValue::Float(f64::NEG_INFINITY)),
        ("zero".into(), ParamValue::Float(0.0)),
        ("empty_list".into(), ParamValue::List(vec![])),
        ("nested".into(), ParamValue::List(vec![
            ParamValue::List(vec![ParamValue::Int(1), ParamValue::Int(2)]),
            ParamValue::List(vec![ParamValue::String("a".into())]),
        ])),
        ("null".into(), ParamValue::Null),
    ];

    let entries = param_values_to_map_entries(&params);
    let roundtripped = map_entries_to_param_values(&entries);

    assert_eq!(roundtripped.len(), params.len());

    // Verify specific values.
    assert_eq!(roundtripped[0].1, ParamValue::String("".into()));
    assert_eq!(roundtripped[1].1, ParamValue::Int(-9999));
    assert_eq!(roundtripped[2].1, ParamValue::Int(i64::MAX));
    assert_eq!(roundtripped[3].1, ParamValue::Int(i64::MIN));
    assert_eq!(roundtripped[7].1, ParamValue::List(vec![]));
    assert_eq!(roundtripped[9].1, ParamValue::Null);

    // Nested list preserved.
    if let ParamValue::List(outer) = &roundtripped[8].1 {
        assert_eq!(outer.len(), 2);
        if let ParamValue::List(inner) = &outer[0] {
            assert_eq!(inner.len(), 2);
        } else {
            panic!("Expected nested list");
        }
    } else {
        panic!("Expected list");
    }
}

/// Large entries that exceed segment max bytes still work (one entry per segment).
#[test]
fn test_large_entry_exceeds_segment_size() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        256, // Very small: 256 bytes (header=128, so 128 for entries)
        100,
        state.clone(),
    );

    // Write a large entry (query string longer than 256 bytes).
    let large_query = format!("CREATE (:N {{data: '{}'}})", "X".repeat(500));
    tx.send(JournalCommand::Write(PendingEntry {
        query: large_query.clone(),
        params: vec![],
    }))
    .unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    assert_eq!(state.sequence.load(std::sync::atomic::Ordering::SeqCst), 1);

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Entry should still be readable.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].entry.query.contains(&"X".repeat(500)));
}

/// Seal an already-sealed file is a no-op.
#[test]
fn test_seal_already_sealed_file() {
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
        query: "CREATE (:N {v: 1})".into(),
        params: vec![],
    }))
    .unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let segment: std::path::PathBuf = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "graphj") {
                Some(p)
            } else {
                None
            }
        })
        .next()
        .unwrap();

    // Already sealed from shutdown. Seal again — should be no-op.
    let hdr1 = graphj::seal_file(&segment).unwrap();
    assert!(hdr1.is_sealed());
    let hdr2 = graphj::seal_file(&segment).unwrap();
    assert!(hdr2.is_sealed());
    assert_eq!(hdr1.entry_count, hdr2.entry_count);
    assert_eq!(hdr1.first_seq, hdr2.first_seq);
    assert_eq!(hdr1.last_seq, hdr2.last_seq);
}

/// Segment file naming uses zero-padded sequence numbers.
#[test]
fn test_segment_naming_format() {
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
        query: "CREATE (:N {v: 1})".into(),
        params: vec![],
    }))
    .unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let segment: std::path::PathBuf = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "graphj") {
                Some(p)
            } else {
                None
            }
        })
        .next()
        .unwrap();

    let filename = segment.file_name().unwrap().to_str().unwrap();
    assert!(
        filename.starts_with("journal-") && filename.ends_with(".graphj"),
        "Unexpected filename: {filename}"
    );
    // Verify 16-digit zero-padded format.
    let stem = filename
        .strip_prefix("journal-")
        .unwrap()
        .strip_suffix(".graphj")
        .unwrap();
    assert_eq!(stem.len(), 16, "Expected 16-digit zero-padded seq, got: {stem}");
    assert_eq!(stem, "0000000000000001");
}

/// Recovery from multiple segments across restarts.
#[test]
fn test_recover_across_multiple_segments() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    // Three separate writer sessions, each creating a segment.
    for session in 0..3 {
        let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
        let state = Arc::new(JournalState::with_sequence_and_hash(seq, hash));
        let tx = journal::spawn_journal_writer(
            journal_dir.clone(),
            1024 * 1024,
            100,
            state.clone(),
        );

        let start = session * 5 + 1;
        for i in start..start + 5 {
            tx.send(JournalCommand::Write(PendingEntry {
                query: format!("CREATE (:N {{v: {}}})", i),
                params: vec![],
            }))
            .unwrap();
        }

        tx.send(JournalCommand::Shutdown).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Recovery should find seq=15 across all segments.
    let (recovered_seq, _) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(recovered_seq, 15);

    // All 15 entries should be readable.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 15);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence, (i + 1) as u64);
    }
}

/// Entries preserve params through write → seal → read cycle.
#[test]
fn test_params_preserved_through_seal() {
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
        query: "CREATE (:N {name: $name, age: $age, active: $active})".into(),
        params: vec![
            ("name".into(), ParamValue::String("Alice".into())),
            ("age".into(), ParamValue::Int(30)),
            ("active".into(), ParamValue::Bool(true)),
        ],
    }))
    .unwrap();

    // Seal and verify.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1);

    // Verify params are preserved via protobuf round-trip.
    let entry = &entries[0];
    let params = graphstream::map_entries_to_param_values(&entry.entry.params);
    assert_eq!(params.len(), 3);
    assert_eq!(params[0], ("name".into(), ParamValue::String("Alice".into())));
    assert_eq!(params[1], ("age".into(), ParamValue::Int(30)));
    assert_eq!(params[2], ("active".into(), ParamValue::Bool(true)));
}

/// Reader from_sequence across multiple segments.
#[test]
fn test_reader_from_sequence_cross_segment() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        512, // Small segments to force rotation.
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

    // Read from sequence 7 — should get entries 7..10.
    let reader = JournalReader::from_sequence(&journal_dir, 7, [0u8; 32]).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0].sequence, 7);
    assert_eq!(entries[3].sequence, 10);
}

/// GraphjHeader new_unsealed creates correct defaults.
#[test]
fn test_header_new_unsealed() {
    let header = graphj::GraphjHeader::new_unsealed(42, 1234567890);
    assert!(!header.is_sealed());
    assert!(!header.is_compressed());
    assert!(!header.is_encrypted());
    assert_eq!(header.first_seq, 42);
    assert_eq!(header.last_seq, 0);
    assert_eq!(header.entry_count, 0);
    assert_eq!(header.body_len, 0);
    assert_eq!(header.created_ms, 1234567890);
}

/// Header encode/decode round-trip.
#[test]
fn test_header_encode_decode_roundtrip() {
    let header = graphj::GraphjHeader {
        flags: graphj::FLAG_SEALED | graphj::FLAG_COMPRESSED,
        compression: graphj::COMPRESSION_ZSTD,
        encryption: graphj::ENCRYPTION_NONE,
        zstd_level: 5,
        first_seq: 100,
        last_seq: 200,
        entry_count: 101,
        body_len: 4096,
        body_checksum: [0xAB; 32],
        nonce: [0xCD; 24],
        created_ms: 9999999,
    };

    let encoded = graphj::encode_header(&header);
    assert_eq!(encoded.len(), graphj::HEADER_SIZE);

    // Read back.
    let mut cursor = std::io::Cursor::new(&encoded[..]);
    let decoded = graphj::read_header(&mut cursor).unwrap().unwrap();

    assert_eq!(decoded.flags, header.flags);
    assert_eq!(decoded.compression, header.compression);
    assert_eq!(decoded.encryption, header.encryption);
    assert_eq!(decoded.zstd_level, header.zstd_level);
    assert_eq!(decoded.first_seq, header.first_seq);
    assert_eq!(decoded.last_seq, header.last_seq);
    assert_eq!(decoded.entry_count, header.entry_count);
    assert_eq!(decoded.body_len, header.body_len);
    assert_eq!(decoded.body_checksum, header.body_checksum);
    assert_eq!(decoded.nonce, header.nonce);
    assert_eq!(decoded.created_ms, header.created_ms);
}

/// Non-graphj file (wrong magic) returns None from read_header.
#[test]
fn test_read_header_wrong_magic() {
    let data = [0u8; 128]; // all zeros — wrong magic
    let mut cursor = std::io::Cursor::new(&data[..]);
    let result = graphj::read_header(&mut cursor).unwrap();
    assert!(result.is_none(), "Should return None for wrong magic");
}

/// Entries with all param types survive journal round-trip.
#[test]
fn test_all_param_types_journal_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        100,
        state.clone(),
    );

    let all_params = vec![
        ("null_val".into(), ParamValue::Null),
        ("bool_val".into(), ParamValue::Bool(false)),
        ("int_val".into(), ParamValue::Int(-42)),
        ("float_val".into(), ParamValue::Float(3.14159)),
        ("str_val".into(), ParamValue::String("hello world".into())),
        ("list_val".into(), ParamValue::List(vec![
            ParamValue::Int(1),
            ParamValue::String("two".into()),
            ParamValue::Null,
        ])),
    ];

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:N $params)".into(),
        params: all_params.clone(),
    }))
    .unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1);

    let roundtripped = graphstream::map_entries_to_param_values(&entries[0].entry.params);
    assert_eq!(roundtripped.len(), all_params.len());

    for (original, recovered) in all_params.iter().zip(roundtripped.iter()) {
        assert_eq!(original.0, recovered.0);
        // Float comparison via pattern match since NaN != NaN.
        match (&original.1, &recovered.1) {
            (ParamValue::Float(a), ParamValue::Float(b)) => {
                assert!((a - b).abs() < 1e-10, "Float mismatch: {a} vs {b}");
            }
            (a, b) => assert_eq!(a, b),
        }
    }
}

/// Test writing entries with chain hash continuity after recovery.
#[test]
fn test_chain_continuity_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    // First writer session: write 5 entries.
    {
        let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
        let tx = journal::spawn_journal_writer(
            journal_dir.clone(),
            1024 * 1024,
            100,
            state.clone(),
        );

        for i in 1..=5 {
            tx.send(JournalCommand::Write(PendingEntry {
                query: format!("CREATE (:A {{v: {}}})", i),
                params: vec![],
            }))
            .unwrap();
        }

        tx.send(JournalCommand::Shutdown).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Recover state.
    let (recovered_seq, recovered_hash) =
        journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(recovered_seq, 5);

    // Second writer session: continue from recovered state.
    {
        let state =
            Arc::new(JournalState::with_sequence_and_hash(recovered_seq, recovered_hash));
        let tx = journal::spawn_journal_writer(
            journal_dir.clone(),
            1024 * 1024,
            100,
            state.clone(),
        );

        for i in 6..=10 {
            tx.send(JournalCommand::Write(PendingEntry {
                query: format!("CREATE (:B {{v: {}}})", i),
                params: vec![],
            }))
            .unwrap();
        }

        tx.send(JournalCommand::Shutdown).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Read all entries — chain should be continuous across both sessions.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 10);

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence, (i + 1) as u64);
    }
}
