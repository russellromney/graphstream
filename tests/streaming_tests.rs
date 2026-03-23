//! Streaming tests: streaming zstd reading and compaction.

use graphstream::journal::{
    self, JournalCommand, JournalReader, JournalState, PendingEntry,
};
use graphstream::types::ParamValue;
use graphstream::graphj;
use std::sync::Arc;

/// Streaming zstd: write 1000 entries, seal (compresses), read back via streaming decoder.
/// Verifies the streaming path produces identical results to the previous full-body decode.
#[test]
fn test_streaming_zstd_read_1000_entries() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        10 * 1024 * 1024, // 10MB — all entries fit in one segment
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

    // Seal to create a compressed .graphj segment.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    let sealed_path = ack_rx.recv().unwrap();
    assert!(sealed_path.is_some(), "Seal should produce a path");

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Verify the sealed segment is indeed compressed.
    let sealed = sealed_path.unwrap();
    let mut f = std::fs::File::open(&sealed).unwrap();
    let hdr = graphj::read_header(&mut f).unwrap().unwrap();
    assert!(hdr.is_sealed(), "Segment should be sealed");
    assert!(hdr.is_compressed(), "Segment should be compressed");

    // Read back all entries via JournalReader (streaming zstd path).
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1000, "Should read all 1000 entries");

    // Verify first and last entries.
    assert_eq!(entries[0].sequence, 1);
    assert_eq!(entries[999].sequence, 1000);

    // Verify chain hash matches state.
    let expected_hash = *state.chain_hash.lock().unwrap();
    assert_eq!(entries[999].chain_hash, expected_hash);

    // Verify recovery also works with streaming path.
    let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 1000);
    assert_eq!(hash, expected_hash);
}

/// Streaming zstd with from_sequence: skip first 500 entries, read remaining 500.
#[test]
fn test_streaming_zstd_from_sequence() {
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

    // Read from sequence 501 — should only return entries 501..=1000.
    let reader = JournalReader::from_sequence(&journal_dir, 501, [0u8; 32]).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 500, "Should read 500 entries (501..=1000)");
    assert_eq!(entries[0].sequence, 501);
    assert_eq!(entries[499].sequence, 1000);
}

/// Streaming compaction: compact 3 segments into 1, verify all entries readable.
#[test]
fn test_compact_streaming_matches_compact() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        4096, // small segments to trigger rotation
        100,
        state.clone(),
    );

    // Write 100 entries — should span multiple segments.
    for i in 1..=100 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}}})", i),
            params: vec![("id".into(), ParamValue::Int(i))],
        }))
        .unwrap();
    }

    // Flush and seal all.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Collect sealed segments.
    let mut sealed: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "graphj"))
        .collect();
    sealed.sort();
    assert!(sealed.len() >= 2, "Should have multiple segments, got {}", sealed.len());

    // Compact via streaming.
    let streaming_output = dir.path().join("compacted-streaming.graphj");
    let streaming_hdr = graphj::compact_streaming(
        &sealed,
        &streaming_output,
        true,
        3,
        None,
    )
    .unwrap();

    // Compact via original.
    let original_output = dir.path().join("compacted-original.graphj");
    let original_hdr = graphj::compact(
        &sealed,
        &original_output,
        true,
        3,
        None,
    )
    .unwrap();

    // Both should have same entry count and sequence range.
    assert_eq!(streaming_hdr.entry_count, original_hdr.entry_count);
    assert_eq!(streaming_hdr.first_seq, original_hdr.first_seq);
    assert_eq!(streaming_hdr.last_seq, original_hdr.last_seq);
    assert_eq!(streaming_hdr.entry_count, 100);

    // Read entries from streaming output and verify all 100 present.
    let compact_dir = dir.path().join("compact_read");
    std::fs::create_dir_all(&compact_dir).unwrap();
    std::fs::copy(&streaming_output, compact_dir.join("journal-00000000001.graphj")).unwrap();
    let reader = JournalReader::open(&compact_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 100);
    assert_eq!(entries[0].sequence, 1);
    assert_eq!(entries[99].sequence, 100);
}

/// Streaming compaction with many segments.
#[test]
fn test_compact_streaming_many_segments() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        2048, // very small — lots of segments
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

    let mut sealed: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "graphj"))
        .collect();
    sealed.sort();
    assert!(sealed.len() >= 5, "Should have many segments, got {}", sealed.len());

    let output = dir.path().join("compacted.graphj");
    let hdr = graphj::compact_streaming(&sealed, &output, true, 3, None).unwrap();
    assert_eq!(hdr.entry_count, 200);
    assert_eq!(hdr.first_seq, 1);
    assert_eq!(hdr.last_seq, 200);
}
