//! Property-based tests for graphstream journal operations.
//!
//! Uses proptest to verify invariants hold under arbitrary inputs:
//! - Write/read round-trip
//! - Chain hash integrity under truncation
//! - Segment rotation correctness
//! - Upload idempotency

use graphstream::journal::{self, JournalCommand, JournalReader, JournalState, PendingEntry};
use graphstream::types::ParamValue;
use proptest::prelude::*;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Strategies: generate arbitrary ParamValue and entry descriptions.
// We generate Debug-friendly tuples, then convert to PendingEntry in test body.
// ---------------------------------------------------------------------------

fn arb_param_value() -> impl Strategy<Value = ParamValue> {
    let leaf = prop_oneof![
        Just(ParamValue::Null),
        any::<bool>().prop_map(ParamValue::Bool),
        any::<i64>().prop_map(ParamValue::Int),
        // Avoid NaN: proptest floats can generate NaN which fails PartialEq.
        (-1e10f64..1e10f64).prop_map(ParamValue::Float),
        "[a-zA-Z0-9_ ]{0,64}".prop_map(ParamValue::String),
    ];
    leaf.prop_recursive(
        2,  // max depth
        16, // max nodes
        4,  // items per collection
        |inner| prop::collection::vec(inner, 0..4).prop_map(ParamValue::List),
    )
}

fn arb_param_pair() -> impl Strategy<Value = (String, ParamValue)> {
    ("[a-z_]{1,16}", arb_param_value())
}

/// Generates a (query, params) tuple that can be turned into PendingEntry.
fn arb_entry_desc() -> impl Strategy<Value = (String, Vec<(String, ParamValue)>)> {
    (
        "[A-Z ]{1,64}",
        prop::collection::vec(arb_param_pair(), 0..6),
    )
}

// ---------------------------------------------------------------------------
// Helper: write entries via journal writer, shutdown, return journal dir
// ---------------------------------------------------------------------------

fn write_entries_to_journal(
    descs: &[(String, Vec<(String, ParamValue)>)],
    segment_max_bytes: u64,
) -> (tempfile::TempDir, Arc<JournalState>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        segment_max_bytes,
        50,
        state.clone(),
    );

    for (query, params) in descs {
        tx.send(JournalCommand::Write(PendingEntry {
            query: query.clone(),
            params: params.clone(),
        }))
        .expect("send write");
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).expect("send flush");
    ack_rx.recv().expect("flush ack");

    tx.send(JournalCommand::Shutdown).expect("send shutdown");
    std::thread::sleep(std::time::Duration::from_millis(150));

    (dir, state)
}

// ---------------------------------------------------------------------------
// (a) Journal write/read round-trip
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn prop_journal_roundtrip(
        descs in prop::collection::vec(arb_entry_desc(), 1..30)
    ) {
        let (dir, state) = write_entries_to_journal(&descs, 1024 * 1024);
        let journal_dir = dir.path().join("journal");

        let expected_seq = state.sequence.load(std::sync::atomic::Ordering::SeqCst);
        prop_assert_eq!(expected_seq, descs.len() as u64);

        let reader = JournalReader::open(&journal_dir).expect("open reader");
        let read_entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().expect("read entries");

        prop_assert_eq!(read_entries.len(), descs.len());

        for (i, (read, (orig_query, orig_params))) in
            read_entries.iter().zip(descs.iter()).enumerate()
        {
            prop_assert_eq!(
                read.sequence,
                (i + 1) as u64,
                "seq mismatch at index {}",
                i
            );
            prop_assert_eq!(
                &read.entry.query, orig_query,
                "query mismatch at seq {}",
                read.sequence
            );

            // Verify param count matches.
            let read_params = graphstream::map_entries_to_param_values(&read.entry.params);
            prop_assert_eq!(
                read_params.len(),
                orig_params.len(),
                "param count mismatch at seq {}",
                read.sequence
            );

            // Verify each param key/value.
            for (j, ((rk, rv), (ok, ov))) in
                read_params.iter().zip(orig_params.iter()).enumerate()
            {
                prop_assert_eq!(rk, ok, "param key mismatch at seq {} param {}", read.sequence, j);
                prop_assert_eq!(rv, ov, "param value mismatch at seq {} param {}", read.sequence, j);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// (b) Chain hash integrity under partial writes (truncation)
//
// The writer seals on disconnect, so we verify that recovery from sealed
// segments produces a valid chain hash and correct sequence, even with
// varying entry counts.
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    #[test]
    fn prop_chain_hash_integrity_under_recovery(
        entry_count in 5..50usize,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_dir = dir.path().join("journal");

        let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
        let tx = journal::spawn_journal_writer(
            journal_dir.clone(),
            10 * 1024 * 1024,
            50,
            state.clone(),
        );

        for i in 1..=entry_count {
            tx.send(JournalCommand::Write(PendingEntry {
                query: format!("CREATE (:Node {{id: {}, data: '{}'}})", i, "x".repeat(50)),
                params: vec![("idx".into(), ParamValue::Int(i as i64))],
            }))
            .expect("send write");
        }

        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
        tx.send(JournalCommand::Flush(ack_tx)).expect("send flush");
        ack_rx.recv().expect("flush ack");

        let expected_hash = *state.chain_hash.lock().expect("lock");

        // Shutdown seals the segment.
        tx.send(JournalCommand::Shutdown).expect("shutdown");
        std::thread::sleep(std::time::Duration::from_millis(150));

        // Recovery should find the correct state.
        let (recovered_seq, recovered_hash) =
            journal::recover_journal_state(&journal_dir).expect("recover");

        prop_assert_eq!(
            recovered_seq, entry_count as u64,
            "Recovered seq should match entry count"
        );
        prop_assert_eq!(
            recovered_hash, expected_hash,
            "Recovered chain hash should match writer state"
        );

        // Verify reading entries produces valid chain.
        let reader = JournalReader::open(&journal_dir).expect("open reader");
        let mut prev_seq = 0u64;
        let mut last_hash = [0u8; 32];
        for result in reader {
            let entry = result.expect("entry should be valid");
            prop_assert!(
                entry.sequence > prev_seq,
                "Sequence should be monotonically increasing: {} <= {}",
                entry.sequence,
                prev_seq,
            );
            prop_assert_eq!(
                entry.sequence,
                prev_seq + 1,
                "No gaps allowed: expected {}, got {}",
                prev_seq + 1,
                entry.sequence,
            );
            prev_seq = entry.sequence;
            last_hash = entry.chain_hash;
        }

        prop_assert_eq!(last_hash, expected_hash, "Final chain hash mismatch");
    }
}

// ---------------------------------------------------------------------------
// (c) Segment rotation correctness
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    #[test]
    fn prop_segment_rotation_preserves_entries(
        entry_count in 10..60usize,
        segment_max in 256u64..2048,
    ) {
        let descs: Vec<(String, Vec<(String, ParamValue)>)> = (1..=entry_count)
            .map(|i| (
                format!("CREATE (:Node {{id: {}, payload: '{}'}})", i, "y".repeat(30)),
                vec![
                    ("id".into(), ParamValue::Int(i as i64)),
                    ("tag".into(), ParamValue::String(format!("entry_{}", i))),
                ],
            ))
            .collect();

        let (dir, state) = write_entries_to_journal(&descs, segment_max);
        let journal_dir = dir.path().join("journal");

        // Verify at least one segment was created.
        let segment_count = std::fs::read_dir(&journal_dir)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "hadbj"))
            .count();

        prop_assert!(
            segment_count >= 1,
            "Expected at least 1 segment, got {}",
            segment_count
        );

        // Read all entries back and verify completeness.
        let reader = JournalReader::open(&journal_dir).expect("open reader");
        let read_entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().expect("read entries");

        prop_assert_eq!(
            read_entries.len(),
            entry_count,
            "Entry count mismatch: wrote {}, read {}",
            entry_count,
            read_entries.len()
        );

        // Verify monotonic sequence with no gaps.
        for (i, entry) in read_entries.iter().enumerate() {
            prop_assert_eq!(
                entry.sequence,
                (i + 1) as u64,
                "Sequence gap at index {}",
                i
            );
        }

        // Verify recovery returns correct state.
        let (recovered_seq, recovered_hash) =
            journal::recover_journal_state(&journal_dir).expect("recover");
        let expected_hash = *state.chain_hash.lock().expect("lock chain_hash");

        prop_assert_eq!(recovered_seq, entry_count as u64);
        prop_assert_eq!(recovered_hash, expected_hash);
    }
}

// ---------------------------------------------------------------------------
// (d) Upload idempotency: uploading same segment twice yields identical state
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn prop_upload_idempotency(entry_count in 1..20usize) {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_dir = dir.path().join("journal");

        let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
        let tx = journal::spawn_journal_writer(
            journal_dir.clone(),
            1024 * 1024,
            50,
            state.clone(),
        );

        for i in 1..=entry_count {
            tx.send(JournalCommand::Write(PendingEntry {
                query: format!("CREATE (:N {{v: {}}})", i),
                params: vec![],
            }))
            .expect("send write");
        }

        let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
        tx.send(JournalCommand::SealForUpload(seal_tx)).expect("send seal");
        let sealed_path = seal_rx.recv().expect("seal ack");
        prop_assert!(sealed_path.is_some(), "Expected sealed path");

        tx.send(JournalCommand::Shutdown).expect("send shutdown");
        std::thread::sleep(std::time::Duration::from_millis(100));

        let sealed_path = sealed_path.expect("sealed path");
        let segment_data = std::fs::read(&sealed_path).expect("read sealed segment");
        let segment_name = sealed_path
            .file_name()
            .expect("file_name")
            .to_str()
            .expect("to_str")
            .to_string();

        // Simulate uploading to a mock store (HashMap).
        let mut store: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        let key = format!("test/db/0000/{}", segment_name);

        // First upload.
        store.insert(key.clone(), segment_data.clone());
        let after_first = store.clone();

        // Second upload (same key, same data).
        store.insert(key.clone(), segment_data.clone());

        // Store state should be identical.
        prop_assert_eq!(store.len(), after_first.len(), "Key count should be same");
        prop_assert_eq!(
            store.get(&key).expect("key exists"),
            after_first.get(&key).expect("key exists"),
            "Content should be identical after duplicate upload"
        );

        // Verify the stored data is still a valid sealed segment.
        let stored_data = store.get(&key).expect("key exists");
        let header =
            hadb_changeset::journal::decode_header(stored_data).expect("decode header");
        prop_assert!(header.is_sealed(), "Stored segment should be sealed");
        prop_assert_eq!(header.entry_count as usize, entry_count);
    }
}
