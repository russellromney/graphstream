//! HADBJ format tests: header, seal, param values.

use graphstream::journal::{
    self, JournalCommand, JournalReader, JournalState, PendingEntry,
};
use graphstream::types::ParamValue;
use graphstream::format;
use std::sync::Arc;

/// Test from_sequence with sealed segments: entries are read correctly after seek.
#[test]
fn test_read_after_seal() {
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

    // Read from unsealed segment.
    let segment: std::path::PathBuf = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().map_or(false, |ext| ext == "hadbj") {
                Some(p)
            } else {
                None
            }
        })
        .next()
        .expect("Should have one segment");

    // Verify it's unsealed.
    let hdr = format::read_header_from_path(&segment).unwrap();
    assert!(!hdr.is_sealed());

    // Shutdown writer (which seals).
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Verify it's now sealed.
    let hdr = format::read_header_from_path(&segment).unwrap();
    assert!(hdr.is_sealed());
    assert!(hdr.is_compressed());
    assert_eq!(hdr.entry_count, 3);

    // Read entries back.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 3);
}

/// HADBJ header encode/decode round-trip via hadb-changeset.
#[test]
fn test_header_encode_decode_roundtrip() {
    use hadb_changeset::journal::{
        encode_header, decode_header, JournalHeader, FLAG_SEALED, FLAG_COMPRESSED,
    };

    let header = JournalHeader {
        flags: FLAG_SEALED | FLAG_COMPRESSED,
        compression: 1, // zstd
        first_seq: 100,
        last_seq: 200,
        entry_count: 101,
        body_len: 4096,
        body_checksum: [0xAB; 32],
        prev_segment_checksum: 0xDEADBEEF,
        created_ms: 9999999,
    };

    let encoded = encode_header(&header);
    assert_eq!(encoded.len(), 128);

    let decoded = decode_header(&encoded).unwrap();
    assert_eq!(decoded.flags, header.flags);
    assert_eq!(decoded.compression, header.compression);
    assert_eq!(decoded.first_seq, header.first_seq);
    assert_eq!(decoded.last_seq, header.last_seq);
    assert_eq!(decoded.entry_count, header.entry_count);
    assert_eq!(decoded.body_len, header.body_len);
    assert_eq!(decoded.body_checksum, header.body_checksum);
    assert_eq!(decoded.prev_segment_checksum, header.prev_segment_checksum);
    assert_eq!(decoded.created_ms, header.created_ms);
}

/// Non-hadbj file returns error from read_header_from_path.
#[test]
fn test_read_header_wrong_magic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not-hadbj.hadbj");
    std::fs::write(&path, &[0u8; 128]).unwrap();
    let result = format::read_header_from_path(&path);
    assert!(result.is_err(), "Should return error for wrong magic");
}

/// ParamValue edge cases.
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
    assert_eq!(roundtripped[0].1, ParamValue::String("".into()));
    assert_eq!(roundtripped[1].1, ParamValue::Int(-9999));
    assert_eq!(roundtripped[2].1, ParamValue::Int(i64::MAX));
    assert_eq!(roundtripped[3].1, ParamValue::Int(i64::MIN));
    assert_eq!(roundtripped[7].1, ParamValue::List(vec![]));
    assert_eq!(roundtripped[9].1, ParamValue::Null);

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

/// is_file_sealed returns true for sealed, false for unsealed.
#[test]
fn test_is_file_sealed() {
    use hadb_changeset::journal::{encode_header, JournalHeader, FLAG_SEALED};

    let dir = tempfile::tempdir().unwrap();

    // Write unsealed segment.
    let unsealed_path = dir.path().join("unsealed.hadbj");
    let header = JournalHeader {
        flags: 0,
        compression: 0,
        first_seq: 1,
        last_seq: 0,
        entry_count: 0,
        body_len: 0,
        body_checksum: [0u8; 32],
        prev_segment_checksum: 0,
        created_ms: 0,
    };
    std::fs::write(&unsealed_path, &encode_header(&header)).unwrap();
    assert!(!format::is_file_sealed(&unsealed_path).unwrap());

    // Write sealed segment.
    let sealed_path = dir.path().join("sealed.hadbj");
    let header = JournalHeader {
        flags: FLAG_SEALED,
        compression: 0,
        first_seq: 1,
        last_seq: 1,
        entry_count: 1,
        body_len: 0,
        body_checksum: [0u8; 32],
        prev_segment_checksum: 0,
        created_ms: 0,
    };
    std::fs::write(&sealed_path, &encode_header(&header)).unwrap();
    assert!(format::is_file_sealed(&sealed_path).unwrap());
}
