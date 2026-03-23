//! GraphJ format tests: header, seal_file, compact, param values.

use graphstream::journal::{
    self, JournalCommand, JournalReader, JournalState, PendingEntry,
};
use graphstream::types::ParamValue;
use graphstream::graphj;
use std::sync::Arc;

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
