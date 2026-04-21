//! End-to-end tests for graphstream S3 integration.
//!
//! These tests require S3 credentials and a test bucket.
//! Run with: soup run -p hadb -e development -- cargo test --test e2e -- --ignored

use graphstream::format;
use graphstream::journal::{self, JournalCommand, JournalReader, JournalState, PendingEntry};
use graphstream::sync::download_new_segments;
use graphstream::types::ParamValue;
use hadb_changeset::storage::{format_key, ChangesetKind, GENERATION_INCREMENTAL};
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use std::sync::Arc;
use std::time::Duration;

/// Create an `S3Storage` from `S3_TEST_BUCKET` (+ optional `S3_ENDPOINT`)
/// and a unique key prefix for this test run.
async fn s3_setup() -> (Arc<S3Storage>, String) {
    let bucket =
        std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET env var required for e2e tests");
    let endpoint = std::env::var("S3_ENDPOINT").ok();
    let s3_config = match endpoint.as_deref() {
        Some(ep) => {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .endpoint_url(ep)
                .load()
                .await
        }
        None => {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .load()
                .await
        }
    };
    let client = aws_sdk_s3::Client::new(&s3_config);
    let backend = S3Storage::new(client, bucket);
    let prefix = format!("graphstream-e2e-{}/", uuid::Uuid::new_v4());
    (Arc::new(backend), prefix)
}

/// Write N entries via journal writer, seal, and return the journal dir.
fn write_and_seal(
    journal_dir: &std::path::Path,
    n: usize,
    segment_max: u64,
) -> Arc<JournalState> {
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.to_path_buf(),
        segment_max,
        50,
        state.clone(),
    );

    for i in 1..=n {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    let sealed = seal_rx.recv().unwrap();
    assert!(sealed.is_some(), "Expected segment to be sealed");

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    state
}

/// Upload all sealed .hadbj files using hadb-changeset key format.
async fn upload_sealed_files(
    store: &dyn StorageBackend,
    journal_dir: &std::path::Path,
    prefix: &str,
    db_name: &str,
) -> Vec<String> {
    let mut uploaded = Vec::new();
    for entry in std::fs::read_dir(journal_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if !name.ends_with(".hadbj") {
            continue;
        }
        if !format::is_file_sealed(&path).unwrap_or(false) {
            continue;
        }

        // Parse seq from filename: journal-{seq:016}.hadbj
        let seq_str = name.strip_prefix("journal-").unwrap().strip_suffix(".hadbj").unwrap();
        let seq: u64 = seq_str.parse().unwrap();

        let key = format_key(prefix, db_name, GENERATION_INCREMENTAL, seq, ChangesetKind::Journal);
        let bytes = std::fs::read(&path).unwrap();
        store.put(&key, &bytes).await.unwrap();
        uploaded.push(key);
    }
    uploaded
}

// ============================================================================
// Tests
// ============================================================================

/// Upload sealed segments manually, then download via download_new_segments.
#[tokio::test]
#[ignore]
async fn test_upload_then_download_roundtrip() {
    let (store, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    write_and_seal(&journal_dir, 10, 1024 * 1024);

    let uploaded = upload_sealed_files(&*store, &journal_dir, &prefix, "testdb").await;
    assert!(!uploaded.is_empty(), "Expected at least one uploaded segment");

    let download_dir = dir.path().join("downloaded");
    let segments =
        download_new_segments(&*store, &prefix, "testdb", &download_dir, 0)
            .await
            .unwrap();
    assert!(!segments.is_empty(), "Expected downloaded segments");

    let reader = JournalReader::open(&download_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 10);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence, (i + 1) as u64);
        assert!(entry.entry.query.contains(&format!("id: {}", i + 1)));
    }
}

/// Test multiple segments with download from a middle sequence.
#[tokio::test]
#[ignore]
async fn test_sync_from_middle_sequence() {
    let (store, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        200,
        50,
        state.clone(),
    );

    for i in 1..=15 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    seal_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    let uploaded = upload_sealed_files(&*store, &journal_dir, &prefix, "testdb").await;
    assert!(
        uploaded.len() >= 2,
        "Expected multiple segments, got {}",
        uploaded.len()
    );

    // Download all from seq 0.
    let full_dir = dir.path().join("full_download");
    let all_segments =
        download_new_segments(&*store, &prefix, "testdb", &full_dir, 0)
            .await
            .unwrap();
    assert_eq!(all_segments.len(), uploaded.len());

    let reader = JournalReader::open(&full_dir).unwrap();
    let all_entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(all_entries.len(), 15);

    // Download from sequence 5.
    let partial_dir = dir.path().join("partial_download");
    let segments =
        download_new_segments(&*store, &prefix, "testdb", &partial_dir, 5)
            .await
            .unwrap();
    assert!(!segments.is_empty(), "Expected segments after seq 5");
}

/// Test download_new_segments with empty prefix (no segments uploaded).
#[tokio::test]
#[ignore]
async fn test_sync_empty_prefix() {
    let (store, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();
    let download_dir = dir.path().join("downloaded");

    let segments =
        download_new_segments(&*store, &prefix, "testdb", &download_dir, 0)
            .await
            .unwrap();
    assert!(segments.is_empty());
}

/// Test that params survive the upload -> download -> read roundtrip.
#[tokio::test]
#[ignore]
async fn test_params_survive_s3_roundtrip() {
    let (store, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        1024 * 1024,
        50,
        state.clone(),
    );

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:Node {id: $id, name: $name, score: $score, active: $active})".into(),
        params: vec![
            ("id".into(), ParamValue::Int(42)),
            ("name".into(), ParamValue::String("test-node".into())),
            ("score".into(), ParamValue::Float(3.14)),
            ("active".into(), ParamValue::Bool(true)),
        ],
    }))
    .unwrap();

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:Node {tags: $tags})".into(),
        params: vec![(
            "tags".into(),
            ParamValue::List(vec![
                ParamValue::String("a".into()),
                ParamValue::String("b".into()),
            ]),
        )],
    }))
    .unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    seal_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    upload_sealed_files(&*store, &journal_dir, &prefix, "testdb").await;

    let download_dir = dir.path().join("downloaded");
    download_new_segments(&*store, &prefix, "testdb", &download_dir, 0)
        .await
        .unwrap();

    let reader = JournalReader::open(&download_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 2);

    let params1 = graphstream::map_entries_to_param_values(&entries[0].entry.params);
    assert_eq!(params1.len(), 4);

    let params2 = graphstream::map_entries_to_param_values(&entries[1].entry.params);
    assert_eq!(params2.len(), 1);
    let (key, val) = &params2[0];
    assert_eq!(key, "tags");
    match val {
        ParamValue::List(items) => assert_eq!(items.len(), 2),
        other => panic!("Expected List, got {:?}", other),
    }
}
