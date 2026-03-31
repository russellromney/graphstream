//! End-to-end tests for graphstream S3 integration.
//!
//! These tests require S3 credentials and a test bucket.
//! Run with: soup run -p hadb -e development -- cargo test --test e2e -- --ignored

use graphstream::graphj;
use graphstream::journal::{self, JournalCommand, JournalReader, JournalState, PendingEntry};
use graphstream::sync::download_new_segments;
use graphstream::types::ParamValue;
use hadb_io::{ObjectStore, S3Backend};
use std::sync::Arc;
use std::time::Duration;

/// Create S3Backend + unique prefix from env vars.
async fn s3_setup() -> (Arc<S3Backend>, String) {
    let bucket =
        std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET env var required for e2e tests");
    let endpoint = std::env::var("S3_ENDPOINT").ok();
    let backend = S3Backend::from_env(bucket, endpoint.as_deref())
        .await
        .expect("Failed to create S3Backend");
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

    // Flush.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Seal.
    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    let sealed = seal_rx.recv().unwrap();
    assert!(sealed.is_some(), "Expected segment to be sealed");

    // Shutdown writer.
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    state
}

/// Manually upload all sealed .graphj files from journal_dir via ObjectStore.
async fn upload_sealed_files(
    store: &dyn ObjectStore,
    journal_dir: &std::path::Path,
    prefix: &str,
) -> Vec<String> {
    let mut uploaded = Vec::new();
    for entry in std::fs::read_dir(journal_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if !name.ends_with(".graphj") {
            continue;
        }
        // Check if sealed.
        let mut f = std::fs::File::open(&path).unwrap();
        let header = graphj::read_header(&mut f).unwrap();
        if header.map_or(true, |h| !h.is_sealed()) {
            continue;
        }

        let key = format!("{}journal/{}", prefix, name);
        let bytes = std::fs::read(&path).unwrap();
        store.upload_bytes(&key, bytes).await.unwrap();
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

    // Write 10 entries and seal.
    write_and_seal(&journal_dir, 10, 1024 * 1024);

    // Upload sealed segments.
    let uploaded = upload_sealed_files(&*store, &journal_dir, &prefix).await;
    assert!(!uploaded.is_empty(), "Expected at least one uploaded segment");

    // Download to a different directory.
    let download_dir = dir.path().join("downloaded");
    let segments =
        download_new_segments(&*store, &prefix, &download_dir, 0)
            .await
            .unwrap();
    assert!(!segments.is_empty(), "Expected downloaded segments");

    // Read entries from downloaded segments.
    let reader = JournalReader::open(&download_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 10);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence, (i + 1) as u64);
        assert!(entry.entry.query.contains(&format!("id: {}", i + 1)));
    }
}

/// Test spawn_journal_uploader actually uploads sealed segments.
#[tokio::test]
#[ignore]
async fn test_uploader_uploads_to_s3() {
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

    // Write entries.
    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    // Flush to ensure entries are written.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Start uploader with 1s interval.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let _uploader = graphstream::spawn_journal_uploader(
        tx.clone(),
        journal_dir.clone(),
        store.clone(),
        prefix.clone(),
        Duration::from_secs(1),
        shutdown_rx,
    );

    // Wait for uploader to seal + upload (2 cycles should be enough).
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Shutdown.
    let _ = shutdown_tx.send(true);
    tx.send(JournalCommand::Shutdown).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify segments exist on S3.
    let journal_prefix = format!("{}journal/", prefix);
    let keys = store.list_objects(&journal_prefix).await.unwrap();
    assert!(
        !keys.is_empty(),
        "Expected uploaded segments on S3, got none. prefix={}",
        journal_prefix
    );

    // Download and verify entries.
    let download_dir = dir.path().join("downloaded");
    let segments =
        download_new_segments(&*store, &prefix, &download_dir, 0)
            .await
            .unwrap();
    assert!(!segments.is_empty());

    let reader = JournalReader::open(&download_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 5);
}

/// Test multiple segments with download from a middle sequence (straddling).
#[tokio::test]
#[ignore]
async fn test_sync_straddling_segment() {
    let (store, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    // Write 3 segments with small max size to force rotation.
    // Each entry is ~50 bytes, segment max of 200 should give ~3-4 entries per segment.
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        200, // Tiny segments to force rotation
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

    // Flush and seal.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    seal_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // Upload all sealed segments.
    let uploaded = upload_sealed_files(&*store, &journal_dir, &prefix).await;
    assert!(
        uploaded.len() >= 2,
        "Expected multiple segments, got {}",
        uploaded.len()
    );

    // Download all segments first (from seq 0) to verify full chain.
    let full_dir = dir.path().join("full_download");
    let all_segments =
        download_new_segments(&*store, &prefix, &full_dir, 0)
            .await
            .unwrap();
    assert_eq!(all_segments.len(), uploaded.len());

    let reader = JournalReader::open(&full_dir).unwrap();
    let all_entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(all_entries.len(), 15);

    // Download from sequence 5 -- should include straddling segment.
    let partial_dir = dir.path().join("partial_download");
    let segments =
        download_new_segments(&*store, &prefix, &partial_dir, 5)
            .await
            .unwrap();
    assert!(
        !segments.is_empty(),
        "Expected segments after seq 5"
    );

    // Straddling segment means we get more segments than just those starting after 5.
    // The straddling segment's first_seq is <= 5 but it contains entries > 5.
    // Read using from_sequence to skip entries <= 5 (chain hash validation is skipped
    // by passing [0; 32] -- the reader only validates forward from the starting point).
    let reader = JournalReader::from_sequence(&partial_dir, 6, [0u8; 32]).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(!entries.is_empty());
    let max_seq = entries.last().unwrap().sequence;
    assert!(max_seq >= 10, "Expected entries up to at least seq 10, got {}", max_seq);
}

/// Test download_new_segments with empty prefix (no segments uploaded).
#[tokio::test]
#[ignore]
async fn test_sync_empty_prefix() {
    let (store, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();
    let download_dir = dir.path().join("downloaded");

    let segments =
        download_new_segments(&*store, &prefix, &download_dir, 0)
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

    // Write entries with various param types.
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

    // Flush and seal.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    seal_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // Upload and download.
    upload_sealed_files(&*store, &journal_dir, &prefix).await;

    let download_dir = dir.path().join("downloaded");
    download_new_segments(&*store, &prefix, &download_dir, 0)
        .await
        .unwrap();

    // Verify params roundtrip.
    let reader = JournalReader::open(&download_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 2);

    // Entry 1: id, name, score, active.
    let params1 = graphstream::map_entries_to_param_values(&entries[0].entry.params);
    assert_eq!(params1.len(), 4);

    // Entry 2: tags list.
    let params2 = graphstream::map_entries_to_param_values(&entries[1].entry.params);
    assert_eq!(params2.len(), 1);
    let (key, val) = &params2[0];
    assert_eq!(key, "tags");
    match val {
        ParamValue::List(items) => assert_eq!(items.len(), 2),
        other => panic!("Expected List, got {:?}", other),
    }
}
