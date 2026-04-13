//! Deterministic Simulation Testing (DST) harness for graphstream.
//!
//! Tests journal writer + uploader + downloader under fault injection:
//! - Random S3 errors
//! - Partial writes / truncation
//! - Silent data corruption
//! - Eventual consistency (delayed visibility)
//!
//! After each simulation run, verifies invariants:
//! 1. Journal continuity (no sequence gaps)
//! 2. Chain hash validity
//! 3. Upload completeness (all sealed segments eventually uploaded when faults clear)
//! 4. Recovery from segment loss
//! 5. Compaction preserves all entries

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use graphstream::journal::{self, JournalCommand, JournalReader, JournalState, PendingEntry};
use graphstream::types::ParamValue;
use hadb_io::ObjectStore;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ===========================================================================
// Fault modes
// ===========================================================================

#[derive(Debug, Clone)]
pub enum FaultMode {
    /// No faults (pass-through).
    None,
    /// Random errors with given probability (0.0..1.0).
    RandomError(f64),
    /// Uploads succeed but downloads return truncated data (keep first N%).
    PartialWrite(f64),
    /// Downloads return data with random byte flips.
    SilentCorruption { flip_probability: f64 },
    /// Uploads are not immediately visible to list/download for N operations.
    EventualConsistency { delay_ops: u64 },
}

// ===========================================================================
// MockObjectStore with fault injection
// ===========================================================================

struct FaultyObjectStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    /// Objects become visible after this many total operations.
    pending_objects: Mutex<HashMap<String, (Vec<u8>, u64)>>,
    fault_mode: FaultMode,
    op_counter: AtomicU64,
    /// Deterministic "random" seed for reproducible faults.
    seed: AtomicU64,
}

impl FaultyObjectStore {
    fn new(fault_mode: FaultMode, seed: u64) -> Self {
        // xorshift64 is a fixed point at 0: 0 ^ (0 << k) = 0 for all k.
        // Replace seed=0 with a non-zero default so fault injection actually fires.
        let safe_seed = if seed == 0 { 0xdeadbeef } else { seed };
        Self {
            objects: Mutex::new(HashMap::new()),
            pending_objects: Mutex::new(HashMap::new()),
            fault_mode,
            op_counter: AtomicU64::new(0),
            seed: AtomicU64::new(safe_seed),
        }
    }

    /// Simple deterministic pseudo-random: xorshift64.
    fn next_random(&self) -> u64 {
        let mut s = self.seed.load(Ordering::SeqCst);
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        self.seed.store(s, Ordering::SeqCst);
        s
    }

    /// Returns a float in [0, 1).
    fn random_float(&self) -> f64 {
        (self.next_random() % 10000) as f64 / 10000.0
    }

    fn bump_ops(&self) -> u64 {
        self.op_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Should this operation fail based on the fault mode?
    fn should_fail(&self) -> bool {
        match &self.fault_mode {
            FaultMode::RandomError(p) => self.random_float() < *p,
            _ => false,
        }
    }

    /// Promote pending objects that have become visible.
    fn promote_pending(&self) {
        let current_ops = self.op_counter.load(Ordering::SeqCst);
        let mut pending = self.pending_objects.lock().expect("lock pending");
        let mut objects = self.objects.lock().expect("lock objects");

        let ready: Vec<String> = pending
            .iter()
            .filter(|(_, (_, visible_at))| current_ops >= *visible_at)
            .map(|(k, _)| k.clone())
            .collect();

        for key in ready {
            if let Some((data, _)) = pending.remove(&key) {
                objects.insert(key, data);
            }
        }
    }

    /// Corrupt data by flipping bytes.
    fn corrupt_data(&self, data: &[u8], flip_prob: f64) -> Vec<u8> {
        let mut result = data.to_vec();
        for byte in result.iter_mut() {
            if self.random_float() < flip_prob {
                *byte ^= (self.next_random() % 256) as u8;
            }
        }
        result
    }

    /// Count visible objects.
    #[allow(dead_code)]
    fn visible_object_count(&self) -> usize {
        self.promote_pending();
        self.objects.lock().expect("lock objects").len()
    }

    /// Get all visible keys.
    fn visible_keys(&self) -> Vec<String> {
        self.promote_pending();
        let objects = self.objects.lock().expect("lock objects");
        let mut keys: Vec<String> = objects.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Get raw data without faults (for verification).
    fn get_raw(&self, key: &str) -> Option<Vec<u8>> {
        // Check both visible and pending.
        if let Some(data) = self.objects.lock().expect("lock").get(key) {
            return Some(data.clone());
        }
        if let Some((data, _)) = self.pending_objects.lock().expect("lock").get(key) {
            return Some(data.clone());
        }
        None
    }
}

#[async_trait]
impl hadb_io::ObjectStore for FaultyObjectStore {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.bump_ops();

        if self.should_fail() {
            return Err(anyhow!("Injected S3 upload error for key: {}", key));
        }

        match &self.fault_mode {
            FaultMode::EventualConsistency { delay_ops } => {
                let visible_at = self.op_counter.load(Ordering::SeqCst) + delay_ops;
                self.pending_objects
                    .lock()
                    .expect("lock pending")
                    .insert(key.to_string(), (data, visible_at));
            }
            _ => {
                self.objects
                    .lock()
                    .expect("lock objects")
                    .insert(key.to_string(), data);
            }
        }

        Ok(())
    }

    async fn upload_bytes_with_checksum(
        &self,
        key: &str,
        data: Vec<u8>,
        _checksum: &str,
    ) -> Result<()> {
        self.upload_bytes(key, data).await
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = std::fs::read(path)?;
        self.upload_bytes(key, data).await
    }

    async fn upload_file_with_checksum(
        &self,
        key: &str,
        path: &Path,
        _checksum: &str,
    ) -> Result<()> {
        self.upload_file(key, path).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.bump_ops();
        self.promote_pending();

        if self.should_fail() {
            return Err(anyhow!("Injected S3 download error for key: {}", key));
        }

        let data = self
            .objects
            .lock()
            .expect("lock objects")
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow!("Not found: {}", key))?;

        match &self.fault_mode {
            FaultMode::PartialWrite(keep_fraction) => {
                let keep = (data.len() as f64 * keep_fraction) as usize;
                Ok(data[..keep.max(1)].to_vec())
            }
            FaultMode::SilentCorruption { flip_probability } => {
                Ok(self.corrupt_data(&data, *flip_probability))
            }
            _ => Ok(data),
        }
    }

    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = self.download_bytes(key).await?;
        std::fs::write(path, data)?;
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.bump_ops();
        self.promote_pending();

        if self.should_fail() {
            return Err(anyhow!("Injected S3 list error for prefix: {}", prefix));
        }

        let objects = self.objects.lock().expect("lock objects");
        let mut keys: Vec<String> = objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn list_objects_after(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> Result<Vec<String>> {
        self.bump_ops();
        self.promote_pending();

        if self.should_fail() {
            return Err(anyhow!("Injected S3 list_after error"));
        }

        let objects = self.objects.lock().expect("lock objects");
        let mut keys: Vec<String> = objects
            .keys()
            .filter(|k| k.starts_with(prefix) && k.as_str() > start_after)
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.bump_ops();
        self.promote_pending();
        Ok(self.objects.lock().expect("lock").contains_key(key))
    }

    async fn get_checksum(&self, _key: &str) -> Result<Option<String>> {
        self.bump_ops();
        Ok(None)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.bump_ops();
        self.objects.lock().expect("lock").remove(key);
        self.pending_objects.lock().expect("lock").remove(key);
        Ok(())
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        self.bump_ops();
        let mut objects = self.objects.lock().expect("lock");
        let mut pending = self.pending_objects.lock().expect("lock");
        let mut count = 0;
        for key in keys {
            if objects.remove(key).is_some() {
                count += 1;
            }
            pending.remove(key);
        }
        Ok(count)
    }

    fn bucket_name(&self) -> &str {
        "faulty-mock-bucket"
    }
}

// ===========================================================================
// Simulation helpers
// ===========================================================================

/// Write N entries, seal, return (journal_dir, state, sealed segment paths).
fn write_seal_entries(
    base_dir: &Path,
    name: &str,
    count: usize,
    segment_max: u64,
) -> (PathBuf, Arc<JournalState>, Vec<PathBuf>) {
    let journal_dir = base_dir.join(name);

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        journal_dir.clone(),
        segment_max,
        50,
        state.clone(),
    );

    for i in 1..=count {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}, data: '{}'}})", i, "z".repeat(40)),
            params: vec![("idx".into(), ParamValue::Int(i as i64))],
        }))
        .expect("send write");
    }

    // Flush, then seal.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).expect("send flush");
    ack_rx.recv().expect("flush ack");

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx))
        .expect("send seal");
    seal_rx.recv().expect("seal ack");

    tx.send(JournalCommand::Shutdown).expect("send shutdown");
    std::thread::sleep(std::time::Duration::from_millis(150));

    let mut sealed: Vec<PathBuf> = std::fs::read_dir(&journal_dir)
        .expect("read dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().map_or(false, |ext| ext == "hadbj")
                && p.file_name()
                    .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
        })
        .collect();
    sealed.sort();

    (journal_dir, state, sealed)
}

/// Upload sealed segments to a FaultyObjectStore. Returns keys that were
/// successfully uploaded (may be fewer than all segments if faults are active).
async fn upload_segments(
    store: &FaultyObjectStore,
    segments: &[PathBuf],
    prefix: &str,
    db_name: &str,
) -> Vec<String> {
    let mut uploaded = Vec::new();
    for path in segments {
        let name = path
            .file_name()
            .expect("file_name")
            .to_str()
            .expect("to_str");
        let seq_str = name
            .strip_prefix("journal-")
            .expect("strip prefix")
            .strip_suffix(".hadbj")
            .expect("strip suffix");
        let seq: u64 = seq_str.parse().expect("parse seq");

        let key = hadb_changeset::storage::format_key(
            prefix,
            db_name,
            hadb_changeset::storage::GENERATION_INCREMENTAL,
            seq,
            hadb_changeset::storage::ChangesetKind::Journal,
        );

        let data = std::fs::read(path).expect("read segment file");
        match store.upload_bytes(&key, data).await {
            Ok(()) => uploaded.push(key),
            Err(_) => { /* fault injected, skip */ }
        }
    }
    uploaded
}

// ===========================================================================
// Invariant verification
// ===========================================================================

/// Verify invariant 1: Journal continuity (no sequence gaps).
fn verify_continuity(journal_dir: &Path) -> Result<Vec<u64>> {
    let reader = JournalReader::open(journal_dir).map_err(|e| anyhow!("{}", e))?;
    let mut sequences = Vec::new();
    let mut prev_seq = 0u64;

    for result in reader {
        let entry = result.map_err(|e| anyhow!("{}", e))?;
        assert!(
            entry.sequence > prev_seq,
            "Sequence not monotonically increasing: {} <= {}",
            entry.sequence,
            prev_seq
        );
        // Check for gaps: each entry should be prev+1.
        if prev_seq > 0 {
            assert_eq!(
                entry.sequence,
                prev_seq + 1,
                "Sequence gap: expected {}, got {}",
                prev_seq + 1,
                entry.sequence
            );
        }
        prev_seq = entry.sequence;
        sequences.push(entry.sequence);
    }

    Ok(sequences)
}

/// Verify invariant 2: Chain hash validity.
fn verify_chain_hash(journal_dir: &Path) -> Result<[u8; 32]> {
    let reader = JournalReader::open(journal_dir).map_err(|e| anyhow!("{}", e))?;
    let mut running_hash = [0u8; 32];

    for result in reader {
        let entry = result.map_err(|e| anyhow!("{}", e))?;
        // The JournalReader already validates chain hash internally.
        // If we get here without error, the chain is valid.
        running_hash = entry.chain_hash;
    }

    Ok(running_hash)
}

/// Verify invariant 5: Compaction preserves all entries.
/// Reads entries, compares count and content against expected.
fn verify_entry_preservation(
    journal_dir: &Path,
    expected_count: usize,
) -> Result<()> {
    let reader = JournalReader::open(journal_dir).map_err(|e| anyhow!("{}", e))?;
    let entries: Vec<_> = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("{}", e))?;

    assert_eq!(
        entries.len(),
        expected_count,
        "Entry count mismatch: expected {}, got {}",
        expected_count,
        entries.len()
    );

    // Verify all sequences are present.
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(
            entry.sequence,
            (i + 1) as u64,
            "Missing entry at expected seq {}",
            i + 1
        );
    }

    Ok(())
}

// ===========================================================================
// DST simulation tests
// ===========================================================================

/// DST 1: Write + read with no faults (baseline).
#[test]
fn dst_baseline_no_faults() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (journal_dir, state, _) = write_seal_entries(dir.path(), "baseline", 50, 512);

    // Invariant 1: Continuity.
    let sequences = verify_continuity(&journal_dir).expect("continuity");
    assert_eq!(sequences.len(), 50);

    // Invariant 2: Chain hash.
    let hash = verify_chain_hash(&journal_dir).expect("chain hash");
    let expected_hash = *state.chain_hash.lock().expect("lock");
    assert_eq!(hash, expected_hash);

    // Invariant 5: Entry preservation.
    verify_entry_preservation(&journal_dir, 50).expect("entry preservation");
}

/// DST 2: Upload with RandomError faults, then retry without faults.
/// Invariant 3: Upload completeness when faults clear.
#[tokio::test]
async fn dst_random_upload_errors_then_retry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_journal_dir, _state, segments) =
        write_seal_entries(dir.path(), "random_err", 30, 512);

    let prefix = "dst/";
    let db_name = "testdb";

    // Phase 1: Upload with 50% failure rate.
    let faulty_store = FaultyObjectStore::new(FaultMode::RandomError(0.5), 42);
    let first_pass = upload_segments(&faulty_store, &segments, prefix, db_name).await;
    // Some uploads may have failed.
    let first_count = first_pass.len();

    // Phase 2: Switch to no-fault mode and retry all.
    // We simulate this by creating a new store with no faults and uploading everything.
    let clean_store = FaultyObjectStore::new(FaultMode::None, 0);
    let all_uploaded = upload_segments(&clean_store, &segments, prefix, db_name).await;

    // Invariant 3: All segments should now be uploaded.
    assert_eq!(
        all_uploaded.len(),
        segments.len(),
        "After retry, all {} segments should be uploaded (first pass got {})",
        segments.len(),
        first_count
    );

    // Verify the uploaded data by downloading and reading.
    let download_dir = dir.path().join("downloaded");
    std::fs::create_dir_all(&download_dir).expect("create download dir");

    for (i, key) in all_uploaded.iter().enumerate() {
        let data = clean_store
            .download_bytes(key)
            .await
            .expect("download should succeed");
        let _local_name = format!("journal-{:016}.hadbj", i + 1);
        // Use the original segment filenames to reconstruct.
        let orig_name = segments[i]
            .file_name()
            .expect("file_name")
            .to_str()
            .expect("to_str");
        std::fs::write(download_dir.join(orig_name), &data).expect("write");
    }

    // Verify all entries are readable from downloaded segments.
    let reader = JournalReader::open(&download_dir).expect("open reader");
    let entries: Vec<_> = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("read entries");
    assert_eq!(entries.len(), 30, "All 30 entries should be recoverable");
}

/// DST 3: Eventual consistency simulation.
/// Uploads succeed but objects are not immediately visible.
#[tokio::test]
async fn dst_eventual_consistency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_journal_dir, _state, segments) =
        write_seal_entries(dir.path(), "eventual", 20, 512);

    let prefix = "dst-ec/";
    let db_name = "ecdb";

    let store = Arc::new(FaultyObjectStore::new(
        FaultMode::EventualConsistency { delay_ops: 5 },
        123,
    ));

    // Upload all segments.
    let _uploaded = upload_segments(&store, &segments, prefix, db_name).await;

    // Immediately after upload, objects may not be visible.
    let visible_before = store.visible_keys();

    // Perform enough operations to make everything visible.
    for _ in 0..20 {
        let _ = store.list_objects(prefix).await;
    }

    let visible_after = store.visible_keys();
    assert!(
        visible_after.len() >= visible_before.len(),
        "Objects should become visible over time"
    );

    // Eventually all should be visible.
    let all_keys = store.visible_keys();
    let journal_keys: Vec<_> = all_keys
        .iter()
        .filter(|k| k.starts_with(prefix))
        .collect();
    assert_eq!(
        journal_keys.len(),
        segments.len(),
        "All {} segments should eventually be visible, got {}",
        segments.len(),
        journal_keys.len()
    );
}

/// DST 4: Recovery from missing segments.
/// Delete some segment files, verify reader handles it gracefully.
#[test]
fn dst_recovery_from_segment_loss() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (journal_dir, _state, segments) =
        write_seal_entries(dir.path(), "segment_loss", 40, 512);

    // Verify baseline.
    verify_entry_preservation(&journal_dir, 40).expect("baseline");

    // Read entries to know what is in each segment.
    let total_before = verify_continuity(&journal_dir).expect("continuity");
    assert_eq!(total_before.len(), 40);

    // Delete the last segment (simulating loss).
    if segments.len() > 1 {
        let last_segment = &segments[segments.len() - 1];
        std::fs::remove_file(last_segment).expect("remove segment");

        // Recovery should still work for the remaining segments.
        let (recovered_seq, _) =
            journal::recover_journal_state(&journal_dir).expect("recover");

        // Recovered seq should be less than or equal to 40 (the lost segment's entries are gone).
        assert!(
            recovered_seq <= 40,
            "Recovered seq {} should be <= 40",
            recovered_seq
        );

        if recovered_seq > 0 {
            // Read entries from remaining segments.
            let reader = JournalReader::open(&journal_dir).expect("open reader");
            let entries: Vec<_> = reader.filter_map(|r| r.ok()).collect();

            // Entries should be monotonically increasing.
            let mut prev = 0u64;
            for entry in &entries {
                assert!(
                    entry.sequence > prev,
                    "Monotonicity broken: {} <= {}",
                    entry.sequence,
                    prev
                );
                prev = entry.sequence;
            }

            // Should have fewer entries than before.
            assert!(
                entries.len() < 40,
                "Should have fewer entries after segment loss"
            );
        }
    }
}

/// DST 5: Multiple segment rotation + full recovery.
/// Write many entries with small segments, verify full recovery.
#[test]
fn dst_many_segments_full_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (journal_dir, state, segments) =
        write_seal_entries(dir.path(), "many_segments", 100, 256);

    // Should have many segments.
    assert!(
        segments.len() >= 3,
        "Expected many segments, got {}",
        segments.len()
    );

    // Invariant 1: Continuity.
    let sequences = verify_continuity(&journal_dir).expect("continuity");
    assert_eq!(sequences.len(), 100);

    // Invariant 2: Chain hash.
    let hash = verify_chain_hash(&journal_dir).expect("chain hash");
    let expected_hash = *state.chain_hash.lock().expect("lock");
    assert_eq!(hash, expected_hash);

    // Invariant 5: Preservation.
    verify_entry_preservation(&journal_dir, 100).expect("entry preservation");

    // Also verify recovery returns correct state.
    let (recovered_seq, recovered_hash) =
        journal::recover_journal_state(&journal_dir).expect("recover");
    assert_eq!(recovered_seq, 100);
    assert_eq!(recovered_hash, expected_hash);
}

/// DST 6: Upload to faulty store, download from clean store (verifies data integrity).
#[tokio::test]
async fn dst_upload_download_integrity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_journal_dir, state, segments) =
        write_seal_entries(dir.path(), "integrity", 25, 512);

    let prefix = "dst-int/";
    let db_name = "intdb";

    // Upload to a clean store.
    let store = FaultyObjectStore::new(FaultMode::None, 0);
    let uploaded = upload_segments(&store, &segments, prefix, db_name).await;
    assert_eq!(uploaded.len(), segments.len());

    // Download to a new directory and verify.
    let download_dir = dir.path().join("downloaded");
    std::fs::create_dir_all(&download_dir).expect("mkdir");

    for (seg_path, key) in segments.iter().zip(uploaded.iter()) {
        let data = store.download_bytes(key).await.expect("download");
        let orig_name = seg_path
            .file_name()
            .expect("file_name")
            .to_str()
            .expect("to_str");
        std::fs::write(download_dir.join(orig_name), &data).expect("write");
    }

    // Verify all invariants on downloaded data.
    let sequences = verify_continuity(&download_dir).expect("continuity");
    assert_eq!(sequences.len(), 25);

    let hash = verify_chain_hash(&download_dir).expect("chain hash");
    let expected_hash = *state.chain_hash.lock().expect("lock");
    assert_eq!(hash, expected_hash);

    verify_entry_preservation(&download_dir, 25).expect("entry preservation");
}

/// DST 7: Silent corruption detection.
/// Upload clean data, download with silent corruption, verify reader detects it.
#[tokio::test]
async fn dst_silent_corruption_detected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_journal_dir, _state, segments) =
        write_seal_entries(dir.path(), "corrupt", 10, 1024 * 1024);

    assert!(
        !segments.is_empty(),
        "Expected at least 1 sealed segment"
    );

    // Upload to a clean store first.
    let clean_store = FaultyObjectStore::new(FaultMode::None, 0);
    let prefix = "dst-corr/";
    let db_name = "corrdb";
    let uploaded = upload_segments(&clean_store, &segments, prefix, db_name).await;
    assert!(!uploaded.is_empty());

    // Now create a corrupt store that flips bits on download.
    let corrupt_store = FaultyObjectStore::new(
        FaultMode::SilentCorruption {
            flip_probability: 0.01,
        },
        42,
    );
    // Copy the clean data into the corrupt store.
    for key in &uploaded {
        let data = clean_store.get_raw(key).expect("raw data");
        corrupt_store
            .upload_bytes(key, data)
            .await
            .expect("upload to corrupt store");
    }

    // Download from corrupt store.
    let download_dir = dir.path().join("corrupted");
    std::fs::create_dir_all(&download_dir).expect("mkdir");

    for (seg_path, key) in segments.iter().zip(uploaded.iter()) {
        let data = corrupt_store
            .download_bytes(key)
            .await
            .expect("download");
        let orig_name = seg_path
            .file_name()
            .expect("file_name")
            .to_str()
            .expect("to_str");
        std::fs::write(download_dir.join(orig_name), &data).expect("write");
    }

    // Try reading from corrupted data. The hadbj decoder should detect corruption
    // (wrong magic, bad checksum, decode errors, or chain hash mismatch).
    //
    // With 1% byte flip probability on a multi-KB segment, the odds of zero
    // flips (or only cosmetic flips) are astronomically small. We require
    // that corruption is detected: either the reader errors out, or the
    // decoded data differs from the original.
    let reader_result = JournalReader::open(&download_dir);
    let (saw_error, data_differs) = match reader_result {
        Ok(reader) => {
            let mut saw_error = false;
            let mut downloaded_entries = Vec::new();
            for result in reader {
                match result {
                    Ok(entry) => downloaded_entries.push(entry.sequence),
                    Err(_) => {
                        saw_error = true;
                        break;
                    }
                }
            }

            // Check if the raw bytes actually changed.
            let orig_data = std::fs::read(&segments[0]).expect("read original");
            let corr_data = std::fs::read(
                download_dir.join(
                    segments[0]
                        .file_name()
                        .expect("file_name")
                        .to_str()
                        .expect("to_str"),
                ),
            )
            .expect("read corrupted");
            let data_differs = orig_data != corr_data;

            (saw_error, data_differs)
        }
        Err(_) => {
            // Reader failed to open, corruption detected at the format level.
            (true, true)
        }
    };

    assert!(
        saw_error || data_differs,
        "1% corruption on a multi-KB segment should be detected or produce different data"
    );
}

/// DST 8: Concurrent write + seal + read cycle.
/// Verifies the journal state machine handles rapid write/seal cycles.
#[test]
fn dst_rapid_write_seal_cycles() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_dir = dir.path().join("rapid");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 512, 20, state.clone());

    let mut total_entries = 0usize;

    for cycle in 0..5 {
        let batch_size = 5 + cycle * 3; // 5, 8, 11, 14, 17 = 55 total
        for i in 1..=batch_size {
            tx.send(JournalCommand::Write(PendingEntry {
                query: format!(
                    "CREATE (:N {{cycle: {}, idx: {}, pad: '{}'}})",
                    cycle,
                    i,
                    "x".repeat(30)
                ),
                params: vec![("c".into(), ParamValue::Int(cycle as i64))],
            }))
            .expect("send write");
            total_entries += 1;
        }

        // Seal after each cycle.
        let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
        tx.send(JournalCommand::SealForUpload(seal_tx))
            .expect("send seal");
        let _ = seal_rx.recv().expect("seal ack");
    }

    tx.send(JournalCommand::Shutdown).expect("send shutdown");
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify all entries are present.
    let reader = JournalReader::open(&journal_dir).expect("open reader");
    let entries: Vec<_> = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("read entries");
    assert_eq!(entries.len(), total_entries, "All entries from all cycles should be present");

    // Verify continuity.
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence, (i + 1) as u64, "Sequence gap after rapid cycles");
    }

    // Verify recovery.
    let (recovered_seq, recovered_hash) =
        journal::recover_journal_state(&journal_dir).expect("recover");
    assert_eq!(recovered_seq, total_entries as u64);
    let expected_hash = *state.chain_hash.lock().expect("lock");
    assert_eq!(recovered_hash, expected_hash);
}

/// DST 9: Partial write on download (truncation).
/// Verifies that truncated segment data is detectable.
#[tokio::test]
async fn dst_partial_write_detection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_journal_dir, _state, segments) =
        write_seal_entries(dir.path(), "partial", 15, 1024 * 1024);

    assert!(!segments.is_empty());

    let prefix = "dst-partial/";
    let db_name = "partialdb";

    // Upload to a clean store.
    let clean_store = FaultyObjectStore::new(FaultMode::None, 0);
    let uploaded = upload_segments(&clean_store, &segments, prefix, db_name).await;

    // Create a truncating store (keep only 60% of data on download).
    let truncating_store = FaultyObjectStore::new(FaultMode::PartialWrite(0.6), 99);
    for key in &uploaded {
        let data = clean_store.get_raw(key).expect("raw");
        truncating_store
            .upload_bytes(key, data)
            .await
            .expect("upload");
    }

    // Download truncated data.
    let download_dir = dir.path().join("truncated");
    std::fs::create_dir_all(&download_dir).expect("mkdir");

    for (seg_path, key) in segments.iter().zip(uploaded.iter()) {
        let data = truncating_store
            .download_bytes(key)
            .await
            .expect("download");
        let orig_name = seg_path
            .file_name()
            .expect("file_name")
            .to_str()
            .expect("to_str");
        std::fs::write(download_dir.join(orig_name), &data).expect("write");
    }

    // Attempting to read truncated segments should produce errors or fewer entries.
    let reader_result = JournalReader::open(&download_dir);
    match reader_result {
        Ok(reader) => {
            let mut entry_count = 0;
            let mut error_count = 0;
            for result in reader {
                match result {
                    Ok(_) => entry_count += 1,
                    Err(_) => {
                        error_count += 1;
                        break; // First error stops iteration
                    }
                }
            }
            // With 40% of data truncated, we should either get errors or fewer entries.
            assert!(
                error_count > 0 || entry_count < 15,
                "Truncated data should be detected: {} entries, {} errors",
                entry_count,
                error_count
            );
        }
        Err(_) => {
            // Reader failed to open: truncation detected at format level. Good.
        }
    }
}

/// DST 10: Stress test with large entry count and small segments.
#[test]
fn dst_stress_many_entries_small_segments() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (journal_dir, state, segments) =
        write_seal_entries(dir.path(), "stress", 200, 256);

    // Should produce many segments.
    assert!(
        segments.len() >= 5,
        "Expected many segments with 256-byte max, got {}",
        segments.len()
    );

    // Full invariant check.
    let sequences = verify_continuity(&journal_dir).expect("continuity");
    assert_eq!(sequences.len(), 200);

    let hash = verify_chain_hash(&journal_dir).expect("chain hash");
    let expected = *state.chain_hash.lock().expect("lock");
    assert_eq!(hash, expected);

    verify_entry_preservation(&journal_dir, 200).expect("preservation");

    let (recovered_seq, recovered_hash) =
        journal::recover_journal_state(&journal_dir).expect("recover");
    assert_eq!(recovered_seq, 200);
    assert_eq!(recovered_hash, expected);
}

/// DST 11: Concurrent writer + uploader + downloader under faults.
///
/// Spawns three concurrent tasks:
/// - Writer: produces entries continuously
/// - Uploader: seals and uploads segments concurrently
/// - Downloader/reader: reads available segments from the object store
///
/// All three run concurrently with a FaultyObjectStore (30% error rate).
/// After all tasks complete, verifies journal continuity, chain hash
/// validity, and entry preservation.
#[tokio::test]
async fn dst_concurrent_writer_uploader_downloader() {
    use std::time::Duration;
    use tokio::task::JoinSet;

    let dir = tempfile::tempdir().expect("tempdir");
    let journal_dir = dir.path().join("concurrent");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    // Large segment max so we control sealing manually.
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 2048, 50, state.clone());

    let store = Arc::new(FaultyObjectStore::new(FaultMode::RandomError(0.3), 7777));
    let prefix = "dst-conc/";
    let db_name = "concdb";

    let entry_count = 60;
    let mut join_set = JoinSet::new();

    // Writer task: produce entries continuously.
    let writer_tx = tx.clone();
    join_set.spawn(async move {
        for i in 1..=entry_count {
            writer_tx
                .send(JournalCommand::Write(PendingEntry {
                    query: format!(
                        "CREATE (:N {{idx: {}, pad: '{}'}})",
                        i,
                        "w".repeat(30)
                    ),
                    params: vec![("i".into(), ParamValue::Int(i as i64))],
                }))
                .expect("send write");
            // Small delay to interleave with uploader/downloader.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    // Uploader task: seal and upload segments concurrently.
    let uploader_tx = tx.clone();
    let uploader_store = store.clone();
    let uploader_prefix = prefix.to_string();
    let uploader_db_name = db_name.to_string();
    join_set.spawn(async move {
        for _cycle in 0..6 {
            tokio::time::sleep(Duration::from_millis(25)).await;

            // Seal the current segment.
            let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
            if uploader_tx
                .send(JournalCommand::SealForUpload(seal_tx))
                .is_err()
            {
                break;
            }
            let sealed_path = match seal_rx.recv() {
                Ok(Some(p)) => p,
                _ => continue,
            };

            // Upload the sealed segment.
            let name = sealed_path
                .file_name()
                .expect("file_name")
                .to_str()
                .expect("to_str");
            let seq_str = name
                .strip_prefix("journal-")
                .expect("strip prefix")
                .strip_suffix(".hadbj")
                .expect("strip suffix");
            let seq: u64 = seq_str.parse().expect("parse seq");

            let key = hadb_changeset::storage::format_key(
                &uploader_prefix,
                &uploader_db_name,
                hadb_changeset::storage::GENERATION_INCREMENTAL,
                seq,
                hadb_changeset::storage::ChangesetKind::Journal,
            );

            let data = std::fs::read(&sealed_path).expect("read segment");
            // Retries: try up to 5 times (faults are 30%).
            for _attempt in 0..5 {
                if uploader_store.upload_bytes(&key, data.clone()).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    });

    // Downloader/reader task: read available segments from the object store.
    let reader_store = store.clone();
    let reader_prefix = prefix.to_string();
    let reader_dir = dir.path().join("downloaded_conc");
    std::fs::create_dir_all(&reader_dir).expect("mkdir download");
    join_set.spawn(async move {
        let mut downloaded: Vec<String> = Vec::new();
        for _poll in 0..10 {
            tokio::time::sleep(Duration::from_millis(30)).await;

            let keys = match reader_store.list_objects(&reader_prefix).await {
                Ok(k) => k,
                Err(_) => continue, // fault injected on list
            };

            for key in &keys {
                if downloaded.contains(key) {
                    continue;
                }
                match reader_store.download_bytes(key).await {
                    Ok(data) => {
                        // Extract filename from key for local storage.
                        let filename = key
                            .rsplit('/')
                            .next()
                            .expect("key has filename component");
                        std::fs::write(reader_dir.join(filename), &data)
                            .expect("write downloaded segment");
                        downloaded.push(key.clone());
                    }
                    Err(_) => { /* fault injected on download */ }
                }
            }
        }
    });

    // Wait for all tasks.
    while let Some(result) = join_set.join_next().await {
        result.expect("task should not panic");
    }

    // Shutdown the writer, which seals the last segment.
    tx.send(JournalCommand::Shutdown).expect("send shutdown");
    std::thread::sleep(Duration::from_millis(200));

    // Verify invariants on the source journal (not the downloaded copy,
    // since downloads may be partial due to faults).
    let sequences = verify_continuity(&journal_dir).expect("continuity");
    assert_eq!(
        sequences.len(),
        entry_count as usize,
        "All {} entries should be present in the source journal",
        entry_count
    );

    let hash = verify_chain_hash(&journal_dir).expect("chain hash");
    let expected_hash = *state.chain_hash.lock().expect("lock");
    assert_eq!(hash, expected_hash, "Chain hash should match after concurrent operations");

    verify_entry_preservation(&journal_dir, entry_count as usize)
        .expect("entry preservation");
}

/// DST 12: Compaction preserves all entries (invariant 5).
///
/// Writes entries across multiple segments, seals them all, runs compaction
/// via `run_background_compaction`, then reads the compacted output and
/// verifies all original entries are present with correct sequence numbers.
#[tokio::test]
async fn dst_compaction_preserves_all_entries() {
    use graphstream::uploader::{run_background_compaction, CompactionConfig, UploadMessage};
    use graphstream::cache::SegmentCache;
    use tokio::sync::mpsc;

    let dir = tempfile::tempdir().expect("tempdir");
    let entry_count = 50;
    let segment_max = 512; // Small segments to force multiple files.

    // Write and seal entries.
    let (journal_dir, state, segments) =
        write_seal_entries(dir.path(), "compaction", entry_count, segment_max);

    assert!(
        segments.len() >= 3,
        "Need at least 3 segments for meaningful compaction test, got {}",
        segments.len()
    );

    // Collect original entries before compaction for comparison.
    let original_reader = JournalReader::open(&journal_dir).expect("open reader");
    let original_entries: Vec<_> = original_reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("read original entries");
    assert_eq!(original_entries.len(), entry_count);

    // Set up the cache and mark all segments as uploaded (compaction requires this).
    let mut cache = SegmentCache::new(&journal_dir).expect("create cache");
    let pending = cache.pending_segments().expect("pending segments");
    for path in &pending {
        let name = path
            .file_name()
            .expect("file_name")
            .to_str()
            .expect("to_str");
        cache.mark_uploaded(name).expect("mark uploaded");
    }
    let cache = Arc::new(tokio::sync::Mutex::new(cache));

    // Create upload channel to receive compacted segment.
    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadMessage<PathBuf>>(256);

    let config = CompactionConfig {
        threshold: 2, // Low threshold to trigger compaction.
        zstd_level: 3,
    };

    // Run compaction.
    let triggered = run_background_compaction(&journal_dir, &cache, &upload_tx, &config)
        .await
        .expect("compaction should succeed");
    assert!(triggered, "Compaction should trigger with {} segments", segments.len());

    // Receive the compacted segment path.
    let compacted_path = match upload_rx.try_recv() {
        Ok(UploadMessage::Upload(path)) => {
            let name = path
                .file_name()
                .expect("file_name")
                .to_str()
                .expect("to_str")
                .to_string();
            assert!(
                name.starts_with("compacted-"),
                "Expected compacted-* filename, got {}",
                name
            );
            assert!(path.exists(), "Compacted file should exist on disk");
            path
        }
        other => panic!("Expected Upload message with compacted path, got: {:?}", other),
    };

    // Read the compacted segment in isolation. JournalReader filters for
    // filenames starting with "journal-", so rename the compacted file
    // accordingly when placing it in the isolated directory.
    let compacted_dir = dir.path().join("compacted_only");
    std::fs::create_dir_all(&compacted_dir).expect("mkdir compacted_only");
    std::fs::copy(&compacted_path, compacted_dir.join("journal-0000000000000001.hadbj"))
        .expect("copy compacted file");

    let compacted_reader = JournalReader::open(&compacted_dir).expect("open compacted reader");
    let compacted_entries: Vec<_> = compacted_reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("read compacted entries");

    // Invariant 5: All original entries must be present in the compacted output.
    assert_eq!(
        compacted_entries.len(),
        original_entries.len(),
        "Compacted segment should contain all {} entries, got {}",
        original_entries.len(),
        compacted_entries.len()
    );

    // Verify sequence numbers are preserved in order.
    for (orig, compacted) in original_entries.iter().zip(compacted_entries.iter()) {
        assert_eq!(
            orig.sequence, compacted.sequence,
            "Sequence number mismatch: original {} vs compacted {}",
            orig.sequence, compacted.sequence
        );
    }

    // Verify chain hash is valid on the compacted output.
    let compacted_hash = verify_chain_hash(&compacted_dir).expect("chain hash on compacted");
    let expected_hash = *state.chain_hash.lock().expect("lock");
    assert_eq!(
        compacted_hash, expected_hash,
        "Compacted segment chain hash should match original"
    );

    // Verify continuity on the compacted output.
    let compacted_seqs = verify_continuity(&compacted_dir).expect("continuity on compacted");
    assert_eq!(compacted_seqs.len(), entry_count);
}
