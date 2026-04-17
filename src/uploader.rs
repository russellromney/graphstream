//! Concurrent uploader for sealed journal segments.
//!
//! Inline JoinSet-based uploader that drives sealed `.hadbj` segments into any
//! [`hadb_storage::StorageBackend`] (S3, Cinch HTTP, in-memory mock, local
//! filesystem). The seal timer loop rotates the journal, scans the directory
//! for sealed segments, and feeds them to a bounded-concurrency uploader
//! task. Retry is the storage backend's concern: `hadb-storage-s3` and
//! `hadb-storage-cinch` both retry transient failures internally, so the
//! uploader wraps `StorageBackend::put` without its own backoff layer.
//!
//! Architecture:
//! ```text
//! Journal Writer Task         Uploader task (JoinSet, max N concurrent)
//!       |                          |
//!   seal_segment()                 |
//!       |                          |
//!   (on rotation/shutdown)         |
//!       |                          |
//! spawn_journal_uploader ──>  ticker fires
//!                               seal + scan sealed
//!                               send Upload(path)
//!                                      |
//!                               spawn upload task ──┐
//!                               spawn upload task ──┤ (up to max_concurrent)
//!                               spawn upload task ──┘
//!                                      |
//!                               reap completed → update stats
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use hadb_storage::StorageBackend;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::journal::{JournalCommand, JournalSender};

/// Message to the uploader task.
#[derive(Debug)]
pub enum UploadMessage<T> {
    /// Fire-and-forget upload.
    Upload(T),
    /// Upload and deliver the result on the oneshot channel. Useful for
    /// tests and synchronous sync() callers that need to know when the
    /// upload durably completes.
    UploadWithAck(T, oneshot::Sender<Result<()>>),
    /// Shut down after draining in-flight uploads.
    Shutdown,
}

/// Cumulative uploader stats.
#[derive(Debug, Default, Clone)]
pub struct UploaderStats {
    pub uploads_attempted: u64,
    pub uploads_succeeded: u64,
    pub uploads_failed: u64,
    pub bytes_uploaded: u64,
}

// ---------------------------------------------------------------------------
// SegmentUploader: owns the JoinSet loop
// ---------------------------------------------------------------------------

/// Bounded-concurrency uploader for sealed journal segments.
///
/// Holds a handler (storage + key-format logic) and runs a `JoinSet`-based
/// upload loop. `max_concurrent` caps in-flight tasks. `run` returns when
/// the channel closes or a `Shutdown` message arrives, after draining.
pub struct SegmentUploader {
    handler: Arc<SegmentUploadHandler>,
    max_concurrent: usize,
    stats: Arc<Mutex<UploaderStats>>,
}

impl SegmentUploader {
    pub fn new(handler: Arc<SegmentUploadHandler>, max_concurrent: usize) -> Self {
        Self {
            handler,
            max_concurrent: max_concurrent.max(1),
            stats: Arc::new(Mutex::new(UploaderStats::default())),
        }
    }

    pub async fn stats(&self) -> UploaderStats {
        self.stats.lock().await.clone()
    }

    /// Run the upload loop until the channel closes or `Shutdown` arrives.
    pub async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<UploadMessage<PathBuf>>) -> Result<()> {
        let mut in_flight: JoinSet<(Result<u64>, Option<oneshot::Sender<Result<()>>>)> =
            JoinSet::new();
        let mut shutting_down = false;

        loop {
            // Drain in_flight to the concurrency limit before blocking on rx.
            while in_flight.len() >= self.max_concurrent {
                if let Some(joined) = in_flight.join_next().await {
                    self.handle_join(joined).await;
                }
            }

            tokio::select! {
                // Reap a completed task even while we're still accepting work.
                Some(joined) = in_flight.join_next(), if !in_flight.is_empty() => {
                    self.handle_join(joined).await;
                }

                msg = rx.recv(), if !shutting_down && in_flight.len() < self.max_concurrent => {
                    let Some(msg) = msg else {
                        // Channel closed: drain and exit.
                        break;
                    };
                    match msg {
                        UploadMessage::Upload(path) => {
                            let handler = self.handler.clone();
                            in_flight.spawn(async move {
                                (handler.upload(path).await, None)
                            });
                        }
                        UploadMessage::UploadWithAck(path, ack) => {
                            let handler = self.handler.clone();
                            in_flight.spawn(async move {
                                (handler.upload(path).await, Some(ack))
                            });
                        }
                        UploadMessage::Shutdown => {
                            shutting_down = true;
                        }
                    }
                }

                else => {
                    // No work to accept and no in-flight tasks. Either we
                    // were told to shut down or the channel closed and all
                    // inflight finished.
                    if in_flight.is_empty() {
                        break;
                    }
                }
            }
        }

        // Final drain.
        while let Some(joined) = in_flight.join_next().await {
            self.handle_join(joined).await;
        }

        Ok(())
    }

    async fn handle_join(
        &self,
        joined: std::result::Result<
            (Result<u64>, Option<oneshot::Sender<Result<()>>>),
            tokio::task::JoinError,
        >,
    ) {
        let mut stats = self.stats.lock().await;
        stats.uploads_attempted += 1;
        match joined {
            Ok((Ok(bytes), ack)) => {
                stats.uploads_succeeded += 1;
                stats.bytes_uploaded += bytes;
                if let Some(ack) = ack {
                    let _ = ack.send(Ok(()));
                }
            }
            Ok((Err(e), ack)) => {
                stats.uploads_failed += 1;
                error!("Segment upload failed: {e}");
                if let Some(ack) = ack {
                    let _ = ack.send(Err(anyhow!("{e}")));
                }
            }
            Err(join_err) => {
                stats.uploads_failed += 1;
                error!("Segment upload task panicked: {join_err}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SegmentUploadHandler: formats the key and calls StorageBackend::put
// ---------------------------------------------------------------------------

/// Upload handler for sealed journal segments.
///
/// Reads the segment file, computes the storage key via
/// [`hadb_changeset::storage::format_key`], and calls `storage.put(&key, &data)`.
/// On success, marks the segment as uploaded in the optional cache.
pub struct SegmentUploadHandler {
    storage: Arc<dyn StorageBackend>,
    prefix: String,
    db_name: String,
    cache: Option<Arc<Mutex<crate::cache::SegmentCache>>>,
}

impl SegmentUploadHandler {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        prefix: String,
        db_name: String,
        cache: Option<Arc<Mutex<crate::cache::SegmentCache>>>,
    ) -> Self {
        Self {
            storage,
            prefix,
            db_name,
            cache,
        }
    }

    /// Resume: return any pending (not-yet-uploaded) segments from the cache.
    /// Called once at startup so the loop can re-enqueue anything the previous
    /// process left behind.
    pub fn pending_items(&self) -> Vec<PathBuf> {
        let Some(ref cache) = self.cache else {
            return Vec::new();
        };
        // pending_items is sync (called once at startup). try_lock to avoid
        // a blocking wait; on contention the next seal timer tick picks them up.
        let Ok(cache_guard) = cache.try_lock() else {
            return Vec::new();
        };
        match cache_guard.pending_segments() {
            Ok(pending) => {
                if !pending.is_empty() {
                    info!(
                        "Journal uploader: resuming {} pending uploads from cache",
                        pending.len()
                    );
                }
                pending
            }
            Err(e) => {
                error!("Journal uploader: failed to read pending segments: {e}");
                Vec::new()
            }
        }
    }

    /// Upload a single segment. Returns the number of bytes uploaded.
    async fn upload(&self, path: PathBuf) -> Result<u64> {
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("Invalid segment path: {}", path.display()))?
            .to_string();

        let seq = parse_seq_from_filename(&file_name)
            .ok_or_else(|| anyhow!("Cannot parse seq from filename: {}", file_name))?;
        let key = hadb_changeset::storage::format_key(
            &self.prefix,
            &self.db_name,
            hadb_changeset::storage::GENERATION_INCREMENTAL,
            seq,
            hadb_changeset::storage::ChangesetKind::Journal,
        );

        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| anyhow!("Read {}: {e}", path.display()))?;
        let data_len = data.len() as u64;

        self.storage
            .put(&key, &data)
            .await
            .map_err(|e| anyhow!("Upload {} → {key}: {e}", path.display()))?;

        if let Some(ref cache) = self.cache {
            let mut cache = cache.lock().await;
            if let Err(e) = cache.mark_uploaded(&file_name) {
                error!("Failed to mark {} as uploaded in cache: {e}", file_name);
            }
        }

        info!(segment = %file_name, key = %key, size_bytes = data_len, "Uploaded segment");
        Ok(data_len)
    }
}

// ---------------------------------------------------------------------------
// spawn_uploader: spawn the uploader loop and return (tx, JoinHandle)
// ---------------------------------------------------------------------------

/// Spawn the uploader loop. Returns `(tx, handle)`; callers must await the
/// handle after sending `Shutdown` (or dropping `tx`) to drain in-flight
/// uploads before the runtime exits.
pub fn spawn_uploader(
    uploader: Arc<SegmentUploader>,
) -> (mpsc::Sender<UploadMessage<PathBuf>>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(256);
    let handle = tokio::spawn(async move {
        if let Err(e) = uploader.run(rx).await {
            error!("Uploader loop failed: {e}");
        }
    });
    (tx, handle)
}

// ---------------------------------------------------------------------------
// spawn_journal_uploader: high-level API (seal timer + concurrent upload)
// ---------------------------------------------------------------------------

/// Spawn a background task that periodically seals the current journal segment
/// and concurrently uploads sealed segments through `storage`.
///
/// Returns `(upload_tx, handle)`:
/// - `upload_tx` can be used to send additional `Upload` messages (e.g. after
///   a synchronous `sync()` call that seals a segment immediately).
/// - `handle` must be awaited after shutdown for graceful drain.
pub fn spawn_journal_uploader(
    journal_tx: JournalSender,
    journal_dir: PathBuf,
    storage: Arc<dyn StorageBackend>,
    prefix: String,
    db_name: String,
    interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> (mpsc::Sender<UploadMessage<PathBuf>>, tokio::task::JoinHandle<()>) {
    spawn_journal_uploader_with_cache(
        journal_tx,
        journal_dir,
        storage,
        prefix,
        db_name,
        interval,
        shutdown,
        4, // default max concurrent uploads
        None,
        None,
    )
}

/// Spawn uploader with cache integration and periodic cleanup.
///
/// `cache` (optional): the segment cache that tracks which local segments have
/// already been uploaded; pending ones are re-enqueued and uploaded ones are
/// skipped.
///
/// `cache_config` (optional): age/size-based cache cleanup policy.
#[allow(clippy::too_many_arguments)]
pub fn spawn_journal_uploader_with_cache(
    journal_tx: JournalSender,
    journal_dir: PathBuf,
    storage: Arc<dyn StorageBackend>,
    prefix: String,
    db_name: String,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    max_concurrent: usize,
    cache: Option<Arc<Mutex<crate::cache::SegmentCache>>>,
    cache_config: Option<crate::cache::CacheConfig>,
) -> (mpsc::Sender<UploadMessage<PathBuf>>, tokio::task::JoinHandle<()>) {
    let handler = Arc::new(SegmentUploadHandler::new(
        storage,
        prefix,
        db_name.clone(),
        cache.clone(),
    ));

    let uploader = Arc::new(SegmentUploader::new(handler.clone(), max_concurrent));
    let (upload_tx, upload_rx) = mpsc::channel::<UploadMessage<PathBuf>>(256);
    let upload_tx_clone = upload_tx.clone();

    // Enqueue any pending segments the previous run left behind.
    for pending in handler.pending_items() {
        if upload_tx.try_send(UploadMessage::Upload(pending)).is_err() {
            break;
        }
    }

    // Spawn the concurrent uploader.
    let uploader_handle = {
        let uploader = uploader.clone();
        tokio::spawn(async move {
            if let Err(e) = uploader.run(upload_rx).await {
                error!("Concurrent uploader failed: {e}");
            }
        })
    };

    let handle = tokio::spawn(async move {
        info!("[{db_name}] Uploader started (max_concurrent={max_concurrent})");
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut cleanup_ticker = tokio::time::interval(Duration::from_secs(300));
        cleanup_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let cleanup_config = cache_config.unwrap_or_default();

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = cleanup_ticker.tick(), if cache.is_some() => {
                    if let Some(ref cache) = cache {
                        let mut cache_guard = cache.lock().await;
                        if let Err(e) = cache_guard.cleanup(&cleanup_config) {
                            error!("Cache cleanup failed: {e}");
                        }
                    }
                    continue;
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Journal uploader: shutting down");
                        let _ = upload_tx_clone.send(UploadMessage::Shutdown).await;
                        let _ = uploader_handle.await;
                        let final_stats = uploader.stats().await;
                        info!(
                            "[{db_name}] Uploader stopped. Stats: {final_stats:?}"
                        );
                        return;
                    }
                }
            }

            // 1. Seal the current segment.
            let seal_result = {
                let tx = journal_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
                    if tx.send(JournalCommand::SealForUpload(ack_tx)).is_err() {
                        return None;
                    }
                    ack_rx.recv().ok().flatten()
                })
                .await
                .unwrap_or(None)
            };

            if let Some(path) = &seal_result {
                info!(segment = %path.display(), "Sealed segment for upload");
            }

            // 2. Scan for sealed .hadbj files, skip already-uploaded, send Upload messages.
            match scan_sealed_segments(&journal_dir) {
                Ok(paths) => {
                    for path in paths {
                        if let Some(ref cache) = cache {
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                let cache_guard = cache.lock().await;
                                if cache_guard.is_uploaded(name) {
                                    continue;
                                }
                            }
                        }
                        if upload_tx_clone.send(UploadMessage::Upload(path)).await.is_err() {
                            error!("Journal uploader: upload channel closed unexpectedly");
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("Journal uploader: failed to scan sealed segments: {e}");
                }
            }
        }
    });

    (upload_tx, handle)
}

/// Parse sequence number from a journal segment filename.
/// Handles both "journal-{seq:016}.hadbj" and "compacted-{timestamp}.hadbj".
fn parse_seq_from_filename(filename: &str) -> Option<u64> {
    if let Some(stem) = filename.strip_prefix("journal-").and_then(|s| s.strip_suffix(".hadbj")) {
        return stem.parse::<u64>().ok();
    }
    if let Some(stem) = filename.strip_prefix("compacted-").and_then(|s| s.strip_suffix(".hadbj")) {
        return stem.parse::<u64>().ok();
    }
    None
}

/// Scan journal_dir for sealed .hadbj files. Returns their paths.
fn scan_sealed_segments(journal_dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = std::fs::read_dir(journal_dir)
        .map_err(|e| anyhow!("Read journal dir: {e}"))?;

    let mut sealed = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| anyhow!("Read dir entry: {e}"))?;
        let path = entry.path();

        let _name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.ends_with(".hadbj") => n.to_string(),
            _ => continue,
        };

        if !crate::format::is_file_sealed(&path).map_err(|e| anyhow!("{e}"))? {
            continue;
        }

        sealed.push(path);
    }

    Ok(sealed)
}

// ---------------------------------------------------------------------------
// Background compaction
// ---------------------------------------------------------------------------

/// Configuration for background compaction.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Compact after this many uploaded segments accumulate.
    pub threshold: usize,
    /// Zstd compression level for compacted output.
    pub zstd_level: i32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            threshold: 10,
            zstd_level: 3,
        }
    }
}

/// Run background compaction on uploaded segments in the cache.
/// Compacts N uploaded segments into one, uploads the compacted segment,
/// then marks originals as candidates for cleanup.
///
/// Returns Ok(true) if compaction was triggered, Ok(false) if below threshold.
pub async fn run_background_compaction(
    journal_dir: &Path,
    cache: &Arc<Mutex<crate::cache::SegmentCache>>,
    upload_tx: &mpsc::Sender<UploadMessage<PathBuf>>,
    config: &CompactionConfig,
) -> Result<bool> {
    let uploaded_paths: Vec<PathBuf> = {
        let cache_guard = cache.lock().await;
        let mut paths = Vec::new();
        if let Ok(entries) = std::fs::read_dir(journal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(".hadbj")
                        && !name.starts_with("compacted-")
                        && cache_guard.is_uploaded(name)
                    {
                        paths.push(path);
                    }
                }
            }
        }
        paths.sort();
        paths
    };

    if uploaded_paths.len() < config.threshold {
        return Ok(false);
    }

    info!(
        "Background compaction: compacting {} uploaded segments",
        uploaded_paths.len()
    );

    let paths = uploaded_paths.clone();
    let dir = journal_dir.to_path_buf();
    let zstd_level = config.zstd_level;
    let compacted_path = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
        use hadb_changeset::journal::{
            decode, decode_entry, decode_header, encode_compressed, seal, HADBJ_MAGIC, HEADER_SIZE,
            JournalEntry as HjEntry,
        };

        let output = dir.join(format!(
            "compacted-{}.hadbj",
            crate::current_timestamp_ms()
        ));

        let mut all_entries: Vec<HjEntry> = Vec::new();
        for path in &paths {
            let file_bytes = std::fs::read(path)
                .map_err(|e| anyhow!("Read {}: {e}", path.display()))?;
            if file_bytes.len() < HEADER_SIZE || file_bytes[0..5] != HADBJ_MAGIC {
                continue;
            }
            let header = decode_header(&file_bytes)
                .map_err(|e| anyhow!("Decode header {}: {e}", path.display()))?;
            if header.is_sealed() {
                let segment = decode(&file_bytes)
                    .map_err(|e| anyhow!("Decode segment {}: {e}", path.display()))?;
                all_entries.extend(segment.entries);
            } else {
                let body = &file_bytes[HEADER_SIZE..];
                let mut offset = 0;
                while offset < body.len() {
                    match decode_entry(body, offset) {
                        Ok((entry, consumed)) => {
                            all_entries.push(entry);
                            offset += consumed;
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        if all_entries.is_empty() {
            return Err(anyhow!("No entries found in input segments"));
        }

        let prev_segment_checksum =
            hadb_changeset::journal::hash_to_u64(&all_entries[0].prev_hash);
        let segment = seal(all_entries, prev_segment_checksum);
        let encoded = encode_compressed(&segment, zstd_level);
        std::fs::write(&output, &encoded).map_err(|e| anyhow!("Write compacted: {e}"))?;

        Ok(output)
    })
    .await
    .map_err(|e| anyhow!("Compaction task panicked: {e}"))??;

    upload_tx
        .send(UploadMessage::Upload(compacted_path))
        .await
        .map_err(|_| anyhow!("Upload channel closed during compaction"))?;

    info!(
        "Background compaction: compacted {} segments, queued for upload",
        uploaded_paths.len()
    );
    Ok(true)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hadb_storage::CasResult;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use tokio::time::timeout;

    /// In-memory StorageBackend for tests. Tracks concurrency and can inject failures.
    struct MockStorage {
        objects: Arc<StdMutex<HashMap<String, Vec<u8>>>>,
        fail_count: Arc<StdMutex<usize>>,
        max_failures: usize,
        upload_delay: Option<Duration>,
        active_uploads: Arc<AtomicUsize>,
        peak_concurrent: Arc<AtomicUsize>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                objects: Arc::new(StdMutex::new(HashMap::new())),
                fail_count: Arc::new(StdMutex::new(0)),
                max_failures: 0,
                upload_delay: None,
                active_uploads: Arc::new(AtomicUsize::new(0)),
                peak_concurrent: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_failures(max_failures: usize) -> Self {
            Self {
                max_failures,
                ..Self::new()
            }
        }

        fn with_delay(delay: Duration) -> Self {
            Self {
                upload_delay: Some(delay),
                ..Self::new()
            }
        }

        fn object_count(&self) -> usize {
            self.objects.lock().unwrap().len()
        }

        fn peak_concurrent(&self) -> usize {
            self.peak_concurrent.load(Ordering::SeqCst)
        }

        fn has_key(&self, key: &str) -> bool {
            self.objects.lock().unwrap().contains_key(key)
        }
    }

    #[async_trait]
    impl StorageBackend for MockStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }

        async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            let active = self.active_uploads.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_concurrent.fetch_max(active, Ordering::SeqCst);

            if let Some(delay) = self.upload_delay {
                tokio::time::sleep(delay).await;
            }

            let result = {
                let mut fail_count = self.fail_count.lock().unwrap();
                if *fail_count < self.max_failures {
                    *fail_count += 1;
                    Err(anyhow!(
                        "Simulated failure {}/{}",
                        fail_count,
                        self.max_failures
                    ))
                } else {
                    self.objects
                        .lock()
                        .unwrap()
                        .insert(key.to_string(), data.to_vec());
                    Ok(())
                }
            };

            self.active_uploads.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }

        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            let map = self.objects.lock().unwrap();
            let mut keys: Vec<String> = map
                .keys()
                .filter(|k| k.starts_with(prefix))
                .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }

        async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
            let mut map = self.objects.lock().unwrap();
            if map.contains_key(key) {
                return Ok(CasResult {
                    success: false,
                    etag: None,
                });
            }
            map.insert(key.to_string(), data.to_vec());
            Ok(CasResult {
                success: true,
                etag: Some("mock".into()),
            })
        }

        async fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            _etag: &str,
        ) -> Result<CasResult> {
            let mut map = self.objects.lock().unwrap();
            if !map.contains_key(key) {
                return Ok(CasResult {
                    success: false,
                    etag: None,
                });
            }
            map.insert(key.to_string(), data.to_vec());
            Ok(CasResult {
                success: true,
                etag: Some("mock".into()),
            })
        }
    }

    fn make_handler(storage: Arc<dyn StorageBackend>) -> Arc<SegmentUploadHandler> {
        Arc::new(SegmentUploadHandler::new(
            storage,
            "test/".to_string(),
            "db".to_string(),
            None,
        ))
    }

    fn make_uploader(
        storage: Arc<dyn StorageBackend>,
        max_concurrent: usize,
    ) -> Arc<SegmentUploader> {
        Arc::new(SegmentUploader::new(make_handler(storage), max_concurrent))
    }

    fn write_fake_segment(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    #[tokio::test]
    async fn test_basic_upload() {
        let storage = Arc::new(MockStorage::new());
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        let path = write_fake_segment(dir.path(), "journal-0000000000000001.hadbj", b"segment");

        tx.send(UploadMessage::Upload(path)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 1);
        assert!(storage.has_key("test/db/0000/0000000000000001.hadbj"));
    }

    #[tokio::test]
    async fn test_multiple_uploads() {
        let storage = Arc::new(MockStorage::new());
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..10 {
            let name = format!("journal-{:016}.hadbj", i);
            let path = write_fake_segment(dir.path(), &name, format!("data-{}", i).as_bytes());
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 10);
    }

    #[tokio::test]
    async fn test_concurrent_uploads_respect_limit() {
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(50)));
        let uploader = make_uploader(storage.clone(), 3);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..9 {
            let name = format!("journal-{:016}.hadbj", i);
            let path = write_fake_segment(dir.path(), &name, format!("data-{}", i).as_bytes());
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(10), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 9);
        assert!(
            storage.peak_concurrent() <= 3,
            "Peak concurrent {} exceeded limit 3",
            storage.peak_concurrent()
        );
    }

    #[tokio::test]
    async fn test_concurrent_is_faster_than_sequential() {
        let delay = Duration::from_millis(50);

        let storage_seq = Arc::new(MockStorage::with_delay(delay));
        let uploader_seq = make_uploader(storage_seq.clone(), 1);
        let (tx_seq, handle_seq) = spawn_uploader(uploader_seq);

        let dir = tempfile::tempdir().unwrap();
        let start_seq = tokio::time::Instant::now();
        for i in 0..4 {
            let path = write_fake_segment(
                dir.path(),
                &format!("journal-{:016}.hadbj", i + 100),
                b"data",
            );
            tx_seq.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx_seq.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle_seq).await.unwrap().unwrap();
        let elapsed_seq = start_seq.elapsed();

        let storage_con = Arc::new(MockStorage::with_delay(delay));
        let uploader_con = make_uploader(storage_con.clone(), 4);
        let (tx_con, handle_con) = spawn_uploader(uploader_con);

        let dir2 = tempfile::tempdir().unwrap();
        let start_con = tokio::time::Instant::now();
        for i in 0..4 {
            let path = write_fake_segment(
                dir2.path(),
                &format!("journal-{:016}.hadbj", i + 200),
                b"data",
            );
            tx_con.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx_con.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle_con).await.unwrap().unwrap();
        let elapsed_con = start_con.elapsed();

        assert_eq!(storage_seq.object_count(), 4);
        assert_eq!(storage_con.object_count(), 4);
        assert!(
            elapsed_con < elapsed_seq,
            "Concurrent ({:?}) should be faster than sequential ({:?})",
            elapsed_con,
            elapsed_seq
        );
    }

    #[tokio::test]
    async fn test_shutdown_drains_in_flight() {
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(100)));
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..4 {
            let path = write_fake_segment(
                dir.path(),
                &format!("journal-{:016}.hadbj", i + 300),
                b"data",
            );
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();
        assert_eq!(storage.object_count(), 4);
    }

    #[tokio::test]
    async fn test_failure_doesnt_block_others() {
        // With retries gone from the uploader, every attempt = one put() call.
        // Inject one failure and send three uploads: one fails, two succeed.
        let storage = Arc::new(MockStorage::with_failures(1));
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..3 {
            let path = write_fake_segment(
                dir.path(),
                &format!("journal-{:016}.hadbj", i + 400),
                b"data",
            );
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();
        assert_eq!(storage.object_count(), 2);
    }

    #[tokio::test]
    async fn test_channel_close_drains() {
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(50)));
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        let path = write_fake_segment(dir.path(), "journal-0000000000000500.hadbj", b"data");
        tx.send(UploadMessage::Upload(path)).await.unwrap();

        drop(tx);

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();
        assert_eq!(storage.object_count(), 1);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let storage = Arc::new(MockStorage::new());
        let uploader = make_uploader(storage.clone(), 4);
        let uploader_ref = uploader.clone();
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        let data = b"segment-payload-bytes";
        for i in 0..5 {
            let path = write_fake_segment(
                dir.path(),
                &format!("journal-{:016}.hadbj", i + 300),
                data,
            );
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        let stats = uploader_ref.stats().await;
        assert_eq!(stats.uploads_attempted, 5);
        assert_eq!(stats.uploads_succeeded, 5);
        assert_eq!(stats.uploads_failed, 0);
        assert_eq!(stats.bytes_uploaded, 5 * data.len() as u64);
    }

    #[tokio::test]
    async fn test_upload_with_ack_success() {
        let storage = Arc::new(MockStorage::new());
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        let path = write_fake_segment(dir.path(), "journal-0000000000000600.hadbj", b"ack data");

        let (ack_tx, ack_rx) = oneshot::channel();
        tx.send(UploadMessage::UploadWithAck(path, ack_tx))
            .await
            .unwrap();

        let result = timeout(Duration::from_secs(5), ack_rx)
            .await
            .expect("ack timed out")
            .expect("ack channel dropped");
        assert!(result.is_ok(), "Expected Ok ack, got: {:?}", result);

        tx.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 1);
        assert!(storage.has_key("test/db/0000/0000000000000258.hadbj"));
    }

    #[tokio::test]
    async fn test_upload_with_ack_failure() {
        let storage = Arc::new(MockStorage::with_failures(1));
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        let path = write_fake_segment(dir.path(), "journal-0000000000000700.hadbj", b"fail");

        let (ack_tx, ack_rx) = oneshot::channel();
        tx.send(UploadMessage::UploadWithAck(path, ack_tx))
            .await
            .unwrap();

        let result = timeout(Duration::from_secs(5), ack_rx)
            .await
            .expect("ack timed out")
            .expect("ack channel dropped");
        assert!(result.is_err(), "Expected Err ack, got: {:?}", result);

        tx.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 0);
    }

    #[tokio::test]
    async fn test_upload_with_ack_mixed_with_fire_and_forget() {
        let storage = Arc::new(MockStorage::new());
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();

        let path1 = write_fake_segment(dir.path(), "journal-0000000000000801.hadbj", b"ff");
        tx.send(UploadMessage::Upload(path1)).await.unwrap();

        let path2 = write_fake_segment(dir.path(), "journal-0000000000000802.hadbj", b"ack");
        let (ack_tx, ack_rx) = oneshot::channel();
        tx.send(UploadMessage::UploadWithAck(path2, ack_tx))
            .await
            .unwrap();

        let path3 = write_fake_segment(dir.path(), "journal-0000000000000803.hadbj", b"ff2");
        tx.send(UploadMessage::Upload(path3)).await.unwrap();

        let result = timeout(Duration::from_secs(5), ack_rx)
            .await
            .expect("ack timed out")
            .expect("ack channel dropped");
        assert!(result.is_ok());

        tx.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 3);
    }

    fn create_sealed_segments(journal_dir: &Path, entry_count: usize, segment_max_bytes: u64) {
        let state = Arc::new(crate::journal::JournalState::with_sequence_and_hash(
            0,
            [0u8; 32],
        ));
        let tx = crate::journal::spawn_journal_writer(
            journal_dir.to_path_buf(),
            segment_max_bytes,
            100,
            state,
        );
        for i in 1..=entry_count {
            tx.send(crate::journal::JournalCommand::Write(
                crate::journal::PendingEntry {
                    query: format!("CREATE (:N {{v: {}}})", i),
                    params: vec![],
                },
            ))
            .unwrap();
        }
        tx.send(crate::journal::JournalCommand::Shutdown).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_background_compaction_triggers() {
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        create_sealed_segments(&journal_dir, 20, 512);

        let mut cache = crate::cache::SegmentCache::new(&journal_dir).unwrap();
        let pending = cache.pending_segments().unwrap();
        assert!(pending.len() >= 3, "Need at least 3 segments, got {}", pending.len());
        for path in &pending {
            let name = path.file_name().unwrap().to_str().unwrap();
            cache.mark_uploaded(name).unwrap();
        }
        let cache = Arc::new(Mutex::new(cache));

        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadMessage<PathBuf>>(256);

        let config = CompactionConfig {
            threshold: 3,
            zstd_level: 3,
        };
        let result =
            run_background_compaction(&journal_dir, &cache, &upload_tx, &config).await.unwrap();
        assert!(result, "Compaction should trigger when uploaded >= threshold");

        match upload_rx.try_recv() {
            Ok(UploadMessage::Upload(path)) => {
                let name = path.file_name().unwrap().to_str().unwrap().to_string();
                assert!(name.starts_with("compacted-"), "Expected compacted-*, got {name}");
                assert!(path.exists(), "Compacted file should exist on disk");

                let reader = crate::journal::JournalReader::open(path.parent().unwrap()).unwrap();
                let entries: Vec<_> = reader.filter_map(|r| r.ok()).collect();
                assert!(entries.len() >= 20, "Expected >= 20 entries, got {}", entries.len());
            }
            other => panic!("Expected Upload message, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_background_compaction_below_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        create_sealed_segments(&journal_dir, 1, 1024 * 1024);

        let mut cache = crate::cache::SegmentCache::new(&journal_dir).unwrap();
        for path in cache.pending_segments().unwrap() {
            let name = path.file_name().unwrap().to_str().unwrap();
            cache.mark_uploaded(name).unwrap();
        }
        let cache = Arc::new(Mutex::new(cache));

        let (upload_tx, _rx) = mpsc::channel::<UploadMessage<PathBuf>>(256);
        let config = CompactionConfig::default();
        let result =
            run_background_compaction(&journal_dir, &cache, &upload_tx, &config).await.unwrap();
        assert!(!result, "Compaction should not trigger below threshold");
    }

    #[tokio::test]
    async fn test_background_compaction_skips_already_compacted() {
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        create_sealed_segments(&journal_dir, 20, 512);

        let mut cache = crate::cache::SegmentCache::new(&journal_dir).unwrap();
        let pending = cache.pending_segments().unwrap();
        for path in &pending {
            let name = path.file_name().unwrap().to_str().unwrap();
            cache.mark_uploaded(name).unwrap();
        }

        let fake_compacted = journal_dir.join("compacted-0000000000.hadbj");
        std::fs::write(&fake_compacted, b"fake").unwrap();

        let cache = Arc::new(Mutex::new(cache));
        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadMessage<PathBuf>>(256);

        let config = CompactionConfig {
            threshold: 3,
            zstd_level: 3,
        };
        let result =
            run_background_compaction(&journal_dir, &cache, &upload_tx, &config).await.unwrap();
        assert!(result, "Compaction should still trigger");

        match upload_rx.try_recv() {
            Ok(UploadMessage::Upload(path)) => {
                let name = path.file_name().unwrap().to_str().unwrap();
                assert_ne!(name, "compacted-0000000000.hadbj");
                assert!(name.starts_with("compacted-"));
            }
            other => panic!("Expected Upload message, got: {other:?}"),
        }
    }

    /// Uploader uses a generic StorageBackend; this is the smoke test for the
    /// trait-object wire-up (the prior version had an ObjectStoreStorage adapter
    /// struct that no longer exists).
    #[tokio::test]
    async fn test_uploader_with_storage_backend() {
        let storage = Arc::new(MockStorage::new());
        let handler = Arc::new(SegmentUploadHandler::new(
            storage.clone(),
            "db/".to_string(),
            "mydb".to_string(),
            None,
        ));
        let uploader = Arc::new(SegmentUploader::new(handler, 4));
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..5 {
            let path = write_fake_segment(
                dir.path(),
                &format!("journal-{:016}.hadbj", i + 1000),
                b"data",
            );
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 5);
        assert!(storage.has_key("db/mydb/0000/00000000000003e8.hadbj"));
    }
}
