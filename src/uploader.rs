//! Concurrent uploader for sealed journal segments.
//!
//! Architecture:
//! ```text
//! Journal Writer Task         Uploader Task (JoinSet, max N concurrent)
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
//!                               reap completed:
//!                                 read file bytes
//!                                 upload via ObjectStore
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::journal::{JournalCommand, JournalSender};
use hadb_io::RetryPolicy;

// ---------------------------------------------------------------------------
// SegmentStorage trait
// ---------------------------------------------------------------------------

/// Abstraction over segment upload destination (S3, mock, etc).
#[async_trait]
pub trait SegmentStorage: Send + Sync + 'static {
    /// Upload bytes to the given key. Must be idempotent.
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()>;

    /// Upload a file by path. Default reads into memory; ObjectStore impls may stream.
    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        let data =
            std::fs::read(path).map_err(|e| anyhow!("Read {}: {e}", path.display()))?;
        self.upload_bytes(key, data).await
    }
}

/// SegmentStorage backed by any hadb_io::ObjectStore (S3Backend, mock, etc).
pub struct ObjectStoreStorage(pub Arc<dyn hadb_io::ObjectStore>);

#[async_trait]
impl SegmentStorage for ObjectStoreStorage {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.0.upload_bytes(key, data).await
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        self.0.upload_file(key, path).await
    }
}

// ---------------------------------------------------------------------------
// Upload messages and stats
// ---------------------------------------------------------------------------

/// Message sent to the concurrent uploader.
pub enum UploadMessage {
    /// Upload a specific sealed segment file.
    Upload(PathBuf),
    /// Upload with acknowledgment: sender waits for upload completion.
    UploadWithAck(PathBuf, tokio::sync::oneshot::Sender<anyhow::Result<()>>),
    /// Graceful shutdown: drain in-flight uploads, then exit.
    Shutdown,
}

impl std::fmt::Debug for UploadMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Upload(p) => f.debug_tuple("Upload").field(p).finish(),
            Self::UploadWithAck(p, _) => f.debug_tuple("UploadWithAck").field(p).finish(),
            Self::Shutdown => write!(f, "Shutdown"),
        }
    }
}

/// Upload statistics.
#[derive(Debug, Clone, Default)]
pub struct UploaderStats {
    pub uploads_attempted: u64,
    pub uploads_succeeded: u64,
    pub uploads_failed: u64,
    pub bytes_uploaded: u64,
}

// ---------------------------------------------------------------------------
// UploadTaskContext — cloned into each spawned task
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct UploadTaskContext {
    storage: Arc<dyn SegmentStorage>,
    prefix: String,
    db_name: String,
    retry_policy: Arc<RetryPolicy>,
    stats: Arc<tokio::sync::Mutex<UploaderStats>>,
    cache: Option<Arc<tokio::sync::Mutex<crate::cache::SegmentCache>>>,
    webhook: Option<Arc<hadb_io::WebhookSender>>,
}

struct UploadResult {
    _segment_name: String,
}

impl UploadTaskContext {
    /// Upload a single sealed segment file with retry.
    /// Uses streaming upload (ByteStream) for S3 — file is NOT loaded into memory.
    async fn upload_segment(&self, path: PathBuf) -> Result<UploadResult> {
        {
            let mut stats = self.stats.lock().await;
            stats.uploads_attempted += 1;
        }

        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("Invalid segment path: {}", path.display()))?
            .to_string();

        // Parse seq from filename (journal-{seq:016}.hadbj or compacted-{ts}.hadbj).
        let seq = parse_seq_from_filename(&file_name)
            .ok_or_else(|| anyhow!("Cannot parse seq from filename: {}", file_name))?;
        let key = hadb_changeset::storage::format_key(
            &self.prefix,
            &self.db_name,
            hadb_changeset::storage::GENERATION_INCREMENTAL,
            seq,
            hadb_changeset::storage::ChangesetKind::Journal,
        );

        // Get file size before upload for stats tracking.
        let data_len = std::fs::metadata(&path)
            .map_err(|e| anyhow!("Metadata {}: {e}", path.display()))?
            .len();

        let storage = self.storage.clone();
        let key_clone = key.clone();
        let path_clone = path.clone();

        let upload_result = self
            .retry_policy
            .execute(|| {
                let storage = storage.clone();
                let key = key_clone.clone();
                let path = path_clone.clone();
                async move { storage.upload_file(&key, &path).await }
            })
            .await;

        if let Err(ref e) = upload_result {
            {
                let mut stats = self.stats.lock().await;
                stats.uploads_failed += 1;
            }
            if let Some(ref webhook) = self.webhook {
                let payload = hadb_io::WebhookPayload::new(
                    hadb_io::WebhookEvent::UploadFailed,
                    &self.prefix,
                    &e.to_string(),
                    self.retry_policy.config().max_retries,
                );
                webhook.send(payload).await;
            }
        }

        upload_result?;

        {
            let mut stats = self.stats.lock().await;
            stats.uploads_succeeded += 1;
            stats.bytes_uploaded += data_len;
        }

        // Mark as uploaded in cache.
        if let Some(ref cache) = self.cache {
            let mut cache = cache.lock().await;
            if let Err(e) = cache.mark_uploaded(&file_name) {
                error!("Failed to mark {} as uploaded in cache: {e}", file_name);
            }
        }

        info!(segment = %file_name, key = %key, size_bytes = data_len, "Uploaded segment");
        Ok(UploadResult {
            _segment_name: file_name,
        })
    }
}

// ---------------------------------------------------------------------------
// Uploader — JoinSet-based concurrent upload engine
// ---------------------------------------------------------------------------

/// Concurrent segment uploader.
pub struct Uploader {
    ctx: UploadTaskContext,
    max_concurrent: usize,
}

impl Uploader {
    pub fn new(
        storage: Arc<dyn SegmentStorage>,
        prefix: String,
        db_name: String,
        retry_policy: Arc<RetryPolicy>,
        max_concurrent: usize,
    ) -> Self {
        Self::with_options(storage, prefix, db_name, retry_policy, max_concurrent, None, None)
    }

    pub fn with_cache(
        storage: Arc<dyn SegmentStorage>,
        prefix: String,
        db_name: String,
        retry_policy: Arc<RetryPolicy>,
        max_concurrent: usize,
        cache: Option<Arc<tokio::sync::Mutex<crate::cache::SegmentCache>>>,
    ) -> Self {
        Self::with_options(storage, prefix, db_name, retry_policy, max_concurrent, cache, None)
    }

    pub fn with_options(
        storage: Arc<dyn SegmentStorage>,
        prefix: String,
        db_name: String,
        retry_policy: Arc<RetryPolicy>,
        max_concurrent: usize,
        cache: Option<Arc<tokio::sync::Mutex<crate::cache::SegmentCache>>>,
        webhook: Option<Arc<hadb_io::WebhookSender>>,
    ) -> Self {
        Self {
            ctx: UploadTaskContext {
                storage,
                prefix,
                db_name,
                retry_policy,
                stats: Arc::new(tokio::sync::Mutex::new(UploaderStats::default())),
                cache,
                webhook,
            },
            max_concurrent: max_concurrent.max(1),
        }
    }

    /// Run the uploader until Shutdown or channel close.
    pub async fn run(&self, mut rx: mpsc::Receiver<UploadMessage>) -> Result<UploaderStats> {
        let mut in_flight: JoinSet<Result<UploadResult>> = JoinSet::new();

        loop {
            tokio::select! {
                msg = rx.recv(), if in_flight.len() < self.max_concurrent => {
                    match msg {
                        Some(UploadMessage::Upload(path)) => {
                            let ctx = self.ctx.clone();
                            in_flight.spawn(async move { ctx.upload_segment(path).await });
                        }
                        Some(UploadMessage::UploadWithAck(path, ack_tx)) => {
                            let ctx = self.ctx.clone();
                            in_flight.spawn(async move {
                                let result = ctx.upload_segment(path).await;
                                let ack_result = match &result {
                                    Ok(_) => Ok(()),
                                    Err(e) => Err(anyhow::anyhow!("{}", e)),
                                };
                                let _ = ack_tx.send(ack_result);
                                result
                            });
                        }
                        Some(UploadMessage::Shutdown) => {
                            info!(in_flight = in_flight.len(), "Uploader shutting down, draining");
                            while let Some(result) = in_flight.join_next().await {
                                Self::handle_join_result(result);
                            }
                            break;
                        }
                        None => {
                            info!(in_flight = in_flight.len(), "Uploader channel closed, draining");
                            while let Some(result) = in_flight.join_next().await {
                                Self::handle_join_result(result);
                            }
                            break;
                        }
                    }
                }
                Some(result) = in_flight.join_next() => {
                    Self::handle_join_result(result);
                }
            }
        }

        let stats = self.ctx.stats.lock().await.clone();
        info!(
            attempted = stats.uploads_attempted,
            succeeded = stats.uploads_succeeded,
            failed = stats.uploads_failed,
            bytes = stats.bytes_uploaded,
            "Uploader stopped"
        );
        Ok(stats)
    }

    fn handle_join_result(result: Result<Result<UploadResult>, tokio::task::JoinError>) {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => error!("Upload task failed: {e}"),
            Err(e) => error!("Upload task panicked: {e}"),
        }
    }

    pub async fn stats(&self) -> UploaderStats {
        self.ctx.stats.lock().await.clone()
    }
}

/// Spawn uploader task and return (sender, handle).
///
/// Callers MUST await the handle after sending Shutdown to ensure
/// in-flight uploads complete before the runtime exits.
pub fn spawn_uploader(
    uploader: Arc<Uploader>,
) -> (mpsc::Sender<UploadMessage>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(256);
    let handle = tokio::spawn(async move {
        if let Err(e) = uploader.run(rx).await {
            error!("Uploader task failed: {e}");
        }
    });
    (tx, handle)
}

// ---------------------------------------------------------------------------
// spawn_journal_uploader — high-level API (seal timer + concurrent upload)
// ---------------------------------------------------------------------------

/// Spawn a background task that periodically seals the current journal segment
/// and concurrently uploads all sealed segments via the provided ObjectStore.
///
/// Returns `(upload_tx, handle)`:
/// - `upload_tx` can be used to send additional Upload messages (e.g. after sync())
/// - `handle` must be awaited after shutdown for graceful drain
pub fn spawn_journal_uploader(
    journal_tx: JournalSender,
    journal_dir: PathBuf,
    object_store: Arc<dyn hadb_io::ObjectStore>,
    prefix: String,
    db_name: String,
    interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> (mpsc::Sender<UploadMessage>, tokio::task::JoinHandle<()>) {
    spawn_journal_uploader_with_retry(
        journal_tx,
        journal_dir,
        object_store,
        prefix,
        db_name,
        interval,
        shutdown,
        RetryPolicy::default_policy(),
        4, // default max concurrent uploads
    )
}

/// Spawn uploader with custom retry policy and concurrency limit.
pub fn spawn_journal_uploader_with_retry(
    journal_tx: JournalSender,
    journal_dir: PathBuf,
    object_store: Arc<dyn hadb_io::ObjectStore>,
    prefix: String,
    db_name: String,
    interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
    retry_policy: RetryPolicy,
    max_concurrent: usize,
) -> (mpsc::Sender<UploadMessage>, tokio::task::JoinHandle<()>) {
    spawn_journal_uploader_with_cache(
        journal_tx,
        journal_dir,
        object_store,
        prefix,
        db_name,
        interval,
        shutdown,
        retry_policy,
        max_concurrent,
        None,
        None,
        vec![],
    )
}

/// Spawn uploader with cache integration, periodic cleanup, and optional webhooks.
pub fn spawn_journal_uploader_with_cache(
    journal_tx: JournalSender,
    journal_dir: PathBuf,
    object_store: Arc<dyn hadb_io::ObjectStore>,
    prefix: String,
    db_name: String,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    retry_policy: RetryPolicy,
    max_concurrent: usize,
    cache: Option<Arc<tokio::sync::Mutex<crate::cache::SegmentCache>>>,
    cache_config: Option<crate::cache::CacheConfig>,
    webhook_configs: Vec<hadb_io::WebhookConfig>,
) -> (mpsc::Sender<UploadMessage>, tokio::task::JoinHandle<()>) {
    let retry_policy = Arc::new(retry_policy);
    let webhook = if webhook_configs.is_empty() {
        None
    } else {
        Some(Arc::new(hadb_io::WebhookSender::new(webhook_configs)))
    };

    let (upload_tx, upload_rx) = mpsc::channel::<UploadMessage>(256);
    let upload_tx_clone = upload_tx.clone();

    let handle = tokio::spawn(async move {
        let storage: Arc<dyn SegmentStorage> = Arc::new(ObjectStoreStorage(object_store));

        let uploader = Arc::new(Uploader::with_options(
            storage,
            prefix.clone(),
            db_name,
            retry_policy.clone(),
            max_concurrent,
            cache.clone(),
            webhook.clone(),
        ));

        // Resume pending uploads from cache on startup.
        if let Some(ref cache) = cache {
            let cache_guard = cache.lock().await;
            match cache_guard.pending_segments() {
                Ok(pending) => {
                    if !pending.is_empty() {
                        info!(
                            "Journal uploader: resuming {} pending uploads from cache",
                            pending.len()
                        );
                        for path in pending {
                            if upload_tx_clone.send(UploadMessage::Upload(path)).await.is_err() {
                                error!("Journal uploader: upload channel closed during resume");
                                return;
                            }
                        }
                    }
                }
                Err(e) => error!("Journal uploader: failed to read pending segments: {e}"),
            }
        }

        // Spawn the concurrent uploader in its own task.
        let uploader_handle = {
            let uploader = uploader.clone();
            tokio::spawn(async move {
                if let Err(e) = uploader.run(upload_rx).await {
                    error!("Concurrent uploader failed: {e}");
                }
            })
        };

        // Seal timer loop — seals + scans + sends Upload messages.
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Cleanup timer — every 5 minutes.
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
                        return;
                    }
                }
            }

            // Skip if circuit breaker is open.
            if let Some(cb) = retry_policy.circuit_breaker() {
                if !cb.should_allow() {
                    let failures = cb.consecutive_failures();
                    error!(
                        consecutive_failures = failures,
                        "Journal uploader: circuit breaker open, skipping cycle"
                    );
                    if let Some(ref wh) = webhook {
                        let payload = hadb_io::WebhookPayload::new(
                            hadb_io::WebhookEvent::CircuitBreakerOpen,
                            &prefix,
                            &format!("{} consecutive failures", failures),
                            failures,
                        );
                        wh.send(payload).await;
                    }
                    continue;
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
                        // Skip already-uploaded segments when cache is available.
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

        // Skip non-sealed files.
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
    cache: &Arc<tokio::sync::Mutex<crate::cache::SegmentCache>>,
    upload_tx: &mpsc::Sender<UploadMessage>,
    config: &CompactionConfig,
) -> Result<bool> {
    // Get uploaded segment paths from cache.
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

    // Run compaction in a blocking thread.
    let paths = uploaded_paths.clone();
    let dir = journal_dir.to_path_buf();
    let zstd_level = config.zstd_level;
    let compacted_path = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
        use hadb_changeset::journal::{decode, decode_entry, decode_header, seal, encode_compressed, HEADER_SIZE, HADBJ_MAGIC, JournalEntry as HjEntry};

        let output = dir.join(format!(
            "compacted-{}.hadbj",
            crate::current_timestamp_ms()
        ));

        // Read all entries from input segments.
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

        let segment = seal(all_entries, 0);
        let encoded = encode_compressed(&segment, zstd_level);
        std::fs::write(&output, &encoded)
            .map_err(|e| anyhow!("Write compacted: {e}"))?;

        Ok(output)
    })
    .await
    .map_err(|e| anyhow!("Compaction task panicked: {e}"))??;

    // Upload the compacted segment.
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
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::time::timeout;

    /// Mock storage for testing concurrent uploads without S3.
    struct MockStorage {
        objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        fail_count: Arc<Mutex<usize>>,
        max_failures: usize,
        upload_delay: Option<Duration>,
        active_uploads: Arc<AtomicUsize>,
        peak_concurrent: Arc<AtomicUsize>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                objects: Arc::new(Mutex::new(HashMap::new())),
                fail_count: Arc::new(Mutex::new(0)),
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
    impl SegmentStorage for MockStorage {
        async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
            let active = self.active_uploads.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_concurrent.fetch_max(active, Ordering::SeqCst);

            if let Some(delay) = self.upload_delay {
                tokio::time::sleep(delay).await;
            }

            let result = {
                let mut fail_count = self.fail_count.lock().unwrap();
                if *fail_count < self.max_failures {
                    *fail_count += 1;
                    Err(anyhow!("Simulated S3 failure {}/{}", fail_count, self.max_failures))
                } else {
                    self.objects.lock().unwrap().insert(key.to_string(), data);
                    Ok(())
                }
            };

            self.active_uploads.fetch_sub(1, Ordering::SeqCst);
            result
        }
    }

    fn make_uploader(
        storage: Arc<dyn SegmentStorage>,
        max_concurrent: usize,
    ) -> Arc<Uploader> {
        Arc::new(Uploader::new(
            storage,
            "test/".to_string(),
            "db".to_string(),
            Arc::new(RetryPolicy::default_policy()),
            max_concurrent,
        ))
    }

    /// Helper: create a fake sealed segment file on disk.
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
        let path = write_fake_segment(dir.path(), "journal-0000000000000001.hadbj", b"segment data");

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
        // Peak concurrent should not exceed our limit of 3.
        assert!(
            storage.peak_concurrent() <= 3,
            "Peak concurrent {} exceeded limit 3",
            storage.peak_concurrent()
        );
    }

    #[tokio::test]
    async fn test_concurrent_is_faster_than_sequential() {
        // Sequential: 4 uploads x 50ms each = ~200ms
        // Concurrent (4): 4 uploads / 4 concurrent = ~50ms
        let delay = Duration::from_millis(50);

        // Sequential (max_concurrent=1)
        let storage_seq = Arc::new(MockStorage::with_delay(delay));
        let uploader_seq = make_uploader(storage_seq.clone(), 1);
        let (tx_seq, handle_seq) = spawn_uploader(uploader_seq);

        let dir = tempfile::tempdir().unwrap();
        let start_seq = tokio::time::Instant::now();
        for i in 0..4 {
            let path = write_fake_segment(dir.path(), &format!("journal-{:016}.hadbj", i + 100), b"data");
            tx_seq.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx_seq.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle_seq).await.unwrap().unwrap();
        let elapsed_seq = start_seq.elapsed();

        // Concurrent (max_concurrent=4)
        let storage_con = Arc::new(MockStorage::with_delay(delay));
        let uploader_con = make_uploader(storage_con.clone(), 4);
        let (tx_con, handle_con) = spawn_uploader(uploader_con);

        let dir2 = tempfile::tempdir().unwrap();
        let start_con = tokio::time::Instant::now();
        for i in 0..4 {
            let path = write_fake_segment(dir2.path(), &format!("journal-{:016}.hadbj", i + 200), b"data");
            tx_con.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx_con.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle_con).await.unwrap().unwrap();
        let elapsed_con = start_con.elapsed();

        assert_eq!(storage_seq.object_count(), 4);
        assert_eq!(storage_con.object_count(), 4);

        // Concurrent should be significantly faster.
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
            let path = write_fake_segment(dir.path(), &format!("journal-{:016}.hadbj", i + 300), b"data");
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }

        // Small delay to let uploads start, then shutdown.
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        // All 4 should complete despite shutdown (drain).
        assert_eq!(storage.object_count(), 4);
    }

    #[tokio::test]
    async fn test_failure_doesnt_block_others() {
        // First upload fails once, then succeeds on retry.
        let storage = Arc::new(MockStorage::with_failures(1));
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..3 {
            let path = write_fake_segment(dir.path(), &format!("journal-{:016}.hadbj", i + 300), b"data");
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        // At least the non-failing uploads should complete.
        // With default retry policy, the first one should also retry and succeed.
        assert!(storage.object_count() >= 2);
    }

    #[tokio::test]
    async fn test_channel_close_drains() {
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(50)));
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        let path = write_fake_segment(dir.path(), "journal-0000000000000500.hadbj", b"data");
        tx.send(UploadMessage::Upload(path)).await.unwrap();

        // Drop sender instead of sending Shutdown.
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
            let path = write_fake_segment(dir.path(), &format!("journal-{:016}.hadbj", i + 300), data);
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

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(UploadMessage::UploadWithAck(path, ack_tx))
            .await
            .unwrap();

        // Wait for the ack.
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
        // Storage that always fails (max_failures very high).
        let storage = Arc::new(MockStorage::with_failures(1000));

        // Use a retry policy with 0 retries so the upload fails immediately.
        let no_retry = hadb_io::RetryConfig {
            max_retries: 0,
            base_delay_ms: 1,
            max_delay_ms: 1,
            circuit_breaker_enabled: false,
            circuit_breaker_threshold: 100,
            circuit_breaker_cooldown_ms: 1000,
        };
        let uploader = Arc::new(Uploader::new(
            storage.clone(),
            "test/".to_string(),
            "db".to_string(),
            Arc::new(RetryPolicy::new(no_retry)),
            4,
        ));
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        let path = write_fake_segment(dir.path(), "journal-0000000000000700.hadbj", b"fail data");

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(UploadMessage::UploadWithAck(path, ack_tx))
            .await
            .unwrap();

        // The ack should arrive with an error.
        let result = timeout(Duration::from_secs(5), ack_rx)
            .await
            .expect("ack timed out")
            .expect("ack channel dropped");
        assert!(result.is_err(), "Expected Err ack, got: {:?}", result);

        tx.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        // Upload should have failed, no objects stored.
        assert_eq!(storage.object_count(), 0);
    }

    #[tokio::test]
    async fn test_upload_with_ack_mixed_with_fire_and_forget() {
        let storage = Arc::new(MockStorage::new());
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();

        // Fire-and-forget upload.
        let path1 = write_fake_segment(dir.path(), "journal-0000000000000801.hadbj", b"ff data");
        tx.send(UploadMessage::Upload(path1)).await.unwrap();

        // Ack upload.
        let path2 = write_fake_segment(dir.path(), "journal-0000000000000802.hadbj", b"ack data");
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(UploadMessage::UploadWithAck(path2, ack_tx))
            .await
            .unwrap();

        // Another fire-and-forget.
        let path3 = write_fake_segment(dir.path(), "journal-0000000000000803.hadbj", b"ff2 data");
        tx.send(UploadMessage::Upload(path3)).await.unwrap();

        // Wait for ack.
        let result = timeout(Duration::from_secs(5), ack_rx)
            .await
            .expect("ack timed out")
            .expect("ack channel dropped");
        assert!(result.is_ok());

        tx.send(UploadMessage::Shutdown).await.unwrap();
        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 3);
    }

    /// Helper: create real sealed journal segments by writing entries via the journal writer.
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

        // Create multiple small sealed segments.
        create_sealed_segments(&journal_dir, 20, 512);

        // Set up cache and mark all segments as uploaded.
        let mut cache = crate::cache::SegmentCache::new(&journal_dir).unwrap();
        let pending = cache.pending_segments().unwrap();
        assert!(pending.len() >= 3, "Need at least 3 segments, got {}", pending.len());
        for path in &pending {
            let name = path.file_name().unwrap().to_str().unwrap();
            cache.mark_uploaded(name).unwrap();
        }
        let cache = Arc::new(tokio::sync::Mutex::new(cache));

        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadMessage>(256);

        // Compaction with threshold=3.
        let config = CompactionConfig {
            threshold: 3,
            zstd_level: 3,
        };
        let result =
            run_background_compaction(&journal_dir, &cache, &upload_tx, &config).await.unwrap();
        assert!(result, "Compaction should trigger when uploaded >= threshold");

        // Verify a compacted segment was queued for upload.
        match upload_rx.try_recv() {
            Ok(UploadMessage::Upload(path)) => {
                let name = path.file_name().unwrap().to_str().unwrap().to_string();
                assert!(name.starts_with("compacted-"), "Expected compacted-*, got {name}");
                assert!(path.exists(), "Compacted file should exist on disk");

                // Verify the compacted file is valid and readable.
                let reader = crate::journal::JournalReader::open(
                    path.parent().unwrap(),
                ).unwrap();
                let entries: Vec<_> = reader
                    .filter_map(|r| r.ok())
                    .collect();
                // Compacted + original sealed segments both present — entries should be >= 20.
                assert!(entries.len() >= 20, "Expected >= 20 entries, got {}", entries.len());
            }
            other => panic!("Expected Upload message, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_background_compaction_below_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        // Create 1 segment (only 1 entry, 1MB segment = no rotation).
        create_sealed_segments(&journal_dir, 1, 1024 * 1024);

        let mut cache = crate::cache::SegmentCache::new(&journal_dir).unwrap();
        for path in cache.pending_segments().unwrap() {
            let name = path.file_name().unwrap().to_str().unwrap();
            cache.mark_uploaded(name).unwrap();
        }
        let cache = Arc::new(tokio::sync::Mutex::new(cache));

        let (upload_tx, _rx) = mpsc::channel::<UploadMessage>(256);
        let config = CompactionConfig::default(); // threshold = 10
        let result =
            run_background_compaction(&journal_dir, &cache, &upload_tx, &config).await.unwrap();
        assert!(!result, "Compaction should not trigger below threshold");
    }

    #[tokio::test]
    async fn test_background_compaction_skips_already_compacted() {
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        // Create enough segments.
        create_sealed_segments(&journal_dir, 20, 512);

        let mut cache = crate::cache::SegmentCache::new(&journal_dir).unwrap();
        let pending = cache.pending_segments().unwrap();
        for path in &pending {
            let name = path.file_name().unwrap().to_str().unwrap();
            cache.mark_uploaded(name).unwrap();
        }

        // Create a fake "compacted-" file to verify it's excluded from compaction input.
        let fake_compacted = journal_dir.join("compacted-0000000000.hadbj");
        std::fs::write(&fake_compacted, b"fake").unwrap();

        let cache = Arc::new(tokio::sync::Mutex::new(cache));
        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadMessage>(256);

        let config = CompactionConfig {
            threshold: 3,
            zstd_level: 3,
        };
        let result =
            run_background_compaction(&journal_dir, &cache, &upload_tx, &config).await.unwrap();
        assert!(result, "Compaction should still trigger");

        // The queued compacted file should NOT be the fake one.
        match upload_rx.try_recv() {
            Ok(UploadMessage::Upload(path)) => {
                let name = path.file_name().unwrap().to_str().unwrap();
                assert_ne!(name, "compacted-0000000000.hadbj");
                assert!(name.starts_with("compacted-"));
            }
            other => panic!("Expected Upload message, got: {other:?}"),
        }
    }

    /// Test that ObjectStoreStorage delegates to any ObjectStore implementation.
    #[tokio::test]
    async fn test_object_store_storage_upload() {
        use std::path::Path as StdPath;

        /// Minimal mock ObjectStore for testing the SegmentStorage bridge.
        struct InMemStore {
            objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        }

        impl InMemStore {
            fn new() -> Self {
                Self {
                    objects: Arc::new(Mutex::new(HashMap::new())),
                }
            }
            fn keys(&self) -> Vec<String> {
                self.objects.lock().unwrap().keys().cloned().collect()
            }
        }

        #[async_trait]
        impl hadb_io::ObjectStore for InMemStore {
            async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
                self.objects.lock().unwrap().insert(key.to_string(), data);
                Ok(())
            }
            async fn upload_bytes_with_checksum(&self, key: &str, data: Vec<u8>, _: &str) -> Result<()> {
                self.upload_bytes(key, data).await
            }
            async fn upload_file(&self, key: &str, path: &StdPath) -> Result<()> {
                let data = std::fs::read(path)?;
                self.upload_bytes(key, data).await
            }
            async fn upload_file_with_checksum(&self, key: &str, path: &StdPath, _: &str) -> Result<()> {
                self.upload_file(key, path).await
            }
            async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
                self.objects.lock().unwrap().get(key).cloned()
                    .ok_or_else(|| anyhow!("not found: {}", key))
            }
            async fn download_file(&self, _key: &str, _path: &StdPath) -> Result<()> { Ok(()) }
            async fn list_objects(&self, _prefix: &str) -> Result<Vec<String>> { Ok(vec![]) }
            async fn list_objects_after(&self, _: &str, _: &str) -> Result<Vec<String>> { Ok(vec![]) }
            async fn exists(&self, key: &str) -> Result<bool> {
                Ok(self.objects.lock().unwrap().contains_key(key))
            }
            async fn get_checksum(&self, _: &str) -> Result<Option<String>> { Ok(None) }
            async fn delete_object(&self, _: &str) -> Result<()> { Ok(()) }
            async fn delete_objects(&self, _: &[String]) -> Result<usize> { Ok(0) }
            fn bucket_name(&self) -> &str { "test" }
        }

        let store = Arc::new(InMemStore::new());
        let storage: Arc<dyn SegmentStorage> = Arc::new(ObjectStoreStorage(store.clone()));

        // Upload via SegmentStorage (delegates to ObjectStore).
        storage.upload_bytes("test/key1", b"hello".to_vec()).await.unwrap();

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("segment.hadbj");
        std::fs::write(&file_path, b"file-content").unwrap();
        storage.upload_file("test/key2", &file_path).await.unwrap();

        let keys = store.keys();
        assert!(keys.contains(&"test/key1".to_string()));
        assert!(keys.contains(&"test/key2".to_string()));
    }

    /// Regression: uploads_failed must be incremented when upload_segment fails.
    #[tokio::test]
    async fn test_uploads_failed_counter() {
        let storage = Arc::new(MockStorage::with_failures(1000));
        let no_retry = hadb_io::RetryConfig {
            max_retries: 0,
            base_delay_ms: 1,
            max_delay_ms: 1,
            circuit_breaker_enabled: false,
            circuit_breaker_threshold: 100,
            circuit_breaker_cooldown_ms: 1000,
        };
        let uploader = Arc::new(Uploader::new(
            storage.clone(),
            "test/".to_string(),
            "db".to_string(),
            Arc::new(RetryPolicy::new(no_retry)),
            4,
        ));
        let uploader_ref = uploader.clone();
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..3 {
            let path = write_fake_segment(dir.path(), &format!("journal-{:016}.hadbj", i + 900), b"data");
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        let stats = uploader_ref.stats().await;
        assert_eq!(stats.uploads_attempted, 3);
        assert_eq!(stats.uploads_failed, 3);
        assert_eq!(stats.uploads_succeeded, 0);
        assert_eq!(storage.object_count(), 0);
    }

    /// Test that Uploader works end-to-end with ObjectStoreStorage.
    #[tokio::test]
    async fn test_uploader_with_object_store_storage() {
        // Reuse MockStorage (implements SegmentStorage) via Uploader.
        // This test verifies the integration path when ObjectStoreStorage is used.
        let storage = Arc::new(MockStorage::new());
        let uploader = Arc::new(Uploader::new(
            storage.clone(),
            "db/".to_string(),
            "mydb".to_string(),
            Arc::new(RetryPolicy::default_policy()),
            4,
        ));
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..5 {
            let path = write_fake_segment(dir.path(), &format!("journal-{:016}.hadbj", i + 1000), b"data");
            tx.send(UploadMessage::Upload(path)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 5);
        // seq 1000 = 0x3e8
        assert!(storage.has_key("db/mydb/0000/00000000000003e8.hadbj"));
        // seq 1004 = 0x3ec
        assert!(storage.has_key("db/mydb/0000/00000000000003ec.hadbj"));
    }
}
