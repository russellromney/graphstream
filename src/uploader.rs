//! Concurrent S3 uploader for sealed journal segments.
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
//!                                 PutObject to S3
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::journal::{JournalCommand, JournalSender};
use crate::retry::RetryPolicy;

// ---------------------------------------------------------------------------
// SegmentStorage trait
// ---------------------------------------------------------------------------

/// Abstraction over segment upload destination (S3, mock, etc).
#[async_trait]
pub trait SegmentStorage: Send + Sync + 'static {
    /// Upload bytes to the given key. Must be idempotent.
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()>;
}

/// S3 implementation of SegmentStorage.
pub struct S3SegmentStorage {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3SegmentStorage {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

#[async_trait]
impl SegmentStorage for S3SegmentStorage {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .send()
            .await
            .map_err(|e| anyhow!("PutObject {}: {e}", key))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Upload messages and stats
// ---------------------------------------------------------------------------

/// Message sent to the concurrent uploader.
#[derive(Debug)]
pub enum UploadMessage {
    /// Upload a specific sealed segment file.
    Upload(PathBuf),
    /// Graceful shutdown — drain in-flight uploads, then exit.
    Shutdown,
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
    retry_policy: Arc<RetryPolicy>,
    stats: Arc<tokio::sync::Mutex<UploaderStats>>,
}

struct UploadResult {
    _path: PathBuf,
}

impl UploadTaskContext {
    /// Upload a single sealed segment file with retry.
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
        let key = format!("{}journal/{}", self.prefix, file_name);

        // Read file once, share across retry attempts.
        let bytes = Arc::new(
            std::fs::read(&path).map_err(|e| anyhow!("Read {}: {e}", path.display()))?,
        );
        let data_len = bytes.len() as u64;

        let storage = self.storage.clone();
        let key_clone = key.clone();
        let bytes_clone = bytes.clone();

        self.retry_policy
            .execute(|| {
                let storage = storage.clone();
                let key = key_clone.clone();
                let bytes = bytes_clone.clone();
                async move { storage.upload_bytes(&key, (*bytes).clone()).await }
            })
            .await?;

        {
            let mut stats = self.stats.lock().await;
            stats.uploads_succeeded += 1;
            stats.bytes_uploaded += data_len;
        }

        info!("Uploaded {} ({} bytes)", key, data_len);
        Ok(UploadResult { _path: path })
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
        retry_policy: Arc<RetryPolicy>,
        max_concurrent: usize,
    ) -> Self {
        Self {
            ctx: UploadTaskContext {
                storage,
                prefix,
                retry_policy,
                stats: Arc::new(tokio::sync::Mutex::new(UploaderStats::default())),
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
                        Some(UploadMessage::Shutdown) => {
                            info!("Uploader: shutting down, draining {} in-flight", in_flight.len());
                            while let Some(result) = in_flight.join_next().await {
                                Self::handle_join_result(result);
                            }
                            break;
                        }
                        None => {
                            info!("Uploader: channel closed, draining {} in-flight", in_flight.len());
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
        info!("Uploader stopped. Stats: {:?}", stats);
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
/// and concurrently uploads all sealed segments to S3.
///
/// Returns `(upload_tx, handle)`:
/// - `upload_tx` can be used to send additional Upload messages (e.g. after sync())
/// - `handle` must be awaited after shutdown for graceful drain
pub fn spawn_journal_uploader(
    journal_tx: JournalSender,
    journal_dir: PathBuf,
    bucket: String,
    prefix: String,
    interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> (mpsc::Sender<UploadMessage>, tokio::task::JoinHandle<()>) {
    spawn_journal_uploader_with_retry(
        journal_tx,
        journal_dir,
        bucket,
        prefix,
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
    bucket: String,
    prefix: String,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    retry_policy: RetryPolicy,
    max_concurrent: usize,
) -> (mpsc::Sender<UploadMessage>, tokio::task::JoinHandle<()>) {
    let retry_policy = Arc::new(retry_policy);

    // Create S3 storage and uploader.
    // S3 client creation is async, so we do it inside the spawned task.
    // But we need to return the sender immediately, so we create the channel now.
    let (upload_tx, upload_rx) = mpsc::channel::<UploadMessage>(256);
    let upload_tx_clone = upload_tx.clone();

    let handle = tokio::spawn(async move {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);
        let storage = Arc::new(S3SegmentStorage::new(client, bucket));

        let uploader = Arc::new(Uploader::new(
            storage,
            prefix,
            retry_policy.clone(),
            max_concurrent,
        ));

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

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Journal uploader: shutting down");
                        // Send shutdown to concurrent uploader.
                        let _ = upload_tx_clone.send(UploadMessage::Shutdown).await;
                        let _ = uploader_handle.await;
                        return;
                    }
                }
            }

            // Skip if circuit breaker is open.
            if let Some(cb) = retry_policy.circuit_breaker() {
                if !cb.should_allow() {
                    warn!(
                        "Journal uploader: circuit breaker open ({} consecutive failures), skipping cycle",
                        cb.consecutive_failures()
                    );
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
                info!("Journal uploader: sealed {}", path.display());
            }

            // 2. Scan for all sealed .graphj files and send Upload messages.
            match scan_sealed_segments(&journal_dir) {
                Ok(paths) => {
                    for path in paths {
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

/// Scan journal_dir for sealed .graphj files. Returns their paths.
fn scan_sealed_segments(journal_dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = std::fs::read_dir(journal_dir)
        .map_err(|e| anyhow!("Read journal dir: {e}"))?;

    let mut sealed = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| anyhow!("Read dir entry: {e}"))?;
        let path = entry.path();

        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.ends_with(".graphj") => n.to_string(),
            _ => continue,
        };

        // Skip non-sealed files.
        if !is_sealed(&path)? {
            continue;
        }

        // Skip the special names that aren't real segments.
        if name == "recovery.json" {
            continue;
        }

        sealed.push(path);
    }

    Ok(sealed)
}

/// Check if a .graphj file is sealed by reading its header.
fn is_sealed(path: &Path) -> Result<bool> {
    let mut f =
        std::fs::File::open(path).map_err(|e| anyhow!("Open {}: {e}", path.display()))?;
    match crate::graphj::read_header(&mut f) {
        Ok(Some(header)) => Ok(header.is_sealed()),
        Ok(None) => Ok(false),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Ok(false)
            } else {
                Err(anyhow!("Read header {}: {e}", path.display()))
            }
        }
    }
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
            "test/db/".to_string(),
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
        let path = write_fake_segment(dir.path(), "seg-0001.graphj", b"segment data");

        tx.send(UploadMessage::Upload(path)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(storage.object_count(), 1);
        assert!(storage.has_key("test/db/journal/seg-0001.graphj"));
    }

    #[tokio::test]
    async fn test_multiple_uploads() {
        let storage = Arc::new(MockStorage::new());
        let uploader = make_uploader(storage.clone(), 4);
        let (tx, handle) = spawn_uploader(uploader);

        let dir = tempfile::tempdir().unwrap();
        for i in 0..10 {
            let name = format!("seg-{:04}.graphj", i);
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
            let name = format!("seg-{:04}.graphj", i);
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
            let path = write_fake_segment(dir.path(), &format!("s-{i}.graphj"), b"data");
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
            let path = write_fake_segment(dir2.path(), &format!("c-{i}.graphj"), b"data");
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
            let path = write_fake_segment(dir.path(), &format!("seg-{i}.graphj"), b"data");
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
            let path = write_fake_segment(dir.path(), &format!("seg-{i}.graphj"), b"data");
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
        let path = write_fake_segment(dir.path(), "seg.graphj", b"data");
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
            let path = write_fake_segment(dir.path(), &format!("seg-{i}.graphj"), data);
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
}
