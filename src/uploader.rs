//! Background uploader: sealed journal segments → S3.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tracing::{info, warn};

use crate::journal::{JournalCommand, JournalSender};
use crate::retry::RetryPolicy;

/// Spawn a background task that periodically seals the current journal segment
/// and uploads all sealed segments to S3.
///
/// S3 PutObject is idempotent — re-uploading sealed segments is harmless.
pub fn spawn_journal_uploader(
    journal_tx: JournalSender,
    journal_dir: PathBuf,
    bucket: String,
    prefix: String,
    interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    spawn_journal_uploader_with_retry(
        journal_tx,
        journal_dir,
        bucket,
        prefix,
        interval,
        shutdown,
        RetryPolicy::default_policy(),
    )
}

/// Spawn uploader with a custom retry policy.
pub fn spawn_journal_uploader_with_retry(
    journal_tx: JournalSender,
    journal_dir: PathBuf,
    bucket: String,
    prefix: String,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    retry_policy: RetryPolicy,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Create S3 client once, reuse for all uploads.
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Journal S3 uploader: shutting down");
                        return;
                    }
                }
            }

            // Skip upload cycle if circuit breaker is open.
            if let Some(cb) = retry_policy.circuit_breaker() {
                if !cb.should_allow() {
                    warn!(
                        "Journal S3 uploader: circuit breaker open ({} consecutive failures), skipping cycle",
                        cb.consecutive_failures()
                    );
                    continue;
                }
            }

            // 1. Ask journal writer to seal the current segment.
            let seal_result = {
                let tx = journal_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
                    if tx.send(JournalCommand::SealForUpload(ack_tx)).is_err() {
                        return None; // journal writer gone
                    }
                    ack_rx.recv().ok().flatten()
                })
                .await
                .unwrap_or(None)
            };

            if let Some(path) = seal_result {
                info!("Journal S3 uploader: sealed {}", path.display());
            }

            // 2. Scan for all sealed .graphj files and upload them.
            if let Err(e) =
                upload_sealed_segments(&client, &journal_dir, &bucket, &prefix, &retry_policy).await
            {
                // RetryPolicy already retried transient errors. If we're here,
                // the error is either non-retryable or retries were exhausted.
                tracing::error!("Journal S3 upload failed after retries: {e}");
            }
        }
    })
}

/// Scan journal_dir for sealed `.graphj` files and upload each to S3 with retry.
async fn upload_sealed_segments(
    client: &aws_sdk_s3::Client,
    journal_dir: &PathBuf,
    bucket: &str,
    prefix: &str,
    retry_policy: &RetryPolicy,
) -> anyhow::Result<()> {
    let entries = std::fs::read_dir(journal_dir)
        .map_err(|e| anyhow!("Read journal dir: {e}"))?;

    for entry in entries {
        let entry = entry.map_err(|e| anyhow!("Read dir entry: {e}"))?;
        let path = entry.path();

        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.ends_with(".graphj") => n.to_string(),
            _ => continue,
        };

        // Check if sealed by reading the header.
        if !is_sealed(&path)? {
            continue;
        }

        let key = format!("{}journal/{}", prefix, name);

        // Read file once, share across retry attempts via Arc.
        let bytes = Arc::new(
            std::fs::read(&path).map_err(|e| anyhow!("Read {}: {e}", path.display()))?,
        );

        let client = client.clone();
        let bucket = bucket.to_string();
        let key_clone = key.clone();
        let bytes_clone = bytes.clone();

        retry_policy
            .execute(|| {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key_clone.clone();
                let bytes = bytes_clone.clone();
                async move {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body((*bytes).clone().into())
                        .send()
                        .await
                        .map_err(|e| anyhow!("Upload {}: {e}", key))?;
                    Ok(())
                }
            })
            .await?;

        info!("Journal S3 uploader: uploaded {}", key);
    }

    Ok(())
}

/// Check if a .graphj file is sealed by reading its header.
fn is_sealed(path: &std::path::Path) -> anyhow::Result<bool> {
    let mut f =
        std::fs::File::open(path).map_err(|e| anyhow!("Open {}: {e}", path.display()))?;
    match crate::graphj::read_header(&mut f) {
        Ok(Some(header)) => Ok(header.is_sealed()),
        Ok(None) => Ok(false), // legacy or non-graphj file
        Err(e) => {
            // File might be too small (being written), skip it.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Ok(false)
            } else {
                Err(anyhow!("Read header {}: {e}", path.display()))
            }
        }
    }
}
