//! Follower sync: download new journal segments from S3.
//!
//! This module provides the S3 download primitives that followers use to
//! fetch journal segments. The caller (hakuzu or graphd) handles replay.

use std::path::{Path, PathBuf};
use tracing::debug;

/// Download journal segments from S3 that are newer than `since_seq`.
/// Returns `Vec<(local_path, seq)>` of downloaded segments, sorted by seq ascending.
///
/// Includes the "straddling" segment — the last segment whose first_seq is at or before
/// `since_seq`, because it may contain entries after `since_seq`.
pub async fn download_new_segments(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    journal_dir: &Path,
    since_seq: u64,
) -> Result<Vec<(PathBuf, u64)>, String> {
    // List all journal segments from S3.
    let segments = list_journal_segments_s3(s3_client, bucket, prefix, since_seq).await?;

    if segments.is_empty() {
        return Ok(Vec::new());
    }

    // Ensure journal dir exists.
    std::fs::create_dir_all(journal_dir)
        .map_err(|e| format!("Failed to create journal dir: {e}"))?;

    let mut downloaded = Vec::new();

    for (s3_key, seq) in &segments {
        let local_path =
            download_single_segment_s3(s3_client, bucket, s3_key, *seq, journal_dir).await?;
        downloaded.push((local_path, *seq));
    }

    Ok(downloaded)
}

/// List journal segments from S3 newer than `since_seq`.
/// Returns `Vec<(s3_key, seq)>` sorted by seq ascending.
///
/// Includes the straddling segment (last segment with first_seq <= since_seq)
/// because it may contain entries after since_seq.
async fn list_journal_segments_s3(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    since_seq: u64,
) -> Result<Vec<(String, u64)>, String> {
    let journal_prefix = if prefix.is_empty() {
        "journal/".to_string()
    } else {
        format!("{}journal/", prefix)
    };

    let list_resp = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&journal_prefix)
        .send()
        .await
        .map_err(|e| format!("S3 list journal failed: {e}"))?;

    let mut segments: Vec<(String, u64)> = Vec::new();
    // Track the last segment that starts at or before since_seq —
    // it may straddle the boundary and contain entries > since_seq.
    let mut last_before: Option<(String, u64)> = None;

    for obj in list_resp.contents() {
        if let Some(key) = obj.key() {
            if let Some(seq) = parse_journal_seq_from_key(key) {
                if seq > since_seq {
                    segments.push((key.to_string(), seq));
                } else if last_before.as_ref().map_or(true, |(_, s)| seq > *s) {
                    last_before = Some((key.to_string(), seq));
                }
            }
        }
    }

    // Include the straddling segment (it may contain entries after since_seq).
    if let Some(straddling) = last_before {
        segments.push(straddling);
    }

    segments.sort_by_key(|(_, seq)| *seq);
    Ok(segments)
}

/// Download a single journal segment from S3 to the local journal dir.
/// Returns the local path of the downloaded file.
async fn download_single_segment_s3(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    seq: u64,
    journal_dir: &Path,
) -> Result<PathBuf, String> {
    let get_resp = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("S3 get journal segment failed: {e}"))?;

    let segment_bytes = get_resp
        .body
        .collect()
        .await
        .map_err(|e| format!("S3 read segment failed: {e}"))?
        .into_bytes();

    let local_filename = format!("journal-{:016}.graphj", seq);
    let local_path = journal_dir.join(&local_filename);

    std::fs::write(&local_path, &segment_bytes)
        .map_err(|e| format!("Write journal segment failed: {e}"))?;

    debug!("Downloaded segment to {}", local_path.display());
    Ok(local_path)
}

/// Parse journal sequence from S3 key like "journal/journal-0000000000000100.graphj".
pub fn parse_journal_seq_from_key(key: &str) -> Option<u64> {
    let filename = key.rsplit('/').next()?;
    parse_journal_seq_from_filename(filename)
}

/// Parse journal sequence from filename like "journal-0000000000000100.graphj".
pub fn parse_journal_seq_from_filename(filename: &str) -> Option<u64> {
    let stem = filename.strip_prefix("journal-")?.strip_suffix(".graphj")?;
    u64::from_str_radix(stem, 10).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_seq_from_key() {
        assert_eq!(
            parse_journal_seq_from_key("journal/journal-0000000000000001.graphj"),
            Some(1)
        );
        assert_eq!(
            parse_journal_seq_from_key("prefix/journal/journal-0000000000000100.graphj"),
            Some(100)
        );
        assert_eq!(parse_journal_seq_from_key("journal/other.txt"), None);
    }

    #[test]
    fn test_parse_seq_from_filename() {
        assert_eq!(
            parse_journal_seq_from_filename("journal-0000000000000001.graphj"),
            Some(1)
        );
        assert_eq!(
            parse_journal_seq_from_filename("journal-0000000000000100.graphj"),
            Some(100)
        );
        assert_eq!(parse_journal_seq_from_filename("other.graphj"), None);
        assert_eq!(parse_journal_seq_from_filename("journal-001.wal"), None);
    }
}
