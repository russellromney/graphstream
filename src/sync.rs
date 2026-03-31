//! Follower sync: download new journal segments from object storage.
//!
//! This module provides the download primitives that followers use to
//! fetch journal segments. The caller (hakuzu or graphd) handles replay.

use std::path::{Path, PathBuf};

use anyhow::anyhow;
use hadb_io::{ObjectStore, RetryPolicy};
use tracing::debug;

/// Download journal segments that are newer than `since_seq`.
/// Returns `Vec<(local_path, seq)>` of downloaded segments, sorted by seq ascending.
///
/// Includes the "straddling" segment — the last segment whose first_seq is at or before
/// `since_seq`, because it may contain entries after `since_seq`.
///
/// Uses default retry policy. For custom retry, use `download_new_segments_with_retry`.
pub async fn download_new_segments(
    object_store: &dyn ObjectStore,
    prefix: &str,
    journal_dir: &Path,
    since_seq: u64,
) -> Result<Vec<(PathBuf, u64)>, String> {
    let policy = RetryPolicy::default_policy();
    download_new_segments_with_retry(object_store, prefix, journal_dir, since_seq, &policy)
        .await
        .map_err(|e| e.to_string())
}

/// Download journal segments with a custom retry policy.
pub async fn download_new_segments_with_retry(
    object_store: &dyn ObjectStore,
    prefix: &str,
    journal_dir: &Path,
    since_seq: u64,
    retry_policy: &RetryPolicy,
) -> anyhow::Result<Vec<(PathBuf, u64)>> {
    let segments =
        list_journal_segments(object_store, prefix, since_seq, retry_policy).await?;

    if segments.is_empty() {
        return Ok(Vec::new());
    }

    std::fs::create_dir_all(journal_dir)
        .map_err(|e| anyhow!("Failed to create journal dir: {e}"))?;

    let mut downloaded = Vec::new();

    for (s3_key, seq) in &segments {
        let local_path = download_single_segment(
            object_store,
            s3_key,
            *seq,
            journal_dir,
            retry_policy,
        )
        .await?;
        downloaded.push((local_path, *seq));
    }

    Ok(downloaded)
}

/// List journal segments newer than `since_seq`.
/// Returns `Vec<(key, seq)>` sorted by seq ascending.
///
/// Includes the straddling segment (last segment with first_seq <= since_seq)
/// because it may contain entries after since_seq.
async fn list_journal_segments(
    object_store: &dyn ObjectStore,
    prefix: &str,
    since_seq: u64,
    retry_policy: &RetryPolicy,
) -> anyhow::Result<Vec<(String, u64)>> {
    let journal_prefix = if prefix.is_empty() {
        "journal/".to_string()
    } else {
        format!("{}journal/", prefix)
    };

    let keys = retry_policy
        .execute(|| {
            let prefix = journal_prefix.clone();
            async move {
                object_store
                    .list_objects(&prefix)
                    .await
            }
        })
        .await?;

    let mut segments: Vec<(String, u64)> = Vec::new();
    let mut last_before: Option<(String, u64)> = None;

    for key in &keys {
        if let Some(seq) = parse_journal_seq_from_key(key) {
            if seq > since_seq {
                segments.push((key.clone(), seq));
            } else if last_before.as_ref().map_or(true, |(_, s)| seq > *s) {
                last_before = Some((key.clone(), seq));
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

/// Download a single journal segment to the local journal dir.
/// Returns the local path of the downloaded file.
async fn download_single_segment(
    object_store: &dyn ObjectStore,
    key: &str,
    seq: u64,
    journal_dir: &Path,
    retry_policy: &RetryPolicy,
) -> anyhow::Result<PathBuf> {
    let key_owned = key.to_string();

    let segment_bytes = retry_policy
        .execute(|| {
            let key = key_owned.clone();
            async move {
                object_store
                    .download_bytes(&key)
                    .await
            }
        })
        .await?;

    let local_filename = format!("journal-{:016}.graphj", seq);
    let local_path = journal_dir.join(&local_filename);

    std::fs::write(&local_path, &segment_bytes)
        .map_err(|e| anyhow!("Write journal segment failed: {e}"))?;

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
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// In-memory ObjectStore for testing sync without S3.
    struct MockObjectStore {
        objects: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MockObjectStore {
        fn new() -> Self {
            Self {
                objects: Mutex::new(HashMap::new()),
            }
        }

        fn put(&self, key: &str, data: Vec<u8>) {
            self.objects.lock().unwrap().insert(key.to_string(), data);
        }
    }

    #[async_trait]
    impl hadb_io::ObjectStore for MockObjectStore {
        async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> anyhow::Result<()> {
            self.objects.lock().unwrap().insert(key.to_string(), data);
            Ok(())
        }
        async fn upload_bytes_with_checksum(&self, key: &str, data: Vec<u8>, _checksum: &str) -> anyhow::Result<()> {
            self.upload_bytes(key, data).await
        }
        async fn upload_file(&self, key: &str, path: &Path) -> anyhow::Result<()> {
            let data = std::fs::read(path)?;
            self.upload_bytes(key, data).await
        }
        async fn upload_file_with_checksum(&self, key: &str, path: &Path, _checksum: &str) -> anyhow::Result<()> {
            self.upload_file(key, path).await
        }
        async fn download_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| anyhow!("Not found: {}", key))
        }
        async fn download_file(&self, key: &str, path: &Path) -> anyhow::Result<()> {
            let data = self.download_bytes(key).await?;
            std::fs::write(path, data)?;
            Ok(())
        }
        async fn list_objects(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            let objects = self.objects.lock().unwrap();
            let mut keys: Vec<String> = objects
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }
        async fn list_objects_after(&self, prefix: &str, start_after: &str) -> anyhow::Result<Vec<String>> {
            let objects = self.objects.lock().unwrap();
            let mut keys: Vec<String> = objects
                .keys()
                .filter(|k| k.starts_with(prefix) && k.as_str() > start_after)
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }
        async fn exists(&self, key: &str) -> anyhow::Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        async fn get_checksum(&self, _key: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }
        async fn delete_objects(&self, keys: &[String]) -> anyhow::Result<usize> {
            let mut objects = self.objects.lock().unwrap();
            let mut count = 0;
            for key in keys {
                if objects.remove(key).is_some() {
                    count += 1;
                }
            }
            Ok(count)
        }
        fn bucket_name(&self) -> &str {
            "mock-bucket"
        }
    }

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

    #[tokio::test]
    async fn test_download_with_mock_object_store() {
        let store = MockObjectStore::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        // Put some fake journal segments.
        store.put("test/journal/journal-0000000000000001.graphj", b"seg1-data".to_vec());
        store.put("test/journal/journal-0000000000000005.graphj", b"seg5-data".to_vec());
        store.put("test/journal/journal-0000000000000010.graphj", b"seg10-data".to_vec());

        // Download all segments (since_seq = 0).
        let segments = download_new_segments(&store, "test/", &journal_dir, 0)
            .await
            .unwrap();

        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].1, 1);
        assert_eq!(segments[1].1, 5);
        assert_eq!(segments[2].1, 10);

        // Verify files exist on disk.
        for (path, _seq) in &segments {
            assert!(path.exists(), "Downloaded file should exist: {}", path.display());
        }

        // Verify content.
        let content = std::fs::read(&segments[0].0).unwrap();
        assert_eq!(content, b"seg1-data");
    }

    #[tokio::test]
    async fn test_download_straddling_with_mock() {
        let store = MockObjectStore::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put("p/journal/journal-0000000000000001.graphj", b"s1".to_vec());
        store.put("p/journal/journal-0000000000000005.graphj", b"s5".to_vec());
        store.put("p/journal/journal-0000000000000010.graphj", b"s10".to_vec());
        store.put("p/journal/journal-0000000000000015.graphj", b"s15".to_vec());

        // Download since_seq=7 -- should get straddling (5) plus 10 and 15.
        let segments = download_new_segments(&store, "p/", &journal_dir, 7)
            .await
            .unwrap();

        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].1, 5);  // straddling
        assert_eq!(segments[1].1, 10);
        assert_eq!(segments[2].1, 15);
    }

    #[tokio::test]
    async fn test_download_empty_store() {
        let store = MockObjectStore::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        let segments = download_new_segments(&store, "empty/", &journal_dir, 0)
            .await
            .unwrap();

        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn test_download_no_prefix() {
        let store = MockObjectStore::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put("journal/journal-0000000000000001.graphj", b"data".to_vec());

        // Empty prefix.
        let segments = download_new_segments(&store, "", &journal_dir, 0)
            .await
            .unwrap();

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].1, 1);
    }
}
