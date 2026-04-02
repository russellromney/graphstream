//! Follower sync: download new journal segments from object storage.
//!
//! Uses hadb-changeset's storage module for S3 key layout and discovery.

use std::path::{Path, PathBuf};

use anyhow::anyhow;
use hadb_changeset::storage::{discover_after, ChangesetKind};
use hadb_io::{ObjectStore, RetryPolicy};
use tracing::debug;

/// Download journal segments that are newer than `since_seq`.
/// Returns `Vec<(local_path, seq)>` of downloaded segments, sorted by seq ascending.
///
/// Uses `discover_after` from hadb-changeset which handles the S3 listing
/// and key parsing. No straddling segment logic needed -- JournalReader
/// already skips entries before start_seq.
pub async fn download_new_segments(
    object_store: &dyn ObjectStore,
    prefix: &str,
    db_name: &str,
    journal_dir: &Path,
    since_seq: u64,
) -> Result<Vec<(PathBuf, u64)>, String> {
    let policy = RetryPolicy::default_policy();
    download_new_segments_with_retry(object_store, prefix, db_name, journal_dir, since_seq, &policy)
        .await
        .map_err(|e| e.to_string())
}

/// Download journal segments with a custom retry policy.
pub async fn download_new_segments_with_retry(
    object_store: &dyn ObjectStore,
    prefix: &str,
    db_name: &str,
    journal_dir: &Path,
    since_seq: u64,
    retry_policy: &RetryPolicy,
) -> anyhow::Result<Vec<(PathBuf, u64)>> {
    // Use hadb-changeset discover_after for S3 listing.
    let discovered = retry_policy
        .execute(|| {
            let prefix = prefix.to_string();
            let db_name = db_name.to_string();
            async move {
                discover_after(
                    object_store,
                    &prefix,
                    &db_name,
                    since_seq,
                    ChangesetKind::Journal,
                )
                .await
            }
        })
        .await?;

    if discovered.is_empty() {
        return Ok(Vec::new());
    }

    std::fs::create_dir_all(journal_dir)
        .map_err(|e| anyhow!("Failed to create journal dir: {e}"))?;

    let mut downloaded = Vec::new();

    for cs in &discovered {
        let local_path = download_single_segment(
            object_store,
            &cs.key,
            cs.seq,
            journal_dir,
            retry_policy,
        )
        .await?;
        downloaded.push((local_path, cs.seq));
    }

    Ok(downloaded)
}

/// Download a single journal segment to the local journal dir.
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

    let local_filename = format!("journal-{:016}.hadbj", seq);
    let local_path = journal_dir.join(&local_filename);

    std::fs::write(&local_path, &segment_bytes)
        .map_err(|e| anyhow!("Write journal segment failed: {e}"))?;

    debug!("Downloaded segment to {}", local_path.display());
    Ok(local_path)
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

    #[tokio::test]
    async fn test_download_with_mock_object_store() {
        let store = MockObjectStore::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        // Keys use hadb-changeset format: {prefix}{db_name}/0000/{seq:016x}.hadbj
        store.put("test/mydb/0000/0000000000000001.hadbj", b"seg1-data".to_vec());
        store.put("test/mydb/0000/0000000000000005.hadbj", b"seg5-data".to_vec());
        store.put("test/mydb/0000/000000000000000a.hadbj", b"seg10-data".to_vec());

        let segments = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap();

        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].1, 1);
        assert_eq!(segments[1].1, 5);
        assert_eq!(segments[2].1, 10);

        for (path, _seq) in &segments {
            assert!(path.exists(), "Downloaded file should exist: {}", path.display());
        }

        let content = std::fs::read(&segments[0].0).unwrap();
        assert_eq!(content, b"seg1-data");
    }

    #[tokio::test]
    async fn test_download_since_seq() {
        let store = MockObjectStore::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put("p/mydb/0000/0000000000000001.hadbj", b"s1".to_vec());
        store.put("p/mydb/0000/0000000000000005.hadbj", b"s5".to_vec());
        store.put("p/mydb/0000/000000000000000a.hadbj", b"s10".to_vec());
        store.put("p/mydb/0000/000000000000000f.hadbj", b"s15".to_vec());

        // Download since_seq=7 -- should get 10 and 15 (discover_after returns only > since_seq).
        let segments = download_new_segments(&store, "p/", "mydb", &journal_dir, 7)
            .await
            .unwrap();

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].1, 10);
        assert_eq!(segments[1].1, 15);
    }

    #[tokio::test]
    async fn test_download_empty_store() {
        let store = MockObjectStore::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        let segments = download_new_segments(&store, "empty/", "mydb", &journal_dir, 0)
            .await
            .unwrap();

        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn test_download_format_key_matches() {
        use hadb_changeset::storage::{format_key, GENERATION_INCREMENTAL};
        // Verify format_key produces the expected S3 key format.
        let key = format_key("test/", "mydb", GENERATION_INCREMENTAL, 42, ChangesetKind::Journal);
        assert_eq!(key, "test/mydb/0000/000000000000002a.hadbj");
    }
}
