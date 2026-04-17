//! Follower sync: download new journal segments from a StorageBackend.
//!
//! Uses hadb-changeset's storage module for key layout and discovery. Retry is
//! the backend's responsibility; `hadb-storage-s3` and `hadb-storage-cinch`
//! both retry transient failures inside `get` / `list`, so this layer stays
//! thin.

use std::path::{Path, PathBuf};

use anyhow::anyhow;
use hadb_changeset::storage::{discover_after, ChangesetKind};
use hadb_storage::StorageBackend;
use tracing::debug;

/// Download journal segments newer than `since_seq`.
///
/// Returns `Vec<(local_path, seq)>` sorted by seq ascending.
pub async fn download_new_segments(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    journal_dir: &Path,
    since_seq: u64,
) -> Result<Vec<(PathBuf, u64)>, String> {
    download_new_segments_inner(storage, prefix, db_name, journal_dir, since_seq)
        .await
        .map_err(|e| e.to_string())
}

async fn download_new_segments_inner(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    journal_dir: &Path,
    since_seq: u64,
) -> anyhow::Result<Vec<(PathBuf, u64)>> {
    let discovered =
        discover_after(storage, prefix, db_name, since_seq, ChangesetKind::Journal).await?;

    if discovered.is_empty() {
        return Ok(Vec::new());
    }

    std::fs::create_dir_all(journal_dir)
        .map_err(|e| anyhow!("Failed to create journal dir: {e}"))?;

    let mut downloaded = Vec::new();
    for cs in &discovered {
        let local_path = download_single_segment(storage, &cs.key, cs.seq, journal_dir).await?;
        downloaded.push((local_path, cs.seq));
    }

    Ok(downloaded)
}

/// Download a single journal segment to the local journal dir.
async fn download_single_segment(
    storage: &dyn StorageBackend,
    key: &str,
    seq: u64,
    journal_dir: &Path,
) -> anyhow::Result<PathBuf> {
    let segment_bytes = storage
        .get(key)
        .await?
        .ok_or_else(|| anyhow!("segment key {key} not found"))?;

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
    use hadb_storage::CasResult;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// In-memory StorageBackend for testing sync without S3.
    struct MockStorage {
        objects: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                objects: Mutex::new(HashMap::new()),
            }
        }

        fn put_sync(&self, key: &str, data: Vec<u8>) {
            self.objects.lock().unwrap().insert(key.to_string(), data);
        }
    }

    #[async_trait]
    impl StorageBackend for MockStorage {
        async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }

        async fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), data.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }

        async fn list(&self, prefix: &str, after: Option<&str>) -> anyhow::Result<Vec<String>> {
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

        async fn put_if_absent(&self, key: &str, data: &[u8]) -> anyhow::Result<CasResult> {
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
        ) -> anyhow::Result<CasResult> {
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

    #[tokio::test]
    async fn test_download_with_mock_storage() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        // Keys use hadb-changeset format: {prefix}{db_name}/0000/{seq:016x}.hadbj
        store.put_sync("test/mydb/0000/0000000000000001.hadbj", b"seg1-data".to_vec());
        store.put_sync("test/mydb/0000/0000000000000005.hadbj", b"seg5-data".to_vec());

        let downloaded =
            download_new_segments(&store, "test/", "mydb", &journal_dir, 0).await.unwrap();
        assert_eq!(downloaded.len(), 2);
        assert_eq!(downloaded[0].1, 1);
        assert_eq!(downloaded[1].1, 5);

        // Skip past seq=1.
        let downloaded =
            download_new_segments(&store, "test/", "mydb", &journal_dir, 1).await.unwrap();
        assert_eq!(downloaded.len(), 1);
        assert_eq!(downloaded[0].1, 5);
    }
}
