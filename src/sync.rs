//! Follower sync: download new journal segments from a StorageBackend.
//!
//! Uses hadb-changeset's storage key layout, then verifies each `.hadbj`
//! segment's key, header, body checksum, and decoded entry coverage before
//! considering it part of the replay range. Retry is the backend's
//! responsibility; `hadb-storage-s3` and `hadb-storage-cinch` both retry
//! transient failures inside `get` / `list`, so this layer stays thin.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail};
use hadb_changeset::journal::{decode, decode_header, JournalSegment};
use hadb_changeset::storage::GENERATION_INCREMENTAL;
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
    let mut segments =
        discover_verified_journal_segments(storage, prefix, db_name, since_seq).await?;
    if segments.is_empty() {
        return Ok(Vec::new());
    }

    segments.sort_by_key(|segment| (segment.first_seq, segment.last_seq));
    let mut expected_next = since_seq.saturating_add(1);
    let mut selected = Vec::new();
    for segment in segments {
        if segment.last_seq < expected_next {
            continue;
        }
        if segment.first_seq > expected_next {
            bail!(
                "journal gap after seq {}: next segment {} starts at {}",
                expected_next.saturating_sub(1),
                segment.key,
                segment.first_seq
            );
        }
        if !selected.is_empty() && segment.first_seq < expected_next {
            bail!(
                "overlapping journal segment {} extends already-selected range at seq {}",
                segment.key,
                expected_next.saturating_sub(1)
            );
        }
        expected_next = segment
            .last_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("journal sequence overflow after {}", segment.key))?;
        selected.push(segment);
    }

    std::fs::create_dir_all(journal_dir)
        .map_err(|e| anyhow!("Failed to create journal dir: {e}"))?;

    let mut downloaded = Vec::new();
    for segment in &selected {
        let local_path = write_verified_segment(segment, journal_dir)?;
        downloaded.push((local_path, segment.first_seq));
    }

    Ok(downloaded)
}

struct VerifiedJournalSegment {
    key: String,
    first_seq: u64,
    last_seq: u64,
    bytes: Vec<u8>,
}

async fn discover_verified_journal_segments(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    since_seq: u64,
) -> anyhow::Result<Vec<VerifiedJournalSegment>> {
    let journal_prefix = format!("{}{}/{:04x}/", prefix, db_name, GENERATION_INCREMENTAL);
    let mut keys = storage.list(&journal_prefix, None).await?;
    keys.sort();

    let mut segments = Vec::new();
    for key in keys {
        let Some(storage_seq) = parse_journal_key(&journal_prefix, &key) else {
            continue;
        };
        let bytes = storage
            .get(&key)
            .await?
            .ok_or_else(|| anyhow!("journal segment key {key} disappeared after list"))?;
        let header =
            decode_header(&bytes).map_err(|e| anyhow!("decode journal header for {key}: {e}"))?;
        if header.first_seq != storage_seq {
            bail!(
                "journal key/header sequence mismatch for {key}: key seq {}, header first_seq {}",
                storage_seq,
                header.first_seq
            );
        }
        if !header.is_sealed() {
            bail!("journal segment {key} is not sealed");
        }
        if header.last_seq < header.first_seq {
            bail!(
                "journal segment {key} has invalid range {}..{}",
                header.first_seq,
                header.last_seq
            );
        }
        let segment =
            decode(&bytes).map_err(|e| anyhow!("verify journal segment {key}: {e}"))?;
        validate_decoded_journal_segment(&key, &segment)?;
        if segment.header.last_seq <= since_seq {
            continue;
        }
        segments.push(VerifiedJournalSegment {
            key,
            first_seq: segment.header.first_seq,
            last_seq: segment.header.last_seq,
            bytes,
        });
    }

    Ok(segments)
}

fn validate_decoded_journal_segment(key: &str, segment: &JournalSegment) -> anyhow::Result<()> {
    if segment.entries.is_empty() {
        bail!("journal segment {key} has no decoded entries");
    }

    let first_entry = segment.entries.first().expect("checked non-empty").sequence;
    let last_entry = segment.entries.last().expect("checked non-empty").sequence;
    if first_entry != segment.header.first_seq {
        bail!(
            "journal segment {key} header first_seq {} does not match first entry {}",
            segment.header.first_seq,
            first_entry
        );
    }
    if last_entry != segment.header.last_seq {
        bail!(
            "journal segment {key} header last_seq {} does not match last entry {}",
            segment.header.last_seq,
            last_entry
        );
    }

    let mut expected = segment.header.first_seq;
    for entry in &segment.entries {
        if entry.sequence != expected {
            bail!(
                "journal segment {key} has non-contiguous entry sequence {}; expected {}",
                entry.sequence,
                expected
            );
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| anyhow!("journal segment {key} sequence overflow"))?;
    }

    Ok(())
}

fn parse_journal_key(journal_prefix: &str, key: &str) -> Option<u64> {
    let filename = key.strip_prefix(journal_prefix)?;
    let seq_hex = filename.strip_suffix(".hadbj")?;
    if seq_hex.contains('/') || seq_hex.is_empty() {
        return None;
    }
    u64::from_str_radix(seq_hex, 16).ok()
}

fn write_verified_segment(
    segment: &VerifiedJournalSegment,
    journal_dir: &Path,
) -> anyhow::Result<PathBuf> {
    let local_filename = format!("journal-{:016}.hadbj", segment.first_seq);
    let local_path = journal_dir.join(&local_filename);

    std::fs::write(&local_path, &segment.bytes)
        .map_err(|e| anyhow!("Write journal segment failed: {e}"))?;

    debug!("Downloaded segment to {}", local_path.display());
    Ok(local_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hadb_changeset::journal::{
        compute_entry_hash, encode, encode_header, seal, JournalEntry, JournalHeader,
    };
    use hadb_changeset::storage::{format_key, ChangesetKind};
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

    fn journal_key(prefix: &str, db_name: &str, seq: u64) -> String {
        format_key(
            prefix,
            db_name,
            GENERATION_INCREMENTAL,
            seq,
            ChangesetKind::Journal,
        )
    }

    fn physical_key(prefix: &str, db_name: &str, seq: u64) -> String {
        format_key(
            prefix,
            db_name,
            GENERATION_INCREMENTAL,
            seq,
            ChangesetKind::Physical,
        )
    }

    fn segment_bytes_for_sequences(sequences: &[u64]) -> Vec<u8> {
        let mut prev_hash = [0u8; 32];
        let mut entries = Vec::new();
        for &sequence in sequences {
            let payload = format!("entry-{sequence}").into_bytes();
            let entry = JournalEntry {
                sequence,
                prev_hash,
                payload,
            };
            prev_hash = compute_entry_hash(&entry.prev_hash, &entry.payload);
            entries.push(entry);
        }
        encode(&seal(entries, 0))
    }

    fn segment_bytes(first_seq: u64, entry_count: u64) -> Vec<u8> {
        let sequences: Vec<u64> = (0..entry_count).map(|offset| first_seq + offset).collect();
        segment_bytes_for_sequences(&sequences)
    }

    fn unsealed_segment_header(first_seq: u64) -> Vec<u8> {
        let header = JournalHeader {
            flags: 0,
            compression: 0,
            first_seq,
            last_seq: 0,
            entry_count: 0,
            body_len: 0,
            body_checksum: [0u8; 32],
            prev_segment_checksum: 0,
            created_ms: 0,
        };
        encode_header(&header).to_vec()
    }

    #[tokio::test]
    async fn downloads_verified_hadbj_segments_and_filters_other_objects() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put_sync(&journal_key("test/", "mydb", 1), segment_bytes(1, 2));
        store.put_sync(&journal_key("test/", "mydb", 3), segment_bytes(3, 2));
        store.put_sync(&physical_key("test/", "mydb", 1), b"not-a-journal".to_vec());
        store.put_sync("test/mydb/0001/0000000000000001.hadbj", segment_bytes(1, 1));
        store.put_sync(
            "test/mydb/0000/0000000000000005.graphj",
            b"wrong-ext".to_vec(),
        );
        store.put_sync(&journal_key("test/", "otherdb", 1), segment_bytes(1, 1));

        let downloaded = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap();
        assert_eq!(downloaded.len(), 2);
        assert_eq!(downloaded[0].1, 1);
        assert_eq!(downloaded[1].1, 3);
        assert!(journal_dir.join("journal-0000000000000001.hadbj").exists());
        assert!(journal_dir.join("journal-0000000000000003.hadbj").exists());
    }

    #[tokio::test]
    async fn since_inside_segment_downloads_covering_segment() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put_sync(&journal_key("test/", "mydb", 1), segment_bytes(1, 10));

        let downloaded = download_new_segments(&store, "test/", "mydb", &journal_dir, 5)
            .await
            .unwrap();
        assert_eq!(downloaded.len(), 1);
        assert_eq!(downloaded[0].1, 1);
        assert!(journal_dir.join("journal-0000000000000001.hadbj").exists());
    }

    #[tokio::test]
    async fn gap_between_journal_segments_is_hard_error() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put_sync(&journal_key("test/", "mydb", 1), segment_bytes(1, 2));
        store.put_sync(&journal_key("test/", "mydb", 5), segment_bytes(5, 2));

        let err = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap_err();
        assert!(err.contains("journal gap"), "{err}");
        assert!(!journal_dir.exists());
    }

    #[tokio::test]
    async fn overlapping_segments_that_extend_selected_range_are_hard_error() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put_sync(&journal_key("test/", "mydb", 1), segment_bytes(1, 10));
        store.put_sync(&journal_key("test/", "mydb", 5), segment_bytes(5, 11));

        let err = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap_err();
        assert!(err.contains("overlapping journal segment"), "{err}");
        assert!(!journal_dir.exists());
    }

    #[tokio::test]
    async fn corrupt_or_truncated_segment_is_rejected_before_local_write() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");
        let mut bytes = segment_bytes(1, 2);
        bytes.truncate(bytes.len() - 8);

        store.put_sync(&journal_key("test/", "mydb", 1), bytes);

        let err = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap_err();
        assert!(err.contains("verify journal segment"), "{err}");
        assert!(!journal_dir.exists());
    }

    #[tokio::test]
    async fn journal_key_must_match_header_first_sequence() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put_sync(&journal_key("test/", "mydb", 5), segment_bytes(1, 2));

        let err = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap_err();
        assert!(err.contains("key/header sequence mismatch"), "{err}");
        assert!(!journal_dir.exists());
    }

    #[tokio::test]
    async fn header_range_must_match_decoded_entries() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");
        let mut bytes = segment_bytes(1, 2);
        bytes[20..28].copy_from_slice(&10u64.to_be_bytes());

        store.put_sync(&journal_key("test/", "mydb", 1), bytes);

        let err = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap_err();
        assert!(err.contains("header last_seq"), "{err}");
        assert!(!journal_dir.exists());
    }

    #[tokio::test]
    async fn decoded_entries_must_be_contiguous() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put_sync(&journal_key("test/", "mydb", 1), segment_bytes_for_sequences(&[1, 3]));

        let err = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap_err();
        assert!(err.contains("non-contiguous"), "{err}");
        assert!(!journal_dir.exists());
    }

    #[tokio::test]
    async fn unsealed_remote_segment_is_rejected() {
        let store = MockStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");

        store.put_sync(&journal_key("test/", "mydb", 1), unsealed_segment_header(1));

        let err = download_new_segments(&store, "test/", "mydb", &journal_dir, 0)
            .await
            .unwrap_err();
        assert!(err.contains("is not sealed"), "{err}");
        assert!(!journal_dir.exists());
    }
}
