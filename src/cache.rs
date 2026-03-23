//! Disk-based segment cache for tracking upload state.
//!
//! Sealed segments already live in the journal directory. The cache tracks which
//! segments have been uploaded to S3 via a manifest file. This enables:
//! - **Crash recovery**: pending uploads survive restart (scan disk, diff vs manifest)
//! - **Cleanup**: delete old uploaded segments by age and size limits
//!
//! Manifest is persisted atomically (write .tmp + rename) to survive crashes.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

// ---------------------------------------------------------------------------
// Manifest — on-disk state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Manifest {
    /// Segment filenames that have been successfully uploaded to S3.
    uploaded: HashSet<String>,
    /// Total bytes of uploaded segments (approximate — not updated on cleanup).
    total_bytes: u64,
}

// ---------------------------------------------------------------------------
// CacheStats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub pending_count: usize,
    pub uploaded_count: usize,
    pub total_bytes_on_disk: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CleanupStats {
    pub files_deleted: usize,
    pub bytes_freed: u64,
}

// ---------------------------------------------------------------------------
// CacheConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Delete uploaded segments older than this.
    pub retention: Duration,
    /// Delete oldest uploaded segments when total size exceeds this.
    pub max_size: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            retention: Duration::from_secs(3600), // 1 hour
            max_size: 256 * 1024 * 1024,          // 256 MB
        }
    }
}

// ---------------------------------------------------------------------------
// SegmentCache
// ---------------------------------------------------------------------------

pub struct SegmentCache {
    journal_dir: PathBuf,
    manifest_path: PathBuf,
    manifest: Manifest,
}

impl SegmentCache {
    /// Open or create a segment cache for the given journal directory.
    /// Loads existing manifest if present; scans for crash recovery.
    pub fn new(journal_dir: &Path) -> Result<Self> {
        let manifest_path = journal_dir.join("upload_manifest.json");
        let manifest = if manifest_path.exists() {
            let data = std::fs::read_to_string(&manifest_path)
                .map_err(|e| anyhow!("Read manifest: {e}"))?;
            serde_json::from_str(&data)
                .map_err(|e| anyhow!("Corrupt upload manifest: {e}"))?
        } else {
            Manifest::default()
        };

        let mut cache = Self {
            journal_dir: journal_dir.to_path_buf(),
            manifest_path,
            manifest,
        };

        // Crash recovery: remove manifest entries for segments that no longer exist on disk.
        cache.reconcile()?;

        Ok(cache)
    }

    /// Reconcile manifest with disk: remove entries for segments that no longer exist.
    fn reconcile(&mut self) -> Result<()> {
        let on_disk = self.sealed_segment_names()?;
        let stale: Vec<String> = self
            .manifest
            .uploaded
            .iter()
            .filter(|name| !on_disk.contains(name.as_str()))
            .cloned()
            .collect();

        if !stale.is_empty() {
            info!(
                "Cache reconcile: removing {} stale manifest entries",
                stale.len()
            );
            for name in stale {
                self.manifest.uploaded.remove(&name);
            }
            self.persist()?;
        }

        Ok(())
    }

    /// Mark a segment as uploaded. Persists manifest atomically.
    pub fn mark_uploaded(&mut self, segment_name: &str) -> Result<()> {
        if self.manifest.uploaded.insert(segment_name.to_string()) {
            // Get file size for stats.
            let path = self.journal_dir.join(segment_name);
            if let Ok(meta) = std::fs::metadata(&path) {
                self.manifest.total_bytes += meta.len();
            }
            self.persist()?;
        }
        Ok(())
    }

    /// Get sealed .graphj files NOT yet marked as uploaded.
    pub fn pending_segments(&self) -> Result<Vec<PathBuf>> {
        let mut pending = Vec::new();
        for entry in std::fs::read_dir(&self.journal_dir)
            .map_err(|e| anyhow!("Read journal dir: {e}"))?
        {
            let entry = entry.map_err(|e| anyhow!("Read dir entry: {e}"))?;
            let path = entry.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) if n.ends_with(".graphj") => n.to_string(),
                _ => continue,
            };

            if self.manifest.uploaded.contains(&name) {
                continue;
            }

            // Check if sealed.
            if crate::graphj::is_file_sealed(&path).map_err(|e| anyhow!("{e}"))? {
                pending.push(path);
            }
        }
        pending.sort();
        Ok(pending)
    }

    /// Delete uploaded segments based on age and size limits.
    /// Never deletes pending (not-yet-uploaded) segments.
    pub fn cleanup(&mut self, config: &CacheConfig) -> Result<CleanupStats> {
        let now = SystemTime::now();
        let mut stats = CleanupStats::default();

        // Collect uploaded segments with their metadata.
        let mut uploaded_files: Vec<(String, PathBuf, u64, SystemTime)> = Vec::new();
        for name in &self.manifest.uploaded {
            let path = self.journal_dir.join(name);
            if let Ok(meta) = std::fs::metadata(&path) {
                let modified = meta.modified().unwrap_or(now);
                uploaded_files.push((name.clone(), path, meta.len(), modified));
            }
        }

        // Sort by age (oldest first) for consistent cleanup.
        uploaded_files.sort_by_key(|(_, _, _, modified)| *modified);

        // Phase 1: age-based cleanup.
        let mut to_delete = Vec::new();
        for (name, path, size, modified) in &uploaded_files {
            let age = now.duration_since(*modified).unwrap_or_default();
            if age > config.retention {
                to_delete.push((name.clone(), path.clone(), *size));
            }
        }

        // Phase 2: size-based cleanup (if still over limit after age cleanup).
        let mut remaining_bytes: u64 = uploaded_files.iter().map(|(_, _, s, _)| s).sum();
        let delete_names: HashSet<String> = to_delete.iter().map(|(n, _, _)| n.clone()).collect();
        remaining_bytes -= to_delete.iter().map(|(_, _, s)| s).sum::<u64>();

        if remaining_bytes > config.max_size {
            for (name, path, size, _) in &uploaded_files {
                if delete_names.contains(name) {
                    continue;
                }
                if remaining_bytes <= config.max_size {
                    break;
                }
                to_delete.push((name.clone(), path.clone(), *size));
                remaining_bytes -= size;
            }
        }

        // Execute deletions.
        for (name, path, size) in to_delete {
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    stats.files_deleted += 1;
                    stats.bytes_freed += size;
                    self.manifest.uploaded.remove(&name);
                    self.manifest.total_bytes = self.manifest.total_bytes.saturating_sub(size);
                }
                Err(e) => {
                    error!("Failed to delete {}: {e}", path.display());
                }
            }
        }

        if stats.files_deleted > 0 {
            self.persist()?;
            info!(
                "Cache cleanup: deleted {} files, freed {} bytes",
                stats.files_deleted, stats.bytes_freed
            );
        }

        Ok(stats)
    }

    /// Get cache statistics.
    pub fn stats(&self) -> Result<CacheStats> {
        let mut total_bytes = 0u64;
        let mut pending_count = 0;
        for entry in std::fs::read_dir(&self.journal_dir)
            .map_err(|e| anyhow!("Read journal dir: {e}"))?
        {
            let entry = entry.map_err(|e| anyhow!("Read dir entry: {e}"))?;
            let path = entry.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) if n.ends_with(".graphj") => n.to_string(),
                _ => continue,
            };
            if let Ok(meta) = std::fs::metadata(&path) {
                total_bytes += meta.len();
            }
            if !self.manifest.uploaded.contains(&name) {
                if crate::graphj::is_file_sealed(&path).unwrap_or(false) {
                    pending_count += 1;
                }
            }
        }

        Ok(CacheStats {
            pending_count,
            uploaded_count: self.manifest.uploaded.len(),
            total_bytes_on_disk: total_bytes,
        })
    }

    /// Is a segment marked as uploaded?
    pub fn is_uploaded(&self, segment_name: &str) -> bool {
        self.manifest.uploaded.contains(segment_name)
    }

    /// Get all sealed segment filenames on disk.
    fn sealed_segment_names(&self) -> Result<HashSet<String>> {
        let mut names = HashSet::new();
        let entries = std::fs::read_dir(&self.journal_dir)
            .map_err(|e| anyhow!("Read journal dir: {e}"))?;
        for entry in entries {
            let entry = entry.map_err(|e| anyhow!("Read dir entry: {e}"))?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".graphj") {
                    names.insert(name.to_string());
                }
            }
        }
        Ok(names)
    }

    /// Persist manifest atomically: write .tmp, fsync, rename.
    fn persist(&self) -> Result<()> {
        let tmp_path = self.manifest_path.with_extension("json.tmp");
        let data = serde_json::to_string_pretty(&self.manifest)
            .map_err(|e| anyhow!("Serialize manifest: {e}"))?;
        std::fs::write(&tmp_path, data.as_bytes())
            .map_err(|e| anyhow!("Write manifest tmp: {e}"))?;

        // fsync the tmp file.
        let f = std::fs::File::open(&tmp_path)
            .map_err(|e| anyhow!("Open manifest tmp for fsync: {e}"))?;
        f.sync_all()
            .map_err(|e| anyhow!("Fsync manifest tmp: {e}"))?;

        std::fs::rename(&tmp_path, &self.manifest_path)
            .map_err(|e| anyhow!("Rename manifest: {e}"))?;
        Ok(())
    }
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a fake sealed segment (128-byte header with sealed flag + body).
    fn write_fake_sealed_segment(dir: &Path, name: &str, body_size: usize) -> PathBuf {
        use crate::graphj;

        let path = dir.join(name);
        let mut header = graphj::GraphjHeader::new_unsealed(1, 0);
        header.flags |= graphj::FLAG_SEALED;
        header.body_len = body_size as u64;
        let header_bytes = graphj::encode_header(&header);
        let body = vec![0u8; body_size];
        let mut data = Vec::new();
        data.extend_from_slice(&header_bytes);
        data.extend_from_slice(&body);
        std::fs::write(&path, &data).unwrap();
        path
    }

    #[test]
    fn test_cache_new_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SegmentCache::new(dir.path()).unwrap();
        let stats = cache.stats().unwrap();
        assert_eq!(stats.pending_count, 0);
        assert_eq!(stats.uploaded_count, 0);
    }

    #[test]
    fn test_cache_mark_uploaded() {
        let dir = tempfile::tempdir().unwrap();
        write_fake_sealed_segment(dir.path(), "seg-0001.graphj", 100);
        write_fake_sealed_segment(dir.path(), "seg-0002.graphj", 200);

        let mut cache = SegmentCache::new(dir.path()).unwrap();
        assert_eq!(cache.pending_segments().unwrap().len(), 2);

        cache.mark_uploaded("seg-0001.graphj").unwrap();
        assert!(cache.is_uploaded("seg-0001.graphj"));
        assert!(!cache.is_uploaded("seg-0002.graphj"));
        assert_eq!(cache.pending_segments().unwrap().len(), 1);
    }

    #[test]
    fn test_cache_crash_recovery() {
        let dir = tempfile::tempdir().unwrap();
        write_fake_sealed_segment(dir.path(), "seg-0001.graphj", 100);
        write_fake_sealed_segment(dir.path(), "seg-0002.graphj", 200);

        // First open: mark one as uploaded.
        {
            let mut cache = SegmentCache::new(dir.path()).unwrap();
            cache.mark_uploaded("seg-0001.graphj").unwrap();
        }

        // Reopen: manifest should persist.
        let cache = SegmentCache::new(dir.path()).unwrap();
        assert!(cache.is_uploaded("seg-0001.graphj"));
        assert!(!cache.is_uploaded("seg-0002.graphj"));
        assert_eq!(cache.pending_segments().unwrap().len(), 1);
    }

    #[test]
    fn test_cache_reconcile_stale_entries() {
        let dir = tempfile::tempdir().unwrap();
        write_fake_sealed_segment(dir.path(), "seg-0001.graphj", 100);

        // Mark as uploaded.
        {
            let mut cache = SegmentCache::new(dir.path()).unwrap();
            cache.mark_uploaded("seg-0001.graphj").unwrap();
        }

        // Delete the file (simulating manual deletion or external cleanup).
        std::fs::remove_file(dir.path().join("seg-0001.graphj")).unwrap();

        // Reopen: reconcile should remove stale entry.
        let cache = SegmentCache::new(dir.path()).unwrap();
        assert!(!cache.is_uploaded("seg-0001.graphj"));
    }

    #[test]
    fn test_cleanup_age_based() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_fake_sealed_segment(dir.path(), "seg-0001.graphj", 100);

        let mut cache = SegmentCache::new(dir.path()).unwrap();
        cache.mark_uploaded("seg-0001.graphj").unwrap();

        // Set file mtime to 2 hours ago.
        let two_hours_ago = std::time::SystemTime::now() - Duration::from_secs(7200);
        filetime::set_file_mtime(
            &path,
            filetime::FileTime::from_system_time(two_hours_ago),
        )
        .unwrap();

        let config = CacheConfig {
            retention: Duration::from_secs(3600), // 1 hour
            max_size: u64::MAX,
        };
        let stats = cache.cleanup(&config).unwrap();
        assert_eq!(stats.files_deleted, 1);
        assert!(!path.exists());
        assert!(!cache.is_uploaded("seg-0001.graphj"));
    }

    #[test]
    fn test_cleanup_size_based() {
        let dir = tempfile::tempdir().unwrap();
        // Create 3 segments: total size exceeds max.
        for i in 1..=3 {
            write_fake_sealed_segment(dir.path(), &format!("seg-{i:04}.graphj"), 1000);
        }

        let mut cache = SegmentCache::new(dir.path()).unwrap();
        for i in 1..=3 {
            cache
                .mark_uploaded(&format!("seg-{i:04}.graphj"))
                .unwrap();
        }

        let config = CacheConfig {
            retention: Duration::from_secs(86400), // 24 hours — won't trigger
            max_size: 1500,                         // Only 1-2 segments fit
        };
        let stats = cache.cleanup(&config).unwrap();
        assert!(stats.files_deleted >= 1, "Should delete at least 1 segment");
    }

    #[test]
    fn test_cleanup_never_deletes_pending() {
        let dir = tempfile::tempdir().unwrap();
        let pending_path = write_fake_sealed_segment(dir.path(), "seg-0001.graphj", 100);

        let mut cache = SegmentCache::new(dir.path()).unwrap();
        // Do NOT mark as uploaded — it's pending.

        let config = CacheConfig {
            retention: Duration::from_secs(0), // instant expiry
            max_size: 0,                        // zero max size
        };
        let stats = cache.cleanup(&config).unwrap();
        assert_eq!(stats.files_deleted, 0, "Should not delete pending segments");
        assert!(pending_path.exists());
    }

    #[test]
    fn test_cache_idempotent_mark() {
        let dir = tempfile::tempdir().unwrap();
        write_fake_sealed_segment(dir.path(), "seg-0001.graphj", 100);

        let mut cache = SegmentCache::new(dir.path()).unwrap();
        cache.mark_uploaded("seg-0001.graphj").unwrap();
        cache.mark_uploaded("seg-0001.graphj").unwrap(); // second call is no-op
        assert_eq!(cache.pending_segments().unwrap().len(), 0);
    }
}
