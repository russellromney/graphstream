//! Prometheus-compatible metrics for graphstream.
//!
//! Lightweight AtomicU64 counters and gauges — no external prometheus crate needed.
//! Thread-safe, zero-allocation on reads. Access via `Arc<GraphstreamMetrics>`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct GraphstreamMetrics {
    // Journal writer
    pub entries_written: AtomicU64,
    pub segments_sealed: AtomicU64,

    // Uploader
    pub segments_uploaded: AtomicU64,
    pub upload_errors: AtomicU64,
    pub upload_bytes: AtomicU64,

    // Sync (follower)
    pub segments_downloaded: AtomicU64,
    pub download_errors: AtomicU64,

    // Integrity
    pub chain_hash_mismatches: AtomicU64,

    // Timing (microseconds)
    pub last_upload_duration_us: AtomicU64,
    pub last_recovery_duration_us: AtomicU64,
}

impl GraphstreamMetrics {
    pub fn new() -> Self {
        Self {
            entries_written: AtomicU64::new(0),
            segments_sealed: AtomicU64::new(0),
            segments_uploaded: AtomicU64::new(0),
            upload_errors: AtomicU64::new(0),
            upload_bytes: AtomicU64::new(0),
            segments_downloaded: AtomicU64::new(0),
            download_errors: AtomicU64::new(0),
            chain_hash_mismatches: AtomicU64::new(0),
            last_upload_duration_us: AtomicU64::new(0),
            last_recovery_duration_us: AtomicU64::new(0),
        }
    }

    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, Ordering::Relaxed);
    }

    pub fn record_duration(&self, target: &AtomicU64, start: Instant) {
        target.store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            entries_written: self.entries_written.load(Ordering::Relaxed),
            segments_sealed: self.segments_sealed.load(Ordering::Relaxed),
            segments_uploaded: self.segments_uploaded.load(Ordering::Relaxed),
            upload_errors: self.upload_errors.load(Ordering::Relaxed),
            upload_bytes: self.upload_bytes.load(Ordering::Relaxed),
            segments_downloaded: self.segments_downloaded.load(Ordering::Relaxed),
            download_errors: self.download_errors.load(Ordering::Relaxed),
            chain_hash_mismatches: self.chain_hash_mismatches.load(Ordering::Relaxed),
            last_upload_duration_us: self.last_upload_duration_us.load(Ordering::Relaxed),
            last_recovery_duration_us: self.last_recovery_duration_us.load(Ordering::Relaxed),
        }
    }
}

impl Default for GraphstreamMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub entries_written: u64,
    pub segments_sealed: u64,
    pub segments_uploaded: u64,
    pub upload_errors: u64,
    pub upload_bytes: u64,
    pub segments_downloaded: u64,
    pub download_errors: u64,
    pub chain_hash_mismatches: u64,
    pub last_upload_duration_us: u64,
    pub last_recovery_duration_us: u64,
}

impl MetricsSnapshot {
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);

        Self::counter(
            &mut out,
            "graphstream_entries_written_total",
            "Total journal entries written",
            self.entries_written,
        );
        Self::counter(
            &mut out,
            "graphstream_segments_sealed_total",
            "Total segments sealed",
            self.segments_sealed,
        );
        Self::counter(
            &mut out,
            "graphstream_segments_uploaded_total",
            "Total segments uploaded to S3",
            self.segments_uploaded,
        );
        Self::counter(
            &mut out,
            "graphstream_upload_errors_total",
            "Total upload errors",
            self.upload_errors,
        );
        Self::counter(
            &mut out,
            "graphstream_upload_bytes_total",
            "Total bytes uploaded to S3",
            self.upload_bytes,
        );
        Self::counter(
            &mut out,
            "graphstream_segments_downloaded_total",
            "Total segments downloaded from S3",
            self.segments_downloaded,
        );
        Self::counter(
            &mut out,
            "graphstream_download_errors_total",
            "Total download errors",
            self.download_errors,
        );
        Self::counter(
            &mut out,
            "graphstream_chain_hash_mismatches_total",
            "Chain hash mismatches detected (should be 0)",
            self.chain_hash_mismatches,
        );

        Self::gauge(
            &mut out,
            "graphstream_last_upload_duration_seconds",
            "Duration of last upload",
            self.last_upload_duration_us as f64 / 1_000_000.0,
        );
        Self::gauge(
            &mut out,
            "graphstream_last_recovery_duration_seconds",
            "Duration of last journal recovery",
            self.last_recovery_duration_us as f64 / 1_000_000.0,
        );

        out
    }

    fn counter(out: &mut String, name: &str, help: &str, value: u64) {
        out.push_str(&format!(
            "# HELP {} {}\n# TYPE {} counter\n{} {}\n",
            name, help, name, name, value
        ));
    }

    fn gauge(out: &mut String, name: &str, help: &str, value: f64) {
        out.push_str(&format!(
            "# HELP {} {}\n# TYPE {} gauge\n{} {:.6}\n",
            name, help, name, name, value
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_increment() {
        let m = GraphstreamMetrics::new();
        m.inc(&m.entries_written);
        m.inc(&m.entries_written);
        m.inc(&m.segments_sealed);
        m.add(&m.upload_bytes, 1024);

        let snap = m.snapshot();
        assert_eq!(snap.entries_written, 2);
        assert_eq!(snap.segments_sealed, 1);
        assert_eq!(snap.upload_bytes, 1024);
        assert_eq!(snap.segments_uploaded, 0);
    }

    #[test]
    fn test_metrics_record_duration() {
        let m = GraphstreamMetrics::new();
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(5));
        m.record_duration(&m.last_upload_duration_us, start);

        let snap = m.snapshot();
        assert!(snap.last_upload_duration_us >= 4000, "Should record >= 4ms");
    }

    #[test]
    fn test_prometheus_format() {
        let m = GraphstreamMetrics::new();
        m.inc(&m.entries_written);
        m.inc(&m.segments_uploaded);
        m.add(&m.upload_bytes, 2048);

        let text = m.snapshot().to_prometheus();
        assert!(text.contains("# TYPE graphstream_entries_written_total counter"));
        assert!(text.contains("graphstream_entries_written_total 1"));
        assert!(text.contains("graphstream_segments_uploaded_total 1"));
        assert!(text.contains("graphstream_upload_bytes_total 2048"));
        assert!(text.contains("# TYPE graphstream_last_upload_duration_seconds gauge"));
    }

    #[test]
    fn test_prometheus_all_counters_present() {
        let m = GraphstreamMetrics::new();
        let text = m.snapshot().to_prometheus();

        let expected = [
            "graphstream_entries_written_total",
            "graphstream_segments_sealed_total",
            "graphstream_segments_uploaded_total",
            "graphstream_upload_errors_total",
            "graphstream_upload_bytes_total",
            "graphstream_segments_downloaded_total",
            "graphstream_download_errors_total",
            "graphstream_chain_hash_mismatches_total",
            "graphstream_last_upload_duration_seconds",
            "graphstream_last_recovery_duration_seconds",
        ];
        for name in expected {
            assert!(text.contains(name), "Missing metric: {}", name);
        }
    }
}
