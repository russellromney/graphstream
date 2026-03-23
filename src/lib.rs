//! graphstream — journal replication engine for graph databases.
//!
//! Logical WAL shipping via .graphj segments to S3. This crate provides the
//! replication machinery that graphd uses: journal format, writer/reader,
//! S3 uploader, and follower sync.
//!
//! graphstream is to Kuzu/graphd what walrust is to SQLite — the replication
//! engine, not the database server.

use std::time::{SystemTime, UNIX_EPOCH};

pub mod cache;
pub mod graphj;
pub mod journal;
pub mod metrics;
pub mod retry;
pub mod sync;
pub mod types;
pub mod uploader;

// Protobuf types (same package name as graphd — wire-compatible).
pub mod graphd {
    include!(concat!(env!("OUT_DIR"), "/graphd.rs"));
}

/// Current timestamp in epoch milliseconds.
pub fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// Re-exports for convenience.
pub use journal::{
    recover_journal_state, spawn_journal_writer, write_recovery_state, JournalCommand,
    JournalReader, JournalReaderEntry, JournalSender, JournalState, PendingEntry,
};
pub use sync::download_new_segments;
pub use types::{
    graph_value_to_param_value, map_entries_to_param_values, param_value_to_graph_value,
    param_values_to_map_entries, ParamValue,
};
pub use cache::{CacheConfig, CacheStats, CleanupStats, SegmentCache};
pub use metrics::GraphstreamMetrics;
pub use retry::{RetryConfig, RetryPolicy};
pub use uploader::{
    run_background_compaction, spawn_journal_uploader, spawn_journal_uploader_with_cache,
    spawn_journal_uploader_with_retry, spawn_uploader, CompactionConfig, SegmentStorage,
    S3SegmentStorage, UploadMessage, Uploader, UploaderStats,
};
