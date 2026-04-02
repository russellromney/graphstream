# graphstream Changelog

## Phase Aether: hadb-io Migration

> After: Phase 8 (Structured Tracing) · Before: Phase Drain

Replaced graphstream's copy-pasted retry module and direct S3 calls with hadb-io shared infrastructure. Deleted `src/retry.rs` (679 lines) and `S3SegmentStorage`. Removed direct `aws-sdk-s3`/`aws-config` dependencies.

- Net: -404 lines (6 files changed, 455 insertions, 859 deletions)
- Tests: 86 passing (42 unit + 8 chain_hash + 9 graphj + 23 journal + 4 streaming), 5 e2e ignored (need S3 creds)
- Bug fixed: `uploads_failed` counter was never incremented on failure (regression test added)

## Phase Drain: Synchronous Upload Ack for Graceful Shutdown

> After: Phase Aether

`UploadWithAck(PathBuf, oneshot::Sender<Result<()>>)` variant added to `UploadMessage`. hakuzu's `KuzuReplicator::sync()` now awaits the oneshot response instead of fire-and-forget. 3 tests (success ack, failure ack, mixed with fire-and-forget).

## Phase 8: Structured Tracing

Replaced string-interpolated `tracing::info!` with structured fields across uploader, journal, and sync modules. Behavioral no-op, same log messages, now machine-parseable.

Examples:
- `info!(segment = %name, size_bytes = len, "Uploaded")`
- `info!(segments_scanned = n, duration_ms = d, "Recovery complete")`

Files: `src/uploader.rs`, `src/journal.rs`, `src/sync.rs`

## Phase 7: Prometheus Metrics

`GraphstreamMetrics`, lock-free `AtomicU64` counters following hadb's pattern. No external prometheus crate.

### Counters
- `entries_written`, `segments_sealed`, `segments_uploaded`, `upload_errors`, `upload_bytes`
- `segments_downloaded`, `download_errors`, `chain_hash_mismatches`

### Gauges
- `last_upload_duration_us`, `last_recovery_duration_us`

### API
- `Arc<GraphstreamMetrics>` passed to journal, uploader, and sync
- `snapshot()` -> `MetricsSnapshot` -> `to_prometheus()` text format

### Tests (4 unit)
- `test_metrics_default_zero`, `test_metrics_increment`, `test_metrics_snapshot`, `test_metrics_prometheus_format`

Files: `src/metrics.rs`, `src/lib.rs`

## Phase 6: Compaction

### Streaming compaction
`compact_streaming()` reads entries one at a time from inputs via `read_raw_entry()` and writes through a zstd encoder. Avoids loading all entries into memory. Encrypted inputs fall back to full-body decode (encryption needs all bytes); encrypted output not supported in streaming mode (use existing `compact()` for that).

### Background compaction
`run_background_compaction()` triggers after N uploaded segments accumulate (configurable via `CompactionConfig`). Collects uploaded segment paths, runs `compact_streaming()` via `spawn_blocking`, uploads the compacted segment, marks originals as compacted in cache.

### Tests (2 integration)
- `test_compact_streaming_matches_compact`, `test_compact_streaming_many_segments`

Files: `src/graphj.rs`, `src/uploader.rs`, `src/lib.rs`

## Phase 5: Disk Cache, Streaming I/O, BufReader

### Disk cache + cleanup
`SegmentCache`, manifest-based upload tracking (`upload_manifest.json`). Atomic persistence (write .tmp + rename). Pending segments survive process restart. Age-based and size-based cleanup never deletes pending uploads.

API: `SegmentCache::new()`, `add_segment()`, `pending_segments()`, `mark_uploaded()`, `cleanup()`, `reconcile()`, `stats()`

Integrated into uploader via `spawn_journal_uploader_with_cache()`, resume pending uploads on startup, mark uploaded after success, cleanup every 5 minutes.

### Streaming S3 upload
`SegmentStorage` trait gained `upload_file(&self, key, path)` with default impl delegating to `upload_bytes`. `S3SegmentStorage` overrides with `ByteStream::from_path()`, streams file to S3 without reading entire segment into memory.

### Streaming zstd decompression
`JournalReader` polymorphic reader (`Box<dyn Read>`). Three paths in `open_segment()`:
- Encrypted: full-body decode to cursor (encryption needs all bytes)
- Compressed-only: `zstd::Decoder::new(file.take(body_len))` streaming
- Raw: `BufReader::with_capacity(1MB, file)`

Same pattern applied to `recover_journal_state_full_scan`.

### BufReader capacity
Default 8KB changed to 1MB for all sequential segment reads.

### Tests (9 unit + 2 integration)
- Cache: `test_cache_empty_dir`, `test_cache_mark_uploaded`, `test_cache_crash_recovery`, `test_cache_reconcile_stale`, `test_cache_cleanup_age`, `test_cache_cleanup_size`, `test_cache_never_delete_pending`, `test_cache_stats`, `test_cache_mark_idempotent`
- Integration: `test_streaming_zstd_read_1000_entries`, `test_streaming_zstd_from_sequence`

Files: `src/cache.rs`, `src/uploader.rs`, `src/journal.rs`, `src/lib.rs`

## Phase 4: Concurrent S3 Uploads via JoinSet

Replaced sequential upload loop with concurrent `JoinSet`-based uploader. Sequential S3 uploads (100-500ms each) were the throughput bottleneck, now up to 4 concurrent uploads (configurable `max_concurrent`).

### Architecture
- `SegmentStorage` trait abstracts upload destination (S3, mock). Enables unit testing without real S3.
- `S3SegmentStorage` wraps `aws_sdk_s3::Client` + bucket.
- `UploadTaskContext` cloned into each spawned task (Arc'd storage, retry_policy, stats).
- `Uploader`, JoinSet-based concurrent upload engine with `run()` loop.
- `spawn_uploader()` returns `(Sender<UploadMessage>, JoinHandle)`, callers must await handle after shutdown for graceful drain.
- `spawn_journal_uploader` / `spawn_journal_uploader_with_retry`, high-level API combining seal timer + concurrent uploader. Now returns `(upload_tx, handle)` instead of just `handle`.

### Key patterns (from walrust v0.6.0)
- `UploadMessage::Upload(PathBuf)` / `UploadMessage::Shutdown`
- `tokio::select!` with `if in_flight.len() < max_concurrent` guard
- Shutdown drains all in-flight uploads before exiting
- File bytes read once, shared across retry attempts via `Arc<Vec<u8>>`

### Tests (8 unit)
- `test_basic_upload`, `test_multiple_uploads`, `test_concurrent_uploads_respect_limit`, `test_concurrent_is_faster_than_sequential`, `test_shutdown_drains_in_flight`, `test_failure_doesnt_block_others`, `test_channel_close_drains`, `test_stats_tracking`

## Phase 3: O(1) Recovery via Chain Hash Trailer

Replaced `recovery.json` (non-atomic, ACID-unsafe) with a 32-byte chain hash trailer appended to sealed `.graphj` segments. The trailer is written atomically with the seal operation (header + compressed body + trailer + fsync), eliminating any consistency window.

### Recovery priority
1. Find last sealed segment with `FLAG_HAS_CHAIN_HASH` (0x08), read `header.last_seq` + 32-byte trailer, O(1)
2. No segments on disk, check `recovery.json` for snapshot bootstrap (hakuzu writes this after extracting a snapshot)
3. Segments exist but no trailer (v1 format), full O(N) scan fallback

### Format change
- New flag: `FLAG_HAS_CHAIN_HASH = 0x08` in header byte offset 8
- 32-byte trailer after body: `[128B header][compressed body][32B chain_hash]`
- `body_len` in header still refers to compressed body only (trailer is separate)
- Backward compatible: v1 segments (no trailer) trigger full scan fallback

### Files changed
- `src/graphj.rs`: `FLAG_HAS_CHAIN_HASH`, `CHAIN_HASH_TRAILER_SIZE`, `has_chain_hash()`, `read_chain_hash_trailer()`
- `src/journal.rs`: `seal_segment` writes trailer; `recover_journal_state` reads trailer; removed `write_recovery_state` from all seal sites
- `write_recovery_state` kept public for hakuzu snapshot bootstrap only

### Tests (8 new/rewritten)
- `test_chain_hash_trailer_written_after_seal`, `test_chain_hash_trailer_recovery`, `test_recovery_v1_segment_falls_back_to_full_scan`, `test_chain_hash_trailer_on_rotation`, `test_chain_hash_trailer_on_shutdown`, `test_recovery_json_no_segments_trusted`, `test_recovery_ignores_corrupt_recovery_json_with_trailer`, `test_recovery_empty_journal_dir_no_cache`

## Phase 1: S3 Retry & Circuit Breaker

Adapted walrust's battle-tested retry module. Transient S3 errors (500, 503, timeout) are retried with exponential backoff + jitter. Auth/client errors fail immediately. Circuit breaker prevents hammering a degraded S3 endpoint.

### retry.rs
- `RetryConfig`: max_retries, base_delay, max_delay, cap
- `ErrorKind`: Transient, ClientError, AuthError, NotFound, Unknown
- `classify_error()` inspects error strings for retryable patterns
- `CircuitBreaker`: Closed -> Open (after N failures) -> HalfOpen (after cooldown) -> Closed (on success)
- `RetryPolicy` wraps config + circuit breaker, `execute()` runs async closures with retry

### Integration
- `uploader.rs`: `spawn_journal_uploader_with_retry()` wraps each PutObject in RetryPolicy
- `sync.rs`: `download_new_segments()` accepts optional `&RetryPolicy` for download retries

### Tests (12 unit)
- Error classification, retryability, backoff bounds, circuit breaker state transitions, retry success/failure/exhaustion scenarios
