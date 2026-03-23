# graphstream Changelog

## Phase 4: Concurrent S3 Uploads via JoinSet

Replaced sequential upload loop with concurrent `JoinSet`-based uploader. Sequential S3 uploads (100-500ms each) were the throughput bottleneck — now up to 4 concurrent uploads (configurable `max_concurrent`).

### Architecture
- `SegmentStorage` trait — abstracts upload destination (S3, mock). Enables unit testing without real S3.
- `S3SegmentStorage` — wraps `aws_sdk_s3::Client` + bucket.
- `UploadTaskContext` — cloned into each spawned task (Arc'd storage, retry_policy, stats).
- `Uploader` — JoinSet-based concurrent upload engine with `run()` loop.
- `spawn_uploader()` returns `(Sender<UploadMessage>, JoinHandle)` — callers must await handle after shutdown for graceful drain.
- `spawn_journal_uploader` / `spawn_journal_uploader_with_retry` — high-level API combining seal timer + concurrent uploader. Now returns `(upload_tx, handle)` instead of just `handle`.

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
1. Find last sealed segment with `FLAG_HAS_CHAIN_HASH` (0x08) → read `header.last_seq` + 32-byte trailer → O(1)
2. No segments on disk → check `recovery.json` for snapshot bootstrap (hakuzu writes this after extracting a snapshot)
3. Segments exist but no trailer (v1 format) → full O(N) scan fallback

### Format change
- New flag: `FLAG_HAS_CHAIN_HASH = 0x08` in header byte offset 8
- 32-byte trailer after body: `[128B header][compressed body][32B chain_hash]`
- `body_len` in header still refers to compressed body only (trailer is separate)
- Backward compatible: v1 segments (no trailer) trigger full scan fallback

### Files changed
- `src/graphj.rs` — `FLAG_HAS_CHAIN_HASH`, `CHAIN_HASH_TRAILER_SIZE`, `has_chain_hash()`, `read_chain_hash_trailer()`
- `src/journal.rs` — `seal_segment` writes trailer; `recover_journal_state` reads trailer; removed `write_recovery_state` from all seal sites
- `write_recovery_state` kept public for hakuzu snapshot bootstrap only

### Tests (8 new/rewritten)
- `test_chain_hash_trailer_written_after_seal`, `test_chain_hash_trailer_recovery`, `test_recovery_v1_segment_falls_back_to_full_scan`, `test_chain_hash_trailer_on_rotation`, `test_chain_hash_trailer_on_shutdown`, `test_recovery_json_no_segments_trusted`, `test_recovery_ignores_corrupt_recovery_json_with_trailer`, `test_recovery_empty_journal_dir_no_cache`

## Phase 1: S3 Retry & Circuit Breaker

Adapted walrust's battle-tested retry module. Transient S3 errors (500, 503, timeout) are retried with exponential backoff + jitter. Auth/client errors fail immediately. Circuit breaker prevents hammering a degraded S3 endpoint.

### retry.rs
- `RetryConfig` — max_retries, base_delay, max_delay, cap
- `ErrorKind` — Transient, ClientError, AuthError, NotFound, Unknown
- `classify_error()` — inspects error strings for retryable patterns
- `CircuitBreaker` — Closed → Open (after N failures) → HalfOpen (after cooldown) → Closed (on success)
- `RetryPolicy` — wraps config + circuit breaker, `execute()` runs async closures with retry

### Integration
- `uploader.rs` — `spawn_journal_uploader_with_retry()` wraps each PutObject in RetryPolicy
- `sync.rs` — `download_new_segments()` accepts optional `&RetryPolicy` for download retries

### Tests (12 unit)
- Error classification, retryability, backoff bounds, circuit breaker state transitions, retry success/failure/exhaustion scenarios
