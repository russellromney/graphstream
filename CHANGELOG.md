# graphstream Changelog

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
