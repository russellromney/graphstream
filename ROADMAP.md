# graphstream Roadmap

## Phase Aether: hadb-io Migration (hadb Phase 1c)

> After: graphstream Phase 8 (Structured Tracing) · Before: hakuzu Phase Cascade

graphstream independently implements retry, S3 operations, and upload concurrency that hadb-io now provides generically. graphstream's `retry.rs` (679 lines) was copy-pasted from walrust-core and has already drifted (different rand API, missing webhook support). walrust completed this migration in hadb Phase 1b (~4,275 lines deleted, 303 tests passing). graphstream is next.

### Aether-a: Replace retry.rs with hadb-io

Delete `src/retry.rs` (~679 lines). Replace all `use crate::retry::` with `use hadb_io::`.

Reconciliation notes:
- graphstream's `RetryPolicy` has identical API to `hadb_io::RetryPolicy`
- graphstream added `consecutive_failures()` helper on CircuitBreaker; verify hadb-io has it (it does, from the reconciliation in Phase 1a)
- graphstream uses `rand::thread_rng()` (0.8 API); hadb-io uses same

Source: `graphstream/src/retry.rs` (delete), `hadb-io/src/retry.rs` (replacement)

Callers to update:
- `src/uploader.rs` — `RetryPolicy` for upload retries
- `src/sync.rs` — optional `&RetryPolicy` for download retries
- `src/lib.rs` — re-export from hadb-io instead of own module

### Aether-b: Replace direct S3 calls with ObjectStore trait

graphstream calls `aws_sdk_s3::Client` directly in `src/uploader.rs` (PutObject) and `src/sync.rs` (ListObjectsV2, GetObject). Replace with `hadb_io::ObjectStore` trait calls. This gains testability (MockStorage) for free.

- `S3SegmentStorage` in `src/uploader.rs` — replace `client.put_object()` with `object_store.upload_file()`
- `download_new_segments` in `src/sync.rs` — replace `client.list_objects_v2()` / `client.get_object()` with `object_store.list()` / `object_store.download()`
- Accept `Arc<dyn ObjectStore>` instead of `aws_sdk_s3::Client` in public APIs

**Cross-repo cascade:** Changing `download_new_segments` signature from `(&aws_sdk_s3::Client, &str, ...)` to `(&dyn ObjectStore, ...)` breaks hakuzu's `KuzuFollowerBehavior` (`hakuzu/src/follower_behavior.rs:102-108`), which calls this function directly. hakuzu Phase Cascade must follow this change.

Source: `walrust-core` migration (hadb Phase 1b) for the pattern. `hadb-io/src/storage.rs` for ObjectStore trait, `hadb-io/src/s3.rs` for S3Backend.

### Aether-c: Update uploader to use ConcurrentUploader

graphstream's `src/uploader.rs` (~1,073 lines) has its own JoinSet-based concurrent upload engine. Replace the core upload loop with `hadb_io::ConcurrentUploader<SegmentUploadItem>`.

- Define `SegmentUploadItem` implementing `hadb_io::UploadItem` (sealed segment path + metadata)
- `spawn_journal_uploader_with_retry` becomes thin wrapper: seal timer + `ConcurrentUploader<SegmentUploadItem>`
- Cache integration (`spawn_journal_uploader_with_cache`) stays; cache mark-uploaded hooks into upload completion callback
- Background compaction trigger stays (fires after N uploads)

Source: `hadb-io/src/uploader.rs` (ConcurrentUploader), `walrust/src/uploader.rs` (reference migration from Phase 1b)

### Aether-d: Wire webhook + retention (free after a-c)

After hadb-io dependency exists:
- Optional `WebhookConfig` on journal uploader — fires on circuit breaker open, upload failure
- Optional `RetentionPolicy` for uploaded segment cleanup — GFS-based, replaces current age-only cleanup in `SegmentCache`

Source: `hadb-io/src/webhook.rs`, `hadb-io/src/retention.rs`

### Aether-e: Update Cargo.toml + tests

- Add `hadb-io` dependency (path = "../hadb/hadb-io")
- Remove direct `rand` dependency (comes via hadb-io)
- All 77 existing tests must pass unchanged (behavioral no-op)
- Add ~4 tests: ObjectStore mock upload/download, ConcurrentUploader segment flow

---

## Phase Drain: Synchronous Upload Ack for Graceful Shutdown

> After: Phase Aether · Before: (none)

`KuzuReplicator::sync()` (`hakuzu/src/replicator.rs:193-217`) seals the current journal segment and enqueues it for upload, but does not wait for the S3 upload to complete. The upload channel is fire-and-forget. This means `close()` in hakuzu cannot guarantee the sealed segment is in S3 before releasing the lease.

### Drain-a: Add upload ack to UploadMessage

Add an `UploadWithAck(PathBuf, oneshot::Sender<Result<()>>)` variant to `UploadMessage`. The uploader completes the upload and sends the result back via the oneshot channel.

Source: `graphstream/src/uploader.rs` (UploadMessage enum, upload loop)

### Drain-b: Wire into KuzuReplicator::sync()

After `upload_tx.send(UploadWithAck(path, ack_tx))`, await the ack receiver. `sync()` now blocks until S3 has the segment.

Source: `hakuzu/src/replicator.rs:207-212` (replace fire-and-forget send with ack)

### Drain-c: Tests

- sync() waits for upload completion before returning
- sync() returns error if upload fails
- sync() is idempotent (no pending segment = no-op, already handled)

### Verification

```bash
cd ~/Documents/Github/graphstream
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" ~/.cargo/bin/cargo test

# Then verify hakuzu still builds (transitive dep)
cd ~/Documents/Github/hakuzu
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" ~/.cargo/bin/cargo test --lib --test ha_database
```
