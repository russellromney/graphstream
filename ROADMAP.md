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

- Add `hadb-io` dependency (path = "../../hadb/hadb-io")
- Remove direct `rand` dependency (comes via hadb-io)
- Remove direct `aws-sdk-s3` and `aws-config` deps if no longer used directly
- All existing tests must pass unchanged (behavioral no-op)
- Add ~4 tests: ObjectStore mock upload/download, ConcurrentUploader segment flow

### Implementation context for a new session

**Ecosystem context:** graphstream is the journal replication engine for graph databases (Kuzu/LadybugDB). It handles journal writing, segment rotation, S3 upload, compaction, and segment replay. hakuzu depends on graphstream for follower replication. hadb-io provides shared infrastructure (S3 client, retry, concurrent uploads) that walrust already migrated to.

**What walrust did (reference pattern):** walrust Phase 1b deleted ~4,275 lines and replaced direct S3 calls + custom retry with hadb-io. The migration is at `personal-website/walrust/crates/walrust-core/`. Key files: `src/sync.rs` (uses `hadb_io::ObjectStore`), `src/replicator.rs` (uses `hadb_io::S3Backend`).

**Current graphstream state:** 93 tests passing (49 unit + 8 integration + 9 cache + 23 compaction + 4 doc). Phase Drain is done (`UploadWithAck` already in uploader.rs).

**Key risk:** Aether-b changes `download_new_segments()` signature from `(&aws_sdk_s3::Client, &str, ...)` to `(&dyn ObjectStore, ...)`. This breaks hakuzu's `KuzuFollowerBehavior` which calls it directly. hakuzu Phase Cascade must follow immediately.

**Execution order:** Aether-a first (retry.rs deletion, mechanical replacement). Then Aether-b (ObjectStore). Then Aether-c (ConcurrentUploader, optional -- can be deferred if complex). Then Aether-d (webhook/retention wiring, optional). Then Aether-e (cleanup + tests).

**hadb-io public API (what you're replacing with):**
- `hadb_io::RetryPolicy` -- drop-in for graphstream's RetryPolicy
- `hadb_io::CircuitBreaker` -- drop-in, has `consecutive_failures()` helper
- `hadb_io::ObjectStore` trait -- `upload_bytes`, `upload_file`, `download_bytes`, `download_file`, `list_objects`, `exists`, `delete_object`
- `hadb_io::S3Backend` -- `S3Backend::new(client, bucket)` implements ObjectStore
- `hadb_io::ConcurrentUploader<T: UploadItem>` -- generic concurrent upload with JoinSet

**Files to read first:**
- `hadb/hadb-io/src/retry.rs` -- the replacement RetryPolicy
- `hadb/hadb-io/src/storage.rs` -- ObjectStore trait definition
- `hadb/hadb-io/src/s3.rs` -- S3Backend implementation
- `graphstream/src/retry.rs` -- what you're deleting (~679 lines)
- `graphstream/src/uploader.rs` -- S3SegmentStorage + Uploader (UploadWithAck already there)
- `graphstream/src/sync.rs` -- download_new_segments with direct S3 calls

---

## Phase Drain: Synchronous Upload Ack for Graceful Shutdown (DONE)

`UploadWithAck(PathBuf, oneshot::Sender<Result<()>>)` variant added to `UploadMessage`. hakuzu's `KuzuReplicator::sync()` now awaits the oneshot response instead of fire-and-forget. 3 tests (success ack, failure ack, mixed with fire-and-forget).

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
