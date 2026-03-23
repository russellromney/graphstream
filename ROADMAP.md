# graphstream Roadmap

## Phase 1: Uploader Improvements

### Concurrent uploads via JoinSet

walrust v0.6.0 proved the pattern: sequential S3 uploads (100-500ms each) are the throughput bottleneck. Replace the sequential upload loop with `tokio::task::JoinSet` bounded by `max_concurrent` (default 4):

```rust
tokio::select! {
    msg = rx.recv(), if in_flight.len() < max_concurrent => { /* spawn */ }
    Some(result) = in_flight.join_next() => { /* reap */ }
}
```

Key pattern: extract an `UploadTaskContext` struct holding cloned `Arc`s (storage, retry_policy, cache, stats) to avoid `&self` lifetime issues with `JoinSet::spawn`. The uploader's `run()` method owns the JoinSet; spawned tasks get the context struct.

### spawn_uploader returns JoinHandle

`spawn_uploader` must return `(Sender, JoinHandle)` — not just Sender. Dropping the handle means fire-and-forget; any in-flight uploads are silently abandoned on shutdown. Callers must await the handle with a timeout after sending Shutdown.

### Disk cache + uploader decoupling

walrust v0.6.0 decoupled "write to local disk" from "upload to S3" via a `LocalCache` + `Uploader` pair connected by an mpsc channel. Benefits:
- **Crash recovery**: pending uploads survive process restart (cache scans disk on startup)
- **Backpressure**: writes proceed at disk speed, uploads drain asynchronously
- **Dual code paths**: `Box::pin()` to select between cache path and direct-S3 path at runtime without duplicating the encode logic

Apply this to graphstream's segment uploader: seal segment → write to local cache → notify uploader via channel → uploader handles S3 with retry/concurrency.

### Cache cleanup timer

Any disk-based cache needs a periodic cleanup timer. walrust v0.6.0 found that shadow mode had a cache but no cleanup — disk usage grew unbounded. Apply retention_duration + max_cache_size limits on each tick.

Files: `src/uploader.rs`

## Phase 2: Memory & Streaming

### BufReader capacity

`JournalReader` uses default 8KB BufReader. Bump to 1MB for sequential segment reads:

```rust
let mut reader = BufReader::with_capacity(1024 * 1024, file);
```

File: `src/journal.rs` (JournalReader::open_segment)

### Streaming zstd decompression

Currently sealed+compressed segments load the entire body into `Vec<u8>` before decompressing. zstd supports streaming decompression — wrap the file reader in `zstd::stream::Decoder` and parse entries directly from the decompressed stream. Saves O(body_len) memory per segment read.

Only matters for large segments or compaction of many segments. Low priority unless segment_max_bytes increases beyond 4MB.

File: `src/graphj.rs` (decode_segment_body), `src/journal.rs` (open_segment)

### Streaming S3 upload

Currently reads entire sealed segment via `std::fs::read()` for PutObject. For 4MB segments this is fine. If segments grow (or for future snapshot uploads), stream via `ByteStream::from_path()` instead:

```rust
let body = aws_sdk_s3::primitives::ByteStream::from_path(&path).await?;
client.put_object().body(body)...
```

File: `src/uploader.rs`

## Phase 3: Compaction Improvements

### Streaming compaction

`compact()` currently reads all entries from input segments into memory, then writes a single output segment. For compacting many large segments, this can spike memory. Stream entries from input directly to output — read one entry, write one entry.

### Compaction with encryption

`compact()` supports compression but not encryption. Add optional encryption key parameter. Processing order: compress → encrypt (same as seal_segment).

### Background compaction

Currently compaction is caller-driven. Add optional background compaction to the uploader: after uploading N segments, compact old segments into fewer larger ones. Reduces S3 ListObjectsV2 pagination on followers and recovery time.

## Phase 4: Observability

### Prometheus metrics

Export from uploader and sync:
- `graphstream_entries_written_total` (counter)
- `graphstream_segments_sealed_total` (counter)
- `graphstream_segments_uploaded_total` (counter)
- `graphstream_upload_errors_total` (counter, by error_kind)
- `graphstream_upload_latency_seconds` (histogram)
- `graphstream_segments_downloaded_total` (counter)
- `graphstream_download_errors_total` (counter)
- `graphstream_recovery_duration_seconds` (gauge)
- `graphstream_chain_hash_mismatches_total` (counter — should always be 0)

### Structured tracing

Replace ad-hoc `tracing::info!` with structured spans:
- `upload_segment{segment=..., size_bytes=..., duration_ms=...}`
- `download_segment{segment=..., source=s3}`
- `seal_segment{segment=..., entries=..., compressed_ratio=...}`
- `recover{segments_scanned=..., entries_scanned=..., duration_ms=...}`
