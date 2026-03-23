# graphstream

Journal replication engine for graph databases. Logical WAL shipping via `.graphj` segments to S3.

graphstream provides the replication primitives for graph database HA. It handles journal writing, segment rotation, S3 upload/download, compaction, and segment replay — the "WAL shipping" layer for Kuzu/LadybugDB.

## What it does

1. **Journal writing**: Cypher queries + params written to `.graphj` segment files with SHA-256 chain hashing and CRC32C integrity checks.
2. **Segment rotation**: Auto-rotate at configurable size (default 4MB). `SealForUpload` seals the current segment for upload.
3. **S3 upload**: Sealed segments streamed to S3 via `ByteStream::from_path` (no full-file memory load). Up to 4 concurrent uploads via `JoinSet`.
4. **Segment download**: Followers poll S3 for new segments and download incrementally.
5. **Journal reader**: Streaming reads from segments — 1MB `BufReader`, streaming zstd decompression for compressed segments (encrypted segments use full-body decode).
6. **Compression + encryption**: Optional zstd compression and ChaCha20-Poly1305 encryption per segment.
7. **Disk cache**: Manifest-based upload tracking with age/size-based cleanup. Crash-safe (write `.tmp` + rename).
8. **Compaction**: Streaming compaction merges segments without loading all into memory. Background compaction triggers after N uploaded segments.
9. **O(1) recovery**: 32-byte chain hash trailer on sealed segments. Recovery reads one header + trailer instead of scanning all entries.
10. **S3 retry + circuit breaker**: Transient S3 errors retried with exponential backoff + jitter. Circuit breaker prevents hammering degraded endpoints.
11. **Prometheus metrics**: Lock-free `AtomicU64` counters — `entries_written`, `segments_sealed`, `segments_uploaded`, `upload_errors`, `upload_bytes`, `segments_downloaded`, `download_errors`, `chain_hash_mismatches`. `snapshot().to_prometheus()` text format.

## Architecture

```
Writer → PendingEntry → JournalWriter → .graphj segments → S3 Uploader → S3
                                              ↓                           ↓
                                     SegmentCache (manifest)    Background Compaction
                                                                          ↓
Follower ← replay_entries ← JournalReader ← .graphj segments ← S3 Download
```

### Entry format (protobuf)

```protobuf
message JournalEntry {
  string query = 1;                    // Cypher query
  repeated ParamEntry params = 2;      // Named parameters
  int64 timestamp_ms = 3;             // Deterministic timestamp
  optional string uuid_override = 4;   // Deterministic UUID
}
```

### Segment format

Each `.graphj` segment is:
- Header (128 bytes): magic bytes + version + flags (compression, encryption) + sequence range + body length
- Body: length-prefixed protobuf entries with CRC32C checksums
- Trailer (sealed only): 32-byte SHA-256 chain hash linking to previous segment

## Usage

```rust
use graphstream::journal::{spawn_journal_writer, JournalState, PendingEntry};
use graphstream::uploader::spawn_journal_uploader;

// Spawn journal writer
let state = Arc::new(JournalState::new());
let tx = spawn_journal_writer(journal_dir, 4 * 1024 * 1024, 100, state.clone());

// Write an entry
tx.send(JournalCommand::Write(PendingEntry {
    query: "CREATE (p:Person {id: $id, name: $name})".into(),
    params: vec![
        ("id".into(), ParamValue::Int(1)),
        ("name".into(), ParamValue::String("Alice".into())),
    ],
    timestamp_ms: 1234567890,
    uuid_override: None,
}))?;

// Spawn S3 uploader (streams sealed segments to S3)
let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
let (upload_tx, uploader_handle) = spawn_journal_uploader(
    tx.clone(), journal_dir, "my-bucket".into(), "prefix/".into(),
    Duration::from_secs(10), shutdown_rx,
);
```

## Dependencies

- `prost` — Protobuf serialization
- `sha2` — Chain hash integrity
- `crc32c` — Entry checksum
- `zstd` — Compression (segment + streaming decompression)
- `chacha20poly1305` — Optional encryption
- `aws-sdk-s3` — S3 upload/download (streaming via `ByteStream`)

## Tests

71 tests across journal, chain hash, streaming, compaction, uploader, cache, and metrics modules.

```bash
cargo test
```

## License

Apache-2.0
