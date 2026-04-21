# graphstream

> **Experimental.** graphstream is under active development and not yet stable. APIs will change without notice.

Journal replication engine for graph databases. Logical WAL shipping via `.hadbj` segments to S3-compatible storage.

graphstream handles journal writing, segment rotation, upload/download, compaction, and segment replay for Kuzu/LadybugDB HA. Used by [hakuzu](https://github.com/russellromney/hakuzu) for graph database high availability.

Part of the [hadb](https://github.com/russellromney/hadb) ecosystem. graphstream is trait-only at the library layer — concrete S3/HTTP backends are picked by the embedder (hakuzu, the cinch engine, …) via the `hadb-storage` `StorageBackend` trait.

## What it does

1. **Journal writing**: Cypher queries + params written to `.hadbj` segment files via hadb-changeset binary format with SHA-256 chain hashing.
2. **Segment rotation**: Auto-rotate at configurable size (default 4MB). `SealForUpload` seals the current segment for upload.
3. **S3 upload**: Sealed segments uploaded via the `hadb_storage::StorageBackend` trait. Up to 4 concurrent uploads via `JoinSet`. Supports `UploadWithAck` for synchronous upload confirmation (used by hakuzu's `sync()` for graceful shutdown).
4. **Segment download**: Followers poll S3 for new segments via `StorageBackend` and download incrementally.
5. **Journal reader**: Streaming reads from segments, 1MB `BufReader`, streaming zstd decompression for compressed segments (encrypted segments use full-body decode).
6. **Compression + encryption**: Optional zstd compression and ChaCha20-Poly1305 encryption per segment.
7. **Disk cache**: Manifest-based upload tracking with age/size-based cleanup. Crash-safe (write `.tmp` + rename).
8. **Compaction**: Streaming compaction merges segments without loading all into memory. Background compaction triggers after N uploaded segments.
9. **O(1) recovery**: 32-byte chain hash trailer on sealed segments. Recovery reads one header + trailer instead of scanning all entries.
10. **Retry + circuit breaker**: Concrete `StorageBackend` impls (e.g. `hadb-storage-s3`) own retry and circuit breaking; graphstream stays out of that policy.
11. **Prometheus metrics**: Lock-free `AtomicU64` counters for entries, segments, uploads, downloads, errors, bytes. `snapshot().to_prometheus()` text format.

## Architecture

```
Writer -> PendingEntry -> JournalWriter -> .hadbj segments -> Uploader -> StorageBackend (S3/HTTP/...)
                                              |                              |
                                     SegmentCache (manifest)     Background Compaction
                                                                             |
Follower <- replay_entries <- JournalReader <- .hadbj segments <- StorageBackend Download
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

Uses hadb-changeset's `.hadbj` binary format:
- Header (128 bytes): HADBJ magic + flags (sealed, compressed, chain hash) + sequence range + body length + checksums
- Body: binary entries (sequence + prev_hash + payload), zstd-compressed when sealed
- Trailer (sealed only): 32-byte SHA-256 chain hash for O(1) recovery and cross-segment integrity

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

// Spawn S3 uploader (streams sealed segments via `hadb_storage::StorageBackend`)
let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
let (upload_tx, uploader_handle) = spawn_journal_uploader(
    tx.clone(), journal_dir, object_store, "prefix/".into(),
    "mydb".into(), Duration::from_secs(10), shutdown_rx,
);
```

## Dependencies

- `hadb-changeset` -- `.hadbj` binary format (encode, decode, seal, chain hashing, S3 key layout)
- `hadb-storage` -- byte-level `StorageBackend` trait. Concrete impls (`hadb-storage-s3`, `hadb-storage-cinch`, …) are picked by the embedder
- `prost` -- Protobuf serialization (journal entry payload)
- `zstd` -- Compression (sealed segments)

## Tests

Covers journal writing, chain hash integrity, streaming decompression, compaction, uploader (including ack), cache, and metrics.

```bash
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" cargo test
```

## License

Apache-2.0
