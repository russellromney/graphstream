# graphstream

Journal replication engine for graph databases. Logical WAL shipping via `.graphj` segments to S3.

graphstream provides the replication primitives for graph database HA. It handles journal writing, segment rotation, S3 upload, and segment download/replay — the "WAL shipping" layer for Kuzu/LadybugDB.

## What it does

1. **Journal writing**: Cypher queries + params are written to `.graphj` segment files with SHA-256 chain hashing and CRC32C integrity checks.
2. **Segment rotation**: Segments auto-rotate at configurable size (default 4MB). `SealForUpload` seals the current segment for upload.
3. **S3 upload**: Sealed segments are uploaded to S3 on a configurable interval (default 10s).
4. **Segment download**: Followers poll S3 for new segments and download incrementally.
5. **Journal reader**: Reads entries from journal segments for replay against a local database.
6. **Compression + encryption**: Optional zstd compression and ChaCha20-Poly1305 encryption per segment.

## Architecture

```
Writer → PendingEntry → JournalWriter → .graphj segments → S3 Uploader → S3
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
- Header: magic bytes + version + flags (compression, encryption)
- Entries: length-prefixed protobuf messages with CRC32C checksums
- Chain hash: SHA-256 of previous segment's hash for integrity verification

## Usage

```rust
use graphstream::journal::{spawn_journal_writer, JournalState, PendingEntry};

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
```

## Dependencies

- `prost` — Protobuf serialization
- `sha2` — Chain hash integrity
- `crc32c` — Entry checksum
- `zstd` — Optional compression
- `chacha20poly1305` — Optional encryption
- `aws-sdk-s3` — S3 upload/download

## License

Apache-2.0
